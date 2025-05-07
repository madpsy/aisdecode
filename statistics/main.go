package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "hash/fnv"
    "log"
    "net/http"
    "os"
    "sync"
    "time"

    "github.com/go-redis/redis/v8"
    _ "github.com/lib/pq"
)

//
// ─── SETTINGS & GLOBALS ───────────────────────────────────────────────────────
//

type Settings struct {
    IngestHost string `json:"ingester_host"`
    IngestPort int    `json:"ingester_port"`
    ListenPort int    `json:"listen_port"`
    CacheTime  int    `json:"cache_time"`  // TTL in seconds
    RedisHost  string `json:"redis_host"`
    RedisPort  int    `json:"redis_port"`
    Debug      bool   `json:"debug"`
}

type ClientInfo struct {
    Description string `json:"description"`
    Ip          string `json:"ip"`
    Port        int    `json:"port"`
    Shards      []int  `json:"shards"`
}

type ClientDBSettings struct {
    Host     string
    Port     int
    User     string
    Password string
    DBName   string
}

type ClientConn struct {
    DbSettings ClientDBSettings
    Db         *sql.DB
    Shards     []int
    HostKey    string // e.g. "ip:port"
}

var (
    conf          *Settings
    redisClient   *redis.Client
    redisCtx      = context.Background()
    clientConns   = make(map[string]*ClientConn)
    clientConnsMu sync.RWMutex
    totalShards   int
)

//
// ─── BOILERPLATE: SETTINGS & REDIS INIT ──────────────────────────────────────
//

func loadSettings(path string) (*Settings, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var s Settings
    if err := json.Unmarshal(data, &s); err != nil {
        return nil, err
    }
    return &s, nil
}

func initRedis() {
    redisClient = redis.NewClient(&redis.Options{
        Addr: fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort),
    })
    if err := redisClient.Ping(redisCtx).Err(); err != nil {
        log.Printf("⚠️ Redis ping failed: %v", err)
    } else {
        log.Printf("✅ Connected to Redis at %s:%d", conf.RedisHost, conf.RedisPort)
    }
}

func ensureRedis() {
    if err := redisClient.Ping(redisCtx).Err(); err != nil {
        log.Printf("⚠️ Redis lost, reconnecting: %v", err)
        initRedis()
    }
}

//
// ─── REDIS CACHE HELPERS ──────────────────────────────────────────────────────
//

// cacheGet attempts to GET key from Redis and unmarshal JSON into dest.
// Returns (true,nil) if hit, (false,nil) if miss, or (false,err) on error.
func cacheGet(key string, dest interface{}) (bool, error) {
    ensureRedis()
    data, err := redisClient.Get(redisCtx, key).Result()
    if err == redis.Nil {
        return false, nil
    }
    if err != nil {
        return false, err
    }
    if err := json.Unmarshal([]byte(data), dest); err != nil {
        return false, err
    }
    return true, nil
}

// cacheSet marshals value to JSON and SETs into Redis with TTL=conf.CacheTime seconds.
func cacheSet(key string, value interface{}) error {
    ensureRedis()
    b, err := json.Marshal(value)
    if err != nil {
        return err
    }
    ttl := time.Duration(conf.CacheTime) * time.Second
    return redisClient.Set(redisCtx, key, b, ttl).Err()
}

//
// ─── SHARDED POSTGRES ACCESS ──────────────────────────────────────────────────
//

func shardForUser(userID string) int {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return int(h.Sum32()) % totalShards
}

func (cc *ClientConn) ensureDB() error {
    if err := cc.Db.Ping(); err != nil {
        log.Printf("⚠️ Lost DB connection for shard %s: %v", cc.HostKey, err)
        dsn := fmt.Sprintf(
            "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
            cc.DbSettings.Host,
            cc.DbSettings.Port,
            cc.DbSettings.User,
            cc.DbSettings.Password,
            cc.DbSettings.DBName,
        )
        newDb, err2 := sql.Open("postgres", dsn)
        if err2 != nil {
            return fmt.Errorf("reconnect open failed for %s: %v (orig: %v)",
                cc.HostKey, err2, err)
        }
        if err2 = newDb.Ping(); err2 != nil {
            newDb.Close()
            return fmt.Errorf("reconnect ping failed for %s: %v (orig: %v)",
                cc.HostKey, err2, err)
        }
        cc.Db.Close()
        cc.Db = newDb
        log.Printf("🔄 Reconnected to DB for shard %s", cc.HostKey)
    }
    return nil
}

func QueryDatabaseForUser(userID, query string) (*sql.Rows, error) {
    shard := shardForUser(userID)
    clientConnsMu.RLock()
    defer clientConnsMu.RUnlock()
    for _, cc := range clientConns {
        for _, s := range cc.Shards {
            if s == shard {
                if err := cc.ensureDB(); err != nil {
                    return nil, err
                }
                return cc.Db.Query(query)
            }
        }
    }
    return nil, fmt.Errorf("no shard for user %s", userID)
}

func QueryDatabasesForAllShards(query string) (map[string][]map[string]interface{}, error) {
    results := make(map[string][]map[string]interface{})
    clientConnsMu.RLock()
    conns := make([]*ClientConn, 0, len(clientConns))
    for _, cc := range clientConns {
        conns = append(conns, cc)
    }
    clientConnsMu.RUnlock()

    for _, cc := range conns {
        if err := cc.ensureDB(); err != nil {
            return nil, err
        }
        rows, err := cc.Db.Query(query)
        if err != nil {
            return nil, err
        }
        cols, _ := rows.Columns()
        for rows.Next() {
            vals := make([]interface{}, len(cols))
            scans := make([]interface{}, len(cols))
            for i := range vals {
                scans[i] = &vals[i]
            }
            rows.Scan(scans...)
            rec := make(map[string]interface{}, len(cols))
            for i, col := range cols {
                rec[col] = vals[i]
            }
            results[cc.HostKey] = append(results[cc.HostKey], rec)
        }
        rows.Close()
    }
    return results, nil
}

//
// ─── INGESTER DISCOVERY & SHARD SYNC ──────────────────────────────────────────
//

func fetchClients() ([]ClientInfo, error) {
    url := fmt.Sprintf("http://%s:%d/clients", conf.IngestHost, conf.IngestPort)
    resp, err := http.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var payload struct {
        ConfiguredShards int          `json:"configured_shards"`
        Clients          []ClientInfo `json:"clients"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
        return nil, err
    }
    totalShards = payload.ConfiguredShards
    return payload.Clients, nil
}

func getClientDatabaseSettings(ip string, port int) (*ClientDBSettings, error) {
    url := fmt.Sprintf("http://%s:%d/settings", ip, port)
    resp, err := http.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var cfg struct {
        DbHost string `json:"db_host"`
        DbPort int    `json:"db_port"`
        DbUser string `json:"db_user"`
        DbPass string `json:"db_pass"`
        DbName string `json:"db_name"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
        return nil, err
    }
    return &ClientDBSettings{
        Host:     cfg.DbHost,
        Port:     cfg.DbPort,
        User:     cfg.DbUser,
        Password: cfg.DbPass,
        DBName:   cfg.DbName,
    }, nil
}

func syncClientConns() {
    clients, err := fetchClients()
    if err != nil {
        log.Printf("Error fetching clients: %v", err)
        return
    }

    newMap := make(map[string]*ClientConn, len(clients))
    for _, ci := range clients {
        key := fmt.Sprintf("%s:%d", ci.Ip, ci.Port)
        clientConnsMu.RLock()
        old, exists := clientConns[key]
        clientConnsMu.RUnlock()

        if exists {
            newMap[key] = old
        } else {
            dbCfg, err := getClientDatabaseSettings(ci.Ip, ci.Port)
            if err != nil {
                log.Printf("Error fetching DB settings from %s: %v", key, err)
                continue
            }
            dsn := fmt.Sprintf(
                "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
                dbCfg.Host, dbCfg.Port, dbCfg.User, dbCfg.Password, dbCfg.DBName,
            )
            db, err := sql.Open("postgres", dsn)
            if err != nil {
                log.Printf("DB open error for %s: %v", key, err)
                continue
            }
            if err := db.Ping(); err != nil {
                log.Printf("DB ping error for %s: %v", key, err)
                db.Close()
                continue
            }
            log.Printf("✅ Connected to shard DB %s, shards=%v", key, ci.Shards)
            newMap[key] = &ClientConn{
                DbSettings: *dbCfg,
                Db:         db,
                Shards:     ci.Shards,
                HostKey:    key,
            }
        }
    }

    clientConnsMu.Lock()
    oldMap := clientConns
    first := len(oldMap) == 0
    if first || !shardMapsEqual(oldMap, newMap) {
        clientConns = newMap
        if first {
            log.Printf("🔄 Initial shard sync complete: %d connections, %d shards",
                len(newMap), totalShards)
        } else {
            log.Printf("⚡️ Shard topology changed: %d connections (total shards: %d)",
                len(newMap), totalShards)
        }
    }
    clientConnsMu.Unlock()
}

func shardMapsEqual(a, b map[string]*ClientConn) bool {
    if len(a) != len(b) {
        return false
    }
    for k, cca := range a {
        ccb, ok := b[k]
        if !ok || !intSlicesEqual(cca.Shards, ccb.Shards) {
            return false
        }
    }
    return true
}

func intSlicesEqual(x, y []int) bool {
    if len(x) != len(y) {
        return false
    }
    for i := range x {
        if x[i] != y[i] {
            return false
        }
    }
    return true
}

func scheduleShardSync(interval time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        for range ticker.C {
            syncClientConns()
        }
    }()
}

//
// ─── MAIN ENTRYPOINT ─────────────────────────────────────────────────────────
//

func main() {
    var err error
    conf, err = loadSettings("settings.json")
    if err != nil {
        log.Fatalf("load settings: %v", err)
    }

    initRedis()
    syncClientConns()
    scheduleShardSync(30 * time.Second)

    // start HTTP (in http.go)
    StartServer(conf.ListenPort)
}
