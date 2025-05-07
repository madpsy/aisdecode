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

// Settings defines service configuration loaded from settings.json
type Settings struct {
    IngestHost string `json:"ingester_host"`
    IngestPort int    `json:"ingester_port"`
    ListenPort int    `json:"listen_port"`
    CacheTime  int    `json:"cache_time"`  // TTL in seconds
    RedisHost  string `json:"redis_host"`
    RedisPort  int    `json:"redis_port"`
    Debug      bool   `json:"debug"`
}

// ClientInfo describes a shard node (from ingester)
type ClientInfo struct {
    Description string `json:"description"`
    Ip          string `json:"ip"`
    Port        int    `json:"port"`
    Shards      []int  `json:"shards"`
}

// ClientDBSettings holds credentials for a shard Postgres DB
type ClientDBSettings struct {
    Host     string
    Port     int
    User     string
    Password string
    DBName   string
}

// ClientConn holds one shard's DB connection and metadata
type ClientConn struct {
    DbSettings ClientDBSettings
    Db         *sql.DB
    Shards     []int
    HostKey    string // host:port
}

var (
    conf          *Settings
    redisClient   *redis.Client
    redisCtx      = context.Background()
    clientConns   = make(map[string]*ClientConn)
    clientConnsMu sync.RWMutex
    totalShards   int
)

// loadSettings loads JSON config
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

// initRedis sets up Redis client and logs status
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

// ensureRedis reconnects if needed
func ensureRedis() {
    if err := redisClient.Ping(redisCtx).Err(); err != nil {
        log.Printf("⚠️ Redis lost, reconnecting: %v", err)
        initRedis()
    } else {
        log.Printf("🔄 Redis healthy")
    }
}

// cacheGet tries to fetch the given key from Redis. If found, it unmarshals the JSON
// into dest and returns (true, nil). If the key is missing, returns (false, nil).
// Any other error returns (false, err).
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

// cacheSet marshals value to JSON and stores it under key with TTL=conf.CacheTime seconds.
func cacheSet(key string, value interface{}) error {
    ensureRedis()

    b, err := json.Marshal(value)
    if err != nil {
        return err
    }
    ttl := time.Duration(conf.CacheTime) * time.Second
    return redisClient.Set(redisCtx, key, b, ttl).Err()
}

// shardForUser hashes userID to a shard index
func shardForUser(userID string) int {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return int(h.Sum32()) % totalShards
}

// ensureDB pings and reconnects a shard DB if needed
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
            return fmt.Errorf("reconnect open failed for %s: %v (orig: %v)", cc.HostKey, err2, err)
        }
        if err2 = newDb.Ping(); err2 != nil {
            newDb.Close()
            return fmt.Errorf("reconnect ping failed for %s: %v (orig: %v)", cc.HostKey, err2, err)
        }
        cc.Db.Close()
        cc.Db = newDb
        log.Printf("🔄 Reconnected to DB for shard %s", cc.HostKey)
    }
    return nil
}

// QueryDatabaseForUser runs a query on the user's assigned shard
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

// QueryDatabasesForAllShards fans out a query to all shards
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

// fetchClients gets shard metadata from ingester
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

// ... the rest of your syncClientConns, shardMapsEqual,
// intSlicesEqual, scheduleShardSync, etc. stays unchanged ...

func main() {
    var err error
    conf, err = loadSettings("settings.json")
    if err != nil {
        log.Fatalf("load settings: %v", err)
    }

    initRedis()
    syncClientConns()
    scheduleShardSync(30 * time.Second)

    // hand off to HTTP server in http.go
    StartServer(conf.ListenPort)
}
