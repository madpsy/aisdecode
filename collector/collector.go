// main.go

package main

import (
    "bytes"
    "context"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"
    "math"

    _ "github.com/lib/pq"
    "github.com/redis/go-redis/v9"

    // SOCKET.IO / ENGINE.IO
    engine "github.com/zishang520/engine.io/v2/engine"
    socketio "github.com/zishang520/socket.io/v2/socket"
    "github.com/zishang520/engine.io/v2/types"
)

// ── SETTINGS ────────────────────────────────────────────────────────────────

type Settings struct {
    IngestPort            int      `json:"ingest_port"`
    IngestHost            string   `json:"ingest_host"`
    DbHost                string   `json:"db_host"`
    DbPort                int      `json:"db_port"`
    DbUser                string   `json:"db_user"`
    DbPass                string   `json:"db_pass"`
    DbName                string   `json:"db_name"`
    ListenPort            int      `json:"listen_port"`
    SocketIOListen        int      `json:"socketio_listen"`
    Description           string   `json:"description"`
    Shards                []int    `json:"shards"`
    Debug                 bool     `json:"debug"`
    ExternalLookup        string   `json:"external_lookup"`
    ExternalLookupTimeout int      `json:"external_lookup_timeout"`
    MinimumDistance       float64  `json:"minimum_distance"`
    TimeFiltersRaw        map[string]string  `json:"time_filters"`  // JSON holds strings ("1h", "30m", …)
    ReceiversBaseURL      string   `json:"receivers_base_url"`      // URL for fetching receiver data
    RedisHost             string   `json:"redis_host"`              // Redis server host
    RedisPort             int      `json:"redis_port"`              // Redis server port
}

var settings *Settings
var db *sql.DB
var redisClient *redis.Client
var ctx = context.Background()

// Map to store IP address to receiver ID mapping and receiver location data
var (
    receiverIPToIDMap = make(map[string]int)
    receiverLocations = make(map[int]struct {
        Latitude  float64
        Longitude float64
    })
    receiverMapMutex  sync.RWMutex
)

type Position struct {
    Lat, Lon float64
}

var movementMsgTypes = map[int]struct{}{
    1:  {},
    2:  {},
    3:  {},
    9:  {},
    18: {},
    19: {},
}

var (
	minimumDistance float64  // in meters, loaded from settings
)

// In-memory fallbacks when Redis is unavailable
var (
	lastPosMu       sync.Mutex
	lastPositions   = make(map[int]Position)   // userID → last seen lat/lon
	
	lastNavStatusMu       sync.Mutex
	lastNavigationalStatus = make(map[int]float64)  // userID → last NavigationalStatus
	
	// Track Redis availability to know when to sync in-memory data
	redisAvailableMu sync.Mutex
	redisWasUnavailable bool = false
)

var (
	lastTimeMu   sync.Mutex
	lastTimeSeen = make(map[int]map[int]time.Time)
)

// Redis key prefixes
const (
	positionKeyPrefix = "collector:position:"
	navStatusKeyPrefix = "collector:navstatus:"
)

func haversine(lat1, lon1, lat2, lon2 float64) float64 {
    const R = 6371000.0 // earth radius in meters
    toRad := func(deg float64) float64 { return deg * math.Pi / 180 }
    φ1, φ2 := toRad(lat1), toRad(lat2)
    Δφ := toRad(lat2 - lat1)
    Δλ := toRad(lon2 - lon1)
    a := math.Sin(Δφ/2)*math.Sin(Δφ/2) +
         math.Cos(φ1)*math.Cos(φ2)*math.Sin(Δλ/2)*math.Sin(Δλ/2)
    c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
    return R * c
}

func readSettings(path string) (*Settings, error) {
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

var timeFilters map[int]time.Duration

func loadTimeFilters(raw map[string]string) (map[int]time.Duration, error) {
    tf := make(map[int]time.Duration, len(raw))
    for k, vs := range raw {
        mid, err := strconv.Atoi(k)
        if err != nil {
            return nil, fmt.Errorf("invalid MessageID in time_filters: %q", k)
        }
        d, err := time.ParseDuration(vs)
        if err != nil {
            return nil, fmt.Errorf("invalid duration for MessageID %d: %v", mid, err)
        }
        tf[mid] = d
    }
    return tf, nil
}

// ── SOCKET.IO STATE ─────────────────────────────────────────────────────────

var (
    ioServer              *socketio.Server
    connectedClients      = make(map[socketio.SocketId]*socketio.Socket)
    connectedClientsMu    sync.RWMutex
    userSubscribers       = make(map[string]map[socketio.SocketId]struct{})
    userSubscribersMu     sync.RWMutex
    clientSubscriptions   = make(map[socketio.SocketId]map[string]struct{})
    clientSubscriptionsMu sync.RWMutex
)

// ── INGESTER MESSAGE STRUCT ───────────────────────────────────────────────────

type Message struct {
    Packet      json.RawMessage `json:"message"`
    ShardID     int             `json:"shard_id"`
    Timestamp   string          `json:"timestamp"`
    SourceIP    string          `json:"source_ip"`
    RawSentence string          `json:"raw_sentence"`
}

// ── MAIN ─────────────────────────────────────────────────────────────────────

func main() {
    cfgPath := flag.String("config", "./settings.json", "Path to settings.json")
    flag.Parse()

    var err error
    settings, err = readSettings(*cfgPath)
    if err != nil {
        log.Fatalf("Error reading settings: %v", err)
    }
    if settings.ExternalLookupTimeout == 0 {
        settings.ExternalLookupTimeout = 1000
    }
    if settings.Debug {
        log.Println("Debug mode enabled")
    }

    minimumDistance = float64(settings.MinimumDistance)

    timeFilters, err = loadTimeFilters(settings.TimeFiltersRaw)
    if err != nil {
        log.Fatalf("Invalid time_filters in settings: %v", err)
    }
    
    // Initialize Redis client
    initRedisClient()
    
    // Start Redis connection monitoring in background
    go monitorRedisConnection()

    go startHTTPServer()
    go startSocketIOServer()

    ingConn, err := connectToIngester(settings.IngestHost, settings.IngestPort, settings.Debug)
    if err != nil {
        log.Fatalf("Error connecting to ingester: %v", err)
    }
    defer ingConn.Close()

    connStr := fmt.Sprintf(
        "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName,
    )
    db, err = sql.Open("postgres", connStr)
    if err != nil {
        log.Fatalf("Error connecting to PostgreSQL database: %v", err)
    }
    defer db.Close()
    if settings.Debug {
        log.Printf("Connected to PostgreSQL database: %s", settings.DbName)
    }

    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            packet JSONB,
            shard_id INT,
            timestamp TIMESTAMP,
            source_ip VARCHAR(45),
            user_id INT,
            message_id INT,
            raw_sentence TEXT,
            receiver_id INT,
            distance INT
        );
    `)
    if err != nil {
        log.Fatal("Error creating messages table: ", err)
    }
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS state (
            packet JSONB,
            shard_id INT,
            timestamp TIMESTAMP,
            user_id INT PRIMARY KEY,
            ais_class VARCHAR(4) DEFAULT 'A',
            count INT DEFAULT 1,
            image_url TEXT,
            name TEXT,
            ext_lookup_complete BOOLEAN DEFAULT FALSE,
            source_ip VARCHAR(45),
            receiver_id INT
        );
    `)
    if err != nil {
        log.Fatal("Error creating state table: ", err)
    }
    createIndexesIfNotExist(db)

    requestData := map[string]interface{}{
        "shards":      settings.Shards,
        "description": settings.Description,
        "port":        settings.ListenPort,
    }
    requestJSON, err := json.Marshal(requestData)
    if err != nil {
        log.Fatal("Error marshalling request JSON: ", err)
    }
    _, err = ingConn.Write(requestJSON)
    if err != nil {
        log.Fatal("Error sending request to ingester: ", err)
    }
    log.Printf("Sent request to ingester: %s", string(requestJSON))

    // Start a goroutine to periodically fetch receiver information if URL is provided
    if settings.ReceiversBaseURL != "" {
        go fetchReceiverData(settings.ReceiversBaseURL)
    }

    go handleIngesterMessages(settings, ingConn, requestJSON, settings.Debug, db)
    select {}
}

// Function to fetch receiver data and update the IP to ID mapping
func fetchReceiverData(baseURL string) {
    // Create a custom HTTP client with a 5-second timeout
    client := &http.Client{
        Timeout: 5 * time.Second,
    }

    // Fetch data immediately on startup
    updateReceiverMapping(client, baseURL)

    // Then fetch every 5 seconds
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        updateReceiverMapping(client, baseURL)
    }
}

// Function to update the receiver IP to ID mapping
func updateReceiverMapping(client *http.Client, baseURL string) {
    url := fmt.Sprintf("%s/admin/receivers", baseURL)
    resp, err := client.Get(url)
    if err != nil {
        log.Printf("Error fetching receivers data: %v", err)
        return // Preserve previous mapping on error
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        log.Printf("Received non-OK response from receivers API: %d", resp.StatusCode)
        return // Preserve previous mapping on error
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Printf("Error reading receivers response body: %v", err)
        return // Preserve previous mapping on error
    }

    var receivers []struct {
        ID        int     `json:"id"`
        IPAddress string  `json:"ip_address"`
        Latitude  float64 `json:"latitude"`
        Longitude float64 `json:"longitude"`
    }

    if err := json.Unmarshal(body, &receivers); err != nil {
        log.Printf("Error unmarshalling receivers data: %v", err)
        return // Preserve previous mapping on error
    }

    // Create new maps to avoid partial updates
    newIPMap := make(map[string]int)
    newLocationMap := make(map[int]struct {
        Latitude  float64
        Longitude float64
    })
    
    for _, receiver := range receivers {
        if receiver.IPAddress != "" {
            newIPMap[receiver.IPAddress] = receiver.ID
        }
        
        // Store location data regardless of IP address
        newLocationMap[receiver.ID] = struct {
            Latitude  float64
            Longitude float64
        }{
            Latitude:  receiver.Latitude,
            Longitude: receiver.Longitude,
        }
    }

    // Only update the global maps if we have at least one entry
    if len(newIPMap) == 0 {
        log.Printf("Received empty receivers list, preserving previous mapping")
        return // Preserve previous mapping if new data is empty
    }

    // Update the global maps atomically
    receiverMapMutex.Lock()
    receiverIPToIDMap = newIPMap
    receiverLocations = newLocationMap
    receiverMapMutex.Unlock()

    // Only log updates if in debug mode
    if settings.Debug {
        log.Printf("Updated receiver mapping with %d entries", len(newIPMap))
    }
}

// ── HTTP SERVER ──────────────────────────────────────────────────────────────

func startHTTPServer() {
    http.HandleFunc("/settings", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(settings)
    })
    addr := fmt.Sprintf(":%d", settings.ListenPort)
    log.Printf("HTTP server on %s", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}

// ── SOCKET.IO SERVER ─────────────────────────────────────────────────────────

func startSocketIOServer() {
    addr := fmt.Sprintf(":%d", settings.SocketIOListen)
    log.Printf("Socket.IO on %s", addr)

    mux := http.NewServeMux()
    eng := types.NewWebServer(nil)
    engine.Attach(eng, nil) // no ServerOptions

    mux.HandleFunc("/socket.io/", eng.ServeHTTP)

    ioServer = socketio.NewServer(eng, nil)
    setupSocketIOHandlers()

    log.Fatal(http.ListenAndServe(addr, mux))
}

func setupSocketIOHandlers() {
    ioServer.On("connection", func(args ...any) {
        sock := args[0].(*socketio.Socket)
        sid := sock.Id()

        connectedClientsMu.Lock()
        connectedClients[sid] = sock
        connectedClientsMu.Unlock()

        sock.On("ais_sub/:userID", func(raw ...any) {
	    // log.Printf("[DEBUG] got ais_sub/:userID → %+v\n", raw)
            userID := raw[0].(string)
	    // log.Printf("[DEBUG] socket %s subscribing to user %s", sock.Id(), userID)
            clientSubscriptionsMu.Lock()
            if clientSubscriptions[sid] == nil {
                clientSubscriptions[sid] = make(map[string]struct{})
            }
            clientSubscriptions[sid][userID] = struct{}{}
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            if userSubscribers[userID] == nil {
                userSubscribers[userID] = make(map[socketio.SocketId]struct{})
            }
            userSubscribers[userID][sid] = struct{}{}
            userSubscribersMu.Unlock()
        })

        sock.On("ais_unsub/:userID", func(raw ...any) {
            userID := raw[0].(string)
            clientSubscriptionsMu.Lock()
            delete(clientSubscriptions[sid], userID)
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            if subs := userSubscribers[userID]; subs != nil {
                delete(subs, sid)
                if len(subs) == 0 {
                    delete(userSubscribers, userID)
                }
            }
            userSubscribersMu.Unlock()
        })

        sock.On("disconnect", func(_ ...any) {
            connectedClientsMu.Lock()
            delete(connectedClients, sid)
            connectedClientsMu.Unlock()

            clientSubscriptionsMu.Lock()
            subs := clientSubscriptions[sid]
            delete(clientSubscriptions, sid)
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            for uid := range subs {
                if subsMap := userSubscribers[uid]; subsMap != nil {
                    delete(subsMap, sid)
                    if len(subsMap) == 0 {
                        delete(userSubscribers, uid)
                    }
                }
            }
            userSubscribersMu.Unlock()
        })
    })
}

// ── INGESTER HANDLING ─────────────────────────────────────────────────────────

func handleIngesterMessages(settings *Settings, conn net.Conn, requestJSON []byte, debug bool, db *sql.DB) {
    buffer := make([]byte, 0)
    for {
        buf := make([]byte, 4096)
        n, err := conn.Read(buf)
        if err != nil {
            log.Printf("Error reading from connection: %v", err)
            conn.Close()
            conn, err = connectToIngester(settings.IngestHost, settings.IngestPort, debug)
            if err != nil {
                log.Printf("Failed to reconnect: %v", err)
                continue
            }
            log.Println("Reconnected to ingester")
            _, err := conn.Write(requestJSON)
            if err != nil {
                log.Printf("Failed to resend request: %v", err)
                continue
            }
            log.Printf("Resent request: %s", string(requestJSON))
            continue
        }

        buffer = append(buffer, buf[:n]...)
        for {
            idx := bytes.IndexByte(buffer, '\x00')
            if idx == -1 {
                break
            }
            frame := buffer[:idx]
            buffer = buffer[idx+1:]
            if err := processMessage(frame, db, settings); err != nil {
            }
        }
    }
}

func processMessage(message []byte, db *sql.DB, settings *Settings) error {
    // 1) Unmarshal the entire incoming JSON (including RawSentence)
    var msg Message
    if err := json.Unmarshal(message, &msg); err != nil {
        log.Printf("[DEBUG] failed to unmarshal outer Message: %v", err)
        return err
    }

    // 2) Persist to DB (includes msg.RawSentence)
    if err := storeMessage(db, msg, settings, msg.RawSentence); err != nil {
        log.Printf("[DEBUG] Error storing message: %v", err)
    }

    // 3) Unwrap the inner envelope to get at the real packet object
    var inner struct {
        Packet json.RawMessage `json:"packet"`
    }
    if err := json.Unmarshal(msg.Packet, &inner); err != nil {
        log.Printf("[DEBUG] failed to unmarshal inner wrapper: %v", err)
        return nil
    }
    rawInner := inner.Packet

    // 4) Parse packet into a map
    var packet map[string]interface{}
    if err := json.Unmarshal(rawInner, &packet); err != nil {
        log.Printf("[DEBUG] failed to unmarshal inner packet: %v", err)
        return nil
    }

    // 5) Determine user key
    var uidKey string
    if v, ok := packet["UserID"].(float64); ok {
        uidKey = strconv.Itoa(int(v))
    } else if s, ok := packet["UserID"].(string); ok {
        uidKey = s
    } else {
        log.Printf("[DEBUG] packet missing UserID, skipping emit")
        return nil
    }

    // 6) Build payload for Socket.IO emit
    
    // Look up receiver ID from source IP
    var receiverID interface{} = nil
    receiverMapMutex.RLock()
    if id, exists := receiverIPToIDMap[msg.SourceIP]; exists {
        receiverID = id
    }
    receiverMapMutex.RUnlock()
    
    emitPayload := map[string]interface{}{
        "data":         packet,
        "type":         packet["MessageID"],
        "timestamp":    msg.Timestamp,
        "raw_sentence": msg.RawSentence,
        "receiver_id":  receiverID,
    }

    // 7) Emit to any subscribers
    userSubscribersMu.RLock()
    subs := userSubscribers[uidKey]
    userSubscribersMu.RUnlock()
    for sid := range subs {
        if sock, ok := connectedClients[sid]; ok {
            sock.Emit("ais_data", emitPayload)
        }
    }

    return nil
}

// ── DATABASE HELPERS AND ORIGINAL LOGIC ─────────────────────────────────────

func createIndexesIfNotExist(db *sql.DB) {
    stmts := []string{
        // messages table: cover history and state CTE queries
        `CREATE INDEX IF NOT EXISTS idx_messages_userid_msgid_ts 
            ON messages(user_id, message_id, timestamp);`,

        // New index: only include rows where ApplicationID.Valid = true
        `CREATE INDEX IF NOT EXISTS idx_messages_appid_valid_true
            ON messages (((packet->'ApplicationID'->>'Valid')::boolean))
            WHERE ((packet->'ApplicationID'->>'Valid')::boolean) = true;`,

        // New functional B‐tree index on user_id, message_id, DAC, FI, with timestamp DESC
        `CREATE INDEX IF NOT EXISTS idx_messages_userid_msgid_dac_fi_ts
            ON messages (
                user_id,
                message_id,
                ((packet->'ApplicationID'->>'DesignatedAreaCode')::int),
                ((packet->'ApplicationID'->>'FunctionIdentifier')::int),
                timestamp DESC
            );`,

        // state table: filter by user and time
        `CREATE INDEX IF NOT EXISTS idx_state_userid_timestamp 
            ON state(user_id, timestamp);`,

        // state table: filter by time alone
        `CREATE INDEX IF NOT EXISTS idx_state_timestamp 
            ON state(timestamp);`,

        // state table: geospatial radius queries
        `CREATE INDEX IF NOT EXISTS idx_state_geo 
            ON state 
            USING GIST (
                ST_SetSRID(
                    ST_Point(
                        (packet->>'Longitude')::double precision,
                        (packet->>'Latitude')::double precision
                    ),
                    4326
                )
            );`,

        // enable trigram support for fast ILIKE/%...% searches
        `CREATE EXTENSION IF NOT EXISTS pg_trgm;`,

        // state table: fast ILIKE on JSONB fields and name column
        `CREATE INDEX IF NOT EXISTS idx_state_name_trgm 
            ON state 
            USING GIN ((packet->>'Name') gin_trgm_ops);`,

        `CREATE INDEX IF NOT EXISTS idx_state_callsign_trgm 
            ON state 
            USING GIN ((packet->>'CallSign') gin_trgm_ops);`,

        `CREATE INDEX IF NOT EXISTS idx_state_imonumber_trgm 
            ON state 
            USING GIN ((packet->>'ImoNumber') gin_trgm_ops);`,

        `CREATE INDEX IF NOT EXISTS idx_state_namecol_trgm 
            ON state 
            USING GIN (name gin_trgm_ops);`,
    }

    for _, s := range stmts {
        if _, err := db.Exec(s); err != nil {
            log.Printf("Error creating index: %v", err)
        }
    }
}

func storeMessage(db *sql.DB, message Message, settings *Settings, rawSentence string) error {
    // 1) Unwrap outer JSON
    var outerMap map[string]interface{}
    if err := json.Unmarshal(message.Packet, &outerMap); err != nil {
        log.Printf("Error unmarshalling outer message: %v", err)
        return err
    }
    packetData, exists := outerMap["packet"]
    if !exists {
        log.Println("Packet field is missing in the JSON message.")
        return fmt.Errorf("Packet field is missing")
    }
    packetMap, ok := packetData.(map[string]interface{})
    if !ok {
        log.Println("Packet data is not in the expected format.")
        return fmt.Errorf("Packet data is not in the expected format")
    }

    // 2) Flatten ReportA/ReportB for MessageID = 24
    if midRaw, ok := packetMap["MessageID"].(float64); ok && int(midRaw) == 24 {
        if reportA, exists := packetMap["ReportA"].(map[string]interface{}); exists {
            if valid, vOK := reportA["Valid"].(bool); vOK && valid {
                for k, v := range reportA {
                    packetMap[k] = v
                }
            }
            delete(packetMap, "ReportA")
        }
        if reportB, exists := packetMap["ReportB"].(map[string]interface{}); exists {
            if valid, vOK := reportB["Valid"].(bool); vOK && valid {
                if shipType, ok := reportB["ShipType"].(float64); ok {
                    reportB["Type"] = int(shipType)
                    delete(reportB, "ShipType")
                }
                for k, v := range reportB {
                    packetMap[k] = v
                }
            }
            delete(packetMap, "ReportB")
        }
    }

    // 3) Extract UserID & MessageID
    userIDf, uOK := packetMap["UserID"].(float64)
    messageIDf, mOK := packetMap["MessageID"].(float64)
    if !uOK || !mOK {
        return fmt.Errorf("UserID or MessageID is missing")
    }
    userID := int(userIDf)
    mid := int(messageIDf)

    // 4) Marshal packet for DB
    packetJSON, err := json.Marshal(packetMap)
    if err != nil {
        return err
    }

    // 5) Movement-based filtering and NavigationalStatus change detection
    shouldInsert := true
    navStatusChanged := false
    
    // Check for NavigationalStatus change
    if navStatus, hasNavStatus := packetMap["NavigationalStatus"].(float64); hasNavStatus {
        // Create a key for this vessel's NavigationalStatus in Redis
        navStatusKey := fmt.Sprintf("%s%d:%d", navStatusKeyPrefix, message.ShardID, userID)
        
        // Try to get previous NavigationalStatus from Redis
        prevNavStatusStr, err := redisClient.Get(ctx, navStatusKey).Result()
        
        if err == redis.Nil {
            // Key doesn't exist - first time seeing this vessel's NavigationalStatus in Redis
            
            // Check in-memory fallback
            lastNavStatusMu.Lock()
            prevNavStatus, seenBefore := lastNavigationalStatus[userID]
            
            if !seenBefore || prevNavStatus != navStatus {
                navStatusChanged = true
                lastNavigationalStatus[userID] = navStatus
            }
            lastNavStatusMu.Unlock()
            
            // Store the new NavigationalStatus in Redis
            if err := redisClient.Set(ctx, navStatusKey, fmt.Sprintf("%f", navStatus), 0).Err(); err != nil && settings.Debug {
                log.Printf("Warning: Failed to store NavigationalStatus in Redis: %v", err)
            }
        } else if err != nil {
            // Redis error - fall back to in-memory comparison
            if settings.Debug {
                log.Printf("Warning: Redis error when getting NavigationalStatus: %v", err)
            }
            
            // Mark Redis as unavailable for later sync
            redisAvailableMu.Lock()
            redisWasUnavailable = true
            redisAvailableMu.Unlock()
            
            // Use in-memory fallback
            lastNavStatusMu.Lock()
            prevNavStatus, seenBefore := lastNavigationalStatus[userID]
            if !seenBefore || prevNavStatus != navStatus {
                navStatusChanged = true
                lastNavigationalStatus[userID] = navStatus
            }
            lastNavStatusMu.Unlock()
        } else {
            // Key exists - compare with current NavigationalStatus
            prevNavStatus, err := strconv.ParseFloat(prevNavStatusStr, 64)
            if err != nil {
                log.Printf("Warning: Invalid NavigationalStatus in Redis: %v", err)
            } else if prevNavStatus != navStatus {
                navStatusChanged = true
                
                // Update the NavigationalStatus in Redis
                if err := redisClient.Set(ctx, navStatusKey, fmt.Sprintf("%f", navStatus), 0).Err(); err != nil && settings.Debug {
                    log.Printf("Warning: Failed to update NavigationalStatus in Redis: %v", err)
                }
                
                // Also update in-memory
                lastNavStatusMu.Lock()
                lastNavigationalStatus[userID] = navStatus
                lastNavStatusMu.Unlock()
            } else {
                // Update in-memory even if unchanged
                lastNavStatusMu.Lock()
                lastNavigationalStatus[userID] = navStatus
                lastNavStatusMu.Unlock()
            }
        }
    }
    
    if _, isMovement := movementMsgTypes[mid]; isMovement {
        lat, lok := packetMap["Latitude"].(float64)
        lon, lok2 := packetMap["Longitude"].(float64)
        if !lok || !lok2 {
            shouldInsert = false
        } else {
            // Create a key for this vessel's position in Redis
            posKey := fmt.Sprintf("%s%d:%d", positionKeyPrefix, message.ShardID, userID)
            
            // Try to get previous position from Redis
            posData, err := redisClient.Get(ctx, posKey).Result()
            
            if err == redis.Nil {
                // Key doesn't exist - first time seeing this vessel in Redis
                
                // Check in-memory fallback
                lastPosMu.Lock()
                prevPos, seenBefore := lastPositions[userID]
                
                if !seenBefore {
                    // First time seeing this vessel anywhere
                    lastPositions[userID] = Position{Lat: lat, Lon: lon}
                } else {
                    // We've seen it in memory but not in Redis
                    dist := haversine(prevPos.Lat, prevPos.Lon, lat, lon)
                    if dist >= minimumDistance {
                        lastPositions[userID] = Position{Lat: lat, Lon: lon}
                    } else {
                        shouldInsert = false
                    }
                }
                lastPosMu.Unlock()
                
                // Store the new position in Redis
                newPos := Position{Lat: lat, Lon: lon}
                posJSON, _ := json.Marshal(newPos)
                if err := redisClient.Set(ctx, posKey, posJSON, 0).Err(); err != nil && settings.Debug {
                    log.Printf("Warning: Failed to store position in Redis: %v", err)
                }
            } else if err != nil {
                // Redis error - fall back to in-memory position tracking
                if settings.Debug {
                    log.Printf("Warning: Redis error when getting position: %v", err)
                }
                
                // Mark Redis as unavailable for later sync
                redisAvailableMu.Lock()
                redisWasUnavailable = true
                redisAvailableMu.Unlock()
                
                // Use in-memory fallback
                lastPosMu.Lock()
                prevPos, seenBefore := lastPositions[userID]
                if !seenBefore {
                    lastPositions[userID] = Position{Lat: lat, Lon: lon}
                } else {
                    dist := haversine(prevPos.Lat, prevPos.Lon, lat, lon)
                    if dist >= minimumDistance {
                        lastPositions[userID] = Position{Lat: lat, Lon: lon}
                    } else {
                        shouldInsert = false
                    }
                }
                lastPosMu.Unlock()
            } else {
                // Key exists - compare with current position
                var prevPos Position
                if err := json.Unmarshal([]byte(posData), &prevPos); err != nil {
                    log.Printf("Warning: Invalid position data in Redis: %v", err)
                } else {
                    dist := haversine(prevPos.Lat, prevPos.Lon, lat, lon)
                    if dist >= minimumDistance {
                        // Update position in Redis
                        newPos := Position{Lat: lat, Lon: lon}
                        posJSON, _ := json.Marshal(newPos)
                        if err := redisClient.Set(ctx, posKey, posJSON, 0).Err(); err != nil && settings.Debug {
                            log.Printf("Warning: Failed to update position in Redis: %v", err)
                        }
                        
                        // Also update in-memory
                        lastPosMu.Lock()
                        lastPositions[userID] = Position{Lat: lat, Lon: lon}
                        lastPosMu.Unlock()
                    } else {
                        shouldInsert = false
                        
                        // Update in-memory even if unchanged
                        lastPosMu.Lock()
                        lastPositions[userID] = Position{Lat: lat, Lon: lon}
                        lastPosMu.Unlock()
                    }
                }
            }
        }
    }
    
    // Override shouldInsert if NavigationalStatus changed
    if navStatusChanged {
        shouldInsert = true
    }

    // 6) Generic time-based filtering
    if window, ok := timeFilters[mid]; ok {
        t, err := time.Parse(time.RFC3339, message.Timestamp)
        if err != nil {
            log.Printf("Warning: could not parse timestamp for rate-limit on mid=%d: %v", mid, err)
        } else {
            lastTimeMu.Lock()
            // initialize inner map if needed
            if _, found := lastTimeSeen[mid]; !found {
                lastTimeSeen[mid] = make(map[int]time.Time)
            }
            if prevT, seen := lastTimeSeen[mid][userID]; seen && t.Sub(prevT) < window {
                // Only apply time filtering if NavigationalStatus hasn't changed
                if !navStatusChanged {
                    shouldInsert = false
                }
            } else {
                lastTimeSeen[mid][userID] = t
            }
            lastTimeMu.Unlock()
        }
    }

    // 7) Look up receiver ID from source IP
    var receiverID interface{} = nil // Use nil (SQL NULL) as default
    receiverMapMutex.RLock()
    if id, exists := receiverIPToIDMap[message.SourceIP]; exists {
        receiverID = id // Only set a value if mapping exists
    }
    receiverMapMutex.RUnlock()

    // 8) Always update the state table
    if err := storeState(db, packetJSON, message.ShardID, message.Timestamp, userID, messageIDf, message.SourceIP, receiverID); err != nil {
        log.Printf("Error storing state: %v", err)
    }

    // 8) Conditionally insert into messages
    if shouldInsert {
        reason := ""
        if settings.Debug {
            if navStatusChanged {
                reason = " (NavigationalStatus changed)"
            }
            log.Printf("Storing MessageID=%d for user %d%s", mid, userID, reason)
        }
        // Look up receiver ID from source IP
        var receiverID interface{} = nil // Use nil (SQL NULL) as default
        receiverMapMutex.RLock()
        if id, exists := receiverIPToIDMap[message.SourceIP]; exists {
            receiverID = id // Only set a value if mapping exists
        }
        receiverMapMutex.RUnlock()

        if err := tryStoreMessage(db, packetJSON, message.ShardID, message.Timestamp, message.SourceIP, userIDf, messageIDf, rawSentence, receiverID, settings); err != nil {
            log.Printf("Error storing message: %v", err)
        }
    }

    return nil
}

func storeState(db *sql.DB, packetJSON []byte, shardID int, timestamp string, userID int, messageID float64, sourceIP string, receiverID interface{}) error {
    var existingPacketJSON []byte
    var existingAisClass string
    var existingCount int
    var existingLookupComplete bool
    err := db.QueryRow(`
        SELECT	packet, ais_class, count, ext_lookup_complete
          FROM	state
         WHERE	user_id = $1
    `, userID).Scan(&existingPacketJSON, &existingAisClass, &existingCount, &existingLookupComplete)

    if err == sql.ErrNoRows {
        existingPacketJSON = []byte("{}")
        existingAisClass = "A"
        existingCount = 1
        existingLookupComplete = false
    } else if err != nil {
        return fmt.Errorf("Error querying existing packet: %v", err)
    }

    var existingPacket map[string]interface{}
    if err := json.Unmarshal(existingPacketJSON, &existingPacket); err != nil {
        return fmt.Errorf("Error unmarshalling existing packet: %v", err)
    }
    var newPacket map[string]interface{}
    if err := json.Unmarshal(packetJSON, &newPacket); err != nil {
        return fmt.Errorf("Error unmarshalling new packet: %v", err)
    }
    for key, value := range newPacket {
        existingPacket[key] = value
    }

    switch messageID {
    case 18, 19:
        if existingAisClass == "A" {
            existingAisClass = "B"
        }
    case 9:
        if existingAisClass == "A" {
            existingAisClass = "SAR"
        }
    case 21:
        if existingAisClass == "A" {
            existingAisClass = "AtoN"
        }
    case 4, 20:
        if existingAisClass == "A" {
            existingAisClass = "BASE"
        }
    }

    delete(existingPacket, "MessageID")
    packetJSON, err = json.Marshal(existingPacket)
    if err != nil {
        return fmt.Errorf("Error marshalling merged packet: %v", err)
    }

    _, err = db.Exec(`
        INSERT INTO state
          (packet, shard_id, timestamp, user_id, ais_class, count, image_url, name, ext_lookup_complete, source_ip, receiver_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (user_id) DO UPDATE SET
          packet              = EXCLUDED.packet,
          shard_id            = EXCLUDED.shard_id,
          timestamp           = EXCLUDED.timestamp,
          ais_class           = EXCLUDED.ais_class,
          count               = state.count + 1,
          source_ip           = EXCLUDED.source_ip,
          receiver_id         = EXCLUDED.receiver_id
    `, packetJSON, shardID, timestamp, userID, existingAisClass, existingCount, "", "", existingLookupComplete, sourceIP, receiverID)
    if err != nil {
        return fmt.Errorf("Error inserting or updating state table: %v", err)
    }

    uidStr := strconv.Itoa(userID)
    if settings.ExternalLookup != "" && len(uidStr) == 9 && !existingLookupComplete {
        go externalLookupAndUpdate(db, userID)
    }
    return nil
}

func externalLookupAndUpdate(db *sql.DB, userID int) {
    uidStr := strconv.Itoa(userID)
    timeout := time.Duration(settings.ExternalLookupTimeout) * time.Millisecond
    client := &http.Client{Timeout: timeout}

    payload, _ := json.Marshal(map[string]string{"UserID": uidStr})
    resp, err := client.Post(settings.ExternalLookup, "application/json", bytes.NewBuffer(payload))
    if err != nil {
        log.Printf("External lookup error for user %d: %v", userID, err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusNotFound {
        if _, err := db.Exec("UPDATE state SET ext_lookup_complete=TRUE WHERE user_id=$1", userID); err != nil {
            log.Printf("Error marking lookup complete for user %d: %v", userID, err)
        }
        return
    }
    if resp.StatusCode != http.StatusOK {
        return
    }

    var ext struct {
        ImageURL string `json:"ImageURL"`
        Name     string `json:"Name"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&ext); err != nil {
        log.Printf("Error decoding lookup response for user %d: %v", userID, err)
        return
    }
    var imgVal, nameVal interface{}
    if ext.ImageURL != "" {
        imgVal = ext.ImageURL
    }
    if ext.Name != "" {
        nameVal = ext.Name
    }
    if _, err := db.Exec(
        "UPDATE state SET image_url=$1, name=$2, ext_lookup_complete=TRUE WHERE user_id=$3",
        imgVal, nameVal, userID,
    ); err != nil {
        log.Printf("Error updating lookup results for user %d: %v", userID, err)
    }
}

// Helper function to check if a message ID is one that contains position data
func hasPositionData(messageID int) bool {
    positionMessageIDs := map[int]bool{
        1: true, 2: true, 3: true, 4: true, 9: true,
        18: true, 19: true, 21: true, 27: true,
    }
    return positionMessageIDs[messageID]
}

// Calculate distance between receiver and vessel if possible
func calculateDistance(packetJSON []byte, receiverID interface{}) (float64, bool) {
    // If no receiver ID, we can't calculate distance
    recID, ok := receiverID.(int)
    if !ok {
        return 0, false
    }
    
    // Get receiver location
    receiverMapMutex.RLock()
    receiverLoc, exists := receiverLocations[recID]
    receiverMapMutex.RUnlock()
    if !exists {
        return 0, false
    }
    
    // Parse packet to get vessel location
    var packet map[string]interface{}
    if err := json.Unmarshal(packetJSON, &packet); err != nil {
        return 0, false
    }
    
    // Extract vessel coordinates
    vesselLat, latOk := packet["Latitude"].(float64)
    vesselLon, lonOk := packet["Longitude"].(float64)
    if !latOk || !lonOk {
        return 0, false
    }
    
    // Check for invalid coordinates (91, 181 are invalid values)
    if vesselLat == 91.0 || vesselLon == 181.0 {
        return 0, false
    }
    
    // Calculate distance using haversine formula and round to nearest meter
    distance := haversine(receiverLoc.Latitude, receiverLoc.Longitude, vesselLat, vesselLon)
    return math.Round(distance), true
}

func tryStoreMessage(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, userID float64, messageID float64, rawSentence string, receiverID interface{}, settings *Settings) error {
    // Calculate distance if this is a position message and we have receiver info
    var distance interface{} = nil // Default to SQL NULL
    if hasPositionData(int(messageID)) {
        if dist, ok := calculateDistance(packetJSON, receiverID); ok {
            distance = dist
        }
    }
    
    stmt := `INSERT INTO messages (packet, shard_id, timestamp, source_ip, user_id, message_id, raw_sentence, receiver_id, distance)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
    _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID, rawSentence, receiverID, distance)
    if err != nil {
        log.Printf("Error executing query: %v", err)
        if isDatabaseConnectionError(err) {
            log.Println("Attempting to reconnect to the PostgreSQL database...")
            db, err = reconnectToDatabase(settings)
            if err != nil {
                log.Printf("Failed to reconnect to the database: %v", err)
                return err
            }
            _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID, rawSentence, receiverID, distance)
            if err != nil {
                log.Printf("Error executing query after reconnecting: %v", err)
                return err
            }
        }
    }
    return nil
}

func isDatabaseConnectionError(err error) bool {
    return err != nil && err.Error() == "pq: connection to server lost"
}

func reconnectToDatabase(settings *Settings) (*sql.DB, error) {
    connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName)
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, err
    }
    if err := db.Ping(); err != nil {
        return nil, err
    }
    log.Println("Successfully reconnected to the PostgreSQL database.")
    return db, nil
}

func connectToIngester(host string, port int, debug bool) (net.Conn, error) {
    for {
        addr := fmt.Sprintf("%s:%d", host, port)
        conn, err := net.Dial("tcp", addr)
        if err != nil {
            log.Printf("Failed to connect to ingester at %s: %v. Retrying in 5 seconds...", addr, err)
            time.Sleep(5 * time.Second)
            continue
        }
        if debug {
            log.Printf("Successfully connected to ingester at %s", addr)
        }
        return conn, nil
    }
}

// Initialize Redis client
func initRedisClient() {
    redisClient = redis.NewClient(&redis.Options{
        Addr:     fmt.Sprintf("%s:%d", settings.RedisHost, settings.RedisPort),
        Password: "", // no password set
        DB:       0,  // use default DB
    })
    
    // Test Redis connection
    _, err := redisClient.Ping(ctx).Result()
    if err != nil {
        log.Printf("Warning: Could not connect to Redis: %v", err)
        log.Println("Position and NavigationalStatus tracking will be less reliable across restarts")
    } else if settings.Debug {
        log.Println("Connected to Redis successfully")
    }
}

// Monitor Redis connection and attempt to reconnect if it fails
func monitorRedisConnection() {
    var redisConnected bool
    
    // Initial connection check
    if _, err := redisClient.Ping(ctx).Result(); err != nil {
        redisConnected = false
    } else {
        redisConnected = true
    }
    
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        // Check if Redis is connected
        _, err := redisClient.Ping(ctx).Result()
        
        if err != nil && redisConnected {
            // Connection was lost
            log.Printf("Lost connection to Redis: %v", err)
            redisConnected = false
        } else if err == nil && !redisConnected {
            // Connection was restored
            log.Println("Redis connection restored")
            redisConnected = true
            
            // Check if we need to sync in-memory data to Redis
            redisAvailableMu.Lock()
            needsSync := redisWasUnavailable
            redisWasUnavailable = false
            redisAvailableMu.Unlock()
            
            if needsSync {
                go syncInMemoryToRedis()
            }
        } else if err != nil && !redisConnected {
            // Still disconnected, try to reconnect
            log.Printf("Attempting to reconnect to Redis...")
            initRedisClient()
            
            // Check if reconnection was successful
            if _, err := redisClient.Ping(ctx).Result(); err == nil {
                log.Println("Successfully reconnected to Redis")
                redisConnected = true
                
                // Check if we need to sync in-memory data to Redis
                redisAvailableMu.Lock()
                needsSync := redisWasUnavailable
                redisWasUnavailable = false
                redisAvailableMu.Unlock()
                
                if needsSync {
                    go syncInMemoryToRedis()
                }
            }
        }
    }
}

// Sync in-memory position and NavigationalStatus data to Redis
func syncInMemoryToRedis() {
    if settings.Debug {
        log.Println("Syncing in-memory data to Redis...")
    }
    
    // Sync positions
    lastPosMu.Lock()
    posCount := 0
    for userID, pos := range lastPositions {
        // For each shard (since we don't know which shard the userID belongs to)
        for _, shardID := range settings.Shards {
            posKey := fmt.Sprintf("%s%d:%d", positionKeyPrefix, shardID, userID)
            posJSON, _ := json.Marshal(pos)
            if err := redisClient.Set(ctx, posKey, posJSON, 0).Err(); err != nil {
                if settings.Debug {
                    log.Printf("Warning: Failed to sync position for user %d to Redis: %v", userID, err)
                }
            } else {
                posCount++
            }
        }
    }
    lastPosMu.Unlock()
    
    // Sync NavigationalStatus
    lastNavStatusMu.Lock()
    navCount := 0
    for userID, navStatus := range lastNavigationalStatus {
        // For each shard (since we don't know which shard the userID belongs to)
        for _, shardID := range settings.Shards {
            navStatusKey := fmt.Sprintf("%s%d:%d", navStatusKeyPrefix, shardID, userID)
            if err := redisClient.Set(ctx, navStatusKey, fmt.Sprintf("%f", navStatus), 0).Err(); err != nil {
                if settings.Debug {
                    log.Printf("Warning: Failed to sync NavigationalStatus for user %d to Redis: %v", userID, err)
                }
            } else {
                navCount++
            }
        }
    }
    lastNavStatusMu.Unlock()
    
    if settings.Debug {
        log.Printf("Synced %d positions and %d NavigationalStatus values to Redis", posCount, navCount)
    }
}