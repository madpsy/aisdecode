// main.go
package main

import (
    "bytes"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"

    _ "github.com/lib/pq"

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
}

var settings *Settings
var db *sql.DB

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
    Packet    json.RawMessage `json:"message"`
    ShardID   int             `json:"shard_id"`
    Timestamp string          `json:"timestamp"`
    SourceIP  string          `json:"source_ip"`
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
            message_id INT
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
            ext_lookup_complete BOOLEAN DEFAULT FALSE
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

    go handleIngesterMessages(settings, ingConn, requestJSON, settings.Debug, db)
    select {}
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
    var msg Message
    if err := json.Unmarshal(message, &msg); err != nil {
        log.Printf("[DEBUG] failed to unmarshal outer Message: %v", err)
        return err
    }

    // unwrap the inner packet
    var envelope map[string]json.RawMessage
    if err := json.Unmarshal(msg.Packet, &envelope); err != nil {
        log.Printf("[DEBUG] failed to unmarshal envelope: %v", err)
        return err
    }
    rawInner, ok := envelope["packet"]
    if !ok {
        rawInner = msg.Packet
    }
    //log.Printf("[DEBUG] rawInner JSON: %s", string(rawInner))

    // persist to DB
    if err := storeMessage(db, msg, settings); err != nil {
        log.Printf("[DEBUG] Error storing message: %v", err)
    }

    // parse into map
    var packet map[string]interface{}
    if err := json.Unmarshal(rawInner, &packet); err != nil {
        log.Printf("[DEBUG] failed to unmarshal inner packet: %v", err)
        return nil
    }

    // determine user key
    var uidKey string
    if v, ok := packet["UserID"].(float64); ok {
        uidKey = strconv.Itoa(int(v))
    } else if s, ok := packet["UserID"].(string); ok {
        uidKey = s
    } else {
        log.Printf("[DEBUG] packet missing UserID, skipping emit")
        return nil
    }

    // build the payload we want to emit
    emitPayload := map[string]interface{}{
        "data":      packet,
        "type":      packet["MessageID"],
        "timestamp": msg.Timestamp,
    }
    // find subscribers
    userSubscribersMu.RLock()
    subs := userSubscribers[uidKey]
    userSubscribersMu.RUnlock()
    if len(subs) == 0 {
        return nil
    }
    for sid := range subs {
        sock, ok := connectedClients[sid]
        if !ok {
            continue
        }
        sock.Emit("ais_data", emitPayload)
    }
    return nil
}

// ── DATABASE HELPERS AND ORIGINAL LOGIC ─────────────────────────────────────

func createIndexesIfNotExist(db *sql.DB) {
    stmts := []string{
        `CREATE INDEX IF NOT EXISTS idx_user_id ON messages (user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_message_id ON messages (message_id);`,
        `CREATE INDEX IF NOT EXISTS idx_shard_id ON messages (shard_id);`,
        `CREATE INDEX IF NOT EXISTS idx_user_id_state ON state (user_id);`,
        `CREATE INDEX IF NOT EXISTS idx_packet_jsonb_search_fields ON state USING GIN (packet jsonb_ops);`,
        `CREATE INDEX IF NOT EXISTS idx_name_state ON state (name);`,
        `CREATE INDEX IF NOT EXISTS idx_messages_geospatial ON messages USING GIST (
            ST_SetSRID(ST_Point((packet->>'Longitude')::float, (packet->>'Latitude')::float), 4326)
        );`,
    }
    for _, s := range stmts {
        if _, err := db.Exec(s); err != nil {
            log.Printf("Error creating index: %v", err)
        }
    }
}

func storeMessage(db *sql.DB, message Message, settings *Settings) error {
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

    messageID, messageIDExists := packetMap["MessageID"].(float64)
    if messageIDExists && messageID == 24 {
        if reportA, reportAExists := packetMap["ReportA"].(map[string]interface{}); reportAExists {
            if valid, validExists := reportA["Valid"].(bool); validExists && valid {
                for key, value := range reportA {
                    packetMap[key] = value
                }
            }
            delete(packetMap, "ReportA")
        }
        if reportB, reportBExists := packetMap["ReportB"].(map[string]interface{}); reportBExists {
            if valid, validExists := reportB["Valid"].(bool); validExists && valid {
                if shipType, shipTypeExists := reportB["ShipType"].(float64); shipTypeExists {
                    reportB["Type"] = int(shipType)
                    delete(reportB, "ShipType")
                }
                for key, value := range reportB {
                    packetMap[key] = value
                }
            }
            delete(packetMap, "ReportB")
        }
    }

    userIDf, userIDExists := packetMap["UserID"].(float64)
    messageIDExtracted, messageIDExists := packetMap["MessageID"].(float64)
    if !userIDExists || !messageIDExists {
        return fmt.Errorf("UserID or MessageID is missing")
    }

    packetJSON, err := json.Marshal(packetMap)
    if err != nil {
        return err
    }

    if err := storeState(db, packetJSON, message.ShardID, message.Timestamp, int(userIDf), messageIDExtracted); err != nil {
        log.Printf("Error storing state: %v", err)
    }
    if err := tryStoreMessage(db, packetJSON, message.ShardID, message.Timestamp, message.SourceIP, userIDf, messageIDExtracted, settings); err != nil {
        log.Printf("Error storing message: %v", err)
    }

    return nil
}

func storeState(db *sql.DB, packetJSON []byte, shardID int, timestamp string, userID int, messageID float64) error {
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
        existingCount = 0
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
    case 18, 19, 24:
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
          (packet, shard_id, timestamp, user_id, ais_class, count, image_url, name, ext_lookup_complete)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (user_id) DO UPDATE SET
          packet              = EXCLUDED.packet,
          shard_id            = EXCLUDED.shard_id,
          timestamp           = EXCLUDED.timestamp,
          ais_class           = EXCLUDED.ais_class,
          count               = state.count + 1
    `, packetJSON, shardID, timestamp, userID, existingAisClass, existingCount, "", "", existingLookupComplete)
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

func tryStoreMessage(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, userID float64, messageID float64, settings *Settings) error {
    stmt := `INSERT INTO messages (packet, shard_id, timestamp, source_ip, user_id, message_id) 
             VALUES ($1, $2, $3, $4, $5, $6)`
    _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID)
    if err != nil {
        log.Printf("Error executing query: %v", err)
        if isDatabaseConnectionError(err) {
            log.Println("Attempting to reconnect to the PostgreSQL database...")
            db, err = reconnectToDatabase(settings)
            if err != nil {
                log.Printf("Failed to reconnect to the database: %v", err)
                return err
            }
            _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID)
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
