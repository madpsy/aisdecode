package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"bytes"
	"time"

	_ "github.com/lib/pq"
)

var buffer []byte

// Settings structure based on settings.json
type Settings struct {
	IngestPort  int      `json:"ingest_port"`
	IngestHost  string   `json:"ingest_host"`
	DbHost      string   `json:"db_host"`
	DbPort      int      `json:"db_port"`
	DbUser      string   `json:"db_user"`
	DbPass      string   `json:"db_pass"`
	DbName      string   `json:"db_name"`
	ListenPort  int      `json:"listen_port"`
	Description string   `json:"description"`
	Shards      []int    `json:"shards"`
	Debug       bool     `json:"debug"`
}

var settings *Settings

// Message structure for incoming data from the ingester
type Message struct {
	Packet    json.RawMessage `json:"message"`  // Store the entire message object for processing
	ShardID   int             `json:"shard_id"`
	Timestamp string          `json:"timestamp"`
	SourceIP  string          `json:"source_ip"`
}

func main() {
	// Default config file path
	defaultConfigPath := "./settings.json"

	// Parse the command-line arguments
	configPath := flag.String("config", defaultConfigPath, "Path to the settings.json file (default is ./settings.json)")
	flag.Parse()

	// Read the settings file
	var err error
	settings, err = readSettings(*configPath) // Assign directly to global settings variable
	if err != nil {
		log.Fatal("Error reading settings: ", err)
	}

	// Enable debug mode if configured
	if settings.Debug {
		log.Printf("Debug mode enabled")
	}

	// Start the HTTP server in a goroutine
	go startHTTPServer()

	// Connect to the ingester (outgoing TCP connection)
	ingesterConn, err := connectToIngester(settings.IngestHost, settings.IngestPort, settings.Debug)
	if err != nil {
		log.Fatal("Error connecting to ingester: ", err)
	}
	defer ingesterConn.Close()

	// Construct the initial request to send to the ingester
	requestData := map[string]interface{}{
		"shards":     settings.Shards,
		"description": settings.Description,
		"port":        settings.ListenPort,
	}
	requestJSON, err := json.Marshal(requestData)
	if err != nil {
		log.Fatal("Error marshalling request JSON: ", err)
	}

	// Send the request to the ingester
	_, err = ingesterConn.Write(requestJSON)
	if err != nil {
		log.Fatal("Error sending request to ingester: ", err)
	}
	log.Printf("Sent request to ingester: %s", string(requestJSON))

	// PostgreSQL connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName)

	// Open PostgreSQL database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL database: ", err)
	}
	defer db.Close()

	// Log successful database connection
	if settings.Debug {
		log.Printf("Connected to PostgreSQL database: %s", settings.DbName)
	}

	// Create the messages table with new user_id and message_id columns
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
		log.Fatal("Error creating table: ", err)
	}

	_, err = db.Exec(`
	    CREATE TABLE IF NOT EXISTS summary (
	        packet JSONB,
	        shard_id INT,
	        timestamp TIMESTAMP,
	        source_ip VARCHAR(45),
	        user_id INT,
	        message_id INT,
	        PRIMARY KEY (user_id, message_id)
	    );
	`)
	if err != nil {
	    log.Fatal("Error creating summary table: ", err)
	}

	// Create indexes if they don't exist
	createIndexesIfNotExist(db)

	// Start reading messages from the ingester
	go handleIngesterMessages(settings, ingesterConn, requestJSON, settings.Debug, db)

	// Block until a termination signal is received
	select {}
}

// Read the settings from the JSON file
func readSettings(path string) (*Settings, error) {
	data, err := os.ReadFile(path) // Replace ioutil with os.ReadFile
	if err != nil {
		return nil, err
	}

	var settings Settings
	err = json.Unmarshal(data, &settings)
	if err != nil {
		return nil, err
	}

	return &settings, nil
}

func storeMessage(db *sql.DB, message Message, settings *Settings) error {
    var outerMap map[string]interface{}
    err := json.Unmarshal(message.Packet, &outerMap)
    if err != nil {
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

    // Check if MessageID is 24 and flatten
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
                for key, value := range reportB {
                    packetMap[key] = value
                }
            }
            delete(packetMap, "ReportB")
        }
    }

    userID, userIDExists := packetMap["UserID"].(float64)
    messageIDExtracted, messageIDExists := packetMap["MessageID"].(float64)

    if !userIDExists || !messageIDExists {
        log.Println("UserID or MessageID is missing in the packet.")
        return fmt.Errorf("UserID or MessageID is missing")
    }

    packetJSON, err := json.Marshal(packetMap)
    if err != nil {
        log.Printf("Error marshalling packet contents: %v", err)
        return err
    }

    // Convert userID and messageIDExtracted to int before passing to storeSummary
    err = storeSummary(db, packetJSON, message.ShardID, message.Timestamp, message.SourceIP, int(userID), int(messageIDExtracted))
    if err != nil {
        log.Printf("Error storing summary: %v", err)
    }

    // Insert into messages as well (optional)
    err = tryStoreMessage(db, packetJSON, message.ShardID, message.Timestamp, message.SourceIP, userID, messageIDExtracted, settings)
    if err != nil {
        log.Printf("Error storing message: %v", err)
    }

    return err
}

func storeSummary(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, userID int, messageID int) error {
    // Special handling for message_id == 24
    if messageID == 24 {
        // Fetch the current packet if exists
        var existingPacketJSON []byte
        err := db.QueryRow(`
            SELECT packet FROM summary WHERE user_id = $1 AND message_id = $2
        `, userID, messageID).Scan(&existingPacketJSON)

        // If an existing packet is found, merge it with the new packet
        if err == nil {
            // Merge the two JSON objects (existing and new)
            var existingPacket map[string]interface{}
            var newPacket map[string]interface{}

            err := json.Unmarshal(existingPacketJSON, &existingPacket)
            if err != nil {
                return fmt.Errorf("Error unmarshalling existing packet: %v", err)
            }

            err = json.Unmarshal(packetJSON, &newPacket)
            if err != nil {
                return fmt.Errorf("Error unmarshalling new packet: %v", err)
            }

            // Merge the packets (flatten and add new data)
            for key, value := range newPacket {
                existingPacket[key] = value
            }

            // Marshal the merged packet back to JSON
            packetJSON, err = json.Marshal(existingPacket)
            if err != nil {
                return fmt.Errorf("Error marshalling merged packet: %v", err)
            }
        } else if err != sql.ErrNoRows {
            return fmt.Errorf("Error querying existing packet: %v", err)
        }
    }

    // Insert or update the summary table
    _, err := db.Exec(`
        INSERT INTO summary (packet, shard_id, timestamp, source_ip, user_id, message_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id, message_id) 
        DO UPDATE SET packet = EXCLUDED.packet, shard_id = EXCLUDED.shard_id, timestamp = EXCLUDED.timestamp, source_ip = EXCLUDED.source_ip
    `, packetJSON, shardID, timestamp, sourceIP, userID, messageID)

    if err != nil {
        return fmt.Errorf("Error inserting or updating summary table: %v", err)
    }

    return nil
}

func tryStoreMessage(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, userID float64, messageID float64, settings *Settings) error {
    // Try to insert the message into the database
    stmt := `INSERT INTO messages (packet, shard_id, timestamp, source_ip, user_id, message_id) 
             VALUES ($1, $2, $3, $4, $5, $6)`

    _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID)
    if err != nil {
        log.Printf("Error executing query: %v", err)
        // If the error is due to the database connection being lost, attempt to reconnect
        if isDatabaseConnectionError(err) {
            log.Println("Attempting to reconnect to the PostgreSQL database...")
            db, err = reconnectToDatabase(settings) // Pass settings to reconnectToDatabase
            if err != nil {
                log.Printf("Failed to reconnect to the database: %v", err)
                return err
            }

            // Try inserting the message again after reconnecting
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

    err = db.Ping()
    if err != nil {
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
                log.Printf("Failed to resend request to ingester: %v", err)
                continue
            }
            log.Printf("Resent request to ingester: %s", string(requestJSON))
            continue
        }

        buffer = append(buffer, buf[:n]...)

        for {
            idx := bytes.IndexByte(buffer, '\x00')
            if idx == -1 {
                break
            }

            message := buffer[:idx]
            buffer = buffer[idx+1:]

            err := processMessage(message, db, settings)
            if err != nil {
                log.Printf("Failed to unmarshal message: %v, Raw Message: %s", err, string(message))
            }
        }
    }
}

func processMessage(message []byte, db *sql.DB, settings *Settings) error {
    var msg Message

    err := json.Unmarshal(message, &msg)
    if err != nil {
        return err
    }

    err = storeMessage(db, msg, settings)
    if err != nil {
        log.Printf("Error storing message: %v", err)
    }

    return err
}

func createIndexesIfNotExist(db *sql.DB) {
    _, err := db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_user_id ON messages (user_id);
    `)
    if err != nil {
        log.Printf("Error creating index for user_id: %v", err)
    }

    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_message_id ON messages (message_id);
    `)
    if err != nil {
        log.Printf("Error creating index for message_id: %v", err)
    }

    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_shard_id ON messages (shard_id);
    `)
    if err != nil {
        log.Printf("Error creating index for shard_id: %v", err)
    }

    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_source_ip ON messages (source_ip);
    `)
    if err != nil {
        log.Printf("Error creating index for source_ip: %v", err)
    }
    _, err = db.Exec(`
	CREATE INDEX IF NOT EXISTS idx_user_id_message_id ON summary (user_id, message_id);
    `)
    if err != nil {
	log.Fatal("Error creating index idx_user_id_message_id for summary table: ", err)
    }
    _, err = db.Exec(`
	CREATE INDEX IF NOT EXISTS idx_user_id ON summary (user_id);
    `)
    if err != nil {
	log.Fatal("Error creating index idx_user_id for summary table: ", err)
    }
    _, err = db.Exec(`
	CREATE INDEX IF NOT EXISTS idx_message_id ON summary (message_id);
    `)
    if err != nil {
	log.Fatal("Error creating index idx_message_id for summary table: ", err)
    }
}

func startHTTPServer() {
    if settings == nil {
        log.Fatal("Settings are not initialized.")
        return
    }

    http.HandleFunc("/settings", getSettingsHandler)

    address := fmt.Sprintf(":%d", settings.ListenPort)
    log.Printf("Starting HTTP server on port %d...\n", settings.ListenPort)
    if err := http.ListenAndServe(address, nil); err != nil {
        log.Fatalf("Error starting HTTP server: %v\n", err)
    }
}

func getSettingsHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    err := json.NewEncoder(w).Encode(settings)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error encoding settings: %v", err), http.StatusInternalServerError)
    }
}
