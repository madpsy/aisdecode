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

	// Create the messages table with a single JSONB column called 'packet' instead of 'message'
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id SERIAL PRIMARY KEY,
			packet JSONB,
			shard_id INT,
			timestamp TIMESTAMP,
			source_ip VARCHAR(45)
		);
	`)
	if err != nil {
		log.Fatal("Error creating table: ", err)
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
    // Unmarshal the outer "message" to extract the inner "packet" object
    var outerMap map[string]interface{}
    err := json.Unmarshal(message.Packet, &outerMap)
    if err != nil {
        log.Printf("Error unmarshalling outer message: %v", err)
        return err
    }

    // Extract the "packet" field (which is the actual data we need)
    packetData, exists := outerMap["packet"]
    if !exists {
        log.Println("Packet field is missing in the JSON message.")
        return fmt.Errorf("Packet field is missing")
    }

    // Extract the source_ip directly from the message struct (it is outside of the "message" object)
    sourceIP := message.SourceIP

    // Marshal the extracted packet data into JSON format to store it in the database
    packetJSON, err := json.Marshal(packetData)
    if err != nil {
        log.Printf("Error marshalling packet contents: %v", err)
        return err
    }

    // Retry mechanism for database connection
    err = tryStoreMessage(db, packetJSON, message.ShardID, message.Timestamp, sourceIP, settings)
    if err != nil {
        log.Printf("Error storing message: %v", err)
    }

    return err
}


func tryStoreMessage(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, settings *Settings) error {
    // Try to insert the message into the database
    stmt := `INSERT INTO messages (packet, shard_id, timestamp, source_ip) 
             VALUES ($1, $2, $3, $4)`

    _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP)
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
            _, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP)
            if err != nil {
                log.Printf("Error executing query after reconnecting: %v", err)
                return err
            }
        }
    }
    return nil
}

func isDatabaseConnectionError(err error) bool {
    // You could improve this function by checking the error type, for now just checking for a generic error.
    return err != nil && err.Error() == "pq: connection to server lost"
}

func reconnectToDatabase(settings *Settings) (*sql.DB, error) {
    connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName)
    
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, err
    }

    // Check if the connection is alive
    err = db.Ping()
    if err != nil {
        return nil, err
    }

    log.Println("Successfully reconnected to the PostgreSQL database.")
    return db, nil
}

// Connect to the ingester (outgoing TCP connection to the ingester)
func connectToIngester(host string, port int, debug bool) (net.Conn, error) {
	for {
		// Real connection to the ingester
		addr := fmt.Sprintf("%s:%d", host, port)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("Failed to connect to ingester at %s: %v. Retrying in 5 seconds...", addr, err)
			time.Sleep(5 * time.Second) // Retry every 5 seconds
			continue
		}

		if debug {
			log.Printf("Successfully connected to ingester at %s", addr)
		}

		// Return the connection object to send and receive data
		return conn, nil
	}
}

func handleIngesterMessages(settings *Settings, conn net.Conn, requestJSON []byte, debug bool, db *sql.DB) {
    buffer := make([]byte, 0) // Buffer to hold the incoming data

    for {
        // Read data from the TCP connection into a temporary buffer
        buf := make([]byte, 4096)
        n, err := conn.Read(buf)

        if err != nil {
            log.Printf("Error reading from connection: %v", err)
            conn.Close() // Close the current connection and attempt reconnection
            conn, err = connectToIngester(settings.IngestHost, settings.IngestPort, debug) // Reconnect
            if err != nil {
                log.Printf("Failed to reconnect: %v", err)
                continue
            }
            log.Println("Reconnected to ingester")

            // Resend the initial request to the ingester
            _, err := conn.Write(requestJSON)
            if err != nil {
                log.Printf("Failed to resend request to ingester: %v", err)
                continue
            }
            log.Printf("Resent request to ingester: %s", string(requestJSON))
            continue
        }

        // Append the newly read data to the buffer
        buffer = append(buffer, buf[:n]...)

        // Process complete messages from the buffer
        for {
            idx := bytes.IndexByte(buffer, '\x00') // Look for the null character as the message delimiter
            if idx == -1 {
                break // No complete message, wait for more data
            }

            // Extract the complete message from the buffer
            message := buffer[:idx]
            buffer = buffer[idx+1:] // Remove the processed message from the buffer

            // Now process the complete message
            err := processMessage(message, db, settings) // Pass settings here
            if err != nil {
                // Log only the failed message with the error
                log.Printf("Failed to unmarshal message: %v, Raw Message: %s", err, string(message))
            }
        }
    }
}

func processMessage(message []byte, db *sql.DB, settings *Settings) error {
    var msg Message

    // Try unmarshalling the message into the Message struct
    err := json.Unmarshal(message, &msg)
    if err != nil {
        // If unmarshalling fails, return the error so it can be logged
        return err
    }

    // Store the message in the database
    err = storeMessage(db, msg, settings) // Pass settings here
    if err != nil {
        log.Printf("Error storing message: %v", err)
    }

    return err
}

func createIndexesIfNotExist(db *sql.DB) {
    // Create a composite index for shard_id and UserID (extracted from the JSON packet)
    _, err := db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_shard_userid ON messages (
            shard_id,                    -- Direct column for shard_id
            (packet->>'UserID')          -- Extract UserID from the "packet" JSON field
        );
    `)
    if err != nil {
        log.Printf("Error creating composite index for shard_id and UserID: %v", err)
    }

    // Create a btree index specifically for UserID in the nested "packet"
    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_packet_userid ON messages (
            (packet->>'UserID')  -- Extract UserID from the "packet" JSON field
        );
    `)
    if err != nil {
        log.Printf("Error creating index for UserID: %v", err)
    }

    // Create a btree index for shard_id directly in the table (no changes needed here)
    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_packet_shard_id ON messages (shard_id);
    `)
    if err != nil {
        log.Printf("Error creating index for shard_id: %v", err)
    }

    // Create an index for the source_ip field
    _, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_source_ip ON messages (source_ip);
    `)
    if err != nil {
        log.Printf("Error creating index for source_ip: %v", err)
    }
}

func startHTTPServer() {
	if settings == nil {
		log.Fatal("Settings are not initialized.")
		return
	}

	http.HandleFunc("/settings", getSettingsHandler)

	// Start the server
	address := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Starting HTTP server on port %d...\n", settings.ListenPort)
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatalf("Error starting HTTP server: %v\n", err)
	}
}

// HTTP handler that returns the contents of settings.json
func getSettingsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(settings)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error encoding settings: %v", err), http.StatusInternalServerError)
	}
}