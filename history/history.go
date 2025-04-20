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
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io/v2/socket"
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
	Debug       bool     `json:"debug"` // New debug setting
}

// Message structure for incoming data from the ingester
type Message struct {
	Packet    json.RawMessage `json:"message"`  // Store the entire message object for processing
	ShardID   int             `json:"shard_id"`
	Timestamp string          `json:"timestamp"`
}

func main() {
	// Default config file path
	defaultConfigPath := "./settings.json"

	// Parse the command-line arguments
	configPath := flag.String("config", defaultConfigPath, "Path to the settings.json file (default is ./settings.json)")
	flag.Parse()

	// Read the settings file
	settings, err := readSettings(*configPath)
	if err != nil {
		log.Fatal("Error reading settings: ", err)
	}

	// Enable debug mode if configured
	if settings.Debug {
		log.Printf("Debug mode enabled")
	}

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
			timestamp TIMESTAMP
		);
	`)
	if err != nil {
		log.Fatal("Error creating table: ", err)
	}

	// Create indexes if they don't exist
	createIndexesIfNotExist(db)

	// Start reading messages from the ingester
	go handleIngesterMessages(settings, ingesterConn, requestJSON, settings.Debug, db)

	// Create an Engine.IO server and a Socket.IO server on top of it
	engineServer := types.CreateServer(nil)
	ioServer := socket.NewServer(engineServer, nil)

	// Handle Socket.IO client connections
	ioServer.On("connection", func(args ...interface{}) {
		client := args[0].(*socket.Socket)
		log.Printf("New Socket.IO client connected: %s", client.Id())

		// Handle the incoming messages from the client (HTTP-based)
		client.On("message", func(args ...interface{}) {
			// Handle the HTTP-based message (JSON)
			data, ok := args[0].(map[string]interface{})
			if !ok {
				log.Println("Received data is not in expected format")
				return
			}

			if settings.Debug {
				messageJSON, err := json.Marshal(data)
				if err == nil {
					log.Printf("Received message from HTTP client: %s", string(messageJSON))
				} else {
					log.Printf("Error marshaling received message: %v", err)
				}
			}

			// Further processing or action based on this HTTP message
			// e.g., store data in DB or send a response
		})

		// Handle disconnection event
		client.On("disconnect", func(args ...interface{}) {
			log.Printf("Socket.IO client disconnected: %s", client.Id())
		})
	})

	// Set up HTTP server for Socket.IO and static files
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", engineServer)

	// Listen on the provided HTTP port (5000 for Socket.IO)
	httpAddr := fmt.Sprintf(":%d", 5000)
	go func() {
		log.Printf("Serving Socket.IO on http://%s", httpAddr)
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

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

func storeMessage(db *sql.DB, message Message) error {
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

    // Marshal the extracted packet data into JSON format to store it in the database
    packetJSON, err := json.Marshal(packetData)
    if err != nil {
        log.Printf("Error marshalling packet contents: %v", err)
        return err
    }

    // Insert only the "packet" contents into the database
    stmt := `INSERT INTO messages (packet, shard_id, timestamp) 
             VALUES ($1, $2, $3)`

    // Execute the query with the extracted packet JSON, shard_id, and timestamp
    _, err = db.Exec(stmt, packetJSON, message.ShardID, message.Timestamp)
    if err != nil {
        log.Printf("Error executing query: %v", err)
        return err
    }

    return nil
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
            err := processMessage(message, db)
            if err != nil {
                // Log only the failed message with the error
                log.Printf("Failed to unmarshal message: %v, Raw Message: %s", err, string(message))
            }
        }
    }
}

func processMessage(message []byte, db *sql.DB) error {
    var msg Message

    // Try unmarshalling the message into the Message struct
    err := json.Unmarshal(message, &msg)
    if err != nil {
        // If unmarshalling fails, return the error so it can be logged
        return err
    }

    // Store the message in the database
    err = storeMessage(db, msg)
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
}
