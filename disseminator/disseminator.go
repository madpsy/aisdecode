package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"hash/fnv"
	"time" // Added the time package

	_ "github.com/lib/pq"
	"github.com/zishang520/socket.io/v2/socket"
	"github.com/zishang520/engine.io/v2/types"
)

type Settings struct {
	IngestHost  string `json:"ingester_host"`
	IngestPort  int    `json:"ingester_port"`
	ListenPort  int    `json:"listen_port"`
	Debug       bool   `json:"debug"`
	PollInterval int   `json:"poll_interval"`
}

// This struct will contain the actual client database connection settings
type ClientDatabaseSettings struct {
	DbHost     string `json:"db_host"`
	DbPort     int    `json:"db_port"`
	DbUser     string `json:"db_user"`
	DbPass     string `json:"db_pass"`
	DbName     string `json:"db_name"`
}

type Client struct {
	Description string   `json:"description"`
	Ip          string   `json:"ip"`
	Port        int      `json:"port"`
	Shards      []int    `json:"shards"`
}

var clientConnections map[string]*ClientConnection
var streamShards int // Global variable to store the total number of shards

type ClientConnection struct {
	Db         *sql.DB
	DbHost     string
	DbPort     int
	DbUser     string
	DbPass     string
	DbName     string
	Shards     []int
}

var ioServer *socket.Server

func shardForUser(userID string) int {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return int(h.Sum32()) % streamShards
}

func QueryDatabaseForUser(userID string, query string) (*sql.Rows, error) {
	shardID := shardForUser(userID)
	var clientDescription string
	var clientConnection *ClientConnection // Use ClientConnection here

	// Iterate over clientConnections to find the ClientConnection for the shard
	for _, conn := range clientConnections {
		// Search through shards to find the one that handles the user
		for _, shard := range conn.Shards {
			if shard == shardID {
				clientDescription = conn.DbHost
				clientConnection = conn
				break
			}
		}
		if clientConnection != nil {
			break
		}
	}

	// If no matching ClientConnection was found, return an error
	if clientConnection == nil {
		return nil, fmt.Errorf("no client found handling shard %d", shardID)
	}

	// Perform the query on the database
	db := clientConnection.Db
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query on database for client %s: %v", clientDescription, err)
	}

	return rows, nil
}

func userStateHandler(w http.ResponseWriter, r *http.Request) {
	// Extract UserID from URL
	userID := r.URL.Path[len("/state/"):]

	// Prepare the SQL query to get the most recent message for each unique MessageID based on the 'id' column, including 'timestamp'
	query := fmt.Sprintf(`
		SELECT packet, timestamp
		FROM messages
		WHERE packet->>'UserID' = '%s'
		AND id IN (
			SELECT MAX(id)
			FROM messages
			WHERE packet->>'UserID' = '%s'
			GROUP BY packet->>'MessageID'
		)
		ORDER BY id DESC;
	`, userID, userID)

	// Call the QueryDatabaseForUser function
	rows, err := QueryDatabaseForUser(userID, query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Create a map to hold the merged results
	mergedResults := make(map[string]interface{})

	// Read the result and merge all packets into the mergedResults map
	for rows.Next() {
		var packet string
		var timestamp string
		if err := rows.Scan(&packet, &timestamp); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning result: %v", err), http.StatusInternalServerError)
			return
		}

		// Unmarshal the packet into a map
		var packetData map[string]interface{}
		if err := json.Unmarshal([]byte(packet), &packetData); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling packet data: %v", err), http.StatusInternalServerError)
			return
		}

		// Ensure MessageID is treated as a string
		messageID := ""
		if id, ok := packetData["MessageID"].(string); ok {
			messageID = id
		} else if id, ok := packetData["MessageID"].(float64); ok {
			// If it's a float64, convert it to string
			messageID = fmt.Sprintf("%v", id)
		}

		// Remove the 'UserID' field from the packet data
		delete(packetData, "UserID")

		// Add the 'timestamp' to the packet data
		packetData["timestamp"] = timestamp

		// Merge the packet data into the mergedResults map
		if _, exists := mergedResults[messageID]; !exists {
			mergedResults[messageID] = make(map[string]interface{})
		}

		// Merge the data into the existing messageID entry
		for key, value := range packetData {
			mergedResults[messageID].(map[string]interface{})[key] = value
		}
	}

	// Send the response as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(mergedResults); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
	}
}

// Utility function to handle debug logging
func logWithDebug(debug bool, format string, args ...interface{}) {
	if debug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// Load settings from the settings.json file
func loadSettings(path string) (*Settings, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read settings file: %v", err)
	}

	var settings Settings
	err = json.Unmarshal(data, &settings)
	if err != nil {
		return nil, fmt.Errorf("failed to parse settings: %v", err)
	}

	return &settings, nil
}

// Fetch client list from ingester
func getClients(ingesterHost string, ingesterPort int, debug bool) ([]Client, error) {
	url := fmt.Sprintf("http://%s:%d/clients", ingesterHost, ingesterPort)
	logWithDebug(debug, "Fetching clients from URL: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch clients: %v", err)
	}
	defer resp.Body.Close()

	// Define a new struct to hold the response
	type ClientResponse struct {
		Clients        []Client `json:"clients"`
		ConfiguredShards int     `json:"configured_shards"` // Store the configured_shards
	}

	var clientResponse ClientResponse
	err = json.NewDecoder(resp.Body).Decode(&clientResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client list: %v", err)
	}

	// Store the configured_shards value in the global streamShards variable
	streamShards = clientResponse.ConfiguredShards

	// Debug log for each client and its shards
	if debug {
		log.Println("[DEBUG] Clients found:")
		for _, client := range clientResponse.Clients {
			log.Printf("[DEBUG] Client %s handles shards %v", client.Description, client.Shards)
		}
	}

	logWithDebug(debug, "Successfully fetched %d clients and total configured_shards: %d", len(clientResponse.Clients), streamShards)
	return clientResponse.Clients, nil
}

// Handle changes in clients' configuration and database connections
func handleClientChanges(ingesterHost string, ingesterPort int, debug bool) {
	// Fetch client list from ingester
	clients, err := getClients(ingesterHost, ingesterPort, debug)
	if err != nil {
		log.Printf("Error fetching clients: %v", err)
		return
	}

	// Map to track existing clients
	existingClients := make(map[string]Client)

	// Process each client
	for _, client := range clients {
		existingClients[client.Description] = client

		// Check if the client already has an open database connection
		if _, exists := clientConnections[client.Description]; !exists {
			// New client or client has lost its connection, so we connect
			logWithDebug(debug, "New client %s detected, connecting to its database.", client.Description)
			clientSettings, err := getClientDatabaseSettings(client.Ip, client.Port, debug)
			if err != nil {
				log.Printf("Error fetching settings for client %s: %v", client.Description, err)
				continue
			}
			// Connect to the database for the new client
			db, err := connectToDatabase(clientSettings)
			if err != nil {
				log.Printf("Error connecting to database for client %s: %v", client.Description, err)
				continue
			}
			clientConnections[client.Description] = &ClientConnection{
				Db:         db,
				DbHost:     clientSettings.DbHost,
				DbPort:     clientSettings.DbPort,
				DbUser:     clientSettings.DbUser,
				DbPass:     clientSettings.DbPass,
				DbName:     clientSettings.DbName,
				Shards:     client.Shards,
			}
			log.Printf("Successfully connected to database for client %s: %s@%s:%d/%s",
				client.Description, clientSettings.DbUser, clientSettings.DbHost, clientSettings.DbPort, clientSettings.DbName)
		}
	}

	// Handle clients that have been removed (no longer in the list)
	for description, conn := range clientConnections {
		if _, exists := existingClients[description]; !exists {
			// Client has been removed, disconnect from its database
			log.Printf("Client %s has been removed, disconnecting.", description)
			conn.Db.Close()
			delete(clientConnections, description)
		}
	}
}

// Fetch client database settings (not global Settings, specific to clients)
func getClientDatabaseSettings(ip string, port int, debug bool) (*ClientDatabaseSettings, error) {
	url := fmt.Sprintf("http://%s:%d/settings", ip, port)
	logWithDebug(debug, "Fetching database settings from URL: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch database settings from client: %v", err)
	}
	defer resp.Body.Close()

	var settings ClientDatabaseSettings
	err = json.NewDecoder(resp.Body).Decode(&settings)
	if err != nil {
		return nil, fmt.Errorf("failed to decode database settings: %v", err)
	}

	// Log the fetched database settings if debug is enabled
	logWithDebug(debug, "Fetched database settings for client: %s:%d/%s",
		settings.DbHost, settings.DbPort, settings.DbName)

	return &settings, nil
}

// Connect to the PostgreSQL database
func connectToDatabase(settings *ClientDatabaseSettings) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Ensure the database connection is successful
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return db, nil
}

// Start the HTTP server with both Socket.IO and static file serving
func startHTTPServer(port int, mux *http.ServeMux) {
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

// Set up the HTTP server with routes
func setupServer(settings *Settings) {
	// Create a new ServeMux
	mux := http.NewServeMux()

	// Define the /state/{UserID} route
	mux.HandleFunc("/state/", userStateHandler)

	// Set up Socket.IO handler
	engineServer := types.CreateServer(nil)
	ioServer = socket.NewServer(engineServer, nil)
	mux.Handle("/socket.io/", engineServer)

	// Serve static files (e.g., index.html)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./index.html")
	})

	// Start the HTTP server
	go startHTTPServer(settings.ListenPort, mux)
}

func main() {
	// Load configuration settings
	settingsFile := "settings.json"
	settings, err := loadSettings(settingsFile)
	if err != nil {
		log.Fatalf("Error loading settings: %v", err)
	}

	// Initialize map to track client database connections
	clientConnections = make(map[string]*ClientConnection)

	// Set up polling interval
	pollInterval := time.Duration(settings.PollInterval) * time.Second
	if pollInterval > 0 {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		// Poll periodically to check for changes
		go func() {
			for {
				<-ticker.C
				logWithDebug(settings.Debug, "Polling clients at %v", time.Now())
				handleClientChanges(settings.IngestHost, settings.IngestPort, settings.Debug)
			}
		}()
	}

	// Initial fetch and setup
	handleClientChanges(settings.IngestHost, settings.IngestPort, settings.Debug)

	// Set up the server with HTTP and Socket.IO routes
	setupServer(settings)

	// Block forever (or handle gracefully shutting down the server if necessary)
	select {}
}
