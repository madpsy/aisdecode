package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time" // Added the time package

	_ "github.com/lib/pq"
	"github.com/gorilla/websocket"
)

type Settings struct {
	IngestHost  string `json:"ingester_host"`  // Updated to match the key in settings.json
	IngestPort  int    `json:"ingester_port"`  // Updated to match the key in settings.json
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

type ClientConnection struct {
	Db         *sql.DB
	DbHost     string
	DbPort     int
	DbUser     string
	DbPass     string
	DbName     string
	Shards     []int
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
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

	// Start WebSocket server on the defined ListenPort
	go startWebSocketServer(settings.ListenPort)

	// Block forever (or handle gracefully shutting down the server if necessary)
	select {}
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

	var clients []Client
	err = json.NewDecoder(resp.Body).Decode(&clients)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client list: %v", err)
	}

	// Debug log for each client and its shards
	if debug {
		log.Println("[DEBUG] Clients found:")
		for _, client := range clients {
			log.Printf("[DEBUG] Client %s handles shards %v", client.Description, client.Shards)
		}
	}

	logWithDebug(debug, "Successfully fetched %d clients", len(clients))
	return clients, nil
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

// Start a WebSocket server for listening on the given port
func startWebSocketServer(port int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Upgrade the HTTP connection to a WebSocket connection
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("Error upgrading HTTP to WebSocket: %v", err)
			return
		}
		defer conn.Close()

		log.Println("New WebSocket connection established.")

		// Handle incoming WebSocket messages
		for {
			messageType, p, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading message: %v", err)
				break
			}

			// Log the received message
			log.Printf("Received WebSocket message: %s", p)

			// Optionally, you could send a response to the client
			if err := conn.WriteMessage(messageType, []byte("Message received")); err != nil {
				log.Printf("Error writing message: %v", err)
				break
			}
		}
	})

	// Start the HTTP server to serve WebSocket connections
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting WebSocket server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Error starting WebSocket server: %v", err)
	}
}
