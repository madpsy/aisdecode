package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"hash/fnv"
	"strconv"
	"time"

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

// QueryDatabasesForAllShards queries all shard databases with the same query.
func QueryDatabasesForAllShards(query string) (map[string][]map[string]interface{}, error) {
	results := make(map[string][]map[string]interface{})

	for _, conn := range clientConnections {
		log.Printf("Querying shard %s at %s:%d", conn.DbHost, conn.DbHost, conn.DbPort)

		// Perform the query on the current shard's database
		db := conn.Db
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query on database for client %s: %v", conn.DbHost, err)
		}
		defer rows.Close()

		// Log the number of rows returned by the query
		var rowCount int
		columns, err := rows.Columns() // Get the column names dynamically
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for client %s: %v", conn.DbHost, err)
		}

		for rows.Next() {
			// Create a slice to hold column values dynamically
			values := make([]interface{}, len(columns))
			for i := range values {
				values[i] = new(interface{})
			}
			if err := rows.Scan(values...); err != nil {
				log.Printf("Error scanning result for shard %s: %v", conn.DbHost, err)
				continue
			}
			rowData := make(map[string]interface{})
			for i, column := range columns {
				// Store the column value in the rowData map
				rowData[column] = *(values[i].(*interface{}))
			}
			results[conn.DbHost] = append(results[conn.DbHost], rowData)
			rowCount++
		}

		// Log the total number of rows returned for the current shard
		log.Printf("Shard %s returned %d rows", conn.DbHost, rowCount)
	}
	// log.Printf("Results collected: %+v", results)
	return results, nil
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
    // Extract lat, lon, and radius from query parameters
    latStr := r.URL.Query().Get("latitude")
    lonStr := r.URL.Query().Get("longitude")
    radiusStr := r.URL.Query().Get("radius")
    
    // Extract 'limit' query parameter and parse it (default to 500)
    limitStr := r.URL.Query().Get("maxResults")
    limit := 500 // Default limit
    if limitStr != "" {
        parsedLimit, err := strconv.Atoi(limitStr)
        if err != nil || parsedLimit <= 0 {
            http.Error(w, "Invalid limit value", http.StatusBadRequest)
            return
        }
        // Ensure the limit does not exceed 500
        if parsedLimit > 500 {
            limit = 500
        } else {
            limit = parsedLimit
        }
    }

    // Extract 'maxage' query parameter and parse it (default to 24 hours)
    maxAgeStr := r.URL.Query().Get("maxAge")
    maxAge := 24 // Default maxage in hours
    if maxAgeStr != "" {
        parsedMaxAge, err := strconv.Atoi(maxAgeStr)
        if err != nil || parsedMaxAge <= 0 {
            http.Error(w, "Invalid maxage value", http.StatusBadRequest)
            return
        }
        // Ensure the maxage does not exceed 720 hours (30 days)
        if parsedMaxAge > 720 {
            maxAge = 720
        } else {
            maxAge = parsedMaxAge
        }
    }

    // Extract 'minSpeed' query parameter and parse it (optional)
    minSpeedStr := r.URL.Query().Get("minSpeed")
    var minSpeed float64
    if minSpeedStr != "" {
        var err error
        minSpeed, err = strconv.ParseFloat(minSpeedStr, 64)
        if err != nil || minSpeed < 0 {
            http.Error(w, "Invalid minSpeed value", http.StatusBadRequest)
            return
        }
    }

    var lat, lon, radius float64
    var err error

    // If lat, lon, and radius are provided, parse them
    if latStr != "" && lonStr != "" && radiusStr != "" {
        lat, err = strconv.ParseFloat(latStr, 64)
        if err != nil {
            http.Error(w, "Invalid latitude", http.StatusBadRequest)
            return
        }

        lon, err = strconv.ParseFloat(lonStr, 64)
        if err != nil {
            http.Error(w, "Invalid longitude", http.StatusBadRequest)
            return
        }

        radius, err = strconv.ParseFloat(radiusStr, 64)
        if err != nil || radius <= 0 {
            http.Error(w, "Invalid radius", http.StatusBadRequest)
            return
        }
    }

    // Build the SQL query
    query := `
        SELECT user_id, packet, timestamp, ais_class, count
        FROM state
    `
    
    whereAdded := false // Flag to track if WHERE clause is already added

    // If lat, lon, and radius are specified, add the condition to filter by distance
    if latStr != "" && lonStr != "" && radiusStr != "" {
        if whereAdded {
            query += fmt.Sprintf(`
                AND ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    ST_SetSRID(ST_Point(%f, %f), 4326)
                ) <= %f
            `, lon, lat, radius)
        } else {
            query += fmt.Sprintf(`
                WHERE ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    ST_SetSRID(ST_Point(%f, %f), 4326)
                ) <= %f
            `, lon, lat, radius)
            whereAdded = true
        }
    }

    // Add a condition to filter results based on the 'timestamp' field and maxage
    currentTime := time.Now()
    maxAgeDuration := time.Duration(maxAge) * time.Hour
    if whereAdded {
        query += fmt.Sprintf(`
            AND timestamp >= '%s'
        `, currentTime.Add(-maxAgeDuration).UTC().Format(time.RFC3339))
    } else {
        query += fmt.Sprintf(`
            WHERE timestamp >= '%s'
        `, currentTime.Add(-maxAgeDuration).UTC().Format(time.RFC3339))
        whereAdded = true
    }

    // If 'minSpeed' is specified, add the filter condition for Speed Over Ground
    if minSpeedStr != "" {
        if whereAdded {
            query += fmt.Sprintf(`
                AND (packet->>'Sog')::float >= %f
            `, minSpeed)
        } else {
            query += fmt.Sprintf(`
                WHERE (packet->>'Sog')::float >= %f
            `, minSpeed)
            whereAdded = true
        }
    }

    // Add ORDER BY timestamp before LIMIT to ensure correct syntax
    query += " ORDER BY timestamp ASC"

    // Add a LIMIT clause to the query based on the 'limit' parameter
    query += fmt.Sprintf(" LIMIT %d", limit)

    // Debugging: Log the full query to check its construction
    fmt.Println("Final SQL Query:", query)

    // Call the QueryDatabasesForAllShards function to query the database
    results, err := QueryDatabasesForAllShards(query)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
        return
    }

    // Create a map to hold the summarized results for each UserID
    summarizedResults := make(map[string]interface{})

    // Iterate over all results from each shard
    for _, shardData := range results {
        for _, row := range shardData {
            // Extract the UserID and packet data from the row
            userID, ok := row["user_id"]
            if !ok {
                continue
            }

            // Handle both int and int64 types for user_id
            var userIDStr string
            switch v := userID.(type) {
            case int:
                userIDStr = fmt.Sprintf("%d", v)
            case int64:
                userIDStr = fmt.Sprintf("%d", v)
            default:
                continue
            }

            // Extract the packet field, which is a byte array
            packetData, ok := row["packet"].([]byte)
            if !ok {
                continue
            }

            // Convert the byte slice to a string (assuming UTF-8 encoding)
            packetStr := string(packetData)

            // Unmarshal the packet data into a map
            var packetMap map[string]interface{}
            if err := json.Unmarshal([]byte(packetStr), &packetMap); err != nil {
                continue
            }

            // Prepare the summary result for this user
            summary := make(map[string]interface{})
            summary["CallSign"] = getFieldString(packetMap, "CallSign")
            summary["Cog"] = getFieldFloat(packetMap, "Cog")
            summary["Destination"] = getFieldString(packetMap, "Destination")
            summary["Dimension"] = getFieldJSON(packetMap, "Dimension")
            summary["Latitude"] = getFieldFloat(packetMap, "Latitude")
            summary["Longitude"] = getFieldFloat(packetMap, "Longitude")
            summary["MaximumStaticDraught"] = getFieldFloat(packetMap, "MaximumStaticDraught")
            summary["Name"] = getFieldString(packetMap, "Name")
            summary["NameExtension"] = getFieldString(packetMap, "NameExtension")
            summary["NavigationalStatus"] = getFieldFloat(packetMap, "NavigationalStatus")
            summary["Sog"] = getFieldFloat(packetMap, "Sog")
            summary["TrueHeading"] = getFieldFloat(packetMap, "TrueHeading")
            summary["Type"] = getFieldFloat(packetMap, "Type")

            // Add AIS Class and Timestamp
            if aisClass, ok := row["ais_class"].(string); ok {
                summary["AISClass"] = aisClass
            }
            if timestamp, ok := row["timestamp"].(time.Time); ok {
                summary["LastUpdated"] = timestamp.UTC().Format(time.RFC3339Nano)
            }

            // **Log the count field directly to check its value**
            if count, ok := row["count"].(int64); ok {
                summary["NumMessages"] = count
            }

            // Add the summary result to the main result map
            summarizedResults[userIDStr] = summary
        }
    }

    // Send the summarized results as JSON
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(summarizedResults); err != nil {
        http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
    }
}


// Helper function to safely get a string value from packetData (returns empty string if not found)
func getFieldString(packetData map[string]interface{}, field string) string {
    if value, ok := packetData[field].(string); ok && value != "" {
        return value
    }
    return ""  // Return empty string if field is nil or not found
}

// Helper function to safely get a float64 value from packetData (returns 0 if not found)
func getFieldFloat(packetData map[string]interface{}, field string) float64 {
    // Check if the value is already a float64
    if value, ok := packetData[field].(float64); ok {
        return value
    }
    // Check if the value is an integer type and convert it to float64
    if value, ok := packetData[field].(int); ok {
        return float64(value)
    }
    if value, ok := packetData[field].(int64); ok {
        return float64(value)
    }
    // Check if the value is a string that can be parsed into a float
    if value, ok := packetData[field].(string); ok && value != "" {
        if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
            return floatValue
        }
    }
    return 0 // Return 0 if value is not found or cannot be converted to float
}

// Helper function to safely parse any JSON field into a map (returns nil if parsing fails)
func getFieldJSON(packetData map[string]interface{}, field string) map[string]interface{} {
    if value, ok := packetData[field].(map[string]interface{}); ok {
        return value  // If the value is already a map, return it directly
    }
    // If the value is a string (and not empty), try unmarshalling it
    if value, ok := packetData[field].(string); ok && value != "" {
        var jsonData map[string]interface{}
        if err := json.Unmarshal([]byte(value), &jsonData); err == nil {
            return jsonData
        }
    }
    return nil  // Return nil if the field is missing or cannot be parsed
}

func formatTimestamp(t time.Time) string {
    // Return the time in UTC with nanosecond precision in the ISO 8601 format
    return t.UTC().Format(time.RFC3339Nano) // Format: "2025-04-21T13:49:56.259736Z"
}

func userStateHandler(w http.ResponseWriter, r *http.Request) {
    // Extract UserID from URL
    userID := r.URL.Path[len("/state/"):]

    // Modify the SQL query to include the 'count' column
    query := fmt.Sprintf(`
        SELECT packet, timestamp, ais_class, count
        FROM state
        WHERE user_id = '%s';
    `, userID)

    // Call the QueryDatabaseForUser function to query the database for the specific user
    rows, err := QueryDatabaseForUser(userID, query)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    // Create a map to hold the merged results
    mergedResults := make(map[string]interface{})

    // Read the result and add packet data to mergedResults map
    for rows.Next() {
        var packet, timestamp, aisClass string
        var count int
        if err := rows.Scan(&packet, &timestamp, &aisClass, &count); err != nil {
            http.Error(w, fmt.Sprintf("Error scanning result: %v", err), http.StatusInternalServerError)
            return
        }

        // Unmarshal the packet into a map
        var packetData map[string]interface{}
        if err := json.Unmarshal([]byte(packet), &packetData); err != nil {
            http.Error(w, fmt.Sprintf("Error unmarshalling packet data: %v", err), http.StatusInternalServerError)
            return
        }

        // Add AISClass to the packet data
        packetData["AISClass"] = aisClass

        // Add the 'timestamp' to the packet data as LastUpdated
        packetData["LastUpdated"] = timestamp

        // Add the number of messages sent (NumMessages)
        packetData["NumMessages"] = count

        // Set the packet data directly as the response
        mergedResults = packetData
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

	// Define the /summary} route
	mux.HandleFunc("/summary", summaryHandler)

	// Define the /state/{UserID} route
	mux.HandleFunc("/state/", userStateHandler)

	// Set up Socket.IO handler
	engineServer := types.CreateServer(nil)
	ioServer = socket.NewServer(engineServer, nil)
	mux.Handle("/socket.io/", engineServer)

	// Serve static files (e.g., index.html)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.FileServer(http.Dir("web")).ServeHTTP(w, r)
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
