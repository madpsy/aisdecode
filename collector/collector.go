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
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	// SOCKET.IO / ENGINE.IO
	engine "github.com/zishang520/engine.io/v2/engine"
	"github.com/zishang520/engine.io/v2/types"
	socketio "github.com/zishang520/socket.io/v2/socket"
)

// ── SETTINGS ────────────────────────────────────────────────────────────────

type Settings struct {
	IngestPort             int               `json:"ingest_port"`
	IngestHost             string            `json:"ingest_host"`
	IngestHTTPPort         int               `json:"ingest_http_port"`       // HTTP port for ingester web interface
	IngestUDPListenPort    int               `json:"ingest_udp_listen_port"` // Main UDP port from ingester
	DbHost                 string            `json:"db_host"`
	DbPort                 int               `json:"db_port"`
	DbUser                 string            `json:"db_user"`
	DbPass                 string            `json:"db_pass"`
	DbName                 string            `json:"db_name"`
	ListenPort             int               `json:"listen_port"`
	SocketIOListen         int               `json:"socketio_listen"`
	Description            string            `json:"description"`
	Shards                 []int             `json:"shards"`
	Debug                  bool              `json:"debug"`
	ExternalLookup         string            `json:"external_lookup"`
	ExternalLookupTimeout  int               `json:"external_lookup_timeout"`
	MinimumDistance        float64           `json:"minimum_distance"`
	TimeFiltersRaw         map[string]string `json:"time_filters"`             // JSON holds strings ("1h", "30m", …)
	ReceiversBaseURL       string            `json:"receivers_base_url"`       // URL for fetching receiver data
	RedisHost              string            `json:"redis_host"`               // Redis server host
	RedisPort              int               `json:"redis_port"`               // Redis server port
	StoreAnonymousMessages bool              `json:"store_anonymous_messages"` // Whether to store messages from the main UDP port
}

var settings *Settings
var db *sql.DB
var redisClient *redis.Client
var ctx = context.Background()

// Map to store IP address to receiver ID mapping and receiver location data
var (
	receiverIPToIDMap   = make(map[string]int) // Map of IP to receiver ID (secondary)
	receiverPortToIDMap = make(map[int]int)    // Map of port to receiver ID (primary)
	receiverLocations   = make(map[int]struct {
		Latitude  float64
		Longitude float64
	})
	receiverMapMutex sync.RWMutex
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
	minimumDistance float64 // in meters, loaded from settings
)

// In-memory fallbacks when Redis is unavailable
var (
	lastPosMu     sync.Mutex
	lastPositions = make(map[int]map[int]Position) // userID → receiverID → last seen lat/lon

	lastNavStatusMu        sync.Mutex
	lastNavigationalStatus = make(map[int]float64) // userID → last NavigationalStatus

	// Track Redis availability to know when to sync in-memory data
	redisAvailableMu    sync.Mutex
	redisWasUnavailable bool = false
)

var (
	lastTimeMu   sync.Mutex
	lastTimeSeen = make(map[int]map[int]time.Time)
)

// Redis key prefixes
const (
	positionKeyPrefix  = "collector:position:" // Will be used as "collector:position:{shardID}:{userID}:{receiverID}"
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

	// Set default value for StoreAnonymousMessages if not specified
	if !strings.Contains(string(data), "store_anonymous_messages") {
		s.StoreAnonymousMessages = true
		log.Println("StoreAnonymousMessages not specified in settings.json, defaulting to true")
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
	UDPPort     int             `json:"udp_port"`
	DedupedPort *int            `json:"deduped_port,omitempty"`
	IsDuplicate bool            `json:"is_duplicate"`
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

	// Fetch UDP listen port from ingest_host:ingest_http_port/settings
	// Keep trying until successful
	if settings.IngestHost != "" && settings.IngestHTTPPort > 0 {
		fetchIngesterSettings(settings)
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

	// Log the discovered UDP port
	log.Printf("Ingester primary UDP port: %d", settings.IngestUDPListenPort)

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
	           distance INT,
	           udp_port INT,
	           receiver_id_duplicated INT
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
            receiver_id INT,
            udp_port INT
        );
    `)
	if err != nil {
		log.Fatal("Error creating state table: ", err)
	}

	// Create met_state table for meteorological data
	_, err = db.Exec(`
	       CREATE TABLE IF NOT EXISTS met_state (
	           user_id INT PRIMARY KEY,
	           receiver_id INT,
	           last_updated TIMESTAMP,
	           met_data JSONB
	       );
	   `)
	if err != nil {
		log.Fatal("Error creating met_state table: ", err)
	}

	// Create vessel_receivers table to track which receivers saw the latest message for each vessel
	_, err = db.Exec(`
	       CREATE TABLE IF NOT EXISTS vessel_receivers (
	           user_id INT PRIMARY KEY,
	           raw_sentence TEXT NOT NULL,
	           timestamp TIMESTAMP NOT NULL,
	           receiver_ids INT[] NOT NULL,
	           last_updated TIMESTAMP NOT NULL
	       );
	       CREATE INDEX IF NOT EXISTS idx_vessel_receivers_timestamp ON vessel_receivers(timestamp);
	   `)
	if err != nil {
		log.Fatal("Error creating vessel_receivers table: ", err)
	}

	createIndexesIfNotExist(db)

	// Create index for the udp_port column
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_udp_port ON messages(udp_port);`)
	if err != nil {
		log.Printf("Error creating index on messages.udp_port: %v", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_receiver_id_duplicated ON messages(receiver_id_duplicated);`)
	if err != nil {
		log.Printf("Error creating index on messages.receiver_id_duplicated: %v", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_state_udp_port ON state(udp_port);`)
	if err != nil {
		log.Printf("Error creating index on state.udp_port: %v", err)
	}

	// Create the ports_ips table to track IP addresses sending messages on UDP ports
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS ports_ips (
            id SERIAL PRIMARY KEY,
            ip_address VARCHAR(45) NOT NULL,
            udp_port INT NOT NULL,
            first_seen TIMESTAMP NOT NULL,
            last_seen TIMESTAMP NOT NULL,
            message_count INT DEFAULT 1,
            UNIQUE(ip_address, udp_port)
        );
        CREATE INDEX IF NOT EXISTS idx_ports_ips_ip ON ports_ips(ip_address);
        CREATE INDEX IF NOT EXISTS idx_ports_ips_port ON ports_ips(udp_port);
        CREATE INDEX IF NOT EXISTS idx_ports_ips_last_seen ON ports_ips(last_seen);
    `)
	if err != nil {
		log.Printf("Error creating ports_ips table: %v", err)
	}

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
		UDPPort   int     `json:"udp_port,omitempty"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	}

	if err := json.Unmarshal(body, &receivers); err != nil {
		log.Printf("Error unmarshalling receivers data: %v", err)
		return // Preserve previous mapping on error
	}

	// Create new maps to avoid partial updates
	newIPMap := make(map[string]int)
	newPortMap := make(map[int]int)
	newLocationMap := make(map[int]struct {
		Latitude  float64
		Longitude float64
	})

	for _, receiver := range receivers {
		// If UDP port is specified, create a port-to-ID mapping (primary)
		if receiver.UDPPort > 0 {
			newPortMap[receiver.UDPPort] = receiver.ID
		}

		// Always store the IP-only mapping as fallback (secondary)
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
	if len(receivers) == 0 {
		log.Printf("Received empty receivers list, preserving previous mapping")
		return // Preserve previous mapping if new data is empty
	}

	// Update the global maps atomically
	receiverMapMutex.Lock()
	receiverIPToIDMap = newIPMap
	receiverPortToIDMap = newPortMap
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

	http.HandleFunc("/portmetrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Parse the time parameter (in hours)
		timeParam := r.URL.Query().Get("time")
		timeHours := 24.0 // Default to 24 hours

		if timeParam != "" {
			parsedHours, err := strconv.ParseFloat(timeParam, 64)
			if err != nil {
				http.Error(w, "Invalid time parameter, must be a number of hours", http.StatusBadRequest)
				return
			}

			// Limit to 30 days (720 hours)
			if parsedHours > 720 {
				parsedHours = 720
			} else if parsedHours < 1 {
				parsedHours = 1 // Minimum 1 hour
			}

			timeHours = parsedHours
		}

		// Query the database for port metrics within the specified time window
		portMetrics, err := getPortMetrics(timeHours)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error retrieving port metrics: %v", err), http.StatusInternalServerError)
			return
		}

		// Return the results as JSON, adding the lastseen field for each port
		_ = json.NewEncoder(w).Encode(portMetrics)
	})

	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("HTTP server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// PortMetric represents a record from the ports_ips table
type PortMetric struct {
	IPAddress    string    `json:"ip_address"`
	UDPPort      int       `json:"udp_port"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int       `json:"message_count"`
}

// getPortMetrics retrieves port metrics from the database within the specified time window
func getPortMetrics(timeHours float64) (map[string]interface{}, error) {
	// Calculate the cutoff time
	cutoffTime := time.Now().UTC().Add(-time.Duration(timeHours) * time.Hour)

	// Query the database for metrics filtered by time
	rows, err := db.Query(`
        SELECT ip_address, udp_port, first_seen, last_seen, message_count
        FROM ports_ips
        WHERE last_seen >= $1
        ORDER BY last_seen DESC
    `, cutoffTime.Format(time.RFC3339))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Process the results
	var metrics []PortMetric
	for rows.Next() {
		var metric PortMetric
		var firstSeenStr, lastSeenStr string

		if err := rows.Scan(&metric.IPAddress, &metric.UDPPort, &firstSeenStr, &lastSeenStr, &metric.MessageCount); err != nil {
			return nil, err
		}

		// Parse the timestamps
		firstSeen, err := time.Parse(time.RFC3339, firstSeenStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing first_seen timestamp: %v", err)
		}
		metric.FirstSeen = firstSeen

		lastSeen, err := time.Parse(time.RFC3339, lastSeenStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing last_seen timestamp: %v", err)
		}
		metric.LastSeen = lastSeen

		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Query for the last seen time for each port (regardless of time parameter)
	lastSeenRows, err := db.Query(`
        SELECT udp_port, MAX(last_seen) as last_seen
        FROM ports_ips
        GROUP BY udp_port
        ORDER BY udp_port
    `)

	if err != nil {
		return nil, err
	}
	defer lastSeenRows.Close()

	// Process the last seen results
	portLastSeen := make(map[int]time.Time)
	for lastSeenRows.Next() {
		var port int
		var lastSeenStr string

		if err := lastSeenRows.Scan(&port, &lastSeenStr); err != nil {
			return nil, err
		}

		// Parse the timestamp
		lastSeen, err := time.Parse(time.RFC3339, lastSeenStr)
		if err != nil {
			return nil, fmt.Errorf("error parsing last_seen timestamp: %v", err)
		}

		portLastSeen[port] = lastSeen
	}

	if err := lastSeenRows.Err(); err != nil {
		return nil, err
	}

	// Create the response with both metrics and lastseen
	response := map[string]interface{}{
		"metrics":  metrics,
		"lastseen": portLastSeen,
	}

	return response, nil
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

	// Debug log to check if DedupedPort is being received
	if settings.Debug && msg.DedupedPort != nil {
		log.Printf("[DEBUG] Received message with DedupedPort=%d", *msg.DedupedPort)
		// Print the raw JSON to see what's actually being received
		log.Printf("[DEBUG] Raw message JSON: %s", string(message))
	}

	// 2) Persist to DB (includes msg.RawSentence)
	if err := storeMessage(db, msg, settings, msg.RawSentence); err != nil {
		log.Printf("[DEBUG] Error storing message: %v", err)
	}

	// Update the ports_ips table to track this IP/port combination
	// Only update if:
	// 1. The UDP port matches the ingester's port, OR
	// 2. There's a matching port for a receiver
	receiverMapMutex.RLock()
	isKnownPort := msg.UDPPort == settings.IngestUDPListenPort
	if !isKnownPort {
		_, isKnownPort = receiverPortToIDMap[msg.UDPPort]
	}
	receiverMapMutex.RUnlock()

	if isKnownPort {
		updatePortsIPs(db, msg.SourceIP, msg.UDPPort)
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

	// Look up receiver ID based on UDP port and source IP
	var receiverID interface{} = nil
	receiverMapMutex.RLock()

	// Special case: if the UDP port is the main ingest port, always set receiver_id to 0
	if msg.UDPPort == settings.IngestUDPListenPort {
		// For the main UDP port, always use receiver_id 0
		receiverID = 0
	} else if id, exists := receiverPortToIDMap[msg.UDPPort]; exists && msg.UDPPort > 0 {
		// For dedicated ports, use port as primary lookup method
		receiverID = id
	}
	// No IP fallback for non-primary ports

	receiverMapMutex.RUnlock()

	emitPayload := map[string]interface{}{
		"data":         packet,
		"type":         packet["MessageID"],
		"timestamp":    msg.Timestamp,
		"raw_sentence": msg.RawSentence,
		"receiver_id":  receiverID,
		"udp_port":     msg.UDPPort,
	}

	// 7) Emit to any subscribers (skip if message is marked as duplicate)
	if !msg.IsDuplicate {
		userSubscribersMu.RLock()
		subs := userSubscribers[uidKey]
		userSubscribersMu.RUnlock()
		for sid := range subs {
			if sock, ok := connectedClients[sid]; ok {
				sock.Emit("ais_data", emitPayload)
			}
		}
	} else if settings.Debug {
		log.Printf("[DEBUG] Skipping emit for duplicate message with UserID=%s", uidKey)
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

		// state table: filter by ais_class
		`CREATE INDEX IF NOT EXISTS idx_state_ais_class 
            ON state(ais_class);`,

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

		// Add index for the Type field to optimize the new type filter
		`CREATE INDEX IF NOT EXISTS idx_state_type
            ON state (CAST(packet->>'Type' AS FLOAT));`,

		// Indexes for time series statistics
		`CREATE INDEX IF NOT EXISTS idx_messages_timestamp
            ON messages(timestamp);`,

		`CREATE INDEX IF NOT EXISTS idx_messages_receiver_id
            ON messages(receiver_id);`,

		`CREATE INDEX IF NOT EXISTS idx_messages_message_id
            ON messages(message_id);`,

		// Combined index for the most common query pattern in time series statistics
		`CREATE INDEX IF NOT EXISTS idx_messages_timestamp_receiver_message
            ON messages(timestamp, receiver_id, message_id);`,

		// Index for efficient COUNT(DISTINCT user_id) operations
		`CREATE INDEX IF NOT EXISTS idx_messages_timestamp_receiver_user
            ON messages(timestamp, receiver_id, user_id);`,
	}

	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			log.Printf("Error creating index: %v", err)
		}
	}
}

// updateVesselReceivers updates the vessel_receivers table to track which receivers saw the latest message for each vessel
func updateVesselReceivers(db *sql.DB, userID int, rawSentence string, timestamp string, receiverID interface{}, isDuplicate bool, dedupedPort *int) error {
	// Convert receiverID to int, defaulting to 0 if nil
	var recID int
	if receiverID != nil {
		if id, ok := receiverID.(int); ok {
			recID = id
		}
	}

	// If this is not a duplicate message, create or update the entry with a new message
	if !isDuplicate {
		_, err := db.Exec(`
			INSERT INTO vessel_receivers (user_id, raw_sentence, timestamp, receiver_ids, last_updated)
			VALUES ($1, $2, $3, ARRAY[$4::integer], NOW())
			ON CONFLICT (user_id) DO UPDATE SET
				raw_sentence = EXCLUDED.raw_sentence,
				timestamp = EXCLUDED.timestamp,
				receiver_ids = ARRAY[$4::integer],  -- Reset the array with just this receiver
				last_updated = NOW()
		`, userID, rawSentence, timestamp, recID)
		return err
	} else {
		// For duplicates, trust the ingester's duplicate detection (dedupedPort)
		// and append this receiver to the array if not already present
		_, err := db.Exec(`
			UPDATE vessel_receivers
			SET receiver_ids = array_append(receiver_ids, $1::integer),
				last_updated = NOW()
			WHERE user_id = $2 AND NOT ($1::integer = ANY(receiver_ids))
		`, recID, userID)

		if err != nil {
			return err
		}

		return nil
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

	// 7) Look up receiver ID from source IP or port
	var receiverID interface{} = nil // Use nil (SQL NULL) as default
	receiverMapMutex.RLock()

	if message.UDPPort == settings.IngestUDPListenPort {
		// For the main UDP port, always use receiver_id 0
		receiverID = 0
	} else if id, exists := receiverPortToIDMap[message.UDPPort]; exists && message.UDPPort > 0 {
		// For dedicated ports, use port as primary lookup method
		receiverID = id
	}
	receiverMapMutex.RUnlock()

	// Look up receiver ID based on UDP port and source IP
	var portMatched, receiverFound bool

	receiverMapMutex.RLock()
	if message.UDPPort == settings.IngestUDPListenPort {
		// For the main UDP port, check StoreAnonymousMessages setting
		portMatched = true
		receiverID = 0
	} else if id, exists := receiverPortToIDMap[message.UDPPort]; exists && message.UDPPort > 0 {
		// For dedicated ports, always process
		receiverFound = true
		receiverID = id
	}
	receiverMapMutex.RUnlock()

	if _, isMovement := movementMsgTypes[mid]; isMovement {
		lat, lok := packetMap["Latitude"].(float64)
		lon, lok2 := packetMap["Longitude"].(float64)
		if !lok || !lok2 {
			shouldInsert = false
		} else {
			// Get receiverID as int for position tracking
			recID := 0
			if receiverID != nil {
				if rid, ok := receiverID.(int); ok {
					recID = rid
				}
			}

			// Create a key for this vessel's position in Redis that includes the receiver ID
			posKey := fmt.Sprintf("%s%d:%d:%d", positionKeyPrefix, message.ShardID, userID, recID)

			// Try to get previous position from Redis
			posData, err := redisClient.Get(ctx, posKey).Result()

			if err == redis.Nil {
				// Key doesn't exist - first time seeing this vessel in Redis

				// Check in-memory fallback
				lastPosMu.Lock()

				// Initialize the nested map if needed
				if _, exists := lastPositions[userID]; !exists {
					lastPositions[userID] = make(map[int]Position)
				}

				prevPos, seenBefore := lastPositions[userID][recID]

				if !seenBefore {
					// First time seeing this vessel from this receiver
					lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
				} else {
					// We've seen it in memory but not in Redis
					dist := haversine(prevPos.Lat, prevPos.Lon, lat, lon)
					if dist >= minimumDistance {
						lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
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

				// Initialize the nested map if needed
				if _, exists := lastPositions[userID]; !exists {
					lastPositions[userID] = make(map[int]Position)
				}

				// Get receiverID as int for position tracking
				recID := 0
				if receiverID != nil {
					if rid, ok := receiverID.(int); ok {
						recID = rid
					}
				}

				prevPos, seenBefore := lastPositions[userID][recID]
				if !seenBefore {
					lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
				} else {
					dist := haversine(prevPos.Lat, prevPos.Lon, lat, lon)
					if dist >= minimumDistance {
						lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
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
						// Get receiverID as int for position tracking
						recID := 0
						if receiverID != nil {
							if rid, ok := receiverID.(int); ok {
								recID = rid
							}
						}

						lastPosMu.Lock()
						// Initialize the nested map if needed
						if _, exists := lastPositions[userID]; !exists {
							lastPositions[userID] = make(map[int]Position)
						}
						lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
						lastPosMu.Unlock()
					} else {
						shouldInsert = false

						// Update in-memory even if unchanged
						// Get receiverID as int for position tracking
						recID := 0
						if receiverID != nil {
							if rid, ok := receiverID.(int); ok {
								recID = rid
							}
						}

						lastPosMu.Lock()
						// Initialize the nested map if needed
						if _, exists := lastPositions[userID]; !exists {
							lastPositions[userID] = make(map[int]Position)
						}
						lastPositions[userID][recID] = Position{Lat: lat, Lon: lon}
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

	// Note: Duplicate messages (DedupedPort != nil) now follow the same minimum_distance logic
	// No override for duplicate messages - they must meet the same distance criteria

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

	// receiverID has already been determined above

	// 8) Conditionally update the state table
	// Only update the state table if:
	// 1. The UDP port matches the ingester's port AND StoreAnonymousMessages is true, OR
	// 2. There's a matching port for a receiver
	// Only update the state table if this is not a duplicate message (DedupedPort is nil)
	if message.DedupedPort == nil && ((portMatched && settings.StoreAnonymousMessages) || receiverFound) {
		if err := storeState(db, packetJSON, message.ShardID, message.Timestamp, userID, messageIDf, message.SourceIP, receiverID, message.UDPPort); err != nil {
			log.Printf("Error storing state: %v", err)
		}
	} else if settings.Debug {
		if message.DedupedPort != nil {
			log.Printf("Skipping state update: Message is a duplicate (DedupedPort=%d)", *message.DedupedPort)
		} else {
			log.Printf("Skipping state update: UDP port %d doesn't match ingester port %d or StoreAnonymousMessages is false",
				message.UDPPort, settings.IngestUDPListenPort)
		}
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
		// receiverID was already set above, no need to look it up again

		// Only insert the message if:
		// 1. The UDP port matches the ingester's port AND StoreAnonymousMessages is true, OR
		// 2. There's a matching port for a receiver
		if (portMatched && settings.StoreAnonymousMessages) || receiverFound {
			if err := tryStoreMessage(db, packetJSON, message.ShardID, message.Timestamp, message.SourceIP, userIDf, messageIDf, rawSentence, receiverID, message.UDPPort, message.DedupedPort, settings); err != nil {
				log.Printf("Error storing message: %v", err)
			} else if message.DedupedPort != nil {
				// Log successful storage of duplicate message
				// log.Printf("DUPLICATE STORED: Successfully stored message with DedupedPort=%d", *message.DedupedPort)
			}

			// Update vessel_receivers table to track which receivers saw this message
			isDuplicate := message.DedupedPort != nil
			if err := updateVesselReceivers(db, userID, rawSentence, message.Timestamp, receiverID, isDuplicate, message.DedupedPort); err != nil {
				log.Printf("Error updating vessel receivers: %v", err)
			}
		} else if settings.Debug {
			log.Printf("Skipping message storage: UDP port %d doesn't match ingester port %d and no matching receiver found",
				message.UDPPort, settings.IngestUDPListenPort)
		}
	}

	return nil
}

func storeState(db *sql.DB, packetJSON []byte, shardID int, timestamp string, userID int, messageID float64, sourceIP string, receiverID interface{}, udpPort int) error {
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
          (packet, shard_id, timestamp, user_id, ais_class, count, image_url, name, ext_lookup_complete, source_ip, receiver_id, udp_port)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (user_id) DO UPDATE SET
          packet              = EXCLUDED.packet,
          shard_id            = EXCLUDED.shard_id,
          timestamp           = EXCLUDED.timestamp,
          ais_class           = EXCLUDED.ais_class,
          count               = state.count + 1,
          source_ip           = EXCLUDED.source_ip,
          receiver_id         = EXCLUDED.receiver_id,
          udp_port            = EXCLUDED.udp_port
    `, packetJSON, shardID, timestamp, userID, existingAisClass, existingCount, "", "", existingLookupComplete, sourceIP, receiverID, udpPort)
	if err != nil {
		return fmt.Errorf("Error inserting or updating state table: %v", err)
	}

	// Check if this is a meteorological message from an AtoN
	// Criteria: ais_class is AtoN, ApplicationID->Valid is true,
	// ApplicationID->DesignatedAreaCode is 1, ApplicationID->FunctionIdentifier is 31,
	// and DecodedBinary exists
	if existingAisClass == "AtoN" {
		var packet map[string]interface{}
		if err := json.Unmarshal(packetJSON, &packet); err == nil {
			// Check for ApplicationID
			if appID, ok := packet["ApplicationID"].(map[string]interface{}); ok {
				// Explicitly check if Valid is true
				if valid, ok := appID["Valid"].(bool); ok && valid {
					// Check for DesignatedAreaCode = 1
					if dac, ok := appID["DesignatedAreaCode"].(float64); ok && int(dac) == 1 {
						// Check for FunctionIdentifier = 31
						if fi, ok := appID["FunctionIdentifier"].(float64); ok && int(fi) == 31 {
							// Check for DecodedBinary
							if decodedBinary, ok := packet["DecodedBinary"].(map[string]interface{}); ok {
								// Convert DecodedBinary to JSON
								metDataJSON, err := json.Marshal(decodedBinary)
								if err == nil {
									// Insert or update met_state table
									_, err = db.Exec(`
                                        INSERT INTO met_state (user_id, receiver_id, last_updated, met_data)
                                        VALUES ($1, $2, $3, $4)
                                        ON CONFLICT (user_id) DO UPDATE SET
                                            receiver_id = EXCLUDED.receiver_id,
                                            last_updated = EXCLUDED.last_updated,
                                            met_data = EXCLUDED.met_data
                                    `, userID, receiverID, timestamp, metDataJSON)

									if err != nil && settings.Debug {
										log.Printf("Error inserting meteorological data: %v", err)
									} else if settings.Debug {
										log.Printf("Inserted meteorological data for user_id %d", userID)
									}
								}
							}
						}
					}
				}
			}
		}
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

func tryStoreMessage(db *sql.DB, packetJSON []byte, shardID int, timestamp string, sourceIP string, userID float64, messageID float64, rawSentence string, receiverID interface{}, udpPort int, dedupedPort *int, settings *Settings) error {
	// Calculate distance if this is a position message and we have receiver info
	var distance interface{} = nil // Default to SQL NULL
	if hasPositionData(int(messageID)) {
		if dist, ok := calculateDistance(packetJSON, receiverID); ok {
			distance = dist
		}
	}

	// Translate dedupedPort to receiver_id_duplicated if available
	var receiverIDDuplicated interface{} = nil // Default to SQL NULL
	if dedupedPort != nil {
		receiverMapMutex.RLock()
		if id, exists := receiverPortToIDMap[*dedupedPort]; exists && *dedupedPort > 0 {
			receiverIDDuplicated = id
			if settings.Debug {
				log.Printf("[DEBUG] Translated DedupedPort=%d to receiver_id_duplicated=%d", *dedupedPort, id)
			}
		} else {
			// If no mapping exists, set receiver_id_duplicated to 0
			receiverIDDuplicated = 0
			if settings.Debug {
				log.Printf("[DEBUG] Could not translate DedupedPort=%d to receiver_id_duplicated, setting to 0", *dedupedPort)
				// Print the current port-to-receiver mapping
				log.Printf("[DEBUG] Current port-to-receiver mapping: %v", receiverPortToIDMap)
			}
		}
		receiverMapMutex.RUnlock()
	}

	stmt := `INSERT INTO messages (packet, shard_id, timestamp, source_ip, user_id, message_id, raw_sentence, receiver_id, distance, udp_port, receiver_id_duplicated)
	            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	_, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID, rawSentence, receiverID, distance, udpPort, receiverIDDuplicated)
	if err != nil {
		log.Printf("Error executing query: %v", err)
		if isDatabaseConnectionError(err) {
			log.Println("Attempting to reconnect to the PostgreSQL database...")
			db, err = reconnectToDatabase(settings)
			if err != nil {
				log.Printf("Failed to reconnect to the database: %v", err)
				return err
			}
			_, err := db.Exec(stmt, packetJSON, shardID, timestamp, sourceIP, userID, messageID, rawSentence, receiverID, distance, udpPort, receiverIDDuplicated)
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

// fetchIngesterSettings attempts to fetch settings from the ingester
// It will keep retrying every 5 seconds until successful
func fetchIngesterSettings(settings *Settings) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	url := fmt.Sprintf("http://%s:%d/settings", settings.IngestHost, settings.IngestHTTPPort)

	for {
		if settings.Debug {
			log.Printf("Fetching UDP listen port from %s", url)
		}

		resp, err := client.Get(url)
		if err != nil {
			log.Printf("Failed to fetch settings from ingester: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Failed to fetch settings from ingester, status code: %d. Retrying in 5 seconds...", resp.StatusCode)
			resp.Body.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		var ingestSettings struct {
			UDPListenPort int `json:"udp_listen_port"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&ingestSettings); err != nil {
			log.Printf("Failed to parse settings from ingester: %v. Retrying in 5 seconds...", err)
			resp.Body.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		resp.Body.Close()

		if ingestSettings.UDPListenPort > 0 {
			if settings.Debug {
				log.Printf("Updated UDP listen port from %d to %d",
					settings.IngestUDPListenPort, ingestSettings.UDPListenPort)
			}
			settings.IngestUDPListenPort = ingestSettings.UDPListenPort
			log.Printf("Successfully fetched settings from ingester")
			return
		}

		log.Printf("Received invalid UDP listen port from ingester. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
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
	addr := fmt.Sprintf("%s:%d", settings.RedisHost, settings.RedisPort)
	log.Printf("Connecting to Redis at %s...", addr)

	redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Test Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("WARNING: Could not connect to Redis: %v", err)
		log.Println("Position and NavigationalStatus tracking will use in-memory fallback")
		log.Println("Data will be synced to Redis when connection is restored")
	} else {
		log.Println("Connected to Redis successfully")
	}
}

// Monitor Redis connection and attempt to reconnect if it fails
func monitorRedisConnection() {
	var redisConnected bool

	// Initial connection check
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		redisConnected = false
		log.Printf("Redis connection check: Not connected")
	} else {
		redisConnected = true
		log.Printf("Redis connection check: Connected")
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	log.Printf("Redis connection monitoring started (checking every 30 seconds)")

	for range ticker.C {
		// Check if Redis is connected
		_, err := redisClient.Ping(ctx).Result()

		if err != nil && redisConnected {
			// Connection was lost
			log.Printf("ALERT: Lost connection to Redis: %v", err)
			redisConnected = false
		} else if err == nil && !redisConnected {
			// Connection was restored
			log.Println("SUCCESS: Redis connection restored")
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
				log.Println("SUCCESS: Successfully reconnected to Redis")
				redisConnected = true

				// Check if we need to sync in-memory data to Redis
				redisAvailableMu.Lock()
				needsSync := redisWasUnavailable
				redisWasUnavailable = false
				redisAvailableMu.Unlock()

				if needsSync {
					go syncInMemoryToRedis()
				}
			} else {
				log.Printf("FAILED: Could not reconnect to Redis: %v", err)
			}
		} else if settings.Debug && redisConnected {
			// Still connected, log if in debug mode
			log.Printf("Redis connection: Healthy")
		}
	}
}

// Sync in-memory position and NavigationalStatus data to Redis
func syncInMemoryToRedis() {
	log.Println("Syncing in-memory data to Redis...")

	// Sync positions
	lastPosMu.Lock()
	posCount := 0
	for userID, receiverMap := range lastPositions {
		for recID, pos := range receiverMap {
			// For each shard (since we don't know which shard the userID belongs to)
			for _, shardID := range settings.Shards {
				posKey := fmt.Sprintf("%s%d:%d:%d", positionKeyPrefix, shardID, userID, recID)
				posJSON, _ := json.Marshal(pos)
				if err := redisClient.Set(ctx, posKey, posJSON, 0).Err(); err != nil {
					log.Printf("Warning: Failed to sync position for user %d, receiver %d to Redis: %v", userID, recID, err)
				} else {
					posCount++
				}
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
				log.Printf("Warning: Failed to sync NavigationalStatus for user %d to Redis: %v", userID, err)
			} else {
				navCount++
			}
		}
	}
	lastNavStatusMu.Unlock()

	log.Printf("Synced %d positions and %d NavigationalStatus values to Redis", posCount, navCount)
}

// updatePortsIPs updates the ports_ips table with the given IP address and UDP port
// It either inserts a new record or updates the last_seen timestamp and increments the message count
func updatePortsIPs(db *sql.DB, ipAddress string, udpPort int) {
	// Get current timestamp
	now := time.Now().UTC().Format(time.RFC3339)

	// Use an upsert (INSERT ... ON CONFLICT DO UPDATE) to handle both new and existing records
	stmt := `
        INSERT INTO ports_ips (ip_address, udp_port, first_seen, last_seen, message_count)
        VALUES ($1, $2, $3, $4, 1)
        ON CONFLICT (ip_address, udp_port) DO UPDATE SET
            last_seen = $4,
            message_count = ports_ips.message_count + 1
    `

	_, err := db.Exec(stmt, ipAddress, udpPort, now, now)
	if err != nil {
		log.Printf("Error updating ports_ips table: %v", err)
	}
}
