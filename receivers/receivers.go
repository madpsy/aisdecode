package main

import (
    "crypto/rand"
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "math/big"
    "net"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "sync"
    "time"

    _ "github.com/lib/pq"
)

type Settings struct {
    DbHost         string `json:"db_host"`
    DbPort         int    `json:"db_port"`
    DbUser         string `json:"db_user"`
    DbPass         string `json:"db_pass"`
    DbName         string `json:"db_name"`
    ListenPort     int    `json:"listen_port"`
    Debug          bool   `json:"debug"`
    MetricsBaseURL string `json:"metrics_base_url"`
    IngestHost     string `json:"ingest_host"`
    IngestHTTPPort int    `json:"ingest_http_port"`
}

type Receiver struct {
	ID          int        `json:"id"`
	LastUpdated time.Time  `json:"lastupdated"`
	Description string     `json:"description"`
	Latitude    float64    `json:"latitude"`
	Longitude   float64    `json:"longitude"`
	Name        string     `json:"name"`
	URL         *string    `json:"url,omitempty"`
	IPAddress   string     `json:"ip_address,omitempty"`
	Password    string     `json:"password"`
	Messages    int        `json:"messages"`
	UDPPort     *int       `json:"udp_port,omitempty"`
	MessageStats map[string]PortMetric `json:"message_stats"` // Added for admin endpoints
}

// PublicReceiver is used for public API responses without sensitive fields
type PublicReceiver struct {
    ID          int        `json:"id"`
    LastUpdated time.Time  `json:"lastupdated"`
    Description string     `json:"description"`
    Latitude    float64    `json:"latitude"`
    Longitude   float64    `json:"longitude"`
    Name        string     `json:"name"`
    URL         *string    `json:"url,omitempty"`
    Messages    int        `json:"messages"`
    UDPPort     *int       `json:"udp_port,omitempty"`
}

type ReceiverInput struct {
    Description string   `json:"description"`
    Latitude    float64  `json:"latitude"`
    Longitude   float64  `json:"longitude"`
    Name        string   `json:"name"`
    URL         *string  `json:"url,omitempty"`
    Password    *string  `json:"password,omitempty"`
    IPAddress   *string  `json:"ip_address,omitempty"`
}

type ReceiverPatch struct {
    Description *string   `json:"description,omitempty"`
    Latitude    *float64  `json:"latitude,omitempty"`
    Longitude   *float64  `json:"longitude,omitempty"`
    Name        *string   `json:"name,omitempty"`
    URL         *string   `json:"url,omitempty"`
    Password    *string   `json:"password,omitempty"`
    IPAddress   *string   `json:"ip_address,omitempty"`
}

var (
	db                *sql.DB
	settings          Settings
	udpDedicatedPorts string
	availablePorts    []int
	
	// New variables for collector tracking
	collectorsMutex   sync.RWMutex
	collectors        []Collector
	portMetricsMutex  sync.RWMutex
	portMetricsMap    map[string]map[int]PortMetric // Map of IP address to map of UDP port to PortMetric
)

// New types for collector tracking
type Collector struct {
	Description string `json:"description"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Shards      []int  `json:"shards"`
}

type CollectorsResponse struct {
	Clients          []Collector `json:"clients"`
	ConfiguredShards int         `json:"configured_shards"`
}

type PortMetric struct {
	IPAddress    string    `json:"ip_address"`
	UDPPort      int       `json:"udp_port"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int       `json:"message_count"`
}

// IngestSettings represents the settings returned by the ingester
type IngestSettings struct {
    UDPListenPort        int      `json:"udp_listen_port"`
    UDPDedicatedPorts    string   `json:"udp_dedicated_ports"`
    UDPDestinations      []interface{} `json:"udp_destinations"`
    MetricWindowSize     int      `json:"metric_window_size"`
    HTTPPort             int      `json:"http_port"`
    NumWorkers           int      `json:"num_workers"`
    DownsampleWindow     int      `json:"downsample_window"`
    DeduplicationWindow  int      `json:"deduplication_window_ms"`
    WebPath              string   `json:"web_path"`
    Debug                bool     `json:"debug"`
    IncludeSource        bool     `json:"include_source"`
    StreamPort           int      `json:"stream_port"`
    StreamShards         int      `json:"stream_shards"`
    MQTTServer           string   `json:"mqtt_server"`
    MQTTTLS              bool     `json:"mqtt_tls"`
    MQTTAuth             string   `json:"mqtt_auth"`
    MQTTTopic            string   `json:"mqtt_topic"`
    DownsampleMessageTypes []string `json:"downsample_message_types"`
    BlockedIPs           []string `json:"blocked_ips"`
    FailedDecodeLog      string   `json:"failed_decode_log"`
}

func getClientIP(r *http.Request) string {
    if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
        // take only the first IP in the list
        parts := strings.Split(xff, ",")
        return strings.TrimSpace(parts[0])
    }
    // fall back to RemoteAddr
    if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
        return host
    }
    return r.RemoteAddr
}

// fetchIngestSettings fetches settings from the ingester
func fetchIngestSettings() (*IngestSettings, error) {
    url := fmt.Sprintf("http://%s:%d/settings", settings.IngestHost, settings.IngestHTTPPort)
    resp, err := http.Get(url)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch settings from ingester: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("received non-OK response from ingester: %v", resp.Status)
    }

    var ingestSettings IngestSettings
    if err := json.NewDecoder(resp.Body).Decode(&ingestSettings); err != nil {
        return nil, fmt.Errorf("error decoding ingester settings: %v", err)
    }

    return &ingestSettings, nil
}

// parsePortRange parses a port range string like "9000-9999" into a slice of integers
func parsePortRange(portRange string) ([]int, error) {
    parts := strings.Split(portRange, "-")
    if len(parts) != 2 {
        return nil, fmt.Errorf("invalid port range format: %s", portRange)
    }

    start, err := strconv.Atoi(parts[0])
    if err != nil {
        return nil, fmt.Errorf("invalid start port: %v", err)
    }

    end, err := strconv.Atoi(parts[1])
    if err != nil {
        return nil, fmt.Errorf("invalid end port: %v", err)
    }

    if start > end {
        return nil, fmt.Errorf("start port cannot be greater than end port")
    }

    var ports []int
    for i := start; i <= end; i++ {
        ports = append(ports, i)
    }

    return ports, nil
}

// allocatePort allocates a random unused port from the available ports
func allocatePort(receiverID int) (int, error) {
    // Ensure database connection
    if err := ensureConnection(); err != nil {
        return 0, err
    }

    // Get an available port that's not already allocated
    var selectedPort int
    err := db.QueryRow(`
        SELECT udp_port FROM receiver_ports
        WHERE receiver_id IS NULL
        ORDER BY RANDOM()
        LIMIT 1
    `).Scan(&selectedPort)
    
    if err == sql.ErrNoRows {
        return 0, fmt.Errorf("no available ports")
    } else if err != nil {
        return 0, fmt.Errorf("failed to query available port: %v", err)
    }

    // Update the receiver_ports table
    _, err = db.Exec(`
        UPDATE receiver_ports
        SET receiver_id = $1, last_updated = NOW()
        WHERE udp_port = $2
    `, receiverID, selectedPort)
    if err != nil {
        return 0, fmt.Errorf("failed to update receiver_ports: %v", err)
    }

    return selectedPort, nil
}

// fetchCollectors fetches the list of collectors from the ingester
func fetchCollectors() ([]Collector, error) {
	url := fmt.Sprintf("http://%s:%d/clients", settings.IngestHost, settings.IngestHTTPPort)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch collectors from ingester: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK response from ingester: %v", resp.Status)
	}

	var collectorsResponse CollectorsResponse
	if err := json.NewDecoder(resp.Body).Decode(&collectorsResponse); err != nil {
		return nil, fmt.Errorf("error decoding collectors response: %v", err)
	}

	return collectorsResponse.Clients, nil
}

// fetchPortMetrics fetches port metrics from a collector
func fetchPortMetrics(collector Collector) ([]PortMetric, error) {
	url := fmt.Sprintf("http://%s:%d/portmetrics", collector.IP, collector.Port)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch port metrics from collector %s:%d: %v", collector.IP, collector.Port, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK response from collector %s:%d: %v", collector.IP, collector.Port, resp.Status)
	}

	var portMetrics []PortMetric
	if err := json.NewDecoder(resp.Body).Decode(&portMetrics); err != nil {
		return nil, fmt.Errorf("error decoding port metrics from collector %s:%d: %v", collector.IP, collector.Port, err)
	}

	return portMetrics, nil
}

// startCollectorTracking starts the background goroutine to track collectors
func startCollectorTracking() {
	// Initialize the port metrics map
	portMetricsMap = make(map[string]map[int]PortMetric)

	// Start the collector tracking goroutine
	go func() {
		for {
			// Fetch collectors
			newCollectors, err := fetchCollectors()
			if err != nil {
				log.Printf("Error fetching collectors: %v", err)
			} else {
				// Update collectors list
				collectorsMutex.Lock()
				collectors = newCollectors
				collectorsMutex.Unlock()

				// For each collector, fetch port metrics
				for _, collector := range newCollectors {
					go func(c Collector) {
						portMetrics, err := fetchPortMetrics(c)
						if err != nil {
							log.Printf("Error fetching port metrics from collector %s:%d: %v", c.IP, c.Port, err)
							return
						}

						// Update port metrics map
						portMetricsMutex.Lock()
						for _, metric := range portMetrics {
							// Create map for IP address if it doesn't exist
							if _, ok := portMetricsMap[metric.IPAddress]; !ok {
								portMetricsMap[metric.IPAddress] = make(map[int]PortMetric)
							}

							// Update or add port metric
							existingMetric, exists := portMetricsMap[metric.IPAddress][metric.UDPPort]
							if exists {
								// Update existing metric
								existingMetric.MessageCount += metric.MessageCount
								if metric.FirstSeen.Before(existingMetric.FirstSeen) {
									existingMetric.FirstSeen = metric.FirstSeen
								}
								if metric.LastSeen.After(existingMetric.LastSeen) {
									existingMetric.LastSeen = metric.LastSeen
								}
								portMetricsMap[metric.IPAddress][metric.UDPPort] = existingMetric
							} else {
								// Add new metric
								portMetricsMap[metric.IPAddress][metric.UDPPort] = metric
							}
						}
						portMetricsMutex.Unlock()
					}(collector)
				}
			}

			// Sleep for 5 seconds
			time.Sleep(5 * time.Second)
		}
	}()
}

// getMessagesByPort gets the message count for a specific UDP port from the port metrics map
// This ignores the receiver's IP address and only filters by UDP port
func getMessagesByPort(udpPort *int) (int, map[string]PortMetric) {
	portMetricsMutex.RLock()
	defer portMetricsMutex.RUnlock()

	totalMessages := 0
	messageStats := make(map[string]PortMetric)

	// If no UDP port is specified, return 0 and empty map
	if udpPort == nil {
		return 0, messageStats
	}

	// Iterate through all IP addresses and find metrics for the specified UDP port
	for ipAddress, portMap := range portMetricsMap {
		// Check if we have metrics for this UDP port
		if metric, ok := portMap[*udpPort]; ok {
			totalMessages += metric.MessageCount
			// Use just the IP address as the key since the port is already in the metric
			messageStats[ipAddress] = metric
		}
	}

	return totalMessages, messageStats
}

func main() {
    // Load settings.json
    data, err := ioutil.ReadFile("settings.json")
    if err != nil {
        log.Fatalf("Error reading settings.json: %v", err)
    }
    if err := json.Unmarshal(data, &settings); err != nil {
        log.Fatalf("Error parsing settings.json: %v", err)
    }

    // Connect to Postgres
    connStr := fmt.Sprintf(
        "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName,
    )
    db, err = sql.Open("postgres", connStr)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    // Ensure connection is alive initially
    if err = ensureConnection(); err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }

    // Fetch settings from ingester with retry
    var ingestSettings *IngestSettings
    for {
        ingestSettings, err = fetchIngestSettings()
        if err == nil {
            break
        }
        log.Printf("Failed to fetch settings from ingester: %v. Retrying in 5 seconds...", err)
        time.Sleep(5 * time.Second)
    }

    // Parse UDP dedicated ports
    udpDedicatedPorts = ingestSettings.UDPDedicatedPorts
    log.Printf("UDP dedicated ports: %s", udpDedicatedPorts)

    // Verify it's a range of ports
    var err2 error
    availablePorts, err2 = parsePortRange(udpDedicatedPorts)
    if err2 != nil {
        log.Fatalf("Invalid UDP dedicated ports format: %v", err2)
    }

    createSchema()

    // Start collector tracking
    startCollectorTracking()

    // Public API: GET /receivers and POST /addreceiver
    http.HandleFunc("/receivers", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        // public list: no IP
        handleListReceiversPublic(w, r)
    })
    
    // Public endpoint to add a new receiver
    http.HandleFunc("/addreceiver", handleAddReceiver)
    
    // Public endpoint to edit a receiver
    http.HandleFunc("/editreceiver", handleEditReceiver)
    
    // Public endpoint to update receiver IP address
    http.HandleFunc("/receiverip", handleReceiverIP)

    http.HandleFunc("/admin/getip", adminGetIPHandler)

    // Admin API: full CRUD under /admin/receivers
    http.HandleFunc("/admin/receivers", adminReceiversHandler)
    http.HandleFunc("/admin/receivers/", adminReceiverHandler)
    http.HandleFunc("/admin/receivers/regenerate-password/", adminRegeneratePasswordHandler)

    // Serve static files at /admin/
    http.Handle(
        "/admin/",
        http.StripPrefix("/admin/", http.FileServer(http.Dir("web"))),
    )

    addr := fmt.Sprintf(":%d", settings.ListenPort)
    log.Printf("Server listening on %s", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}

func createSchema() {
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS receivers (
            id SERIAL PRIMARY KEY,
            lastupdated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            description VARCHAR(30) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            name VARCHAR(15) NOT NULL UNIQUE,
            url TEXT,
            ip_address TEXT NOT NULL DEFAULT '',
            password VARCHAR(20) NOT NULL DEFAULT ''
        );
    `)
    if err != nil {
        log.Fatalf("Error creating receivers table: %v", err)
    }
    _, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receivers_id ON receivers(id);`)
    if err != nil {
        log.Fatalf("Error creating index: %v", err)
    }
    // Create a unique index on the name column if it doesn't exist
    _, err = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_receivers_name ON receivers(name);`)
    if err != nil {
        log.Fatalf("Error creating unique index on name: %v", err)
    }
    // Sync the SERIAL sequence to start at 1 to ensure the first receiver has ID 1
    _, err = db.Exec(`
        SELECT setval(
          pg_get_serial_sequence('receivers','id'),
          1,
          false
        );
    `)
    if err != nil {
        log.Fatalf("Error syncing receivers_id_seq: %v", err)
    }

    // Create receiver_ports table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS receiver_ports (
            udp_port INT PRIMARY KEY,
            receiver_id INT REFERENCES receivers(id) ON DELETE SET NULL,
            last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    `)
    if err != nil {
        log.Fatalf("Error creating receiver_ports table: %v", err)
    }

    // Create index on receiver_id
    _, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receiver_ports_receiver_id ON receiver_ports(receiver_id);`)
    if err != nil {
        log.Fatalf("Error creating index on receiver_id: %v", err)
    }

    // Populate receiver_ports table with available ports
    if len(availablePorts) > 0 {
        // First, check which ports already exist in the table
        rows, err := db.Query(`SELECT udp_port FROM receiver_ports`)
        if err != nil {
            log.Fatalf("Error querying receiver_ports: %v", err)
        }
        defer rows.Close()

        existingPorts := make(map[int]bool)
        for rows.Next() {
            var port int
            if err := rows.Scan(&port); err != nil {
                log.Fatalf("Error scanning port: %v", err)
            }
            existingPorts[port] = true
        }

        // Insert any ports that don't already exist
        for _, port := range availablePorts {
            if !existingPorts[port] {
                _, err := db.Exec(`
                    INSERT INTO receiver_ports (udp_port)
                    VALUES ($1)
                `, port)
                if err != nil {
                    log.Fatalf("Error inserting port %d: %v", port, err)
                }
            }
        }
    }
}

// Ensure connection is alive, attempt to reconnect if needed
func ensureConnection() error {
    if err := db.Ping(); err != nil {
        log.Println("Database connection lost, attempting to reconnect...")
        db, err = sql.Open("postgres", fmt.Sprintf(
            "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
            settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName,
        ))
        if err != nil {
            return fmt.Errorf("failed to reconnect to database: %v", err)
        }
        if err := db.Ping(); err != nil {
            return fmt.Errorf("unable to verify reconnected database: %v", err)
        }
        log.Println("Reconnected to the database successfully.")
    }
    return nil
}

// Helper function to build the query and fetch filtered receivers based on id and ip_address.
func getFilteredReceivers(w http.ResponseWriter, filters map[string]string) ([]Receiver, error) {
    if err := ensureConnection(); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return []Receiver{}, nil // Return empty slice here instead of `nil`
    }

    // Extract parameters
    idParam := filters["id"]
    ipParam := filters["ipaddress"]

    // Base query
    baseQuery := `
        SELECT r.id,
               r.lastupdated,
               r.description,
               r.latitude,
               r.longitude,
               r.name,
               r.url,
               r.ip_address,
               r.password,
               rp.udp_port
          FROM receivers r
          LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id`
    
    var (
        conditions []string
        args       []interface{}
    )
    idx := 1
    if idParam != "" {
        // Validate and add id filter
        idVal, err := strconv.Atoi(idParam)
        if err != nil {
            return nil, fmt.Errorf("invalid id parameter")
        }
        conditions = append(conditions, fmt.Sprintf("id = $%d", idx))
        args = append(args, idVal)
        idx++
    }
    if ipParam != "" {
        // Add ip_address filter
        conditions = append(conditions, fmt.Sprintf("ip_address = $%d", idx))
        args = append(args, ipParam)
        idx++
    }

    // Add WHERE clause if filters are provided
    if len(conditions) > 0 {
        baseQuery += " WHERE " + strings.Join(conditions, " AND ")
    }
    baseQuery += " ORDER BY id"

    // Execute query
    rows, err := db.Query(baseQuery, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    // Scan results
    var list []Receiver
    for rows.Next() {
        var rec Receiver
        // Create a nullable int for UDP port
        var udpPort sql.NullInt64
        
        if err := rows.Scan(
            &rec.ID,
            &rec.LastUpdated,
            &rec.Description,
            &rec.Latitude,
            &rec.Longitude,
            &rec.Name,
            &rec.URL,
            &rec.IPAddress,  // Ensure this is correctly mapped
            &rec.Password,   // Add password field
            &udpPort,        // Scan into nullable int
        ); err != nil {
            return nil, err
        }
        
        // Only set UDPPort if it's not null
        if udpPort.Valid {
            port := int(udpPort.Int64)
            rec.UDPPort = &port
        }

        list = append(list, rec)
    }

    // Return empty list if no receivers are found
    if len(list) == 0 {
        return []Receiver{}, nil // Return empty slice explicitly
    }

    return list, nil
}

// adminGetIPHandler handles GET /admin/getip?id=<id>
func adminGetIPHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // 1) Parse and validate 'id' query parameter
    idStr := r.URL.Query().Get("id")
    if idStr == "" {
        http.Error(w, "`id` query parameter is required", http.StatusBadRequest)
        return
    }
    id, err := strconv.Atoi(idStr)
    if err != nil || id < 1 {
        http.Error(w, "`id` must be a positive integer", http.StatusBadRequest)
        return
    }

    // 2) Ensure DB connection
    if err := ensureConnection(); err != nil {
        http.Error(w, "database error", http.StatusInternalServerError)
        return
    }

    // 3) Query the ip_address column
    var ip sql.NullString
    err = db.
        QueryRow(`SELECT ip_address FROM receivers WHERE id = $1`, id).
        Scan(&ip)
    if err == sql.ErrNoRows {
        // No matching receiver → return empty string
        ip.String = ""
    } else if err != nil {
        // Some other DB error
        http.Error(w, "database error", http.StatusInternalServerError)
        return
    }

    // 4) Always return a string (never null)
    addr := ip.String
    if addr == "" {
        // normalize NULL or empty column to the empty string
        addr = ""
    }

    // 5) Write response
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "ip_address": addr,
    })
}

// Function to get the message count for a given IP address from the metrics API.
type SimpleMetrics struct {
    Messages int `json:"messages"` // Messages field inside simple_metrics
}

type MetricsResponse struct {
    SimpleMetrics SimpleMetrics `json:"simple_metrics"` // This is where the simple_metrics field is mapped
}

func getMessagesByIP(ipAddress string, udpPort *int) (int, error) {
    // Use the new port metrics tracking system instead of the metrics API
    // We only care about the UDP port, not the IP address
    messages, _ := getMessagesByPort(udpPort)
    return messages, nil
}

// --- Public listing only ---
func handleListReceivers(w http.ResponseWriter, r *http.Request) {
    // Ensure the connection is alive before performing the query
    if err := ensureConnection(); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    rows, err := db.Query(`
        SELECT id, lastupdated, description, latitude, longitude, name, url
        FROM receivers ORDER BY id
    `)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    var list []Receiver
    for rows.Next() {
        var rec Receiver
        if err := rows.Scan(
            &rec.ID, &rec.LastUpdated, &rec.Description,
            &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL,
        ); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        list = append(list, rec)
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(list)
}

// --- Admin handlers (full CRUD) ---
func adminReceiversHandler(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodGet:
        handleListReceiversAdmin(w, r)
    case http.MethodPost:
        handleCreateReceiver(w, r)
    default:
        w.WriteHeader(http.StatusMethodNotAllowed)
    }
}

// Public list: exactly as before, but *without* ip_address
func handleListReceiversPublic(w http.ResponseWriter, r *http.Request) {
    // Ensure the connection is alive before performing the query
    if err := ensureConnection(); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Extract query parameters
    idParam := r.URL.Query().Get("id")
    ipParam := r.URL.Query().Get("ipaddress")

    // Build the query based on filters
    baseQuery := `
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address
        FROM receivers
        WHERE 1=1
    `

    var conditions []string
    var args []interface{}
    idx := 1

    if idParam != "" {
        // Validate and add id filter
        idVal, err := strconv.Atoi(idParam)
        if err != nil {
            http.Error(w, "Invalid id parameter", http.StatusBadRequest)
            return
        }
        conditions = append(conditions, fmt.Sprintf("id = $%d", idx))
        args = append(args, idVal)
        idx++
    }

    if ipParam != "" {
        // Add ip_address filter
        conditions = append(conditions, fmt.Sprintf("ip_address = $%d", idx))
        args = append(args, ipParam)
        idx++
    }

    if len(conditions) > 0 {
        baseQuery += " AND " + strings.Join(conditions, " AND ")
    }

    baseQuery += " ORDER BY id"

    // Execute the query with the parameters
    rows, err := db.Query(baseQuery, args...)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    var list []PublicReceiver
    for rows.Next() {
        var rec Receiver
        if err := rows.Scan(
            &rec.ID, &rec.LastUpdated, &rec.Description,
            &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.IPAddress,
        ); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        // Fetch message count (0 on error)
        msgs, err := getMessagesByIP(rec.IPAddress, rec.UDPPort)
        if err != nil {
            msgs = 0
        }

        // Convert to PublicReceiver (which doesn't include password or IP address)
        publicRec := PublicReceiver{
            ID:          rec.ID,
            LastUpdated: rec.LastUpdated,
            Description: rec.Description,
            Latitude:    rec.Latitude,
            Longitude:   rec.Longitude,
            Name:        rec.Name,
            URL:         rec.URL,
            Messages:    msgs,
        }

        list = append(list, publicRec)
    }

    // If no receivers are found, return an empty array instead of null
    if len(list) == 0 {
        w.Header().Set("Content-Type", "application/json")
        w.Write([]byte("[]")) // Send empty array explicitly
        return
    }

    // Return the list of receivers in JSON format
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(list)
}

// Admin list: same as public but includes ip_address
func handleListReceiversAdmin(w http.ResponseWriter, r *http.Request) {
    // Parse filters from query parameters (can be the same as public)
    filters := map[string]string{
        "id":        r.URL.Query().Get("id"),
        "ipaddress": r.URL.Query().Get("ipaddress"),
    }

    // Call the helper function to get the filtered receivers
    list, err := getFilteredReceivers(w, filters)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // For each receiver in the list, fetch messages based on the ip_address
    for i, rec := range list {
        // Fetch messages count and message stats for each receiver based on ip_address and udp_port
        if rec.IPAddress == "" {
            log.Printf("Error: IP Address is empty for Receiver ID: %d", rec.ID)
        }

        msgs, messageStats := getMessagesByPort(rec.UDPPort)

        // Set the messages field and message stats
        list[i].Messages = msgs
        list[i].MessageStats = messageStats
    }

    // Add dummy entry for receiver ID 0
    // Fetch the UDP listen port from the ingester settings
    ingestSettings, err := fetchIngestSettings()
    var udpListenPort int
    if err != nil {
        log.Printf("Error fetching ingester settings: %v", err)
        udpListenPort = 0 // Default to 0 if we can't fetch the settings
    } else {
        udpListenPort = ingestSettings.UDPListenPort
    }

    // Create dummy receiver
    dummyReceiver := Receiver{
        ID:          0,
        LastUpdated: time.Now(),
        Description: "Anonymous",
        Latitude:    0,
        Longitude:   0,
        Name:        "Anonymous",
        URL:         nil,
        IPAddress:   "255.255.255.255",
        Password:    "",
        Messages:    0,
        MessageStats: make(map[string]PortMetric),
    }
    
    // Set the UDP port
    dummyReceiver.UDPPort = &udpListenPort
    
    // Fetch message stats for the dummy receiver based on the UDP listen port
    // This will aggregate all messages for this UDP port across all IP addresses
    msgs, messageStats := getMessagesByPort(&udpListenPort)
    dummyReceiver.Messages = msgs
    dummyReceiver.MessageStats = messageStats

    // Add the dummy receiver to the beginning of the list
    list = append([]Receiver{dummyReceiver}, list...)

    // Return the list of receivers in JSON format, including ip_address, messages, and message stats
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(list)
}

func adminReceiverHandler(w http.ResponseWriter, r *http.Request) {
    parts := strings.Split(r.URL.Path, "/")
    if len(parts) < 4 {
        w.WriteHeader(http.StatusBadRequest)
        return
    }
    id, err := strconv.Atoi(parts[3])
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        return
    }
    switch r.Method {
    case http.MethodGet:
        handleGetReceiver(w, r, id)
    case http.MethodPut:
        handlePutReceiver(w, r, id)
    case http.MethodPatch:
        handlePatchReceiver(w, r, id)
    case http.MethodDelete:
        handleDeleteReceiver(w, r, id)
    default:
        w.WriteHeader(http.StatusMethodNotAllowed)
    }
}

// generateRandomPassword creates a random 8-character password
func generateRandomPassword() (string, error) {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    password := make([]byte, 8)
    
    for i := range password {
        n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
        if err != nil {
            return "", err
        }
        password[i] = charset[n.Int64()]
    }
    
    return string(password), nil
}

func handleCreateReceiver(w http.ResponseWriter, r *http.Request) {
    // 1) decode JSON body
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 2) Generate a random password if not provided
    password := ""
    if input.Password != nil {
        password = *input.Password
    } else {
        var err error
        password, err = generateRandomPassword()
        if err != nil {
            http.Error(w, "Failed to generate password", http.StatusInternalServerError)
            return
        }
    }

    // 3) Use provided IP address or empty string if not provided
    ipAddress := ""
    if input.IPAddress != nil {
        ipAddress = *input.IPAddress
    }

    // 4) build Receiver and validate
    rec := Receiver{
        Description: input.Description,
        Latitude:    input.Latitude,
        Longitude:   input.Longitude,
        Name:        strings.ToUpper(input.Name),
        URL:         input.URL,
        Password:    password,
        IPAddress:   ipAddress,
    }
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 5) INSERT including ip_address and password
    err := db.QueryRow(`
        INSERT INTO receivers (
            description,
            latitude,
            longitude,
            name,
            url,
            ip_address,
            password
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id, lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.IPAddress, rec.Password).
        Scan(&rec.ID, &rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 6) Allocate a UDP port for this receiver
    udpPort, err := allocatePort(rec.ID)
    if err != nil {
        if err.Error() == "no available ports" {
            log.Printf("ERROR: No available UDP ports found for receiver ID %d", rec.ID)
            http.Error(w, "Failed to create receiver: No available UDP ports", http.StatusServiceUnavailable)
        } else {
            log.Printf("ERROR: Failed to allocate UDP port for receiver ID %d: %v", rec.ID, err)
            http.Error(w, fmt.Sprintf("Failed to create receiver: UDP port allocation error: %v", err), http.StatusInternalServerError)
        }
        
        // Clean up the created receiver since we couldn't allocate a port
        _, cleanupErr := db.Exec(`DELETE FROM receivers WHERE id = $1`, rec.ID)
        if cleanupErr != nil {
            log.Printf("ERROR: Failed to clean up receiver %d after port allocation failure: %v", rec.ID, cleanupErr)
        }
        
        return
    }
    
    rec.UDPPort = &udpPort

    // 7) send response
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}

func handleGetReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var rec Receiver
    // Query the receiver from the database.
    // Create a nullable int for UDP port
    var udpPort sql.NullInt64
    
    err := db.QueryRow(`
        SELECT r.id, r.lastupdated, r.description, r.latitude, r.longitude, r.name, r.url, r.ip_address, r.password, rp.udp_port
        FROM receivers r
        LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
        WHERE r.id = $1
    `, id).Scan(
        &rec.ID, &rec.LastUpdated, &rec.Description,
        &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.IPAddress, &rec.Password, &udpPort,
    )
    
    // Only set UDPPort if it's not null
    if udpPort.Valid {
        port := int(udpPort.Int64)
        rec.UDPPort = &port
    }
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Now, fetch the number of messages and message stats for the receiver's UDP port.
    messages, messageStats := getMessagesByPort(rec.UDPPort)

    // Add the messages field and message stats to the receiver struct.
    rec.Messages = messages
    rec.MessageStats = messageStats

    // Send the full receiver response with messages count and message map.
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handlePutReceiver(w http.ResponseWriter, r *http.Request, id int) {
    // 1) decode JSON body
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 2) Get current password and IP address if not provided in the input
    var password string
    var ipAddress string
    
    if input.Password != nil {
        password = *input.Password
    } else {
        // Fetch the current password from the database
        err := db.QueryRow(`SELECT password FROM receivers WHERE id = $1`, id).Scan(&password)
        if err != nil && err != sql.ErrNoRows {
            http.Error(w, "Failed to retrieve current password", http.StatusInternalServerError)
            return
        }
    }
    
    if input.IPAddress != nil {
        ipAddress = *input.IPAddress
    } else {
        // Fetch the current IP address from the database
        err := db.QueryRow(`SELECT ip_address FROM receivers WHERE id = $1`, id).Scan(&ipAddress)
        if err != nil && err != sql.ErrNoRows {
            http.Error(w, "Failed to retrieve current IP address", http.StatusInternalServerError)
            return
        }
    }

    // 3) build Receiver and validate
    rec := Receiver{
        ID:          id,
        Description: input.Description,
        Latitude:    input.Latitude,
        Longitude:   input.Longitude,
        Name:        strings.ToUpper(input.Name),
        URL:         input.URL,
        Password:    password,
        IPAddress:   ipAddress,
    }
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 4) UPSERT including ip_address and password
    err := db.QueryRow(`
        INSERT INTO receivers (
            id,
            description,
            latitude,
            longitude,
            name,
            url,
            ip_address,
            password
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (id) DO UPDATE
          SET description = EXCLUDED.description,
              latitude    = EXCLUDED.latitude,
              longitude   = EXCLUDED.longitude,
              name        = EXCLUDED.name,
              url         = EXCLUDED.url,
              ip_address  = EXCLUDED.ip_address,
              password    = EXCLUDED.password,
              lastupdated = NOW()
        RETURNING lastupdated
    `, rec.ID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.IPAddress, rec.Password).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 6) send response
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handlePatchReceiver(w http.ResponseWriter, r *http.Request, id int) {
    // 1) decode JSON body
    var patch ReceiverPatch
    if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 3) load existing record
    var rec Receiver
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address, password
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID,
        &rec.LastUpdated,
        &rec.Description,
        &rec.Latitude,
        &rec.Longitude,
        &rec.Name,
        &rec.URL,
        &rec.IPAddress,
        &rec.Password,
    )
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 4) apply patch
    if patch.Description != nil {
        rec.Description = *patch.Description
    }
    if patch.Latitude != nil {
        rec.Latitude = *patch.Latitude
    }
    if patch.Longitude != nil {
        rec.Longitude = *patch.Longitude
    }
    
    if patch.Name != nil {
        rec.Name = strings.ToUpper(*patch.Name)
    }
    if patch.URL != nil {
        rec.URL = patch.URL
    }
    if patch.Password != nil {
        rec.Password = *patch.Password
    }
    if patch.IPAddress != nil {
        rec.IPAddress = *patch.IPAddress
    }

    // 5) validate updated rec
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 6) perform UPDATE including ip_address
    err = db.QueryRow(`
        UPDATE receivers
           SET description  = $1,
               latitude     = $2,
               longitude    = $3,
               name         = $4,
               url          = $5,
               ip_address   = $6,
               password     = $7,
               lastupdated  = NOW()
         WHERE id = $8
         RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.IPAddress, rec.Password, rec.ID).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 8) send response
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handleDeleteReceiver(w http.ResponseWriter, r *http.Request, id int) {
    // Begin a transaction
    tx, err := db.Begin()
    if err != nil {
        http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
        return
    }
    defer tx.Rollback() // Rollback if not committed

    // First, unallocate the UDP port by setting receiver_id to NULL
    _, err = tx.Exec(`
        UPDATE receiver_ports
        SET receiver_id = NULL, last_updated = NOW()
        WHERE receiver_id = $1
    `, id)
    if err != nil {
        http.Error(w, fmt.Sprintf("Failed to unallocate UDP port: %v", err), http.StatusInternalServerError)
        return
    }

    // Then delete the receiver
    res, err := tx.Exec(`DELETE FROM receivers WHERE id = $1`, id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    
    n, _ := res.RowsAffected()
    if n == 0 {
        w.WriteHeader(http.StatusNotFound)
        return
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        http.Error(w, fmt.Sprintf("Failed to commit transaction: %v", err), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusNoContent)
}

func validateReceiver(r Receiver) error {
    if len(r.Description) > 30 {
        return fmt.Errorf("description must be ≤30 characters")
    }
    if len(r.Name) > 15 {
        return fmt.Errorf("name must be ≤15 characters")
    }
    if r.Latitude < -90 || r.Latitude > 90 {
        return fmt.Errorf("latitude must be between -90 and 90")
    }
    if r.Longitude < -180 || r.Longitude > 180 {
        return fmt.Errorf("longitude must be between -180 and 180")
    }
    if r.URL != nil && *r.URL != "" {
        if _, err := url.ParseRequestURI(*r.URL); err != nil {
            return fmt.Errorf("invalid URL")
        }
    }
    
    // Validate password length
    if len(r.Password) < 8 {
        return fmt.Errorf("password must be at least 8 characters")
    }
    if len(r.Password) > 20 {
        return fmt.Errorf("password must be no more than 20 characters")
    }
    
    // Check if the name is already in use by another receiver
    var count int
    query := `SELECT COUNT(*) FROM receivers WHERE name = $1`
    args := []interface{}{r.Name}
    
    // If we're updating an existing receiver, exclude it from the check
    if r.ID > 0 {
        query += ` AND id != $2`
        args = append(args, r.ID)
    }
    
    err := db.QueryRow(query, args...).Scan(&count)
    if err != nil {
        return fmt.Errorf("database error while checking name uniqueness: %v", err)
    }
    
    if count > 0 {
        return fmt.Errorf("name '%s' is already in use by another receiver", r.Name)
    }
    
    return nil
}

// adminRegeneratePasswordHandler handles POST /admin/receivers/regenerate-password/{id}
func adminRegeneratePasswordHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // Extract receiver ID from URL
    parts := strings.Split(r.URL.Path, "/")
    if len(parts) < 5 {
        http.Error(w, "Invalid URL format", http.StatusBadRequest)
        return
    }
    
    id, err := strconv.Atoi(parts[4])
    if err != nil {
        http.Error(w, "Invalid receiver ID", http.StatusBadRequest)
        return
    }

    // Generate a new random password
    newPassword, err := generateRandomPassword()
    if err != nil {
        http.Error(w, "Failed to generate password", http.StatusInternalServerError)
        return
    }
    
    // Fetch the current receiver to validate with the new password
    var rec Receiver
    err = db.QueryRow(`
        SELECT id, description, latitude, longitude, name, url, ip_address
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID,
        &rec.Description,
        &rec.Latitude,
        &rec.Longitude,
        &rec.Name,
        &rec.URL,
        &rec.IPAddress,
    )
    
    if err == sql.ErrNoRows {
        http.Error(w, "Receiver not found", http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, "Database error", http.StatusInternalServerError)
        return
    }
    
    // Set the new password and validate
    rec.Password = newPassword
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // Update the password in the database
    var lastUpdated time.Time
    err = db.QueryRow(`
        UPDATE receivers
        SET password = $1, lastupdated = NOW()
        WHERE id = $2
        RETURNING lastupdated
    `, newPassword, id).Scan(&lastUpdated)

    if err == sql.ErrNoRows {
        http.Error(w, "Receiver not found", http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, "Database error", http.StatusInternalServerError)
        return
    }

    // Return the new password
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "password": newPassword,
    })
}

// handleReceiverIP handles POST /receiverip
// This endpoint updates the IP address of a receiver
// It requires id and password, with optional ip_address
// If ip_address is not provided, it uses the client's IP
func handleReceiverIP(w http.ResponseWriter, r *http.Request) {
    // Only allow POST method
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // Parse JSON request body
    var input struct {
        ID        int     `json:"id"`
        Password  string  `json:"password"`
        IPAddress *string `json:"ip_address,omitempty"`
    }

    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }

    // Validate required fields
    if input.ID <= 0 {
        http.Error(w, "id is required and must be positive", http.StatusBadRequest)
        return
    }
    if input.Password == "" {
        http.Error(w, "password is required", http.StatusBadRequest)
        return
    }

    // Ensure database connection
    if err := ensureConnection(); err != nil {
        http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Verify the receiver exists and password is correct
    var storedPassword string
    err := db.QueryRow(`SELECT password FROM receivers WHERE id = $1`, input.ID).Scan(&storedPassword)
    if err == sql.ErrNoRows {
        http.Error(w, "Receiver not found", http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Check if password matches
    if input.Password != storedPassword {
        http.Error(w, "Invalid password", http.StatusUnauthorized)
        return
    }

    // Determine the IP address to use
    var ipAddress string
    if input.IPAddress != nil && *input.IPAddress != "" {
        // Use provided IP address
        ipAddress = *input.IPAddress
        
        // Validate IP address format
        if net.ParseIP(ipAddress) == nil {
            http.Error(w, "Invalid IP address format", http.StatusBadRequest)
            return
        }
    } else {
        // Use client's IP address
        ipAddress = getClientIP(r)
    }

    // Update the receiver's IP address
    var lastUpdated time.Time
    err = db.QueryRow(`
        UPDATE receivers
        SET ip_address = $1, lastupdated = NOW()
        WHERE id = $2
        RETURNING lastupdated
    `, ipAddress, input.ID).Scan(&lastUpdated)

    if err != nil {
        http.Error(w, "Failed to update IP address: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Return success response
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "id": input.ID,
        "ip_address": ipAddress,
        "updated_at": lastUpdated,
    })
}

// handleEditReceiver handles POST /editreceiver
// This is a public endpoint that allows editing an existing receiver
// It requires id and password for authentication
// Users can update all fields including their own password
func handleEditReceiver(w http.ResponseWriter, r *http.Request) {
    // Only allow POST method
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // Parse JSON request body
    var input struct {
        ID          int       `json:"id"`
        Password    string    `json:"password"`
        Description *string   `json:"description,omitempty"`
        Latitude    *float64  `json:"latitude,omitempty"`
        Longitude   *float64  `json:"longitude,omitempty"`
        Name        *string   `json:"name,omitempty"`
        URL         *string   `json:"url,omitempty"`
        IPAddress   *string   `json:"ip_address,omitempty"`
        NewPassword *string   `json:"new_password,omitempty"`
    }

    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }

    // Validate required fields
    if input.ID <= 0 {
        http.Error(w, "id is required and must be positive", http.StatusBadRequest)
        return
    }
    if input.Password == "" {
        http.Error(w, "password is required", http.StatusBadRequest)
        return
    }

    // Ensure database connection
    if err := ensureConnection(); err != nil {
        http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Fetch the current receiver to verify password and get existing values
    var rec Receiver
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address, password
        FROM receivers WHERE id = $1
    `, input.ID).Scan(
        &rec.ID,
        &rec.LastUpdated,
        &rec.Description,
        &rec.Latitude,
        &rec.Longitude,
        &rec.Name,
        &rec.URL,
        &rec.IPAddress,
        &rec.Password,
    )
    if err == sql.ErrNoRows {
        http.Error(w, "Receiver not found", http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Check if password matches
    if input.Password != rec.Password {
        http.Error(w, "Invalid password", http.StatusUnauthorized)
        return
    }

    // Apply updates to the receiver
    if input.Description != nil {
        rec.Description = *input.Description
    }
    if input.Latitude != nil {
        rec.Latitude = *input.Latitude
    }
    if input.Longitude != nil {
        rec.Longitude = *input.Longitude
    }
    if input.Name != nil {
        rec.Name = strings.ToUpper(*input.Name)
    }
    if input.URL != nil {
        rec.URL = input.URL
    }
    if input.IPAddress != nil {
        rec.IPAddress = *input.IPAddress
    }
    if input.NewPassword != nil {
        rec.Password = *input.NewPassword
    }

    // Validate the updated receiver
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // Update the receiver in the database
    err = db.QueryRow(`
        UPDATE receivers
        SET description  = $1,
            latitude     = $2,
            longitude    = $3,
            name         = $4,
            url          = $5,
            ip_address   = $6,
            password     = $7,
            lastupdated  = NOW()
        WHERE id = $8
        RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.IPAddress, rec.Password, rec.ID).
        Scan(&rec.LastUpdated)
    
    if err != nil {
        // Check for name uniqueness violation
        if strings.Contains(err.Error(), "unique constraint") && strings.Contains(err.Error(), "idx_receivers_name") {
            http.Error(w, fmt.Sprintf("name '%s' is already in use by another receiver", rec.Name), http.StatusBadRequest)
            return
        }
        http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Get message count and message stats for the updated receiver
    messages, messageStats := getMessagesByPort(rec.UDPPort)
    rec.Messages = messages
    rec.MessageStats = messageStats

    // Return the updated receiver
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

// handleAddReceiver handles POST /addreceiver
// This is a public endpoint that allows adding a new receiver
// It requires name, description, lat, long, and ipaddress
// URL is optional, and password is automatically generated
func handleAddReceiver(w http.ResponseWriter, r *http.Request) {
    // Only allow POST method
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }
    
    // Parse JSON request body
    var input struct {
        Name        string   `json:"name"`
        Description string   `json:"description"`
        Latitude    float64  `json:"latitude"`
        Longitude   float64  `json:"longitude"`
        IPAddress   string   `json:"ipaddress"`
        URL         *string  `json:"url,omitempty"`
    }

    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
        return
    }

    // Validate required fields
    if input.Name == "" {
        http.Error(w, "name is required", http.StatusBadRequest)
        return
    }
    if input.Description == "" {
        http.Error(w, "description is required", http.StatusBadRequest)
        return
    }
    if input.IPAddress == "" {
        http.Error(w, "ipaddress is required", http.StatusBadRequest)
        return
    }

    // Generate a random password
    password, err := generateRandomPassword()
    if err != nil {
        http.Error(w, "Failed to generate password: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Create receiver object
    rec := Receiver{
        Description: input.Description,
        Latitude:    input.Latitude,
        Longitude:   input.Longitude,
        Name:        strings.ToUpper(input.Name),
        URL:         input.URL,
        Password:    password,
        IPAddress:   input.IPAddress,
    }

    // Validate the receiver
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // Ensure database connection
    if err := ensureConnection(); err != nil {
        http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
        return
    }
    
    // Check if there's already a receiver with the same IP address
    var existingCount int
    err = db.QueryRow(`SELECT COUNT(*) FROM receivers WHERE ip_address = $1`, input.IPAddress).Scan(&existingCount)
    if err != nil {
        http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
        return
    }
    
    if existingCount > 0 {
        http.Error(w, fmt.Sprintf("IP address '%s' is already in use by another receiver", input.IPAddress), http.StatusBadRequest)
        return
    }
    
    // For new receivers, we check if the IP address has any messages on any port
    // since we haven't allocated a UDP port yet
    portMetricsMutex.RLock()
    messageCount := 0
    if portMap, ok := portMetricsMap[input.IPAddress]; ok {
        for _, metric := range portMap {
            messageCount += metric.MessageCount
        }
    }
    portMetricsMutex.RUnlock()
    
    if messageCount <= 0 {
        http.Error(w, fmt.Sprintf("IP address '%s' has not sent any AIS data", input.IPAddress), http.StatusBadRequest)
        return
    }

    // Insert the new receiver into the database
    err = db.QueryRow(`
        INSERT INTO receivers (
            description,
            latitude,
            longitude,
            name,
            url,
            ip_address,
            password
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id, lastupdated`,
        rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.IPAddress, rec.Password).
        Scan(&rec.ID, &rec.LastUpdated)
    
    if err != nil {
        // Check for name uniqueness violation
        if strings.Contains(err.Error(), "unique constraint") && strings.Contains(err.Error(), "idx_receivers_name") {
            http.Error(w, fmt.Sprintf("name '%s' is already in use by another receiver", rec.Name), http.StatusBadRequest)
            return
        }
        http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Allocate a UDP port for this receiver
    udpPort, err := allocatePort(rec.ID)
    if err != nil {
        if err.Error() == "no available ports" {
            log.Printf("ERROR: No available UDP ports found for receiver ID %d", rec.ID)
            http.Error(w, "Failed to create receiver: No available UDP ports", http.StatusServiceUnavailable)
        } else {
            log.Printf("ERROR: Failed to allocate UDP port for receiver ID %d: %v", rec.ID, err)
            http.Error(w, fmt.Sprintf("Failed to create receiver: UDP port allocation error: %v", err), http.StatusInternalServerError)
        }
        
        // Clean up the created receiver since we couldn't allocate a port
        _, cleanupErr := db.Exec(`DELETE FROM receivers WHERE id = $1`, rec.ID)
        if cleanupErr != nil {
            log.Printf("ERROR: Failed to clean up receiver %d after port allocation failure: %v", rec.ID, cleanupErr)
        }
        
        return
    }
    
    rec.UDPPort = &udpPort

    // Return the complete receiver object including password and UDP port
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}
