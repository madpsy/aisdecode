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
    "time"

    _ "github.com/lib/pq"
)

type Settings struct {
    DbHost     string `json:"db_host"`
    DbPort     int    `json:"db_port"`
    DbUser     string `json:"db_user"`
    DbPass     string `json:"db_pass"`
    DbName     string `json:"db_name"`
    ListenPort int    `json:"listen_port"`
    Debug      bool   `json:"debug"`
    MetricsBaseURL string `json:"metrics_base_url"`
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
    db       *sql.DB
    settings Settings
)

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

    createSchema()

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
        log.Fatalf("Error creating table: %v", err)
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
    // Sync the SERIAL sequence
    _, err = db.Exec(`
        SELECT setval(
          pg_get_serial_sequence('receivers','id'),
          COALESCE(MAX(id), 1),
          true
        ) FROM receivers;
    `)
    if err != nil {
        log.Fatalf("Error syncing receivers_id_seq: %v", err)
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
        SELECT id,
               lastupdated,
               description,
               latitude,
               longitude,
               name,
               url,
               ip_address,
               password
          FROM receivers`
    
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
        ); err != nil {
            return nil, err
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

func getMessagesByIP(ipAddress string) (int, error) {
    // Create the URL for the metrics API endpoint.
    metricsURL := fmt.Sprintf("%s/metrics/bysource?ipaddress=%s", settings.MetricsBaseURL, ipAddress)

    // Send a GET request to the metrics API.
    resp, err := http.Get(metricsURL)
    if err != nil {
        return 0, fmt.Errorf("error making request to metrics API: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return 0, fmt.Errorf("received non-OK response from metrics API: %v", resp.Status)
    }

    // Decode the JSON response into the MetricsResponse struct.
    var metricsResponse MetricsResponse
    if err := json.NewDecoder(resp.Body).Decode(&metricsResponse); err != nil {
        return 0, fmt.Errorf("error decoding metrics API response: %v", err)
    }

    // Access the messages field correctly from SimpleMetrics
    return metricsResponse.SimpleMetrics.Messages, nil
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
        msgs, err := getMessagesByIP(rec.IPAddress)
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

    // Log the list of receivers to debug if IP addresses are being pulled correctly
    //log.Printf("Filtered Receivers: %+v", list)

    // For each receiver in the list, fetch messages based on the ip_address
    for i, rec := range list {
        //log.Printf("Fetching messages for IP address: %s", rec.IPAddress)  // Debug log for IP address

        // Fetch messages count for each receiver based on ip_address
        if rec.IPAddress == "" {
            log.Printf("Error: IP Address is empty for Receiver ID: %d", rec.ID)
        }

        msgs, err := getMessagesByIP(rec.IPAddress)
        if err != nil {
            msgs = 0
        }

        // Set the messages field
        list[i].Messages = msgs
        //log.Printf("Receiver ID: %d, IP: %s, Messages: %d", rec.ID, rec.IPAddress, msgs)  // Debug log for messages
    }

    // Return the list of receivers in JSON format, including ip_address and messages
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

    // 6) send response
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}

func handleGetReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var rec Receiver
    // Query the receiver from the database.
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address, password
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID, &rec.LastUpdated, &rec.Description,
        &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.IPAddress, &rec.Password,
    )
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Now, fetch the number of messages for the receiver's IP.
    messages, err := getMessagesByIP(rec.IPAddress)
    if err != nil {
        // If the metrics API call fails, set messages to 0.
        messages = 0
    }

    // Add the messages field to the receiver struct.
    rec.Messages = messages

    // Send the full receiver response with messages count.
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
    res, err := db.Exec(`DELETE FROM receivers WHERE id = $1`, id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    n, _ := res.RowsAffected()
    if n == 0 {
        w.WriteHeader(http.StatusNotFound)
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

    // Get message count for the updated receiver
    messages, err := getMessagesByIP(rec.IPAddress)
    if err != nil {
        messages = 0
    }
    rec.Messages = messages

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
    
    // Check if the IP address has a message count > 0
    messageCount, err := getMessagesByIP(input.IPAddress)
    if err != nil {
        http.Error(w, "Failed to verify message count: "+err.Error(), http.StatusInternalServerError)
        return
    }
    
    if messageCount <= 0 {
        http.Error(w, fmt.Sprintf("IP address '%s' has no messages", input.IPAddress), http.StatusBadRequest)
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

    // Return the complete receiver object including password
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}
