package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
    "net"

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
    Messages    int        `json:"messages"`
}

type ReceiverInput struct {
    Description string   `json:"description"`
    Latitude    float64  `json:"latitude"`
    Longitude   float64  `json:"longitude"`
    Name        string   `json:"name"`
    URL         *string  `json:"url,omitempty"`
}

type ReceiverPatch struct {
    Description *string   `json:"description,omitempty"`
    Latitude    *float64  `json:"latitude,omitempty"`
    Longitude   *float64  `json:"longitude,omitempty"`
    Name        *string   `json:"name,omitempty"`
    URL         *string   `json:"url,omitempty"`
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

    // Public API: only GET /receivers
    http.HandleFunc("/receivers", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        // public list: no IP
        handleListReceiversPublic(w, r)
    })

    // Admin API: full CRUD under /admin/receivers
    http.HandleFunc("/admin/receivers", adminReceiversHandler)
    http.HandleFunc("/admin/receivers/", adminReceiverHandler)

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
            name VARCHAR(15) NOT NULL,
            url TEXT,
	    ip_address TEXT NOT NULL DEFAULT ''
        );
    `)
    if err != nil {
        log.Fatalf("Error creating table: %v", err)
    }
    _, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receivers_id ON receivers(id);`)
    if err != nil {
        log.Fatalf("Error creating index: %v", err)
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
               ip_address
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

// Function to get the message count for a given IP address from the metrics API.
func getMessagesByIP(ipAddress string) (int, error) {
    // Create the URL for the metrics API endpoint.
    metricsURL := fmt.Sprintf("%s/metrics/bysource?ipaddress=%s", settings.MetricsBaseURL, ipAddress)
    //log.Printf("Fetching messages from: %s", metricsURL)  // Debug log for URL

    // Send a GET request to the metrics API.
    resp, err := http.Get(metricsURL)
    if err != nil {
        return 0, fmt.Errorf("error making request to metrics API: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return 0, fmt.Errorf("received non-OK response from metrics API: %v", resp.Status)
    }

    // Decode the JSON response from the metrics API.
    var metricsResponse struct {
        Messages int `json:"messages"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&metricsResponse); err != nil {
        return 0, fmt.Errorf("error decoding metrics API response: %v", err)
    }

    //log.Printf("Received message count: %d for IP: %s", metricsResponse.Messages, ipAddress)  // Debug log for message count

    return metricsResponse.Messages, nil
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

    var list []Receiver
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
        rec.Messages = msgs

        // Exclude IP Address in public response
        rec.IPAddress = "" // Clear IP address before sending the response

        list = append(list, rec)
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

func handleCreateReceiver(w http.ResponseWriter, r *http.Request) {
    // 1) extract client IP
    ip := getClientIP(r)

    // 2) decode JSON body
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 3) build Receiver and validate
    rec := Receiver{
        Description: input.Description,
        Latitude:    input.Latitude,
        Longitude:   input.Longitude,
        Name:        input.Name,
        URL:         input.URL,
    }
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 4) INSERT including ip_address
    err := db.QueryRow(`
        INSERT INTO receivers (
            description,
            latitude,
            longitude,
            name,
            url,
            ip_address
        ) VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING id, lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, ip).
        Scan(&rec.ID, &rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 5) populate IPAddress on the struct for JSON response
    rec.IPAddress = ip

    // 6) send response
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}

func handleGetReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var rec Receiver
    // Query the receiver from the database.
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID, &rec.LastUpdated, &rec.Description,
        &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.IPAddress,
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
    // 1) extract client IP
    ip := getClientIP(r)

    // 2) decode JSON body
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 3) build Receiver and validate
    rec := Receiver{
        ID:          id,
        Description: input.Description,
        Latitude:    input.Latitude,
        Longitude:   input.Longitude,
        Name:        input.Name,
        URL:         input.URL,
    }
    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 4) UPSERT including ip_address
    err := db.QueryRow(`
        INSERT INTO receivers (
            id,
            description,
            latitude,
            longitude,
            name,
            url,
            ip_address
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (id) DO UPDATE
          SET description = EXCLUDED.description,
              latitude    = EXCLUDED.latitude,
              longitude   = EXCLUDED.longitude,
              name        = EXCLUDED.name,
              url         = EXCLUDED.url,
              ip_address  = EXCLUDED.ip_address,
              lastupdated = NOW()
        RETURNING lastupdated
    `, rec.ID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, ip).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 5) populate IPAddress on the struct for JSON response
    rec.IPAddress = ip

    // 6) send response
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handlePatchReceiver(w http.ResponseWriter, r *http.Request, id int) {
    // 1) extract client IP
    ip := getClientIP(r)

    // 2) decode JSON body
    var patch ReceiverPatch
    if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    // 3) load existing record
    var rec Receiver
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, ip_address
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
        rec.Name = *patch.Name
    }
    if patch.URL != nil {
        rec.URL = patch.URL
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
               lastupdated  = NOW()
         WHERE id = $7
         RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, ip, rec.ID).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // 7) populate IPAddress on the struct for JSON response
    rec.IPAddress = ip

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
    return nil
}
