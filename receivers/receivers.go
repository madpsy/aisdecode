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
    "sync"
    "time"
    "regexp"

    _ "github.com/lib/pq"
)

type Settings struct {
    DbHost      string `json:"db_host"`
    DbPort      int    `json:"db_port"`
    DbUser      string `json:"db_user"`
    DbPass      string `json:"db_pass"`
    DbName      string `json:"db_name"`
    ListenPort  int    `json:"listen_port"`
    Debug       bool   `json:"debug"`
    IngestHost  string `json:"ingest_host"`
    IngestPort  int    `json:"ingest_port"`
}

type Receiver struct {
    ID          int        `json:"id"`
    LastUpdated time.Time  `json:"lastupdated"`
    Description string     `json:"description"`
    Latitude    float64    `json:"latitude"`
    Longitude   float64    `json:"longitude"`
    Name        string     `json:"name"`
    URL         *string    `json:"url,omitempty"`
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

// FullMetrics mirrors the parts of the JSON we filter by source IP.
type FullMetrics struct {
    BytesReceivedBySource    []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"bytes_received_by_source"`
    FailuresBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"failures_by_source"`
    MessagesBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"messages_by_source"`
    PerDeduplicatedSource    map[string]int                                                    `json:"per_deduplicated_source"`
    WindowBytesBySource      map[string]int                                                    `json:"window_bytes_by_source"`
    WindowMessagesBySource   map[string]int                                                    `json:"window_messages_by_source"`
    UniqueMMSIBySource       []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"unique_mmsi_by_source"`
    WindowUniqueUIDsBySource map[string]int                                                    `json:"window_unique_uids_by_source"`
}

// SimpleMetrics is the flattened response for /metrics/bysource
type SimpleMetrics struct {
    BytesReceived    int `json:"bytes_received"`
    Messages         int `json:"messages"`
    UniqueMMSI       int `json:"unique_mmsi"`
    Failures         int `json:"failures"`
    Deduplicated     int `json:"deduplicated"`
    WindowBytes      int `json:"window_bytes"`
    WindowMessages   int `json:"window_messages"`
    WindowUniqueUIDs int `json:"window_unique_uids"`
}

var (
    db            *sql.DB
    settings      Settings
    metricsLock   sync.RWMutex
    latestMetrics interface{}
)

func main() {
    // Load settings.json
    data, err := ioutil.ReadFile("settings.json")
    if err != nil {
        log.Fatalf("Error reading settings.json: %v", err)
    }
    if err := json.Unmarshal(data, &settings); err != nil {
        log.Fatalf("Error parsing settings.json: %v", err)
    }

    // Start background ingestion of metrics
    go ingestMetricsLoop()

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
    if err = db.Ping(); err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }

    createSchema()

    // Public API: list receivers
    http.HandleFunc("/receivers", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
        handleListReceivers(w, r)
    })

    // Admin API: full CRUD
    http.HandleFunc("/admin/receivers", adminReceiversHandler)
    http.HandleFunc("/admin/receivers/", adminReceiverHandler)

    // Serve static files for admin UI
    http.Handle("/admin/", http.StripPrefix("/admin/", http.FileServer(http.Dir("web"))))

    // Latest ingested metrics
    http.HandleFunc("/metrics/latest", func(w http.ResponseWriter, r *http.Request) {
        metricsLock.RLock()
        defer metricsLock.RUnlock()
        if latestMetrics == nil {
            http.Error(w, "no metrics yet", http.StatusNoContent)
            return
        }

        // Marshal the raw metrics to JSON
        blob, err := json.Marshal(latestMetrics)
        if err != nil {
            log.Printf("Error marshaling metrics: %v", err)
            http.Error(w, "internal error", http.StatusInternalServerError)
            return
        }

        // Mask first two octets of every IPv4 address (e.g. 127.0.0.1 → x.x.0.1)
        re := regexp.MustCompile(`\b(\d{1,3})\.(\d{1,3})\.(\d{1,3}\.\d{1,3})\b`)
        masked := re.ReplaceAll(blob, []byte("x.x.$3"))

        w.Header().Set("Content-Type", "application/json")
        w.Write(masked)
    })

    // New: metrics by source IP
    http.HandleFunc("/metrics/bysource", handleMetricsBySource)

    addr := fmt.Sprintf(":%d", settings.ListenPort)
    log.Printf("Server listening on %s", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}

// ingestMetricsLoop polls /metrics every second with a 5s timeout.
func ingestMetricsLoop() {
    client := &http.Client{Timeout: 5 * time.Second}
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        url := fmt.Sprintf("http://%s:%d/metrics", settings.IngestHost, settings.IngestPort)
        resp, err := client.Get(url)
        if err != nil {
            log.Printf("Error fetching metrics: %v", err)
            continue
        }
        body, err := ioutil.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil {
            log.Printf("Error reading metrics response: %v", err)
            continue
        }

        var m interface{}
        if err := json.Unmarshal(body, &m); err != nil {
            log.Printf("Error parsing metrics JSON: %v", err)
            continue
        }

        metricsLock.Lock()
        latestMetrics = m
        metricsLock.Unlock()
    }
}

// handleMetricsBySource filters latestMetrics for a single source IP
// and returns a flat SimpleMetrics JSON.
func handleMetricsBySource(w http.ResponseWriter, r *http.Request) {
    ip := r.URL.Query().Get("ipaddress")
    if ip == "" {
        http.Error(w, "missing ipaddress parameter", http.StatusBadRequest)
        return
    }

    metricsLock.RLock()
    raw := latestMetrics
    metricsLock.RUnlock()
    if raw == nil {
        http.Error(w, "no metrics yet", http.StatusNoContent)
        return
    }

    blob, err := json.Marshal(raw)
    if err != nil {
        log.Printf("Error marshaling metrics: %v", err)
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }
    var full FullMetrics
    if err := json.Unmarshal(blob, &full); err != nil {
        log.Printf("Error parsing FullMetrics: %v", err)
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    flat := SimpleMetrics{
        BytesReceived:    findCount(full.BytesReceivedBySource, ip),
        Messages:         findCount(full.MessagesBySource, ip),
        UniqueMMSI:       findCount(full.UniqueMMSIBySource, ip),
        Failures:         findCount(full.FailuresBySource, ip),
        Deduplicated:     full.PerDeduplicatedSource[ip],
        WindowBytes:      full.WindowBytesBySource[ip],
        WindowMessages:   full.WindowMessagesBySource[ip],
        WindowUniqueUIDs: full.WindowUniqueUIDsBySource[ip],
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(flat)
}

// findCount extracts the Count for a given IP from a slice of structs.
func findCount[T any](slice []T, ip string) int {
    data, _ := json.Marshal(slice)
    var arr []struct {
        SourceIP string `json:"source_ip"`
        Count    int    `json:"count"`
    }
    if err := json.Unmarshal(data, &arr); err != nil {
        return 0
    }
    for _, e := range arr {
        if e.SourceIP == ip {
            return e.Count
        }
    }
    return 0
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
            url TEXT
        );
    `)
    if err != nil {
        log.Fatalf("Error creating table: %v", err)
    }
    _, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receivers_id ON receivers(id);`)
    if err != nil {
        log.Fatalf("Error creating index: %v", err)
    }
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

// --- Public listing only ---
func handleListReceivers(w http.ResponseWriter, r *http.Request) {
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
        handleListReceivers(w, r)
    case http.MethodPost:
        handleCreateReceiver(w, r)
    default:
        w.WriteHeader(http.StatusMethodNotAllowed)
    }
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
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }
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

    err := db.QueryRow(`
        INSERT INTO receivers (description, latitude, longitude, name, url)
        VALUES ($1,$2,$3,$4,$5)
        RETURNING id, lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL).
        Scan(&rec.ID, &rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(rec)
}

func handleGetReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var rec Receiver
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID, &rec.LastUpdated, &rec.Description,
        &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL,
    )
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handlePutReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var input ReceiverInput
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }
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

    err := db.QueryRow(`
        INSERT INTO receivers (id, description, latitude, longitude, name, url)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (id) DO UPDATE
          SET description = EXCLUDED.description,
              latitude    = EXCLUDED.latitude,
              longitude   = EXCLUDED.longitude,
              name        = EXCLUDED.name,
              url         = EXCLUDED.url,
              lastupdated = NOW()
        RETURNING lastupdated
    `, rec.ID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(rec)
}

func handlePatchReceiver(w http.ResponseWriter, r *http.Request, id int) {
    var patch ReceiverPatch
    if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
        http.Error(w, "invalid JSON", http.StatusBadRequest)
        return
    }

    var rec Receiver
    err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url
        FROM receivers WHERE id = $1
    `, id).Scan(
        &rec.ID, &rec.LastUpdated, &rec.Description,
        &rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL,
    )
    if err == sql.ErrNoRows {
        w.WriteHeader(http.StatusNotFound)
        return
    } else if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

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

    if err := validateReceiver(rec); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    err = db.QueryRow(`
        UPDATE receivers
           SET description = $1,
               latitude    = $2,
               longitude   = $3,
               name        = $4,
               url         = $5,
               lastupdated = NOW()
         WHERE id = $6
         RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.ID).
        Scan(&rec.LastUpdated)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

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
