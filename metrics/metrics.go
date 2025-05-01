package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net"
    "net/http"
    "regexp"
    "strings"
    "sync"
    "time"
)

type Settings struct {
    DbHost     string `json:"db_host"`
    DbPort     int    `json:"db_port"`
    DbUser     string `json:"db_user"`
    DbPass     string `json:"db_pass"`
    DbName     string `json:"db_name"`
    ListenPort int    `json:"listen_port"`
    Debug      bool   `json:"debug"`
    IngestHost string `json:"ingest_host"`
    IngestPort int    `json:"ingest_port"`
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

// SimpleMetrics is the flattened response for a single source
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

// ResponseMetrics embeds the simple metrics plus the client IP
type ResponseMetrics struct {
    IPAddress string `json:"ip_address"`
    SimpleMetrics
}

var (
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

    // Public API: metrics ingestion and retrieval (now /metrics/ingester)
    http.HandleFunc("/metrics/ingester", func(w http.ResponseWriter, r *http.Request) {
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

    // Handle /metrics/bysource (this logic is intact)
    http.HandleFunc("/metrics/bysource", handleMetricsBySource)

    // Serve static files for /metrics/ from the 'web' directory
    http.Handle("/metrics/", http.StripPrefix("/metrics/", http.FileServer(http.Dir("web"))))

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

// --- Public listing of latest metrics ---
func handleMetricsBySource(w http.ResponseWriter, r *http.Request) {
    // Determine client IP: prefer X-Forwarded-For, else RemoteAddr
    ip := r.URL.Query().Get("ipaddress")

    if ip == "" {
        // 1) Else check X-Forwarded-For
        if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
            parts := strings.Split(xff, ",")
            ip = strings.TrimSpace(parts[0])
        } else {
            // 2) Fallback to RemoteAddr
            host, _, err := net.SplitHostPort(r.RemoteAddr)
            if err != nil {
                ip = r.RemoteAddr
            } else {
                ip = host
            }
        }
    }

    metricsLock.RLock()
    raw := latestMetrics
    metricsLock.RUnlock()
    if raw == nil {
        http.Error(w, "no metrics yet", http.StatusNoContent)
        return
    }

    // Marshal and unmarshal into our FullMetrics struct
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

    // Build the flattened metrics for this IP
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

    // Respond with IP + metrics
    resp := ResponseMetrics{
        IPAddress:     ip,
        SimpleMetrics: flat,
    }
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
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
