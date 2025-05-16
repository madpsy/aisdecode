package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"io"
	"net/http"
	"regexp"
	"strings"
	"strconv"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

var ctx = context.Background()

// ─────────────────────────────────────────────────────────────────────────────
// Settings & Data Models
// ─────────────────────────────────────────────────────────────────────────────

type Settings struct {
	IngestHost string `json:"ingest_host"`
	IngestPort int    `json:"ingest_port"`
	InfluxHost string `json:"influx_host"`
	InfluxPort int    `json:"influx_port"`
	InfluxDB   string `json:"influx_db"`
	RedisHost  string `json:"redis_host"`
	RedisPort  int    `json:"redis_port"`
	CacheTime  int    `json:"cache_time"`  // seconds
	ListenPort int    `json:"listen_port"`
	Debug      bool   `json:"debug"`
	ReceiversBaseURL string `json:"receivers_base_url"`
}

type FullMetrics struct {
	BytesReceivedBySource      []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"bytes_received_by_source"`
	FailuresBySource           []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"failures_by_source"`
	MessagesBySource           []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"messages_by_source"`
	UniqueMMSIBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"unique_mmsi_by_source"`
	WindowBytesBySource        map[string]int                                       `json:"window_bytes_by_source"`
	WindowMessagesBySource     map[string]int                                       `json:"window_messages_by_source"`
	WindowUniqueUIDsBySource   map[string]int                                       `json:"window_unique_uids_by_source"`
	WindowUserIDsBySource      map[string][]string                                  `json:"window_user_ids_by_source"`
	PerDeduplicatedSource      map[string]int                                       `json:"per_deduplicated_source"`
	PerDownsampledMessageID    map[string]int                                       `json:"per_downsampled_message_id"`
	PerMessageID               map[string]int                                       `json:"per_message_id"`
	Top25DeduplicatedPerSource []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"top25_deduplicated_per_source"`
	TotalBytesForwarded        int                                                  `json:"total_bytes_forwarded"`
	TotalBytesReceived         int                                                  `json:"total_bytes_received"`
	TotalFailures              int                                                  `json:"total_failures"`
	TotalMessages              int                                                  `json:"total_messages"`
	TotalMessagesForwarded     int                                                  `json:"total_messages_forwarded"`
	UptimeSeconds              int                                                  `json:"uptime_seconds"`

	// New ingestions
	MemoryStats struct {
		AllocBytes     int `json:"alloc_bytes"`
		HeapAllocBytes int `json:"heap_alloc_bytes"`
		SysBytes       int `json:"sys_bytes"`
		NumGC          int `json:"num_gc"`
	} `json:"memory_stats"`
	TotalClients int `json:"total_clients"`
}

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

type Aggregated struct {
	BytesReceivedMinute  int `json:"bytes_received_minute"`
	MessagesMinute       int `json:"messages_minute"`
	FailuresMinute       int `json:"failures_minute"`
	UniqueMMSIMinute     int `json:"unique_mmsi_minute"`

	BytesReceivedHour    int `json:"bytes_received_hour"`
	MessagesHour         int `json:"messages_hour"`
	FailuresHour         int `json:"failures_hour"`
	UniqueMMSIHour       int `json:"unique_mmsi_hour"`

	BytesReceivedDay     int `json:"bytes_received_day"`
	MessagesDay          int `json:"messages_day"`
	FailuresDay          int `json:"failures_day"`
	UniqueMMSIDay        int `json:"unique_mmsi_day"`
}

type ResponseMetrics struct {
	IPAddress      string     `json:"ip_address"`
	SimpleMetrics             // real-time counts
	Aggregated    Aggregated `json:"aggregated"`
	WindowUserIDs []string   `json:"window_user_ids"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Historic Endpoint Support Types
// ─────────────────────────────────────────────────────────────────────────────

// metricConfig drives each series we expose in /metrics/historic
type metricConfig struct {
	Key       string // JSON key & Influx "metric" tag
	Func      string // Influx function: SUM, MEAN, LAST
	ValueType string // "int", "float", or "string"
}

// All the metrics we expose in /metrics/historic
var metricConfigs = []metricConfig{
    // per-source & windowed counts
    {"bytes_received", "SUM", "int"},
    {"messages",       "SUM", "int"},
    {"failures",       "SUM", "int"},
    {"unique_mmsi",    "SUM", "int"},
    {"window_bytes",   "SUM", "int"},
    {"window_messages","SUM", "int"},
    {"window_unique_uids", "SUM", "int"},

    // totals, uptime, clients
    {"total_bytes_forwarded",    "LAST", "int"},
    {"total_bytes_received",     "LAST", "int"},
    {"total_failures",           "LAST", "int"},
    {"total_messages",           "LAST", "int"},
    {"total_messages_forwarded", "LAST", "int"},
    {"uptime_seconds",           "LAST", "int"},
    {"total_clients",            "LAST", "int"},

    // memory
    {"alloc_bytes",      "MEAN", "int"},
    {"heap_alloc_bytes", "MEAN", "int"},
    {"sys_bytes",        "MEAN", "int"},
    {"num_gc",           "SUM",  "int"},

    // UID lists
    {"window_user_ids", "LAST", "string"},
}

// genericPoint for arbitrary timestamped values
type genericPoint struct {
	Timestamp time.Time   `json:"timestamp"`
	Value     interface{} `json:"value"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Globals
// ─────────────────────────────────────────────────────────────────────────────

var (
	settings      Settings
	metricsLock   sync.RWMutex
	latestMetrics interface{}
	influxClient  client.Client
	redisClient   *redis.Client
)

// ─────────────────────────────────────────────────────────────────────────────
// Entry Point
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	// Load settings
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}

	// Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", settings.RedisHost, settings.RedisPort),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Error connecting to Redis: %v", err)
	}

	// InfluxDB
	influxURL := fmt.Sprintf("http://%s:%d", settings.InfluxHost, settings.InfluxPort)
	influxClient, err = client.NewHTTPClient(client.HTTPConfig{Addr: influxURL})
	if err != nil {
		log.Fatalf("Error creating InfluxDB client: %v", err)
	}
	defer influxClient.Close()

	// Ensure DB exists
	if err := ensureDatabase(settings.InfluxDB); err != nil {
		log.Fatalf("Could not create InfluxDB database %q: %v", settings.InfluxDB, err)
	}

	// Start polling & writes
	go ingestMetricsLoop()

	// HTTP handlers
	http.HandleFunc("/metrics/ingester", metricsIngesterHandler)
	http.HandleFunc("/metrics/bysource", handleMetricsBySource)
	http.HandleFunc("/metrics/historic", historicMetricsHandler)
	http.Handle("/metrics/", http.StripPrefix("/metrics/", http.FileServer(http.Dir("web"))))

	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// ─────────────────────────────────────────────────────────────────────────────
// Influx DB utility
// ─────────────────────────────────────────────────────────────────────────────

func ensureDatabase(db string) error {
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE \"%s\"", db), "", "")
	resp, err := influxClient.Query(q)
	if err != nil {
		return err
	}
	return resp.Error()
}

// ─────────────────────────────────────────────────────────────────────────────
// Ingest Loop & Original Handlers (unchanged)
// ─────────────────────────────────────────────────────────────────────────────

func ingestMetricsLoop() {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		url := fmt.Sprintf("http://%s:%d/metrics", settings.IngestHost, settings.IngestPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			log.Printf("Error fetching metrics: %v", err)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		var m interface{}
		if err := json.Unmarshal(body, &m); err != nil {
			log.Printf("Error parsing metrics JSON: %v", err)
			continue
		}

		metricsLock.Lock()
		latestMetrics = m
		metricsLock.Unlock()

		if err := writeMetricsToInfluxDB(m); err != nil {
			log.Printf("Error writing to InfluxDB: %v", err)
		}
	}
}

func metricsIngesterHandler(w http.ResponseWriter, r *http.Request) {
	metricsLock.RLock()
	defer metricsLock.RUnlock()
	if latestMetrics == nil {
		http.Error(w, "no metrics yet", http.StatusNoContent)
		return
	}
	blob, _ := json.Marshal(latestMetrics)
	re := regexp.MustCompile(`\b(\d{1,3})\.(\d{1,3})\.(\d{1,3}\.\d{1,3})\b`)
	masked := re.ReplaceAll(blob, []byte("x.x.$3"))
	w.Header().Set("Content-Type", "application/json")
	w.Write(masked)
}

func writeMetricsToInfluxDB(metrics interface{}) error {
    // Marshal into FullMetrics
    blob, err := json.Marshal(metrics)
    if err != nil {
        return err
    }
    var full FullMetrics
    if err := json.Unmarshal(blob, &full); err != nil {
        return err
    }

    // Create batch
    bp, err := client.NewBatchPoints(client.BatchPointsConfig{
        Database: settings.InfluxDB,
    })
    if err != nil {
        return err
    }

    // Helper for numeric “metrics” writes
    add := func(tags map[string]string, fields map[string]interface{}) {
        pt, err := client.NewPoint("metrics", tags, fields, time.Now())
        if err != nil {
            log.Printf("Point error tags=%v fields=%v: %v", tags, fields, err)
            return
        }
        bp.AddPoint(pt)
    }

    // --- 1) Per-source ---
    for _, m := range full.BytesReceivedBySource {
        add(map[string]string{"source_ip": m.SourceIP, "metric": "bytes_received"},
            map[string]interface{}{"value": m.Count})
    }
    for _, m := range full.FailuresBySource {
        add(map[string]string{"source_ip": m.SourceIP, "metric": "failures"},
            map[string]interface{}{"value": m.Count})
    }
    for _, m := range full.MessagesBySource {
        add(map[string]string{"source_ip": m.SourceIP, "metric": "messages"},
            map[string]interface{}{"value": m.Count})
    }
    for _, m := range full.UniqueMMSIBySource {
        add(map[string]string{"source_ip": m.SourceIP, "metric": "unique_mmsi"},
            map[string]interface{}{"value": m.Count})
    }

    // --- 2) Windowed per-source ---
    for ip, v := range full.WindowBytesBySource {
        add(map[string]string{"source_ip": ip, "metric": "window_bytes"},
            map[string]interface{}{"value": v})
    }
    for ip, v := range full.WindowMessagesBySource {
        add(map[string]string{"source_ip": ip, "metric": "window_messages"},
            map[string]interface{}{"value": v})
    }
    for ip, v := range full.WindowUniqueUIDsBySource {
        add(map[string]string{"source_ip": ip, "metric": "window_unique_uids"},
            map[string]interface{}{"value": v})
    }

    // --- 3) Deduplicated / per-message ---
    for ip, v := range full.PerDeduplicatedSource {
        add(map[string]string{"source_ip": ip, "metric": "per_deduplicated"},
            map[string]interface{}{"value": v})
    }
    for mid, v := range full.PerDownsampledMessageID {
        add(map[string]string{"message_id": mid, "metric": "per_downsampled_message_id"},
            map[string]interface{}{"value": v})
    }
    for mid, v := range full.PerMessageID {
        add(map[string]string{"message_id": mid, "metric": "per_message_id"},
            map[string]interface{}{"value": v})
    }

    // --- 4) Totals, uptime, clients ---
    totals := []struct{ metric string; value int }{
        {"total_bytes_forwarded", full.TotalBytesForwarded},
        {"total_bytes_received", full.TotalBytesReceived},
        {"total_failures", full.TotalFailures},
        {"total_messages", full.TotalMessages},
        {"total_messages_forwarded", full.TotalMessagesForwarded},
        {"uptime_seconds", full.UptimeSeconds},
        {"total_clients", full.TotalClients},
    }
    for _, t := range totals {
        add(map[string]string{"metric": t.metric}, map[string]interface{}{"value": t.value})
    }

    // --- 5) Memory stats ---
    add(map[string]string{"metric": "alloc_bytes"}, map[string]interface{}{"value": full.MemoryStats.AllocBytes})
    add(map[string]string{"metric": "heap_alloc_bytes"}, map[string]interface{}{"value": full.MemoryStats.HeapAllocBytes})
    add(map[string]string{"metric": "sys_bytes"}, map[string]interface{}{"value": full.MemoryStats.SysBytes})
    add(map[string]string{"metric": "num_gc"}, map[string]interface{}{"value": full.MemoryStats.NumGC})

    // --- 6) WINDOWED UID LISTS — into their own measurement ---
    for ip, uids := range full.WindowUserIDsBySource {
        idList := strings.Join(uids, ",")
        pt, err := client.NewPoint(
            "metrics_user_ids",                  // new measurement
            map[string]string{"source_ip": ip},  
            map[string]interface{}{"uids": idList}, 
            time.Now(),
        )
        if err != nil {
            log.Printf("Point error for metrics_user_ids: %v", err)
        } else {
            bp.AddPoint(pt)
        }
    }

    // Write batch
    return influxClient.Write(bp)
}

func handleMetricsBySource(w http.ResponseWriter, r *http.Request) {
    // 1) Determine the requestor’s IP
    var requestorIP string
    if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
        requestorIP = strings.TrimSpace(strings.Split(xff, ",")[0])
    } else if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
        requestorIP = host
    } else {
        requestorIP = r.RemoteAddr
    }

    // 2) Determine the requested IP: param ipaddress overrides requestorIP
    requestedIP := r.URL.Query().Get("ipaddress")
    if requestedIP == "" {
        requestedIP = requestorIP
    }

    // 3) Determine the filter IP: try id lookup, else requestedIP
    filterIP := ""
    receiverExists := false
    receiverHasEmptyIP := false
    receiverID := r.URL.Query().Get("id")
    
    if receiverID != "" {
        if _, err := strconv.Atoi(receiverID); err == nil {
            // First check if the receiver exists at all
            receiverExists = true // Assume it exists if we have an ID
            
            // Try to get the IP for this receiver
            cacheKey := "receiver:" + receiverID
            if cached, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
                // We found a cached IP
                filterIP = cached
                log.Printf("Found cached IP for receiver %s: '%s'", receiverID, filterIP)
                
                // Only consider it as having no IP if the IP is actually empty
                receiverHasEmptyIP = (filterIP == "")
            } else {
                // No cached IP, try to get it from the API
                url := fmt.Sprintf("%s/admin/getip?id=%s", settings.ReceiversBaseURL, receiverID)
                resp, err := http.Get(url)
                if err == nil {
                    defer resp.Body.Close()
                    body, _ := io.ReadAll(resp.Body)
                    var out struct{ IP string `json:"ip_address"` }
                    if json.Unmarshal(body, &out) == nil {
                        filterIP = out.IP
                        log.Printf("Got IP from API for receiver %s: '%s'", receiverID, filterIP)
                        
                        // Only consider it as having no IP if the IP is actually empty
                        receiverHasEmptyIP = (filterIP == "")
                        
                        if filterIP != "" {
                            // Cache the IP for future requests
                            redisClient.Set(ctx, cacheKey, filterIP, 60*time.Second)
                        }
                    }
                }
            }
            
            log.Printf("Receiver %s: exists=%v, hasNoIP=%v, filterIP='%s'",
                       receiverID, receiverExists, receiverHasEmptyIP, filterIP)
        }
    }
    
    // Only use requestedIP if we're not dealing with a receiver that has an empty IP
    if filterIP == "" && !receiverHasEmptyIP {
        filterIP = requestedIP
    }
    
    // Check if we have any data for this IP before proceeding
    hasData := false

    // 4) Load latest metrics
    metricsLock.RLock()
    raw := latestMetrics
    metricsLock.RUnlock()
    if raw == nil {
        http.Error(w, "no metrics yet", http.StatusNoContent)
        return
    }

    // 5) Unmarshal into FullMetrics
    blob, _ := json.Marshal(raw)
    var full FullMetrics
    _ = json.Unmarshal(blob, &full)

    // 6) Build SimpleMetrics for filterIP
    rt := SimpleMetrics{}
    
    // Check if we have any data for this IP
    for _, m := range full.BytesReceivedBySource {
        if m.SourceIP == filterIP {
            rt.BytesReceived = m.Count
            hasData = true
        }
    }
    for _, m := range full.MessagesBySource {
        if m.SourceIP == filterIP {
            rt.Messages = m.Count
            hasData = true
        }
    }
    for _, m := range full.UniqueMMSIBySource {
        if m.SourceIP == filterIP {
            rt.UniqueMMSI = m.Count
            hasData = true
        }
    }
    for _, m := range full.FailuresBySource {
        if m.SourceIP == filterIP {
            rt.Failures = m.Count
            hasData = true
        }
    }
    
    // Check for data in maps
    if _, exists := full.PerDeduplicatedSource[filterIP]; exists {
        rt.Deduplicated = full.PerDeduplicatedSource[filterIP]
        hasData = true
    }
    if _, exists := full.WindowBytesBySource[filterIP]; exists {
        rt.WindowBytes = full.WindowBytesBySource[filterIP]
        hasData = true
    }
    if _, exists := full.WindowMessagesBySource[filterIP]; exists {
        rt.WindowMessages = full.WindowMessagesBySource[filterIP]
        hasData = true
    }
    if _, exists := full.WindowUniqueUIDsBySource[filterIP]; exists {
        rt.WindowUniqueUIDs = full.WindowUniqueUIDsBySource[filterIP]
        hasData = true
    }
    
    // If we have no data for the requested IP from a receiver ID lookup,
    // fall back to the requestor's IP
    if !hasData && filterIP != requestorIP && r.URL.Query().Get("id") != "" {
        filterIP = requestorIP
        
        // Recalculate metrics for the fallback IP
        for _, m := range full.BytesReceivedBySource {
            if m.SourceIP == filterIP {
                rt.BytesReceived = m.Count
            }
        }
        for _, m := range full.MessagesBySource {
            if m.SourceIP == filterIP {
                rt.Messages = m.Count
            }
        }
        for _, m := range full.UniqueMMSIBySource {
            if m.SourceIP == filterIP {
                rt.UniqueMMSI = m.Count
            }
        }
        for _, m := range full.FailuresBySource {
            if m.SourceIP == filterIP {
                rt.Failures = m.Count
            }
        }
        rt.Deduplicated     = full.PerDeduplicatedSource[filterIP]
        rt.WindowBytes      = full.WindowBytesBySource[filterIP]
        rt.WindowMessages   = full.WindowMessagesBySource[filterIP]
        rt.WindowUniqueUIDs = full.WindowUniqueUIDsBySource[filterIP]
    }

    // 7) Load or compute Aggregated (cached)
    aggKey := fmt.Sprintf("agg:%s", filterIP)
    var agg Aggregated
    if data, err := redisClient.Get(ctx, aggKey).Bytes(); err == nil {
        _ = json.Unmarshal(data, &agg)
    } else {
        agg = queryAggregates(filterIP)
        if b, err := json.Marshal(agg); err == nil {
            redisClient.Set(ctx, aggKey, b, time.Duration(settings.CacheTime)*time.Second)
        }
    }

    // 8) Pull windowed user IDs
    windowIDs := full.WindowUserIDsBySource[filterIP]

    // 9) Build and send response with requested_ip_address
    respBody := struct {
        RequestedIP      string         `json:"requested_ip_address"`
        SimpleMetrics    SimpleMetrics  `json:"simple_metrics"`
        Aggregated       Aggregated     `json:"aggregated"`
        WindowUserIDs    []string       `json:"window_user_ids"`
        ReceiverExists   bool           `json:"receiver_exists,omitempty"`
        ReceiverHasNoIP  bool           `json:"receiver_has_no_ip,omitempty"`
    }{
        RequestedIP:     requestedIP,
        SimpleMetrics:   rt,
        Aggregated:      agg,
        WindowUserIDs:   windowIDs,
        ReceiverExists:  receiverExists && receiverID != "",
        ReceiverHasNoIP: receiverHasEmptyIP,
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(respBody)
}

func queryAggregates(ip string) Aggregated {
	var a Aggregated
	queries := []struct {
		field    string
		interval string
		setter   func(int)
	}{
		{"bytes_received", "1m", func(v int) { a.BytesReceivedMinute = v }},
		{"messages", "1m", func(v int) { a.MessagesMinute = v }},
		{"failures", "1m", func(v int) { a.FailuresMinute = v }},
		{"unique_mmsi", "1m", func(v int) { a.UniqueMMSIMinute = v }},
		{"bytes_received", "1h", func(v int) { a.BytesReceivedHour = v }},
		{"messages", "1h", func(v int) { a.MessagesHour = v }},
		{"failures", "1h", func(v int) { a.FailuresHour = v }},
		{"unique_mmsi", "1h", func(v int) { a.UniqueMMSIHour = v }},
		{"bytes_received", "24h", func(v int) { a.BytesReceivedDay = v }},
		{"messages", "24h", func(v int) { a.MessagesDay = v }},
		{"failures", "24h", func(v int) { a.FailuresDay = v }},
		{"unique_mmsi", "24h", func(v int) { a.UniqueMMSIDay = v }},
	}

	for _, q := range queries {
		influxQL := fmt.Sprintf(
			`SELECT SUM("value") FROM "metrics"
             WHERE "metric"='%s' AND "source_ip"='%s' AND time > now() - %s`,
			q.field, ip, q.interval,
		)
		res, err := influxClient.Query(client.NewQuery(influxQL, settings.InfluxDB, "ns"))
		if err != nil || res.Error() != nil || len(res.Results[0].Series) == 0 {
			continue
		}
		if sum, ok := res.Results[0].Series[0].Values[0][1].(json.Number); ok {
			if iv, err := sum.Int64(); err == nil {
				q.setter(int(iv))
			}
		}
	}

	return a
}

// ─────────────────────────────────────────────────────────────────────────────
// New: historicMetricsHandler & queryTimeSeriesGeneric
// ─────────────────────────────────────────────────────────────────────────────

func pickInterval(from, to time.Time) string {
	diff := to.Sub(from)
	switch {
	case diff <= 2*time.Hour:
		return "1m"
	case diff <= 48*time.Hour:
		return "1h"
	default:
		return "24h"
	}
}

// queryTimeSeriesGeneric fetches any numeric or string series out of the "metrics" measurement
// using millisecond precision for the time column.
func queryTimeSeriesGeneric(
    cfg metricConfig,
    ip *string,
    interval string,
    from, to time.Time,
) ([]genericPoint, error) {
    // 1) Build WHERE clauses
    where := []string{
        fmt.Sprintf("time >= '%s' AND time <= '%s'",
            from.UTC().Format(time.RFC3339Nano),
            to.UTC().Format(time.RFC3339Nano)),
        fmt.Sprintf(`"metric" = '%s'`, cfg.Key),
    }
    if ip != nil && *ip != "" {
        where = append(where, fmt.Sprintf(`"source_ip" = '%s'`, *ip))
    }

    // 2) Construct the InfluxQL
    fill := "0"
    if cfg.ValueType == "string" {
        fill = "previous"
    }
    influxQL := fmt.Sprintf(`
        SELECT %s("value") FROM "metrics"
         WHERE %s
         GROUP BY time(%s) fill(%s)
         ORDER BY time ASC
    `,
        cfg.Func,
        strings.Join(where, " AND "),
        interval,
        fill,
    )

    // 3) Query with millisecond time precision
    res, err := influxClient.Query(client.NewQuery(influxQL, settings.InfluxDB, "ms"))
    if err != nil {
        return nil, err
    }
    if res.Error() != nil || len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
        return nil, nil
    }

    series := res.Results[0].Series[0]
    pts := make([]genericPoint, 0, len(series.Values))

    // 4) Parse each row
    for _, row := range series.Values {
        // --- Timestamp (row[0]) comes back in milliseconds ---
        var ts time.Time
        switch v := row[0].(type) {
        case json.Number:
            if ms, e := v.Int64(); e == nil {
                ts = time.Unix(0, ms*int64(time.Millisecond))
            } else {
                continue
            }
        case float64:
            ts = time.Unix(0, int64(v)*int64(time.Millisecond))
        case int64:
            ts = time.Unix(0, v*int64(time.Millisecond))
        case string:
            t, e := time.Parse(time.RFC3339Nano, v)
            if e != nil {
                continue
            }
            ts = t
        default:
            continue
        }

        // --- Value (row[1]) ---
        var val interface{}
        raw := row[1]
        switch cfg.ValueType {
        case "int":
            switch x := raw.(type) {
            case json.Number:
                if i, e := x.Int64(); e == nil {
                    val = i
                }
            case float64:
                val = int64(x)
            case int64:
                val = x
            }
        case "float":
            switch x := raw.(type) {
            case json.Number:
                if f, e := x.Float64(); e == nil {
                    val = f
                }
            case float64:
                val = x
            case int64:
                val = float64(x)
            }
        case "string":
            if s, ok := raw.(string); ok {
                val = s
            }
        }

        pts = append(pts, genericPoint{Timestamp: ts, Value: val})
    }

    return pts, nil
}

// queryUserIDsTimeSeries fetches the comma-joined UID lists out of the
// separate "metrics_user_ids" measurement, again using millisecond precision.
func queryUserIDsTimeSeries(
    ip *string,
    interval string,
    from, to time.Time,
) ([]genericPoint, error) {
    // 1) Build WHERE clauses
    where := []string{
        fmt.Sprintf("time >= '%s' AND time <= '%s'",
            from.UTC().Format(time.RFC3339Nano),
            to.UTC().Format(time.RFC3339Nano)),
    }
    if ip != nil && *ip != "" {
        where = append(where, fmt.Sprintf(`"source_ip" = '%s'`, *ip))
    }

    // 2) InfluxQL for the "uids" field
    influxQL := fmt.Sprintf(`
        SELECT LAST("uids")
          FROM "metrics_user_ids"
         WHERE %s
      GROUP BY time(%s) fill(previous)
      ORDER BY time ASC
    `,
        strings.Join(where, " AND "),
        interval,
    )

    // 3) Query with millisecond precision
    res, err := influxClient.Query(client.NewQuery(influxQL, settings.InfluxDB, "ms"))
    if err != nil {
        return nil, err
    }
    if res.Error() != nil || len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
        return nil, nil
    }

    series := res.Results[0].Series[0]
    pts := make([]genericPoint, 0, len(series.Values))

    // 4) Parse each row
    for _, row := range series.Values {
        // Timestamp in ms
        var ts time.Time
        switch v := row[0].(type) {
        case json.Number:
            if ms, e := v.Int64(); e == nil {
                ts = time.Unix(0, ms*int64(time.Millisecond))
            } else {
                continue
            }
        case float64:
            ts = time.Unix(0, int64(v)*int64(time.Millisecond))
        case int64:
            ts = time.Unix(0, v*int64(time.Millisecond))
        case string:
            t, e := time.Parse(time.RFC3339Nano, v)
            if e != nil {
                continue
            }
            ts = t
        default:
            continue
        }

        // The UIDs field is always a string
        var val interface{}
        if s, ok := row[1].(string); ok {
            val = s
        }

        pts = append(pts, genericPoint{Timestamp: ts, Value: val})
    }

    return pts, nil
}

func historicMetricsHandler(w http.ResponseWriter, r *http.Request) {
    // ——— 1) Parse & validate from/to ———
    fromStr, toStr := r.URL.Query().Get("from"), r.URL.Query().Get("to")
    if fromStr == "" || toStr == "" {
        http.Error(w, "missing from/to", http.StatusBadRequest)
        return
    }
    from, err1 := time.Parse(time.RFC3339, fromStr)
    to,   err2 := time.Parse(time.RFC3339, toStr)
    if err1 != nil || err2 != nil || !to.After(from) {
        http.Error(w, "invalid from/to", http.StatusBadRequest)
        return
    }

    // ——— 2) Optional IP filter ———
    var ipPtr *string
    if ip := r.URL.Query().Get("ip"); ip != "" {
        ipPtr = &ip
    }

    // ——— 3) Pick bucket size ———
    interval := pickInterval(from, to)

    // ——— 4) Fetch each series ———
    series := make(map[string]interface{}, len(metricConfigs))
    for _, cfg := range metricConfigs {
        if cfg.Key == "window_user_ids" {
            // read from the separate measurement + field
            pts, err := queryUserIDsTimeSeries(ipPtr, interval, from, to)
            if err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
                return
            }
            series[cfg.Key] = pts
        } else {
            // everything else lives in "metrics.value"
            pts, err := queryTimeSeriesGeneric(cfg, ipPtr, interval, from, to)
            if err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
                return
            }
            series[cfg.Key] = pts  // nil or []genericPoint
        }
    }

    // ——— 5) JSON‐encode response ———
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(struct {
        IP       *string                `json:"ip,omitempty"`
        From     time.Time              `json:"from"`
        To       time.Time              `json:"to"`
        Interval string                 `json:"interval"`
        Series   map[string]interface{} `json:"series"`
    }{
        IP:       ipPtr,
        From:     from,
        To:       to,
        Interval: interval,
        Series:   series,
    })
}