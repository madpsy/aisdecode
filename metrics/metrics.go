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

	client "github.com/influxdata/influxdb1-client/v2"
)

type Settings struct {
	IngestHost string `json:"ingest_host"`
	IngestPort int    `json:"ingest_port"`
	InfluxHost string `json:"influx_host"`
	InfluxPort int    `json:"influx_port"`
	InfluxDB   string `json:"influx_db"`
	ListenPort int    `json:"listen_port"`
	Debug      bool   `json:"debug"`
}

// FullMetrics matches the JSON from /metrics
type FullMetrics struct {
	BytesReceivedBySource      []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"bytes_received_by_source"`
	FailuresBySource           []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"failures_by_source"`
	MessagesBySource           []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"messages_by_source"`
	UniqueMMSIBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"unique_mmsi_by_source"`
	WindowBytesBySource        map[string]int                                       `json:"window_bytes_by_source"`
	WindowMessagesBySource     map[string]int                                       `json:"window_messages_by_source"`
	WindowUniqueUIDsBySource   map[string]int                                       `json:"window_unique_uids_by_source"`
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

type ResponseMetrics struct {
	IPAddress     string        `json:"ip_address"`
	SimpleMetrics               // embedded
}

var (
	settings      Settings
	metricsLock   sync.RWMutex
	latestMetrics interface{}
	influxClient  client.Client
)

func main() {
	// 1) Load settings
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}

	// 2) Connect to InfluxDB
	influxURL := fmt.Sprintf("http://%s:%d", settings.InfluxHost, settings.InfluxPort)
	influxClient, err = client.NewHTTPClient(client.HTTPConfig{Addr: influxURL})
	if err != nil {
		log.Fatalf("Error creating InfluxDB client: %v", err)
	}
	defer influxClient.Close()

	// 3) Ensure database exists
	if err := ensureDatabase(settings.InfluxDB); err != nil {
		log.Fatalf("Could not create InfluxDB database %q: %v", settings.InfluxDB, err)
	}

	// 4) Start ingest loop
	go ingestMetricsLoop()

	// 5) HTTP handlers
	http.HandleFunc("/metrics/ingester", metricsIngesterHandler)
	http.HandleFunc("/metrics/bysource", handleMetricsBySource)
	http.Handle("/metrics/", http.StripPrefix("/metrics/", http.FileServer(http.Dir("web"))))

	// 6) Listen
	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// ensureDatabase issues CREATE DATABASE if it doesn't already exist.
func ensureDatabase(db string) error {
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE \"%s\"", db), "", "")
	resp, err := influxClient.Query(q)
	if err != nil {
		return err
	}
	if resp.Error() != nil {
		return resp.Error()
	}
	return nil
}

// ingestMetricsLoop polls external /metrics
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
	        if err := writeMetricsToInfluxDB(m); err != nil {
         	   log.Printf("Error writing polled metrics to InfluxDB: %v", err)
       	        }
	}
}

// metricsIngesterHandler writes to Influx, then masks and returns JSON
func metricsIngesterHandler(w http.ResponseWriter, r *http.Request) {
	metricsLock.RLock()
	defer metricsLock.RUnlock()

	if latestMetrics == nil {
		http.Error(w, "no metrics yet", http.StatusNoContent)
		return
	}

	// Mask IPs in the raw JSON
	blob, _ := json.Marshal(latestMetrics)
	re := regexp.MustCompile(`\b(\d{1,3})\.(\d{1,3})\.(\d{1,3}\.\d{1,3})\b`)
	masked := re.ReplaceAll(blob, []byte("x.x.$3"))

	w.Header().Set("Content-Type", "application/json")
	w.Write(masked)
}

// writeMetricsToInfluxDB unmarshals and writes every field
func writeMetricsToInfluxDB(metrics interface{}) error {
	blob, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("error marshaling metrics: %v", err)
	}
	var full FullMetrics
	if err := json.Unmarshal(blob, &full); err != nil {
		return fmt.Errorf("error parsing FullMetrics: %v", err)
	}

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: settings.InfluxDB,
	})
	if err != nil {
		return fmt.Errorf("error creating batch points: %v", err)
	}

	add := func(tags map[string]string, fields map[string]interface{}) {
		p, err := client.NewPoint("metrics", tags, fields, time.Now())
		if err != nil {
			log.Printf("Point error tags=%v fields=%v: %v", tags, fields, err)
			return
		}
		bp.AddPoint(p)
	}

	// --- Per-source ---
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

	// --- Windowed per-source ---
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

	// --- Deduplicated per-source ---
	for ip, v := range full.PerDeduplicatedSource {
		add(map[string]string{"source_ip": ip, "metric": "per_deduplicated"},
			map[string]interface{}{"value": v})
	}

	// --- Per-message ID ---
	for mid, v := range full.PerDownsampledMessageID {
		add(map[string]string{"message_id": mid, "metric": "per_downsampled_message_id"},
			map[string]interface{}{"value": v})
	}
	for mid, v := range full.PerMessageID {
		add(map[string]string{"message_id": mid, "metric": "per_message_id"},
			map[string]interface{}{"value": v})
	}

	// --- Totals & Uptime ---
	for _, t := range []struct {
		metric string
		value  int
	}{
		{"total_bytes_forwarded", full.TotalBytesForwarded},
		{"total_bytes_received", full.TotalBytesReceived},
		{"total_failures", full.TotalFailures},
		{"total_messages", full.TotalMessages},
		{"total_messages_forwarded", full.TotalMessagesForwarded},
		{"uptime_seconds", full.UptimeSeconds},
	} {
		add(map[string]string{"metric": t.metric},
			map[string]interface{}{"value": t.value})
	}

	return influxClient.Write(bp)
}

// handleMetricsBySource unchanged except inline lookups
func handleMetricsBySource(w http.ResponseWriter, r *http.Request) {
	ip := r.URL.Query().Get("ipaddress")
	if ip == "" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			ip = strings.TrimSpace(parts[0])
		} else {
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

	blob, _ := json.Marshal(raw)
	var full FullMetrics
	json.Unmarshal(blob, &full)

	flat := SimpleMetrics{}
	for _, m := range full.BytesReceivedBySource {
		if m.SourceIP == ip {
			flat.BytesReceived = m.Count
			break
		}
	}
	for _, m := range full.MessagesBySource {
		if m.SourceIP == ip {
			flat.Messages = m.Count
			break
		}
	}
	for _, m := range full.UniqueMMSIBySource {
		if m.SourceIP == ip {
			flat.UniqueMMSI = m.Count
			break
		}
	}
	for _, m := range full.FailuresBySource {
		if m.SourceIP == ip {
			flat.Failures = m.Count
			break
		}
	}
	flat.Deduplicated = full.PerDeduplicatedSource[ip]
	flat.WindowBytes = full.WindowBytesBySource[ip]
	flat.WindowMessages = full.WindowMessagesBySource[ip]
	flat.WindowUniqueUIDs = full.WindowUniqueUIDsBySource[ip]

	resp := ResponseMetrics{IPAddress: ip, SimpleMetrics: flat}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
