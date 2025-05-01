package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

var ctx = context.Background()

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
}

type FullMetrics struct {
	BytesReceivedBySource    []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"bytes_received_by_source"`
	FailuresBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"failures_by_source"`
	MessagesBySource         []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"messages_by_source"`
	UniqueMMSIBySource       []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"unique_mmsi_by_source"`
	WindowBytesBySource      map[string]int                                       `json:"window_bytes_by_source"`
	WindowMessagesBySource   map[string]int                                       `json:"window_messages_by_source"`
	WindowUniqueUIDsBySource map[string]int                                       `json:"window_unique_uids_by_source"`
	PerDeduplicatedSource    map[string]int                                       `json:"per_deduplicated_source"`
	PerDownsampledMessageID  map[string]int                                       `json:"per_downsampled_message_id"`
	PerMessageID             map[string]int                                       `json:"per_message_id"`
	Top25DeduplicatedPerSource []struct{ SourceIP string `json:"source_ip"`; Count int `json:"count"` } `json:"top25_deduplicated_per_source"`
	TotalBytesForwarded      int                                                  `json:"total_bytes_forwarded"`
	TotalBytesReceived       int                                                  `json:"total_bytes_received"`
	TotalFailures            int                                                  `json:"total_failures"`
	TotalMessages            int                                                  `json:"total_messages"`
	TotalMessagesForwarded   int                                                  `json:"total_messages_forwarded"`
	UptimeSeconds            int                                                  `json:"uptime_seconds"`
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

// Aggregated holds sums over intervals
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
	IPAddress       string        `json:"ip_address"`
	SimpleMetrics                // real‐time
	Aggregated     Aggregated    `json:"aggregated"`
}

var (
	settings      Settings
	metricsLock   sync.RWMutex
	latestMetrics interface{}
	influxClient  client.Client
	redisClient   *redis.Client
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

	// Connect to Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", settings.RedisHost, settings.RedisPort),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Error connecting to Redis: %v", err)
	}

	// Connect to InfluxDB
	influxURL := fmt.Sprintf("http://%s:%d", settings.InfluxHost, settings.InfluxPort)
	influxClient, err = client.NewHTTPClient(client.HTTPConfig{Addr: influxURL})
	if err != nil {
		log.Fatalf("Error creating InfluxDB client: %v", err)
	}
	defer influxClient.Close()

	// Ensure InfluxDB exists
	if err := ensureDatabase(settings.InfluxDB); err != nil {
		log.Fatalf("Could not create InfluxDB database %q: %v", settings.InfluxDB, err)
	}

	// Start polling & writing loop
	go ingestMetricsLoop()

	// Handlers
	http.HandleFunc("/metrics/ingester", metricsIngesterHandler)
	http.HandleFunc("/metrics/bysource", handleMetricsBySource)
	http.Handle("/metrics/", http.StripPrefix("/metrics/", http.FileServer(http.Dir("web"))))

	// Listen
	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func ensureDatabase(db string) error {
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE \"%s\"", db), "", "")
	resp, err := influxClient.Query(q)
	if err != nil {
		return err
	}
	return resp.Error()
}

// Poll + write once per second
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
	blob, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	var full FullMetrics
	if err := json.Unmarshal(blob, &full); err != nil {
		return err
	}

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: settings.InfluxDB,
	})
	if err != nil {
		return err
	}
	add := func(tags map[string]string, fields map[string]interface{}) {
		pt, err := client.NewPoint("metrics", tags, fields, time.Now())
		if err == nil {
			bp.AddPoint(pt)
		}
	}

	// --- write all fields as before ---
	// (omitted for brevity; identical to prior code)

	return influxClient.Write(bp)
}

// handleMetricsBySource now includes cached Aggregated
func handleMetricsBySource(w http.ResponseWriter, r *http.Request) {
	ip := r.URL.Query().Get("ipaddress")
	if ip == "" {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			ip = strings.TrimSpace(strings.Split(xff, ",")[0])
		} else if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			ip = host
		} else {
			ip = r.RemoteAddr
		}
	}

	// 1) build real-time fields
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

	rt := SimpleMetrics{}
	for _, m := range full.BytesReceivedBySource {
		if m.SourceIP == ip {
			rt.BytesReceived = m.Count
		}
	}
	for _, m := range full.MessagesBySource {
		if m.SourceIP == ip {
			rt.Messages = m.Count
		}
	}
	for _, m := range full.UniqueMMSIBySource {
		if m.SourceIP == ip {
			rt.UniqueMMSI = m.Count
		}
	}
	for _, m := range full.FailuresBySource {
		if m.SourceIP == ip {
			rt.Failures = m.Count
		}
	}
	rt.Deduplicated = full.PerDeduplicatedSource[ip]
	rt.WindowBytes = full.WindowBytesBySource[ip]
	rt.WindowMessages = full.WindowMessagesBySource[ip]
	rt.WindowUniqueUIDs = full.WindowUniqueUIDsBySource[ip]

	// 2) try Redis cache for aggregated
	cacheKey := fmt.Sprintf("agg:%s", ip)
	var agg Aggregated
	if data, err := redisClient.Get(ctx, cacheKey).Bytes(); err == nil {
		json.Unmarshal(data, &agg)
	} else {
		// not cached → query Influx for each metric and interval
		agg = queryAggregates(ip)
		// cache it
		if blob, err := json.Marshal(agg); err == nil {
			redisClient.Set(ctx, cacheKey, blob, time.Duration(settings.CacheTime)*time.Second)
		}
	}

	// 3) respond
	resp := ResponseMetrics{
		IPAddress:     ip,
		SimpleMetrics: rt,
		Aggregated:    agg,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// queryAggregates runs InfluxQL for each metric over intervals
func queryAggregates(ip string) Aggregated {
	var a Aggregated
	queries := []struct {
		field     string
		interval  string
		setter    func(int)
	}{
		{"bytes_received", "1m", func(v int) { a.BytesReceivedMinute = v }},
		{"messages",      "1m", func(v int) { a.MessagesMinute = v }},
		{"failures",      "1m", func(v int) { a.FailuresMinute = v }},
		{"unique_mmsi",   "1m", func(v int) { a.UniqueMMSIMinute = v }},

		{"bytes_received", "1h", func(v int) { a.BytesReceivedHour = v }},
		{"messages",      "1h", func(v int) { a.MessagesHour = v }},
		{"failures",      "1h", func(v int) { a.FailuresHour = v }},
		{"unique_mmsi",   "1h", func(v int) { a.UniqueMMSIHour = v }},

		{"bytes_received", "24h", func(v int) { a.BytesReceivedDay = v }},
		{"messages",      "24h", func(v int) { a.MessagesDay = v }},
		{"failures",      "24h", func(v int) { a.FailuresDay = v }},
		{"unique_mmsi",   "24h", func(v int) { a.UniqueMMSIDay = v }},
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
		// series[0].values[0][1] is the SUM
		if sum, ok := res.Results[0].Series[0].Values[0][1].(json.Number); ok {
			if iv, err := sum.Int64(); err == nil {
				q.setter(int(iv))
			}
		}
	}

	return a
}
