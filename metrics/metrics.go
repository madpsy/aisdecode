package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
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
	IPAddress      string        `json:"ip_address"`
	SimpleMetrics               // real-time
	Aggregated    Aggregated    `json:"aggregated"`
	WindowUserIDs []string      `json:"window_user_ids"`
}

// New types for historic endpoint
type HistoricSeriesPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     int64     `json:"value"`
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

	// Ensure DB
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

	// Serve
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

// ingestMetricsLoop and writeMetricsToInfluxDB unchanged from original…

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
		if err != nil {
			log.Printf("Point error tags=%v fields=%v: %v", tags, fields, err)
			return
		}
		bp.AddPoint(pt)
	}

	// Per‑source, windowed, per‑message, totals & uptime (same as original)…
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

	for _, t := range []struct{ metric string; value int }{
		{"total_bytes_forwarded", full.TotalBytesForwarded},
		{"total_bytes_received", full.TotalBytesReceived},
		{"total_failures", full.TotalFailures},
		{"total_messages", full.TotalMessages},
		{"total_messages_forwarded", full.TotalMessagesForwarded},
		{"uptime_seconds", full.UptimeSeconds},
	} {
		add(map[string]string{"metric": t.metric}, map[string]interface{}{"value": t.value})
	}

	return influxClient.Write(bp)
}

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

	// Real-time metrics
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
	windowIDs := full.WindowUserIDsBySource[ip]
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

	// Aggregated (Redis-cached)
	cacheKey := fmt.Sprintf("agg:%s", ip)
	var agg Aggregated
	if data, err := redisClient.Get(ctx, cacheKey).Bytes(); err == nil {
		json.Unmarshal(data, &agg)
	} else {
		agg = queryAggregates(ip)
		if blob, err := json.Marshal(agg); err == nil {
			redisClient.Set(ctx, cacheKey, blob, time.Duration(settings.CacheTime)*time.Second)
		}
	}

	resp := ResponseMetrics{
		IPAddress:      ip,
		SimpleMetrics:  rt,
		Aggregated:     agg,
		WindowUserIDs:  windowIDs,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func queryAggregates(ip string) Aggregated {
	var a Aggregated
	queries := []struct {
		field    string
		interval string
		setter   func(int)
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
		if sum, ok := res.Results[0].Series[0].Values[0][1].(json.Number); ok {
			if iv, err := sum.Int64(); err == nil {
				q.setter(int(iv))
			}
		}
	}

	return a
}

// ---- New helper & handler for historic data ----

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

func queryTimeSeries(ip *string, field, interval string, from, to time.Time) ([]HistoricSeriesPoint, error) {
    var where []string
    where = append(where, fmt.Sprintf("time >= '%s' AND time <= '%s'",
        from.UTC().Format(time.RFC3339Nano),
        to.UTC().Format(time.RFC3339Nano)))
    where = append(where, fmt.Sprintf(`"metric" = '%s'`, field))
    if ip != nil && *ip != "" {
        where = append(where, fmt.Sprintf(`"source_ip" = '%s'`, *ip))
    }

    influxQL := fmt.Sprintf(`
        SELECT SUM("value")
        FROM "metrics"
        WHERE %s
        GROUP BY time(%s) fill(0)
        ORDER BY time ASC
    `, strings.Join(where, " AND "), interval)

    q := client.NewQuery(influxQL, settings.InfluxDB, "ns")
    res, err := influxClient.Query(q)
    if res != nil && len(res.Results) > 0 {
        log.Printf("Influx returned: %+v", res.Results[0])
    }
    if err != nil {
        return nil, err
    }
    if res.Error() != nil || len(res.Results[0].Series) == 0 {
        return nil, nil
    }

    series := res.Results[0].Series[0]
    pts := make([]HistoricSeriesPoint, 0, len(series.Values))
    for _, row := range series.Values {
        // Normalize the time value to a string
        rawTime := row[0]
        var timeStr string
        switch tval := rawTime.(type) {
        case string:
            timeStr = tval
        case json.Number:
            timeStr = tval.String()
        default:
            timeStr = fmt.Sprint(tval)
        }

        t, err := time.Parse(time.RFC3339Nano, timeStr)
        if err != nil {
            continue
        }

        // Normalize the value to int64
        var v int64
        switch num := row[1].(type) {
        case json.Number:
            v, _ = num.Int64()
        case float64:
            v = int64(num)
        case int64:
            v = num
        default:
            if tmp, err := strconv.ParseInt(fmt.Sprint(num), 10, 64); err == nil {
                v = tmp
            }
        }

        pts = append(pts, HistoricSeriesPoint{Timestamp: t, Value: v})
    }
    return pts, nil
}

func historicMetricsHandler(w http.ResponseWriter, r *http.Request) {
	fromStr, toStr := r.URL.Query().Get("from"), r.URL.Query().Get("to")
	if fromStr == "" || toStr == "" {
		http.Error(w, "missing from or to", http.StatusBadRequest)
		return
	}
	from, err1 := time.Parse(time.RFC3339, fromStr)
	to, err2   := time.Parse(time.RFC3339, toStr)
	if err1 != nil || err2 != nil || !to.After(from) {
		http.Error(w, "invalid from/to", http.StatusBadRequest)
		return
	}

	var ipPtr *string
	if ip := r.URL.Query().Get("ip"); ip != "" {
		ipPtr = &ip
	}

	interval := pickInterval(from, to)
	fields := []string{"bytes_received", "messages", "failures", "unique_mmsi"}
	series := make(map[string][]HistoricSeriesPoint, len(fields))

	for _, field := range fields {
		pts, err := queryTimeSeries(ipPtr, field, interval, from, to)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		series[field] = pts
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		IP       *string                          `json:"ip,omitempty"`
		From     time.Time                        `json:"from"`
		To       time.Time                        `json:"to"`
		Interval string                           `json:"interval"`
		Series   map[string][]HistoricSeriesPoint `json:"series"`
	}{
		IP:       ipPtr,
		From:     from,
		To:       to,
		Interval: interval,
		Series:   series,
	})
}
