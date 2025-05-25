package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	//_ "net/http/pprof"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/madpsy/aisdecode/ingester/decoders"
	decoders "github.com/madpsy/aisdecode/ingester/decoders"
)

var cfg Config

var (
	startTime = time.Now()
)

var cfgDir string

var failedDecodeLogger *log.Logger

// per-IP sliding-window rate limiter
const (
	rateLimitWindow = time.Minute // 60s window
	rateLimitCount  = 1200        // max 1200 messages/window
)

type rateLimiter struct {
	mu         sync.Mutex
	timestamps []time.Time
}

var (
	rlMu         sync.Mutex
	rateLimiters = make(map[string]*rateLimiter)
)

// allow returns true if ip is under the RATE_LIMIT_COUNT per RATE_LIMIT_WINDOW
func allow(ip string) bool {
	// retrieve-or-create limiter
	rlMu.Lock()
	lim, ok := rateLimiters[ip]
	if !ok {
		lim = &rateLimiter{}
		rateLimiters[ip] = lim
	}
	rlMu.Unlock()

	lim.mu.Lock()
	defer lim.mu.Unlock()
	now := time.Now()
	// drop timestamps older than window
	i := 0
	for ; i < len(lim.timestamps); i++ {
		if now.Sub(lim.timestamps[i]) <= rateLimitWindow {
			break
		}

	}
	lim.timestamps = lim.timestamps[i:]
	if len(lim.timestamps) >= rateLimitCount {
		return false
	}
	// record this event
	lim.timestamps = append(lim.timestamps, now)
	return true
}

// how many events we keep per “keyed” counter
const maxPerKeyEvents = 10000

// how many events we keep in the global ring buffers
const globalEvents = 2048

// bufPool for UDP reads
var bufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 2048) },
}

// packetPool for UDPPacket reuse
var packetPool = sync.Pool{
	New: func() interface{} { return new(UDPPacket) },
}

// jsonBufPool for JSON encoding of StreamMessage
var jsonBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

var msgIDCache = struct {
	sync.RWMutex
	m map[uint8]string
}{m: make(map[uint8]string)}

var userIDCache = struct {
	sync.RWMutex
	m map[uint32]string
}{m: make(map[uint32]string)}

var (
	msgWindowBySource   = make(map[string]*FixedWindowCounter)
	bytesWindowBySource = make(map[string]*FixedWindowBytesCounter)
	failWindowBySource  = make(map[string]*FixedWindowCounter)
	usersWindowBySource = make(map[string]map[string]struct{})
	prevWindowUserIDs   = make(map[string][]string)
	prevWindowBySource  = struct {
		Msgs  map[string]int64
		Bytes map[string]int64
		Fails map[string]int64
		Uids  map[string]int64
	}{
		Msgs:  make(map[string]int64),
		Bytes: make(map[string]int64),
		Fails: make(map[string]int64),
		Uids:  make(map[string]int64),
	}
)

var prevClientWindows []map[string]interface{}

// FixedWindowCounter counts events in a fixed-duration window and resets at each tick.
type FixedWindowCounter struct {
	mu    sync.Mutex
	count int64
}

func NewFixedWindowCounter() *FixedWindowCounter {
	return &FixedWindowCounter{}
}

func (c *FixedWindowCounter) AddEvent() {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()
}

func (c *FixedWindowCounter) Count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *FixedWindowCounter) Reset() {
	c.mu.Lock()
	c.count = 0
	c.mu.Unlock()
}

func fnvHash(message string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(message))
	return h.Sum32()
}

// FixedWindowBytesCounter sums bytes in a fixed-duration window and resets at each tick.
type FixedWindowBytesCounter struct {
	mu  sync.Mutex
	sum int64
}

func NewFixedWindowBytesCounter() *FixedWindowBytesCounter {
	return &FixedWindowBytesCounter{}
}

func (c *FixedWindowBytesCounter) Add(n int64) {
	c.mu.Lock()
	c.sum += n
	c.mu.Unlock()
}

func (c *FixedWindowBytesCounter) Sum() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sum
}

func (c *FixedWindowBytesCounter) Reset() {
	c.mu.Lock()
	c.sum = 0
	c.mu.Unlock()
}

type fragmentEntry struct {
	parts     []string  // slot 0 == fragment #1, etc
	received  int       // how many slots filled
	firstSeen time.Time // when we saw fragment #1
	ts        time.Time // last‐touched timestamp
}

var (
	fragmentBufMu sync.Mutex
	fragmentBuf   = make(map[string]*fragmentEntry)
)

func addFragment(raw string) (bool, string) {
	raw = strings.TrimSpace(raw)
	f := strings.Split(raw, ",")
	if len(f) < 6 {
		return true, raw
	}
	talker := f[0]
	total, err1 := strconv.Atoi(f[1])
	part, err2 := strconv.Atoi(f[2])
	seqID := f[3]
	channel := f[4]
	if err1 != nil || err2 != nil || total <= 1 {
		return true, raw
	}

	key := fmt.Sprintf("%s|%s|%s", talker, seqID, channel)

	fragmentBufMu.Lock()
	defer fragmentBufMu.Unlock()

	e, ok := fragmentBuf[key]
	if !ok {
		now := time.Now()
		e = &fragmentEntry{
			parts:     make([]string, total),
			firstSeen: now,
			ts:        now,
		}
		fragmentBuf[key] = e
		if debugFlag {
			log.Printf("[DEBUG] started assembling %d-part message %q", total, key)
		}
	}
	if e.parts[part-1] == "" {
		e.parts[part-1] = raw
		e.received++
		e.ts = time.Now()
		if debugFlag && e.received == 1 {
			log.Printf("[DEBUG] received fragment %d/%d for %q", part, total, key)
		}
	}
	e.ts = time.Now()

	if e.received == total {
		if debugFlag {
			dur := time.Since(e.firstSeen)
			log.Printf("[DEBUG] assembled %d/%d fragments for %q in %v", total, total, key, dur)
		}
		joined := strings.Join(e.parts, "\r\n")
		delete(fragmentBuf, key)
		return true, joined
	}
	return false, ""
}

// cleanupFragments drops any incomplete groups older than ttl
func cleanupFragments(ttl time.Duration) {
	fragmentBufMu.Lock()
	defer fragmentBufMu.Unlock()
	now := time.Now()
	for k, e := range fragmentBuf {
		if now.Sub(e.ts) > ttl {
			delete(fragmentBuf, k)
		}
	}
}

// UDPPacket holds raw data, source IP and the port it was received on
type UDPPacket struct {
	raw      []byte
	sourceIP string
	port     int
}

// StreamMessage is what we send to TCP clients
type StreamMessage struct {
	Message     interface{} `json:"message"`
	SourceIP    string      `json:"source_ip,omitempty"`
	Timestamp   string      `json:"timestamp"`
	ShardID     int         `json:"shard_id"`
	RawSentence string      `json:"raw_sentence"`
	UDPPort     int         `json:"udp_port"`
	DedupedPort *int        `json:"deduped_port,omitempty"`
	IsDuplicate bool        `json:"is_duplicate"`
}

// handshakeReq is the JSON clients must send on connect
type handshakeReq struct {
	Shards      []int  `json:"shards"`
	Description string `json:"description"`
	Port        int    `json:"port"`
}

// StreamClient holds a connected client's state and send-metrics
type StreamClient struct {
	conn          net.Conn
	shards        []int
	description   string
	ip            string
	port          int
	mu            sync.Mutex
	bytesSent     int64
	messagesSent  int64
	messageWindow *FixedWindowCounter
	bytesWindow   *FixedWindowBytesCounter
}

type UDPDestination struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Shards      []int  `json:"shards"`
	Description string `json:"description"`
}

type UDPDestinationMetrics struct {
	Destination  string `json:"destination"`
	Description  string `json:"description"`
	Shards       []int  `json:"shards"` // Added field for shard IDs
	MessagesSent int64  `json:"messages_sent"`
	BytesSent    int64  `json:"bytes_sent"`
}

var udpDestinationMetrics = map[string]*UDPDestinationMetrics{}

type Config struct {
	UDPListenPort              int              `json:"udp_listen_port"`
	UDPDedicatedPorts          string           `json:"udp_dedicated_ports"`
	Destinations               []UDPDestination `json:"udp_destinations"`
	MetricWindowSize           int              `json:"metric_window_size"`
	HTTPPort                   int              `json:"http_port"`
	NumWorkers                 int              `json:"num_workers"`
	DownsampleWindow           int              `json:"downsample_window"`
	DeduplicationWindowMs      int              `json:"deduplication_window_ms"`
	DeduplicationForwardButTag bool             `json:"deduplication_forward_but_tag"`
	WebPath                    string           `json:"web_path"`
	Debug                      bool             `json:"debug"`
	IncludeSource              bool             `json:"include_source"`
	StreamPort                 int              `json:"stream_port"`
	StreamShards               int              `json:"stream_shards"`
	MQTTServer                 string           `json:"mqtt_server"`
	MQTTTLS                    bool             `json:"mqtt_tls"`
	MQTTAuth                   string           `json:"mqtt_auth"`
	MQTTTopic                  string           `json:"mqtt_topic"`
	DownsampleMessageTypes     []string         `json:"downsample_message_types"`
	BlockedIPs                 []string         `json:"blocked_ips"`
	FailedDecodeLog            string           `json:"failed_decode_log"`
	ReceiversBaseURL           string           `json:"receivers_base_url"`
}

var (
	udpPort                    int
	destinations               string
	metricWindowSize           time.Duration
	downsampleWindow           time.Duration
	deduplicationWindowMs      int
	httpPort                   int
	numWorkers                 int
	webPath                    string
	debugFlag                  bool
	includeSource              bool
	streamPort                 int
	streamShards               int
	mqttServer                 string
	mqttTLS                    bool
	mqttAuth                   string
	mqttTopic                  string
	dedupWindow                time.Duration
	windowSize                 int
	blockedIPs                 map[string]struct{}
	deduplicationForwardButTag bool
	receiversBaseURL           string
)

// Receiver represents a receiver from the /admin/receivers endpoint
type Receiver struct {
	ID            int                    `json:"id"`
	LastUpdated   time.Time              `json:"lastupdated"`
	Description   string                 `json:"description"`
	Latitude      float64                `json:"latitude"`
	Longitude     float64                `json:"longitude"`
	Name          string                 `json:"name"`
	URL           *string                `json:"url,omitempty"`
	IPAddress     string                 `json:"ip_address,omitempty"`
	Email         string                 `json:"email"`
	Notifications bool                   `json:"notifications"`
	Messages      int                    `json:"messages"`
	UDPPort       *int                   `json:"udp_port,omitempty"`
	MessageStats  map[string]MessageStat `json:"message_stats"`
	LastSeen      *time.Time             `json:"lastseen,omitempty"`
}

// MessageStat represents message statistics for a receiver
type MessageStat struct {
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int       `json:"message_count"`
}

var (
	receiversMutex sync.RWMutex
	receiversMap   = make(map[int]Receiver) // Map of receiver ID to Receiver
	portToIDMap    = make(map[int]int)      // Map of UDP port to receiver ID
)

func initDecodeLogger() error {
	if cfg.FailedDecodeLog == "" {
		return nil
	}

	var path string
	if filepath.IsAbs(cfg.FailedDecodeLog) {
		path = cfg.FailedDecodeLog
	} else {
		path = filepath.Join(cfgDir, cfg.FailedDecodeLog)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening failed_decode_log %q: %w", path, err)
	}
	failedDecodeLogger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	return nil
}

func settingsHandler(w http.ResponseWriter, r *http.Request) {
	cfgPath := filepath.Join(cfgDir, "settings.json")

	switch r.Method {
	case http.MethodGet:
		// Serve the raw settings.json file
		http.ServeFile(w, r, cfgPath)

	case http.MethodPut:
		// Read incoming body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		// Validate that it is valid JSON
		var tmp interface{}
		if err := json.Unmarshal(body, &tmp); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		// Unmarshal into the Config struct
		var newCfg Config
		if err := json.Unmarshal(body, &newCfg); err != nil {
			http.Error(w, "failed to unmarshal settings", http.StatusInternalServerError)
			return
		}

		// Validate the blocked IPs field (if present)
		for _, ip := range newCfg.BlockedIPs {
			// You can add more validation if necessary, e.g., check if the IP is a valid format.
			if net.ParseIP(ip) == nil {
				http.Error(w, "invalid IP in blocked_ips", http.StatusBadRequest)
				return
			}
		}

		// Save the new configuration to disk
		newCfgData, err := json.MarshalIndent(newCfg, "", "  ")
		if err != nil {
			http.Error(w, "failed to marshal updated settings", http.StatusInternalServerError)
			return
		}

		if err := ioutil.WriteFile(cfgPath, newCfgData, 0644); err != nil {
			http.Error(w, "failed to write settings", http.StatusInternalServerError)
			return
		}

		// Update global config variable
		cfg = newCfg

		// Rebuild the blocked IPs set after settings update
		blockedIPs = make(map[string]struct{})
		for _, ip := range cfg.BlockedIPs {
			blockedIPs[ip] = struct{}{}
		}

		if err := initDecodeLogger(); err != nil {
			log.Printf("warning: could not open failed_decode_log after settings update: %v", err)
		}

		// Success, no content to return
		w.WriteHeader(http.StatusNoContent)

		go func() {
			// Wait for 1 second before exiting
			time.Sleep(2 * time.Second)
			log.Println("Settings successfully written. Exiting program.")
			os.Exit(0)
		}()

	default:
		// Only GET and PUT are allowed
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// Globals for streaming clients
var (
	clients   []*StreamClient
	clientsMu sync.Mutex
)

var mqttClient mqtt.Client

// Metrics for shards
var (
	messagesPerShard = map[int]int64{}
	userIDsPerShard  = map[int]map[string]struct{}{}
	userIDsPerSource = map[string]map[string]struct{}{}
)

// Cumulative totals
var (
	totalMessages                    int64
	messageIDTotals                  = map[string]int64{}
	userIDTotals                     = map[string]int64{}
	totalFailures                    int64
	failureSourceTotals              = map[string]int64{}
	totalDownsampled                 int64
	downsampledMessageTypeTotals     = map[string]int64{}
	downsampledUserIDTotals          = map[string]int64{}
	downsampledPerUserMessageIDCount = map[string]map[string]int64{}
	totalSourceTotals                = map[string]int64{}
	perUserMessageIDCount            = map[string]map[string]int64{}

	totalDeduplicated          int64
	deduplicatedUserIDTotals   = map[string]int64{}
	deduplicatedSourceTotals   = map[string]int64{}
	dedupPerUserMessageIDCount = map[string]map[string]int64{}

	bytesReceivedTotals = map[string]int64{}
	totalBytesForwarded int64
	totalBytesReceived  int64
	totalForwarded      int64

	blockedIPCounters = make(map[string]*FixedWindowCounter)
)

// Fixed-window counters
var (
	totalCounter                   *FixedWindowCounter
	messageIDCounters              = map[string]*FixedWindowCounter{}
	userIDCounters                 = map[string]*FixedWindowCounter{}
	failureCounter                 *FixedWindowCounter
	failureSourceCounters          = map[string]*FixedWindowCounter{}
	downsampledCounter             *FixedWindowCounter
	downsampledMessageTypeCounters = map[string]*FixedWindowCounter{}
	downsampledUserIDCounters      = map[string]*FixedWindowCounter{}
	totalSourceCounters            *FixedWindowCounter

	dedupCounter        *FixedWindowCounter
	dedupUserIDCounters = map[string]*FixedWindowCounter{}
	dedupSourceCounters = map[string]*FixedWindowCounter{}

	forwardedCounter     *FixedWindowCounter
	bytesReceivedWindow  *FixedWindowBytesCounter
	bytesForwardedWindow *FixedWindowBytesCounter
)

var metricsMu sync.RWMutex

var (
	downsampleTypes = make(map[string]bool)
	downMu          sync.Mutex
	lastForward     = map[string]map[string]time.Time{}
)

// DedupInfo stores information about a deduplicated message
// DedupInfo stores information about a deduplicated message
type DedupInfo struct {
	Timestamp time.Time
	Port      int
}

var (
	dedupMu   sync.Mutex
	lastDedup = map[uint32]DedupInfo{}
)

var previousPeriodMetrics struct {
	windowMsgs           int64
	windowFailures       int64
	windowDownsampled    int64
	windowDedup          int64
	windowForwarded      int64
	windowBytesReceived  int64
	windowBytesForwarded int64
}

func cleanupMetrics() {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	// prune zero-count totals
	for k, v := range userIDTotals {
		if v == 0 {
			delete(userIDTotals, k)
		}
	}
	for k, v := range messageIDTotals {
		if v == 0 {
			delete(messageIDTotals, k)
		}
	}
	for k, v := range totalSourceTotals {
		if v == 0 {
			delete(totalSourceTotals, k)
		}
	}
}

func clientsHandler(w http.ResponseWriter, r *http.Request) {
	// Lock the clients slice to ensure safe access
	clientsMu.Lock()
	defer clientsMu.Unlock()

	// Prepare a response structure
	connectedClients := []map[string]interface{}{}

	// Loop through each client and extract their details
	for _, c := range clients {
		c.mu.Lock()
		clientInfo := map[string]interface{}{
			"ip":          c.ip,
			"shards":      c.shards,
			"description": c.description,
			"port":        c.port,
		}
		connectedClients = append(connectedClients, clientInfo)
		c.mu.Unlock()
	}

	response := map[string]interface{}{
		"configured_shards": streamShards,
		"clients":           connectedClients,
	}

	// Set the response content type to JSON
	w.Header().Set("Content-Type", "application/json")
	// Return the connected clients as JSON
	json.NewEncoder(w).Encode(response)
}

func cleanupDeduplicationState() {
	dedupMu.Lock()
	defer dedupMu.Unlock()

	now := time.Now()

	// Log the number of entries in lastDedup before cleanup
	// log.Printf("Before cleanup: lastDedup size = %d", len(lastDedup))

	// Track and log cleanup of expired entries in lastDedup map
	expiredDedupCount := 0
	notExpiredCount := 0
	for raw, info := range lastDedup {
		if now.Sub(info.Timestamp) > dedupWindow {
			delete(lastDedup, raw)
			expiredDedupCount++
		} else {
			notExpiredCount++
		}
	}

	// Log how many entries were pruned and how many are still valid
	// log.Printf("Cleanup: Removed %d expired entries from lastDedup, %d entries are still valid", expiredDedupCount, notExpiredCount)

	// Log how many entries are left in lastDedup after cleanup
	// log.Printf("After cleanup: lastDedup size = %d", len(lastDedup))

	// Log total size of lastDedup map
	if len(lastDedup) > 100000 { // Arbitrary threshold to indicate if map is getting too large
		log.Printf("WARNING: lastDedup has grown significantly. Total entries: %d", len(lastDedup))
	}
}

func initializeUDPDestinationMetrics() {
	for _, dest := range cfg.Destinations {
		if len(dest.Shards) == 0 { // Skip destinations with no shards
			log.Printf("Skipping UDP destination %s:%d because it has no shards configured.", dest.Host, dest.Port)
			continue
		}

		destStr := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
		udpDestinationMetrics[destStr] = &UDPDestinationMetrics{
			Destination:  destStr,
			Description:  dest.Description,
			Shards:       dest.Shards,
			MessagesSent: 0,
			BytesSent:    0,
		}
	}
}

// startMetricsReset resets all fixed-window counters every metricWindowSize period.
// at top‐level, alongside your other “prev” globals:
func startMetricsReset() {
	ticker := time.NewTicker(metricWindowSize)
	defer ticker.Stop()

	for range ticker.C {
		// 1) Snapshot & reset global/window‐by‐source metrics
		metricsMu.Lock()
		// — snapshot the globals —
		previousPeriodMetrics.windowMsgs = totalCounter.Count()
		previousPeriodMetrics.windowFailures = failureCounter.Count()
		previousPeriodMetrics.windowDownsampled = downsampledCounter.Count()
		previousPeriodMetrics.windowDedup = dedupCounter.Count()
		previousPeriodMetrics.windowForwarded = forwardedCounter.Count()
		previousPeriodMetrics.windowBytesReceived = bytesReceivedWindow.Sum()
		previousPeriodMetrics.windowBytesForwarded = bytesForwardedWindow.Sum()

		// — prepare per‐source snapshots —
		prevWindowBySource.Msgs = make(map[string]int64, len(msgWindowBySource))
		prevWindowBySource.Bytes = make(map[string]int64, len(bytesWindowBySource))
		prevWindowBySource.Fails = make(map[string]int64, len(failWindowBySource))
		prevWindowBySource.Uids = make(map[string]int64, len(usersWindowBySource))

		// reset our slice‐of‐IDs snapshot too:
		prevWindowUserIDs = make(map[string][]string, len(usersWindowBySource))

		// — snapshot counts & actual IDs, then reset each windowed structure —
		for src, ctr := range msgWindowBySource {
			if cnt := ctr.Count(); cnt > 0 {
				prevWindowBySource.Msgs[src] = cnt
			}
			ctr.Reset()
		}
		for src, bctr := range bytesWindowBySource {
			if sum := bctr.Sum(); sum > 0 {
				prevWindowBySource.Bytes[src] = sum
			}
			bctr.Reset()
		}
		for src, fctr := range failWindowBySource {
			if fails := fctr.Count(); fails > 0 {
				prevWindowBySource.Fails[src] = fails
			}
			fctr.Reset()
		}
		for src, uset := range usersWindowBySource {
			// count
			if u := int64(len(uset)); u > 0 {
				prevWindowBySource.Uids[src] = u
			}
			// actual list of user‐IDs
			ids := make([]string, 0, len(uset))
			for uid := range uset {
				ids = append(ids, uid)
			}
			prevWindowUserIDs[src] = ids
		}
		// clear the per‐source UID sets for the next interval
		usersWindowBySource = make(map[string]map[string]struct{})

		// — reset the global windowed counters —
		totalCounter.Reset()
		failureCounter.Reset()
		downsampledCounter.Reset()
		dedupCounter.Reset()
		forwardedCounter.Reset()
		bytesReceivedWindow.Reset()
		bytesForwardedWindow.Reset()
		metricsMu.Unlock()

		// 2) Snapshot & reset each client’s windows
		clientsMu.Lock()
		newPrev := make([]map[string]interface{}, 0, len(clients))
		for _, c := range clients {
			c.mu.Lock()
			msgsInWindow := c.messageWindow.Count()
			bytesInWindow := c.bytesWindow.Sum()
			c.mu.Unlock()

			// record the snapshot
			newPrev = append(newPrev, map[string]interface{}{
				"ip":                  c.ip,
				"shards":              c.shards,
				"description":         c.description,
				"port":                c.port,
				"messages_sent":       c.messagesSent, // cumulative
				"bytes_sent":          c.bytesSent,    // cumulative
				"messages_per_window": msgsInWindow,   // this interval
				"bytes_per_window":    bytesInWindow,  // this interval
			})

			// now reset for the next interval
			c.messageWindow.Reset()
			c.bytesWindow.Reset()
		}
		prevClientWindows = newPrev
		clientsMu.Unlock()
	}
}

func main() {

	flag.Parse()
	args := flag.Args()

	var dir string
	switch len(args) {
	case 0:
		// No argument: use current directory
		var err error
		dir, err = os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current directory: %v", err)
		}
	case 1:
		// One argument: use it
		dir = args[0]
	default:
		log.Fatalf("Usage: %s [config-dir]", os.Args[0])
	}

	cfgDir = dir

	cfgPath := filepath.Join(cfgDir, "settings.json")

	// Read and unmarshal
	data, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Fatalf("Failed to read config %q: %v", cfgPath, err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Invalid JSON in %q: %v", cfgPath, err)
	}

	if err := initDecodeLogger(); err != nil {
		log.Printf("warning: could not open failed_decode_log: %v", err)
	}

	blockedIPs = make(map[string]struct{})
	for _, ip := range cfg.BlockedIPs {
		blockedIPs[ip] = struct{}{}
	}

	downsampleTypes = make(map[string]bool)
	for _, msgType := range cfg.DownsampleMessageTypes {
		downsampleTypes[msgType] = true
	}

	log.Println("Configured UDP Destinations and their Shards:")
	for _, dest := range cfg.Destinations {
		log.Printf("Destination: %s:%d, Shards: %v", dest.Host, dest.Port, dest.Shards)
	}

	var destinationStrings []string
	for _, dest := range cfg.Destinations {
		destString := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
		destinationStrings = append(destinationStrings, destString)
	}

	// durations
	metricWindowSize = time.Duration(cfg.MetricWindowSize) * time.Second
	downsampleWindow = time.Duration(cfg.DownsampleWindow) * time.Second
	deduplicationWindowMs = cfg.DeduplicationWindowMs
	dedupWindow = time.Duration(deduplicationWindowMs) * time.Millisecond
	windowSize = int(metricWindowSize.Seconds())

	// ints & strings
	udpPort = cfg.UDPListenPort
	destinations = strings.Join(destinationStrings, ",")
	httpPort = cfg.HTTPPort
	numWorkers = cfg.NumWorkers
	webPath = cfg.WebPath
	debugFlag = cfg.Debug
	includeSource = cfg.IncludeSource
	streamPort = cfg.StreamPort
	streamShards = cfg.StreamShards
	deduplicationForwardButTag = cfg.DeduplicationForwardButTag

	// MQTT
	mqttServer = cfg.MQTTServer
	mqttTLS = cfg.MQTTTLS
	mqttAuth = cfg.MQTTAuth
	mqttTopic = cfg.MQTTTopic

	// Receivers
	receiversBaseURL = cfg.ReceiversBaseURL

	// Start polling receivers if base URL is configured
	if receiversBaseURL != "" {
		go pollReceivers()
	}

	if mqttServer != "" {
		// Set MQTT options
		opts := mqtt.NewClientOptions()
		opts.AddBroker("tcp://" + mqttServer)

		// Set MQTT TLS if enabled
		if mqttTLS {
			tlsConfig := &tls.Config{InsecureSkipVerify: true}
			opts.SetTLSConfig(tlsConfig)
		}

		// Set MQTT authentication if provided
		if mqttAuth != "" {
			authParts := strings.SplitN(mqttAuth, ":", 2)
			if len(authParts) == 2 {
				opts.SetUsername(authParts[0])
				opts.SetPassword(authParts[1])
			} else {
				log.Printf("Invalid MQTT authentication format. Expected user:pass.")
			}
		}

		// Set client ID and clean session
		opts.SetClientID("go-ais-decoder")
		opts.SetCleanSession(true)

		// Create MQTT client
		mqttClient = mqtt.NewClient(opts)

		// Attempt to connect to the MQTT broker
		token := mqttClient.Connect()
		if token.Wait() && token.Error() != nil {
			log.Printf("Failed to connect to MQTT broker: %v", token.Error())
		} else {
			log.Printf("Successfully connected to MQTT broker: %s", mqttServer)
		}
	}

	if streamShards < 1 {
		streamShards = 1
	}
	dedupWindow = time.Duration(deduplicationWindowMs) * time.Millisecond
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		for range ticker.C {
			cleanupMetrics()
			debug.FreeOSMemory()
		}
	}()

	go func() {
		if dedupWindow > 0 {
			ticker := time.NewTicker(dedupWindow)
			defer ticker.Stop() // Ensure the ticker is stopped when the function exits.
			for range ticker.C {
				cleanupDeduplicationState()
			}
		} else {
			log.Println("Deduplication is disabled because window is set to 0")
		}
	}()

	// periodically prune stale fragment buffers
	go func() {
		ttl := 250 * time.Millisecond
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()
		for range ticker.C {
			cleanupFragments(ttl)
		}
	}()

	// determine window size in seconds (not used for bytes counters)
	windowSize = int(metricWindowSize.Seconds())

	// Initialize fixed-window counters
	totalCounter = NewFixedWindowCounter()
	totalSourceCounters = NewFixedWindowCounter()
	failureCounter = NewFixedWindowCounter()
	downsampledCounter = NewFixedWindowCounter()
	dedupCounter = NewFixedWindowCounter()
	forwardedCounter = NewFixedWindowCounter()
	bytesReceivedWindow = NewFixedWindowBytesCounter()
	bytesForwardedWindow = NewFixedWindowBytesCounter()

	// Start periodic resets
	go startMetricsReset()

	http.HandleFunc("/clients", clientsHandler)

	http.Handle("/", http.FileServer(http.Dir(webPath)))
	http.HandleFunc("/metrics", metricsHandler)
	http.HandleFunc("/settings", settingsHandler)

	go func() {
		addr := fmt.Sprintf(":%d", httpPort)
		log.Printf("HTTP serving on http://localhost%s/metrics", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	go startStreamListener(streamPort)

	// Set up UDP connections
	var udpConns []*net.UDPConn
	if destinations != "" {
		for _, dst := range strings.Split(destinations, ",") {
			dst = strings.TrimSpace(dst)
			udpAddr, err := net.ResolveUDPAddr("udp", dst)
			if err != nil {
				log.Fatalf("Invalid destination %q: %v", dst, err)
			}
			conn, err := net.DialUDP("udp", nil, udpAddr)
			if err != nil {
				log.Fatalf("Failed to dial %s: %v", dst, err)
			}
			udpConns = append(udpConns, conn)
		}
	}

	// Initialize UDP destination metrics
	initializeUDPDestinationMetrics()

	// Listen for UDP packets on main port and dedicated ports
	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	packetChan := make(chan *UDPPacket, 1000)
	for i := 0; i < numWorkers; i++ {
		go worker(packetChan, udpConns, nmeaCodec)
	}

	// Set up listeners for all ports
	var listeners []net.PacketConn

	// Main UDP port
	udpAddrStr := fmt.Sprintf(":%d", udpPort)
	pc, err := net.ListenPacket("udp", udpAddrStr)
	if err != nil {
		log.Fatalf("UDP listen %s: %v", udpAddrStr, err)
	}
	listeners = append(listeners, pc)
	log.Printf("Listening on UDP port %d", udpPort)

	// Parse and set up dedicated port range if specified
	if cfg.UDPDedicatedPorts != "" {
		portRange := strings.Split(cfg.UDPDedicatedPorts, "-")
		if len(portRange) == 2 {
			startPort, err1 := strconv.Atoi(portRange[0])
			endPort, err2 := strconv.Atoi(portRange[1])

			if err1 != nil || err2 != nil {
				log.Printf("Invalid port range format: %s. Expected format: start-end", cfg.UDPDedicatedPorts)
			} else if startPort > endPort {
				log.Printf("Invalid port range: start port %d is greater than end port %d", startPort, endPort)
			} else {
				log.Printf("Setting up dedicated UDP ports from %d to %d", startPort, endPort)
				for port := startPort; port <= endPort; port++ {
					if port == udpPort {
						// Skip if it's the same as the main UDP port
						continue
					}

					dedicatedAddrStr := fmt.Sprintf(":%d", port)
					dedicatedPC, err := net.ListenPacket("udp", dedicatedAddrStr)
					if err != nil {
						log.Printf("Failed to listen on dedicated UDP port %d: %v", port, err)
						continue
					}
					listeners = append(listeners, dedicatedPC)
					log.Printf("Listening on dedicated UDP port %d", port)

					// Start a goroutine to handle packets from this dedicated port
					go func(pc net.PacketConn, portNum int) {
						for {
							buf := bufPool.Get().([]byte)
							n, addr, err := pc.ReadFrom(buf)
							if err != nil {
								log.Printf("UDP read error on port %d: %v", portNum, err)
								bufPool.Put(buf)
								continue
							}
							pkt := packetPool.Get().(*UDPPacket)
							pkt.raw = buf[:n]
							pkt.sourceIP = strings.Split(addr.String(), ":")[0]
							pkt.port = portNum

							// Rate limiting
							if !allow(pkt.sourceIP) {
								log.Printf("Rate limit exceeded for IP %s on port %d, dropping packet", pkt.sourceIP, portNum)
								bufPool.Put(buf)
								packetPool.Put(pkt)
								continue
							}
							packetChan <- pkt
						}
					}(dedicatedPC, port)
				}
			}
		} else {
			log.Printf("Invalid port range format: %s. Expected format: start-end", cfg.UDPDedicatedPorts)
		}
	}

	// Ensure all listeners are closed on exit
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}

		if mqttClient.IsConnected() {
			mqttClient.Disconnect(250)
			log.Println("Disconnected from MQTT broker")
		}
	}()

	// Main packet loop for the primary UDP port
	for {
		buf := bufPool.Get().([]byte)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("UDP read error on main port: %v", err)
			bufPool.Put(buf)
			continue
		}
		pkt := packetPool.Get().(*UDPPacket)
		// Give worker the full buffer and the length it needs
		pkt.raw = buf[:n]
		pkt.sourceIP = strings.Split(addr.String(), ":")[0]
		pkt.port = udpPort

		// Rate limiting
		if !allow(pkt.sourceIP) {
			// drop the packet and recycle buffers
			log.Printf("Rate limit exceeded for IP %s on main port, dropping packet", pkt.sourceIP)
			bufPool.Put(buf)
			packetPool.Put(pkt)
			continue
		}
		packetChan <- pkt
	}

	defer func() {
		if mqttClient.IsConnected() {
			mqttClient.Disconnect(250)
			log.Println("Disconnected from MQTT broker")
		}
	}()

}

func startStreamListener(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to start stream listener on %d: %v", port, err)
	}
	log.Printf("Stream listener accepting on :%d", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Stream accept error: %v", err)
			continue
		}
		go handleStreamConn(conn)
	}
}

func handleStreamConn(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var req handshakeReq
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		conn.Close()
		return
	}
	for _, s := range req.Shards {
		if s < 0 || s >= streamShards {
			conn.Close()
			return
		}
	}

	port := req.Port

	conn.SetReadDeadline(time.Time{})

	client := &StreamClient{
		conn:          conn,
		shards:        req.Shards,
		description:   req.Description,
		ip:            strings.Split(conn.RemoteAddr().String(), ":")[0],
		port:          req.Port,
		messageWindow: NewFixedWindowCounter(),
		bytesWindow:   NewFixedWindowBytesCounter(),
	}

	clientsMu.Lock()
	clients = append(clients, client)
	clientsMu.Unlock()
	log.Printf("New stream client connected from IP: %s, Shards: %v, Description: %q, Port: %d",
		strings.Split(conn.RemoteAddr().String(), ":")[0], req.Shards, req.Description, port)

	go func() {
		io.Copy(io.Discard, conn)
		conn.Close()
		clientsMu.Lock()
		defer clientsMu.Unlock()
		for i, c := range clients {
			if c == client {
				clients = append(clients[:i], clients[i+1:]...)
				break
			}
		}
		log.Printf("Stream client disconnected: %s", client.ip)
	}()
}

func shardForUser(userID string) int {
	h := fnv.New32a()
	h.Write([]byte(userID))
	return int(h.Sum32()) % streamShards
}

func worker(ch <-chan *UDPPacket, udpConns []*net.UDPConn, nmea *aisnmea.NMEACodec) {
	for pkt := range ch {
		rawBytes, srcIP := pkt.raw, pkt.sourceIP
		rawStr := string(rawBytes)

		// ── Fragment reassembly ────────────────────────────────────────────────
		complete, joined := addFragment(rawStr)

		// ── Blocked IP ─────────────────────────────────────────────────────────
		if _, blocked := blockedIPs[srcIP]; blocked {
			metricsMu.Lock()
			if blockedIPCounters[srcIP] == nil {
				blockedIPCounters[srcIP] = NewFixedWindowCounter()
			}
			blockedIPCounters[srcIP].AddEvent()
			metricsMu.Unlock()

			pkt.raw = nil
			pkt.sourceIP = ""
			packetPool.Put(pkt)
			continue
		}

		// ── Inbound metrics ────────────────────────────────────────────────────
		metricsMu.Lock()
		totalMessages++
		totalCounter.AddEvent()
		totalSourceTotals[srcIP]++
		bytesReceivedTotals[srcIP] += int64(len(pkt.raw))
		totalBytesReceived += int64(len(pkt.raw))
		totalSourceCounters.AddEvent()

		if msgWindowBySource[srcIP] == nil {
			msgWindowBySource[srcIP] = NewFixedWindowCounter()
		}
		msgWindowBySource[srcIP].AddEvent()

		if bytesWindowBySource[srcIP] == nil {
			bytesWindowBySource[srcIP] = NewFixedWindowBytesCounter()
		}
		bytesWindowBySource[srcIP].Add(int64(len(pkt.raw)))

		metricsMu.Unlock()

		bytesReceivedWindow.Add(int64(len(pkt.raw)))

		// ── Deduplication hash ─────────────────────────────────────────────────
		hashedMsg := fnvHash(rawStr)

		// ── NMEA parse ─────────────────────────────────────────────────────────
		decoded, err := nmea.ParseSentence(rawStr)
		if err != nil || decoded == nil || decoded.Packet == nil {
			if err == nil && decoded == nil {
				// both nil → silently drop
				pkt.raw = nil
				pkt.sourceIP = ""
				packetPool.Put(pkt)
				continue
			}
			// real error
			metricsMu.Lock()
			totalFailures++
			failureCounter.AddEvent()
			failureSourceTotals[srcIP]++
			if failureSourceCounters[srcIP] == nil {
				failureSourceCounters[srcIP] = NewFixedWindowCounter()
			}
			failureSourceCounters[srcIP].AddEvent()
			if failWindowBySource[srcIP] == nil {
				failWindowBySource[srcIP] = NewFixedWindowCounter()
			}
			failWindowBySource[srcIP].AddEvent()
			metricsMu.Unlock()

			if failedDecodeLogger != nil {
				failedDecodeLogger.Printf("decode failure: %v | raw: %s\n", err, rawStr)
			}
			if debugFlag {
				log.Printf("[DEBUG] decode failure: %v | raw: %s", err, rawStr)
			}

			pkt.raw = nil
			pkt.sourceIP = ""
			packetPool.Put(pkt)
			continue
		}

		// ── Extract header & prepare for plugin ─────────────────────────────────
		hdr := decoded.Packet.GetHeader()

		// Re-marshal the packet into a map so we can inject DecodedBinary
		rawJSON, err := json.Marshal(decoded.Packet)
		if err != nil {
			log.Printf("json.Marshal error: %v", err)
		}
		// copy out of the pooled buffer so it can't be mutated underfoot
		copyBuf := append([]byte(nil), rawJSON...)
		var pktMap map[string]interface{}
		_ = json.Unmarshal(copyBuf, &pktMap)

		// MessageID always comes from the header
		mid := int(hdr.MessageID)

		// DAC/FI live under the nested "ApplicationID" JSON object
		var dac, fi int
		if app, ok := pktMap["ApplicationID"].(map[string]interface{}); ok {
			if d, ok := app["DesignatedAreaCode"].(float64); ok {
				dac = int(d)
			}
			if f, ok := app["FunctionIdentifier"].(float64); ok {
				fi = int(f)
			}
		}

		if decoderFn, found := decoders.Get(mid, dac, fi); found {
			if meta, err := decoderFn(pktMap); err != nil {
				log.Printf("plugin %d/%d/%d decode error: %v", mid, dac, fi, err)
			} else {
				pktMap["DecodedBinary"] = meta
			}
		}

		// ── Cache msgID/userID as strings ──────────────────────────────────────
		msgIDCache.RLock()
		midKey, ok := msgIDCache.m[hdr.MessageID]
		msgIDCache.RUnlock()
		if !ok {
			midKey = strconv.Itoa(int(hdr.MessageID))
			msgIDCache.Lock()
			msgIDCache.m[hdr.MessageID] = midKey
			msgIDCache.Unlock()
		}
		userIDCache.RLock()
		uidKey, ok := userIDCache.m[hdr.UserID]
		userIDCache.RUnlock()
		if !ok {
			uidKey = strconv.FormatUint(uint64(hdr.UserID), 10)
			userIDCache.Lock()
			userIDCache.m[hdr.UserID] = uidKey
			userIDCache.Unlock()
		}
		msgID, userID := midKey, uidKey

		// ── Track unique users in the rolling‐window per source ────────────────
		metricsMu.Lock()
		if usersWindowBySource[srcIP] == nil {
			usersWindowBySource[srcIP] = make(map[string]struct{})
		}
		usersWindowBySource[srcIP][userID] = struct{}{}
		metricsMu.Unlock()

		// ── Track unique users per source IP ────────────────────────────────
		metricsMu.Lock()
		if _, seen := userIDsPerSource[srcIP]; !seen {
			userIDsPerSource[srcIP] = make(map[string]struct{})
		}
		userIDsPerSource[srcIP][userID] = struct{}{}
		metricsMu.Unlock()

		// ── Deduplication window ───────────────────────────────────────────────
		var dedupedPort *int = nil
		isDuplicate := false
		if dedupWindow > 0 {
			dedupMu.Lock()
			now := time.Now()
			if info, seen := lastDedup[hashedMsg]; seen && now.Sub(info.Timestamp) < dedupWindow {
				// Message is a duplicate
				dedupMu.Unlock()
				metricsMu.Lock()
				totalDeduplicated++
				dedupCounter.AddEvent()
				deduplicatedUserIDTotals[userID]++
				if dedupUserIDCounters[userID] == nil {
					dedupUserIDCounters[userID] = NewFixedWindowCounter()
				}
				dedupUserIDCounters[userID].AddEvent()
				deduplicatedSourceTotals[srcIP]++
				if dedupSourceCounters[srcIP] == nil {
					dedupSourceCounters[srcIP] = NewFixedWindowCounter()
				}
				dedupSourceCounters[srcIP].AddEvent()
				if dedupPerUserMessageIDCount[userID] == nil {
					dedupPerUserMessageIDCount[userID] = map[string]int64{}
				}
				dedupPerUserMessageIDCount[userID][msgID]++
				metricsMu.Unlock()

				if deduplicationForwardButTag {
					// Store the port that first saw this message
					portCopy := info.Port
					dedupedPort = &portCopy
					isDuplicate = true

					if debugFlag {
						log.Printf("DUPLICATE: Message from user %s with hash %d is a duplicate. Original port: %d, Current port: %d",
							userID, hashedMsg, info.Port, pkt.port)
						log.Printf("[DEBUG] Setting dedupedPort=%d (pointer=%p)", *dedupedPort, dedupedPort)
					}
				} else {
					// Original behavior: discard the packet
					if debugFlag {
						log.Printf("DUPLICATE DISCARDED: Message from user %s with hash %d is a duplicate and being discarded. Original port: %d, Current port: %d",
							userID, hashedMsg, info.Port, pkt.port)
					}
					pkt.raw = nil
					pkt.sourceIP = ""
					packetPool.Put(pkt)
					continue
				}
			} else {
				// New message, store it in the deduplication map
				lastDedup[hashedMsg] = DedupInfo{
					Timestamp: now,
					Port:      pkt.port,
				}

				if debugFlag {
					log.Printf("NEW MESSAGE: First time seeing message from user %s with hash %d on port %d",
						userID, hashedMsg, pkt.port)
				}

				dedupMu.Unlock()
			}
		}

		// ── Downsample logic ────────────────────────────────────────────────────
		shouldDownsample := false

		if downsampleWindow > 0 && downsampleTypes[msgID] {
			now := time.Now()
			downMu.Lock()
			if lastForward[msgID] == nil {
				lastForward[msgID] = map[string]time.Time{}
			}

			// For duplicate messages, we need special handling
			if dedupedPort != nil {
				// Get the zero time for comparison
				var zeroTime time.Time

				// Check if we've seen an original message for this msgID/userID
				lastTime, exists := lastForward[msgID][userID]

				if debugFlag {
					log.Printf("[DEBUG] Duplicate message check: exists=%v, lastTime=%v", exists, lastTime)
				}

				// Only apply downsampling if we've seen an original message AND
				// it's within the downsampling window
				if exists && lastTime != zeroTime && now.Sub(lastTime) < downsampleWindow {
					shouldDownsample = true
					if debugFlag {
						log.Printf("[DEBUG] Duplicate will be downsampled: time since last=%v, window=%v",
							now.Sub(lastTime), downsampleWindow)
					}
				} else {
					if debugFlag {
						log.Printf("[DEBUG] Duplicate will be forwarded: no recent original message")
					}
				}
			} else {
				// Original message - normal downsampling logic
				lastTime := lastForward[msgID][userID]
				var zeroTime time.Time

				// Check if this message should be downsampled based on time window
				if lastTime != zeroTime && now.Sub(lastTime) < downsampleWindow {
					shouldDownsample = true
					if debugFlag {
						log.Printf("[DEBUG] Original message will be downsampled: time since last=%v",
							now.Sub(lastTime))
					}
				} else {
					// Update the last forward time for this message type and user ID
					lastForward[msgID][userID] = now
					if debugFlag {
						log.Printf("[DEBUG] Original message will be forwarded and timestamp updated")
					}
				}
			}
			downMu.Unlock()

			// If we should downsample, do it now
			if shouldDownsample {
				metricsMu.Lock()
				totalDownsampled++
				downsampledCounter.AddEvent()
				downsampledMessageTypeTotals[msgID]++
				if downsampledMessageTypeCounters[msgID] == nil {
					downsampledMessageTypeCounters[msgID] = NewFixedWindowCounter()
				}
				downsampledMessageTypeCounters[msgID].AddEvent()
				downsampledUserIDTotals[userID]++
				if downsampledUserIDCounters[userID] == nil {
					downsampledUserIDCounters[userID] = NewFixedWindowCounter()
				}
				downsampledUserIDCounters[userID].AddEvent()
				if downsampledPerUserMessageIDCount[userID] == nil {
					downsampledPerUserMessageIDCount[userID] = map[string]int64{}
				}
				downsampledPerUserMessageIDCount[userID][msgID]++
				metricsMu.Unlock()

				pkt.raw = nil
				pkt.sourceIP = ""
				packetPool.Put(pkt)
				continue
			}
		}

		// ── Shard & forward raw UDP ────────────────────────────────────────────
		shardID := shardForUser(userID)
		metricsMu.Lock()
		messagesPerShard[shardID]++
		if userIDsPerShard[shardID] == nil {
			userIDsPerShard[shardID] = make(map[string]struct{})
		}
		userIDsPerShard[shardID][userID] = struct{}{}
		metricsMu.Unlock()

		for _, dest := range cfg.Destinations {
			for _, s := range dest.Shards {
				if s == shardID {
					addr := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
					conn, err := net.Dial("udp", addr)
					if err != nil {
						log.Printf("Failed to dial %s: %v", addr, err)
						continue
					}
					if _, err := conn.Write(pkt.raw); err != nil {
						log.Printf("UDP write to %s error: %v", addr, err)
					}
					conn.Close()
					key := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
					udpDestinationMetrics[key].MessagesSent++
					udpDestinationMetrics[key].BytesSent += int64(len(pkt.raw))
					break
				}
			}
		}

		// ── Post-forward metrics ───────────────────────────────────────────────
		metricsMu.Lock()
		if perUserMessageIDCount[userID] == nil {
			perUserMessageIDCount[userID] = map[string]int64{}
		}
		perUserMessageIDCount[userID][msgID]++

		messageIDTotals[msgID]++
		if messageIDCounters[msgID] == nil {
			messageIDCounters[msgID] = NewFixedWindowCounter()
		}
		messageIDCounters[msgID].AddEvent()

		userIDTotals[userID]++
		if userIDCounters[userID] == nil {
			userIDCounters[userID] = NewFixedWindowCounter()
		}
		userIDCounters[userID].AddEvent()

		totalForwarded++
		forwardedCounter.AddEvent()
		totalBytesForwarded += int64(len(pkt.raw))
		bytesForwardedWindow.Add(int64(len(pkt.raw)))
		metricsMu.Unlock()

		// ── Build & send StreamMessage ────────────────────────────────────────
		parts := strings.Split(rawStr, ",")
		channel := "Unknown"
		if len(parts) > 4 && len(parts[4]) > 0 {
			channel = string(parts[4][0])
		}

		// choose payload
		var packetPayload interface{}
		if pktMap != nil {
			packetPayload = pktMap
		} else {
			packetPayload = decoded.Packet
		}

		rawToSend := rawStr
		if complete && joined != "" {
			rawToSend = joined
		}

		// Simple debug log before creating StreamMessage
		if debugFlag && dedupedPort != nil {
			log.Printf("[DEBUG] Before creating StreamMessage")
		}

		streamObj := StreamMessage{
			Message: map[string]interface{}{
				"packet":  packetPayload,
				"channel": channel,
			},
			Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
			ShardID:     shardID,
			RawSentence: rawToSend,
			UDPPort:     pkt.port,
			DedupedPort: dedupedPort,
			IsDuplicate: isDuplicate,
		}

		// Debug log to track execution flow
		if debugFlag && dedupedPort != nil {
			log.Printf("DUPLICATE FLOW: Created StreamMessage object with DedupedPort=%d", *dedupedPort)
		}
		if includeSource {
			streamObj.SourceIP = srcIP
		}

		// Before JSON encoding, log the StreamMessage object
		if debugFlag && dedupedPort != nil {
			log.Printf("DUPLICATE BEFORE ENCODING: streamObj=%+v, dedupedPort=%d", streamObj, *dedupedPort)
		}

		buf := jsonBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		encodeErr := json.NewEncoder(buf).Encode(streamObj)
		if encodeErr != nil && debugFlag {
			log.Printf("ERROR ENCODING JSON: %v", encodeErr)
		}
		out := buf.Bytes()

		// Debug log for duplicate messages to verify deduped_port is in the JSON
		if debugFlag && dedupedPort != nil {
			log.Printf("DUPLICATE JSON: %s", string(out))

			// Decode the JSON back to verify the field is there
			var decoded map[string]interface{}
			if err := json.Unmarshal(out, &decoded); err != nil {
				log.Printf("ERROR DECODING JSON: %v", err)
			} else {
				if dp, ok := decoded["deduped_port"]; ok {
					log.Printf("DUPLICATE JSON VERIFIED: deduped_port=%v found in JSON", dp)
				} else {
					log.Printf("DUPLICATE JSON MISSING FIELD: deduped_port not found in JSON")
				}
			}
		}

		// ── MQTT ───────────────────────────────────────────────────────────────
		if mqttClient != nil && mqttClient.IsConnected() {
			// Get receiver ID for this UDP port
			receiverID := 0
			if pkt.port != udpPort { // Only look up non-default ports
				receiversMutex.RLock()
				if id, ok := portToIDMap[pkt.port]; ok {
					receiverID = id
				}
				receiversMutex.RUnlock()
			}

			topic := fmt.Sprintf("%s/%d/%s/%d/%s/message", mqttTopic, shardID, userID, receiverID, msgID)
			// make a private copy of the buffer bytes to avoid mutation races
			payload := make([]byte, len(out))
			copy(payload, out)
			var mqttMap map[string]interface{}
			if err := json.Unmarshal(payload, &mqttMap); err == nil {
				delete(mqttMap, "source_ip")
				delete(mqttMap, "udp_port")
				delete(mqttMap, "deduped_port")
				if mqttBuf, err := json.Marshal(mqttMap); err == nil {
					token := mqttClient.Publish(topic, 0, false, mqttBuf)
					token.Wait()
				}
			}
		}

		// ── TCP stream to clients ─────────────────────────────────────────────
		out = append(out, 0)

		// Debug log right before TCP transmission
		if debugFlag && dedupedPort != nil {
			log.Printf("DUPLICATE TCP: About to send message with DedupedPort=%d to %d clients", *dedupedPort, len(clients))
		}

		clientsMu.Lock()
		for _, c := range clients {
			for _, s := range c.shards {
				if s == shardID {
					c.mu.Lock()

					// Debug log for each client connection
					if debugFlag && dedupedPort != nil {
						log.Printf("DUPLICATE TCP CLIENT: Sending to client with shards=%v", c.shards)
					}

					n, err := c.conn.Write(out)
					if err == nil {
						c.messagesSent++
						c.bytesSent += int64(n)
						c.messageWindow.AddEvent()
						c.bytesWindow.Add(int64(n))
					}
					c.mu.Unlock()
				}
			}
		}
		clientsMu.Unlock()

		// ── Cleanup ────────────────────────────────────────────────────────────
		jsonBufPool.Put(buf)
		original := pkt.raw[:cap(pkt.raw)]
		bufPool.Put(original)
		pkt.raw = nil
		pkt.sourceIP = ""
		packetPool.Put(pkt)
	}
}

func getTotalClients() int {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	return len(clients)
}

func getShardsMissing() []int {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	used := make(map[int]bool)

	// Track shards used by clients
	for _, c := range clients {
		for _, s := range c.shards {
			used[s] = true
		}
	}

	// Track shards used by UDP destinations
	for _, dest := range cfg.Destinations {
		for _, s := range dest.Shards {
			used[s] = true
		}
	}

	// Determine missing shards
	missing := []int{}
	for i := 0; i < streamShards; i++ {
		if !used[i] {
			missing = append(missing, i)
		}
	}
	return missing
}

func getShardsMultiple() map[int][]map[string]string {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	shardMap := make(map[int][]map[string]string)

	// Track shards used by clients (including IP:Port and description)
	for _, c := range clients {
		for _, s := range c.shards {
			clientAddr := fmt.Sprintf("%s:%d", c.ip, c.port)
			shardMap[s] = append(shardMap[s], map[string]string{
				"address":     clientAddr,
				"description": c.description,
			})
		}
	}

	// Track shards used by UDP destinations (including host:Port and description)
	for _, dest := range cfg.Destinations {
		for _, s := range dest.Shards {
			udpDest := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
			shardMap[s] = append(shardMap[s], map[string]string{
				"address":     udpDest,
				"description": dest.Description,
			})
		}
	}

	// Filter out shards that have multiple sources (either clients or multiple UDP destinations)
	multiple := make(map[int][]map[string]string)
	for s, sources := range shardMap {
		if len(sources) > 1 {
			multiple[s] = sources
		}
	}
	return multiple
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	uptime := int64(math.Round(time.Since(startTime).Seconds()))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	metricsMu.RLock()
	defer metricsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	// rolling‐window snapshots
	windowMsgs := previousPeriodMetrics.windowMsgs
	windowFailures := previousPeriodMetrics.windowFailures
	windowDownsampled := previousPeriodMetrics.windowDownsampled
	windowDedup := previousPeriodMetrics.windowDedup
	windowForwarded := previousPeriodMetrics.windowForwarded
	windowBytesReceived := previousPeriodMetrics.windowBytesReceived
	windowBytesForwarded := previousPeriodMetrics.windowBytesForwarded

	// client‐level stream metrics
	clientMetrics := prevClientWindows
	totalClients := getTotalClients()
	shardsMissing := getShardsMissing()
	shardsMultiple := getShardsMultiple()

	var ratioTotal, ratioWindow float64
	if totalMessages > 0 {
		ratioTotal = float64(totalForwarded) / float64(totalMessages)
	}
	if windowMsgs > 0 {
		ratioWindow = float64(windowForwarded) / float64(windowMsgs)
	}

	type userStat struct {
		UserID       string           `json:"user_id"`
		Count        int64            `json:"count"`
		PerMessageID map[string]int64 `json:"per_message_id"`
	}
	type sourceStat struct {
		SourceIP string `json:"source_ip"`
		Count    int64  `json:"count"`
	}

	// top-25 per‐user aggregates (unchanged)
	users := make([]userStat, 0, len(userIDTotals))
	for uid, cnt := range userIDTotals {
		users = append(users, userStat{UserID: uid, Count: cnt, PerMessageID: perUserMessageIDCount[uid]})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Count > users[j].Count })
	if len(users) > 25 {
		users = users[:25]
	}

	dsUsers := make([]userStat, 0, len(downsampledUserIDTotals))
	for uid, cnt := range downsampledUserIDTotals {
		dsUsers = append(dsUsers, userStat{UserID: uid, Count: cnt, PerMessageID: downsampledPerUserMessageIDCount[uid]})
	}
	sort.Slice(dsUsers, func(i, j int) bool { return dsUsers[i].Count > dsUsers[j].Count })
	if len(dsUsers) > 25 {
		dsUsers = dsUsers[:25]
	}

	dupUsers := make([]userStat, 0, len(deduplicatedUserIDTotals))
	for uid, cnt := range deduplicatedUserIDTotals {
		dupUsers = append(dupUsers, userStat{UserID: uid, Count: cnt, PerMessageID: dedupPerUserMessageIDCount[uid]})
	}
	sort.Slice(dupUsers, func(i, j int) bool { return dupUsers[i].Count > dupUsers[j].Count })
	if len(dupUsers) > 25 {
		dupUsers = dupUsers[:25]
	}

	// **All** failures by source IP
	failuresBySource := make([]sourceStat, 0, len(failureSourceTotals))
	for src, cnt := range failureSourceTotals {
		failuresBySource = append(failuresBySource, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(failuresBySource, func(i, j int) bool {
		return failuresBySource[i].Count > failuresBySource[j].Count
	})

	// **All** total messages by source IP
	totalMsgsBySource := make([]sourceStat, 0, len(totalSourceTotals))
	for src, cnt := range totalSourceTotals {
		totalMsgsBySource = append(totalMsgsBySource, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(totalMsgsBySource, func(i, j int) bool {
		return totalMsgsBySource[i].Count > totalMsgsBySource[j].Count
	})

	// **All** bytes received by source IP
	bytesBySource := make([]sourceStat, 0, len(bytesReceivedTotals))
	for src, cnt := range bytesReceivedTotals {
		bytesBySource = append(bytesBySource, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(bytesBySource, func(i, j int) bool {
		return bytesBySource[i].Count > bytesBySource[j].Count
	})

	// **All** cumulative unique MMSI per source IP
	uniqueMMSIBySource := make([]sourceStat, 0, len(userIDsPerSource))
	for ip, uset := range userIDsPerSource {
		uniqueMMSIBySource = append(uniqueMMSIBySource, sourceStat{SourceIP: ip, Count: int64(len(uset))})
	}
	sort.Slice(uniqueMMSIBySource, func(i, j int) bool {
		return uniqueMMSIBySource[i].Count > uniqueMMSIBySource[j].Count
	})

	// Deduplicated per‐source stays as top 25
	dupSources := make([]sourceStat, 0, len(deduplicatedSourceTotals))
	for src, cnt := range deduplicatedSourceTotals {
		dupSources = append(dupSources, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(dupSources, func(i, j int) bool { return dupSources[i].Count > dupSources[j].Count })
	if len(dupSources) > 25 {
		dupSources = dupSources[:25]
	}

	// build shard maps
	msgs := make(map[int]int64, len(messagesPerShard))
	for s, cnt := range messagesPerShard {
		msgs[s] = cnt
	}
	uids := make(map[int]int, len(userIDsPerShard))
	for s, m := range userIDsPerShard {
		uids[s] = len(m)
	}

	// udp destination metrics
	udpMetrics := make([]UDPDestinationMetrics, 0, len(udpDestinationMetrics))
	for _, dm := range udpDestinationMetrics {
		udpMetrics = append(udpMetrics, *dm)
	}

	// connected‐client blocking stats
	blockedIPMetrics := make([]map[string]interface{}, 0, len(blockedIPCounters))
	for ip, ctr := range blockedIPCounters {
		blockedIPMetrics = append(blockedIPMetrics, map[string]interface{}{
			"source_ip":        ip,
			"messages_blocked": ctr.Count(),
		})
	}

	// stream‐client details
	connected := make([]map[string]interface{}, 0, len(clients))
	clientsMu.Lock()
	for _, c := range clients {
		c.mu.Lock()
		connected = append(connected, map[string]interface{}{
			"ip":                  c.ip,
			"shards":              c.shards,
			"description":         c.description,
			"port":                c.port,
			"bytes_sent":          c.bytesSent,
			"messages_sent":       c.messagesSent,
			"messages_per_window": c.messageWindow.Count(),
			"bytes_per_window":    c.bytesWindow.Sum(),
		})
		c.mu.Unlock()
	}
	clientsMu.Unlock()

	// assemble full JSON payload
	resp := map[string]interface{}{
		"uptime_seconds":                 uptime,
		"total_messages":                 totalMessages,
		"total_failures":                 totalFailures,
		"total_downsampled":              totalDownsampled,
		"total_deduplicated":             totalDeduplicated,
		"per_message_id":                 messageIDTotals,
		"per_downsampled_message_id":     downsampledMessageTypeTotals,
		"per_deduplicated_user_id":       deduplicatedUserIDTotals,
		"per_deduplicated_source":        deduplicatedSourceTotals,
		"top25_per_user_id":              users,
		"top25_downsampled_per_user_id":  dsUsers,
		"top25_deduplicated_per_user_id": dupUsers,

		// **all** source‐IP metrics
		"failures_by_source":       failuresBySource,
		"messages_by_source":       totalMsgsBySource,
		"bytes_received_by_source": bytesBySource,
		"unique_mmsi_by_source":    uniqueMMSIBySource,

		// dedup‐by‐source still top25
		"top25_deduplicated_per_source": dupSources,

		// rolling‐window totals
		"window_messages":           windowMsgs,
		"window_failures":           windowFailures,
		"window_downsampled":        windowDownsampled,
		"window_deduplicated":       windowDedup,
		"window_messages_forwarded": windowForwarded,
		"bytes_received_window":     windowBytesReceived,
		"bytes_forwarded_window":    windowBytesForwarded,

		// per‐source window snapshots
		"window_messages_by_source":    prevWindowBySource.Msgs,
		"window_bytes_by_source":       prevWindowBySource.Bytes,
		"window_failures_by_source":    prevWindowBySource.Fails,
		"window_unique_uids_by_source": prevWindowBySource.Uids,
		"window_user_ids_by_source":    prevWindowUserIDs,
		// shard & client metrics
		"messages_per_shard": msgs,
		"user_ids_per_shard": uids,
		"connected_clients":  clientMetrics,

		// data‐transfer totals & ratios
		"total_bytes_received":               totalBytesReceived,
		"total_messages_forwarded":           totalForwarded,
		"total_bytes_forwarded":              totalBytesForwarded,
		"ratio_forwarded_to_received":        ratioTotal,
		"window_ratio_forwarded_to_received": ratioWindow,

		// schedule & runtime
		"downsample_window_sec":    downsampleWindow.Seconds(),
		"deduplication_window_sec": dedupWindow.Seconds(),
		"metric_window_size_sec":   metricWindowSize.Seconds(),
		"total_clients":            totalClients,
		"shards_missing":           shardsMissing,
		"shards_multiple":          shardsMultiple,

		// MQTT/UDP destinations
		"udp_destinations":   udpMetrics,
		"blocked_ip_metrics": blockedIPMetrics,

		"memory_stats": map[string]uint64{
			"alloc_bytes":       mem.Alloc,
			"total_alloc_bytes": mem.TotalAlloc,
			"sys_bytes":         mem.Sys,
			"heap_alloc_bytes":  mem.HeapAlloc,
			"heap_sys_bytes":    mem.HeapSys,
			"num_gc":            uint64(mem.NumGC),
		},
	}

	json.NewEncoder(w).Encode(resp)
}

// pollReceivers polls the receivers endpoint every 5 seconds
func pollReceivers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		updateReceivers()
		<-ticker.C
	}
}

// updateReceivers fetches the receivers list from the admin endpoint
// If the fetch fails, it preserves the previously known values
func updateReceivers() {
	if receiversBaseURL == "" {
		return
	}

	url := fmt.Sprintf("%s/admin/receivers", receiversBaseURL)
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Warning: Failed to fetch receivers: %v", err)
		log.Printf("Using previously cached receiver values")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Warning: Received non-OK response from receivers endpoint: %v", resp.Status)
		log.Printf("Using previously cached receiver values")
		return
	}

	var receivers []Receiver
	if err := json.NewDecoder(resp.Body).Decode(&receivers); err != nil {
		log.Printf("Warning: Failed to decode receivers response: %v", err)
		log.Printf("Using previously cached receiver values")
		return
	}

	// Update the receivers map and port-to-ID map
	receiversMutex.Lock()
	defer receiversMutex.Unlock()

	// Clear the port-to-ID map
	portToIDMap = make(map[int]int)

	// Update the maps with the new receivers
	for _, receiver := range receivers {
		receiversMap[receiver.ID] = receiver
		if receiver.UDPPort != nil {
			portToIDMap[*receiver.UDPPort] = receiver.ID
		}
	}

	if debugFlag {
		log.Printf("[DEBUG] Updated receivers map with %d receivers", len(receivers))
	}
}
