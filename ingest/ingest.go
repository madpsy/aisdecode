package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
)

var bufPool = sync.Pool{
    New: func() interface{} { return make([]byte, 2048) },
}

// pool of UDPPacket pointers
var packetPool = sync.Pool{
    New: func() interface{} { return new(UDPPacket) },
}

// SlidingWindowCounter tracks timestamps for rolling‑window counts.
type SlidingWindowCounter struct {
	mu     sync.Mutex
	events []time.Time
}

func logMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	log.Printf("Memory Stats: Alloc = %v MB, TotalAlloc = %v MB, Sys = %v MB, HeapAlloc = %v MB, HeapSys = %v MB, NumGC = %v\n",
		m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.HeapAlloc/1024/1024, m.HeapSys/1024/1024, m.NumGC)
}

func startMemoryStatsLogging() {
	// Start a ticker to log memory stats every minute (60 seconds)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// Run in a separate goroutine so that other tasks can run concurrently
	for {
		select {
		case <-ticker.C:
			// Log memory stats every minute
			logMemoryStats()
		}
	}
}

func (c *SlidingWindowCounter) AddEvent() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, time.Now())
}

func (c *SlidingWindowCounter) Count(window time.Duration) int {
    c.mu.Lock()
    defer c.mu.Unlock()
    cutoff := time.Now().Add(-window)
    i := sort.Search(len(c.events), func(i int) bool {
        return c.events[i].After(cutoff)
    })
    // shrink slice and also reallocate to free underlying array if it's grown too large
    newLen := len(c.events)-i
    newEvents := make([]time.Time, newLen)
    copy(newEvents, c.events[i:])
    c.events = newEvents
    return newLen
}

// SlidingWindowBytesCounter tracks bytes for rolling‑window sums.
type SlidingWindowBytesCounter struct {
	mu     sync.Mutex
	events []time.Time
	values []int64
}

func (c *SlidingWindowBytesCounter) Add(bytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, time.Now())
	c.values = append(c.values, bytes)
}

func (c *SlidingWindowBytesCounter) Sum(window time.Duration) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	cutoff := time.Now().Add(-window)
	idx := 0
	for ; idx < len(c.events); idx++ {
		if c.events[idx].After(cutoff) {
			break
		}
	}
	var sum int64
	for _, v := range c.values[idx:] {
		sum += v
	}
	c.events = c.events[idx:]
	c.values = c.values[idx:]
	return sum
}

type UDPPacket struct {
	raw      string
	sourceIP string
}

// StreamMessage is what we send to TCP clients:
type StreamMessage struct {
	Message     interface{} `json:"message"`
	SourceIP    string      `json:"source_ip"`
	Timestamp   string      `json:"timestamp"`
	ShardID     int         `json:"shard_id"`
	RawSentence string      `json:"raw_sentence"`
}

// handshakeReq is the JSON clients must send on connect
type handshakeReq struct {
	Shards  []int  `json:"shards"`
	Version string `json:"version"`
}

// StreamClient holds a connected client's state and send‑metrics
type StreamClient struct {
	conn          net.Conn
	shards        []int
	version       string
	ip            string
	mu            sync.Mutex
	bytesSent     int64
	messagesSent  int64
	messageWindow *SlidingWindowCounter
	bytesWindow   *SlidingWindowBytesCounter
}

var (
	udpPort               = flag.Int("udp-listen-port", 8101, "UDP listen port for NMEA sentences")
	destinations          = flag.String("udp-destinations", "", "Comma‑delimited list of host:port to forward valid sentences")
	metricWindowSize      = flag.Duration("metric-window-size", time.Minute, "Sliding window size for metrics")
	httpPort              = flag.Int("http-port", 8080, "HTTP port for metrics endpoint")
	numWorkers            = flag.Int("num-workers", 0, "Number of concurrent decode workers (0=CPUs)")
	downsampleWindow      = flag.Duration("downsample-window", 0, "Time window to downsample types 1,2,3,18,19 per UserID")
	deduplicationWindowMs = flag.Int("deduplication-window", 0, "Rolling window length in milliseconds for deduplication. 0 means no deduplication")
	webPath               = flag.String("web-path", "web", "Directory to serve static files from")
	debug                 = flag.Bool("debug", false, "Enable debug output")

	// new flags:
	streamPort   = flag.Int("stream-port", 8102, "TCP port to listen on for decoded‑sentence stream")
	streamShards = flag.Int("stream-shards", 1, "Number of shards for stream partitioning")

	dedupWindow time.Duration
)

// Globals for streaming clients
var (
	clients   []*StreamClient
	clientsMu sync.Mutex
)

// Metrics for shards
var (
	messagesPerShard = map[int]int64{}
	userIDsPerShard  = map[int]map[string]struct{}{}
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

	// Deduplication metrics
	totalDeduplicated          int64
	deduplicatedUserIDTotals   = map[string]int64{}
	deduplicatedSourceTotals   = map[string]int64{}
	dedupPerUserMessageIDCount = map[string]map[string]int64{}

	// bytes received per source IP
	bytesReceivedTotals = map[string]int64{}

	// bytes forwarded per destination
	totalBytesForwarded int64

	// overall totals
	totalBytesReceived int64
	totalForwarded     int64
)

// Rolling‑window counters
var (
	totalCounter                   = &SlidingWindowCounter{}
	messageIDCounters              = map[string]*SlidingWindowCounter{}
	userIDCounters                 = map[string]*SlidingWindowCounter{}
	failureCounter                 = &SlidingWindowCounter{}
	failureSourceCounters          = map[string]*SlidingWindowCounter{}
	downsampledCounter             = &SlidingWindowCounter{}
	downsampledMessageTypeCounters = map[string]*SlidingWindowCounter{}
	// fixed: now a map from userID to window counter
	downsampledUserIDCounters = map[string]*SlidingWindowCounter{}
	totalSourceCounters       = &SlidingWindowCounter{}

	// Deduplication rolling-window counters
	dedupCounter        = &SlidingWindowCounter{}
	dedupUserIDCounters = map[string]*SlidingWindowCounter{}
	dedupSourceCounters = map[string]*SlidingWindowCounter{}

	// New rolling-window counters: bytes received and forwarded
	bytesReceivedWindow  = &SlidingWindowBytesCounter{}
	forwardedCounter     = &SlidingWindowCounter{}
	bytesForwardedWindow = &SlidingWindowBytesCounter{}
)

// Protect metrics
var metricsMu sync.RWMutex

// Downsampling state
var (
	downsampleTypes = map[string]bool{"1": true, "2": true, "3": true, "18": true, "19": true}
	downMu          sync.Mutex
	lastForward     = map[string]map[string]time.Time{} // msgID -> userID -> last time
)

// Deduplication state
var (
	dedupMu   sync.Mutex
	lastDedup = map[string]time.Time{} // raw -> last time seen
)

// Cleanup function to remove old metrics and client data
func cleanupMetrics() {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	// Trim userID totals and messageID totals
	for userID, count := range userIDTotals {
		if count == 0 {
			delete(userIDTotals, userID)
		}
	}
	for messageID, count := range messageIDTotals {
		if count == 0 {
			delete(messageIDTotals, messageID)
		}
	}
	for sourceIP, count := range totalSourceTotals {
		if count == 0 {
			delete(totalSourceTotals, sourceIP)
		}
	}
}

// Cleanup function to remove old deduplication and downsampling data
func cleanupDeduplicationState() {
	dedupMu.Lock()
	defer dedupMu.Unlock()

	// Remove old deduplication and downsampling entries
	now := time.Now()
	for raw, t := range lastDedup {
		if now.Sub(t) > dedupWindow {
			delete(lastDedup, raw)
		}
	}

	for msgID, users := range lastForward {
		for userID, t := range users {
			if now.Sub(t) > *downsampleWindow {
				delete(users, userID)
			}
		}
		if len(users) == 0 {
			delete(lastForward, msgID)
		}
	}
}

func main() {
	flag.Parse()

	// enforce sane shards
	if *streamShards < 1 {
		*streamShards = 1
	}

	// compute deduplication window
	dedupWindow = time.Duration(*deduplicationWindowMs) * time.Millisecond

	if *numWorkers <= 0 {
		*numWorkers = runtime.NumCPU()
	}

	go startMemoryStatsLogging()

	// start HTTP metrics endpoint
	http.Handle("/", http.FileServer(http.Dir(*webPath)))
	http.HandleFunc("/metrics", metricsHandler)
	go func() {
		addr := fmt.Sprintf(":%d", *httpPort)
		log.Printf("HTTP serving on http://localhost%s/metrics", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	// start TCP stream listener
	go startStreamListener(*streamPort)

	// prepare UDP forwarding destinations
	var udpConns []*net.UDPConn
	if *destinations != "" {
		for _, dst := range strings.Split(*destinations, ",") {
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

	// listen for UDP NMEA packets
	udpAddrStr := fmt.Sprintf(":%d", *udpPort)
	pc, err := net.ListenPacket("udp", udpAddrStr)
	if err != nil {
		log.Fatalf("UDP listen %s: %v", udpAddrStr, err)
	}
	defer pc.Close()

	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	packetChan := make(chan *UDPPacket, 1000)
	for i := 0; i < *numWorkers; i++ {
		go worker(packetChan, udpConns, nmeaCodec)
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cleanupMetrics()
				cleanupDeduplicationState()
			}
		}
	}()

	for {
		buf := bufPool.Get().([]byte)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}
		raw := string(buf[:n])
		bufPool.Put(buf)
		pkt := packetPool.Get().(*UDPPacket)
		pkt.raw = raw
		pkt.sourceIP = strings.Split(addr.String(), ":")[0]

		// record receive metrics
		metricsMu.Lock()
		totalMessages++
		totalCounter.AddEvent()
		totalSourceTotals[pkt.sourceIP]++
		bytesReceivedTotals[pkt.sourceIP] += int64(n)
		totalBytesReceived += int64(n)
		totalSourceCounters.AddEvent()
		metricsMu.Unlock()

		bytesReceivedWindow.Add(int64(n))
		packetChan <- pkt
	}

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
	// 1 second deadline for handshake
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var req handshakeReq
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		conn.Close()
		return
	}
	// validate shard
	for _, s := range req.Shards {
		if s < 0 || s >= *streamShards {
			conn.Close()
			return
		}
	}
	// clear deadline
	conn.SetReadDeadline(time.Time{})

	client := &StreamClient{
		conn:          conn,
		shards:        req.Shards,
		version:       req.Version,
		ip:            strings.Split(conn.RemoteAddr().String(), ":")[0],
		messageWindow: &SlidingWindowCounter{},
		bytesWindow:   &SlidingWindowBytesCounter{},
	}
	clientsMu.Lock()
	clients = append(clients, client)
	clientsMu.Unlock()
	log.Printf("New stream client %s shards=%v version=%q", client.ip, client.shards, client.version)

	// detect disconnect
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
	return int(h.Sum32()) % *streamShards
}

func worker(ch <-chan *UDPPacket, udpConns []*net.UDPConn, nmea *aisnmea.NMEACodec) {
   for pkt := range ch {
		raw, srcIP := pkt.raw, pkt.sourceIP
		decoded, err := nmea.ParseSentence(raw)
		if err != nil || decoded == nil || decoded.Packet == nil {
			// decode failure metrics
			metricsMu.Lock()
			totalFailures++
			failureCounter.AddEvent()
			failureSourceTotals[srcIP]++
			if failureSourceCounters[srcIP] == nil {
				failureSourceCounters[srcIP] = &SlidingWindowCounter{}
			}
			failureSourceCounters[srcIP].AddEvent()
			metricsMu.Unlock()
			if *debug {
				log.Printf("[DEBUG] decode failure: %v | raw: %s", err, raw)
			}
 		        pkt.raw = ""
		        pkt.sourceIP = ""
		        packetPool.Put(pkt)
			continue
		}

		// extract MessageID & UserID
		msgID, userID := "unknown", "unknown"
		var m map[string]interface{}
		b, _ := json.Marshal(decoded.Packet)
		json.Unmarshal(b, &m)
		if mid, ok := m["MessageID"].(float64); ok {
			msgID = fmt.Sprintf("%.0f", mid)
		}
		if uid, ok := m["UserID"].(float64); ok {
			userID = fmt.Sprintf("%.0f", uid)
		}

		// ─── Record message for the user and message ID counts ─────────────────
		metricsMu.Lock()
		if perUserMessageIDCount[userID] == nil {
		    perUserMessageIDCount[userID] = map[string]int64{}
		}
		perUserMessageIDCount[userID][msgID]++
		metricsMu.Unlock()

		// ─── Deduplication ───────────────────────────────
		if dedupWindow > 0 {
			dedupMu.Lock()
			now := time.Now()
			if t, seen := lastDedup[raw]; seen && now.Sub(t) < dedupWindow {
				dedupMu.Unlock()
				// record dedup drop
				metricsMu.Lock()
				totalDeduplicated++
				dedupCounter.AddEvent()
				deduplicatedUserIDTotals[userID]++
				if dedupUserIDCounters[userID] == nil {
					dedupUserIDCounters[userID] = &SlidingWindowCounter{}
				}
				dedupUserIDCounters[userID].AddEvent()
				deduplicatedSourceTotals[srcIP]++
				if dedupSourceCounters[srcIP] == nil {
					dedupSourceCounters[srcIP] = &SlidingWindowCounter{}
				}
				dedupSourceCounters[srcIP].AddEvent()
				if dedupPerUserMessageIDCount[userID] == nil {
					dedupPerUserMessageIDCount[userID] = map[string]int64{}
				}
				dedupPerUserMessageIDCount[userID][msgID]++
				metricsMu.Unlock()
				continue
			}
			lastDedup[raw] = now
			dedupMu.Unlock()
		}

		// ─── Downsampling ───────────────────────────────
		if *downsampleWindow > 0 && downsampleTypes[msgID] {
			now := time.Now()
			downMu.Lock()
			if lastForward[msgID] == nil {
				lastForward[msgID] = map[string]time.Time{}
			}
			if now.Sub(lastForward[msgID][userID]) < *downsampleWindow {
				downMu.Unlock()
				// record downsample drop
				metricsMu.Lock()
				totalDownsampled++
				downsampledCounter.AddEvent()
				downsampledMessageTypeTotals[msgID]++
				if downsampledMessageTypeCounters[msgID] == nil {
					downsampledMessageTypeCounters[msgID] = &SlidingWindowCounter{}
				}
				downsampledMessageTypeCounters[msgID].AddEvent()
				downsampledUserIDTotals[userID]++
				if downsampledUserIDCounters[userID] == nil {
					downsampledUserIDCounters[userID] = &SlidingWindowCounter{}
				}
				downsampledUserIDCounters[userID].AddEvent()
				if downsampledPerUserMessageIDCount[userID] == nil {
					downsampledPerUserMessageIDCount[userID] = map[string]int64{}
				}
				downsampledPerUserMessageIDCount[userID][msgID]++
				metricsMu.Unlock()
				continue
			}
			lastForward[msgID][userID] = now
			downMu.Unlock()
		}

		// ─── Forward to UDP destinations ───────────────────────────────
		for _, conn := range udpConns {
			conn.Write([]byte(raw))
		}

		// ─── Record successful forward ────────────────────────────────
		metricsMu.Lock()
		messageIDTotals[msgID]++
		if messageIDCounters[msgID] == nil {
			messageIDCounters[msgID] = &SlidingWindowCounter{}
		}
		messageIDCounters[msgID].AddEvent()
		userIDTotals[userID]++
		if userIDCounters[userID] == nil {
			userIDCounters[userID] = &SlidingWindowCounter{}
		}
		userIDCounters[userID].AddEvent()

		totalForwarded++
		forwardedCounter.AddEvent()
		totalBytesForwarded += int64(len(raw))
		bytesForwardedWindow.Add(int64(len(raw)))
		metricsMu.Unlock()

		// ─── Compute shard and update shard metrics ────────────────────
		shard := shardForUser(userID)
		metricsMu.Lock()
		messagesPerShard[shard]++
		if userIDsPerShard[shard] == nil {
			userIDsPerShard[shard] = make(map[string]struct{})
		}
		userIDsPerShard[shard][userID] = struct{}{}
		metricsMu.Unlock()

		// ─── Stream to TCP clients for this shard ─────────────────────
		streamObj := StreamMessage{Message: decoded.Packet, SourceIP: srcIP, Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ShardID: shard, RawSentence: raw}
		out, err := json.Marshal(streamObj)
		if err == nil {
			clientsMu.Lock()
			for _, c := range clients {
				for _, sub := range c.shards {
					if sub == shard {
						c.mu.Lock()
						n, err := c.conn.Write(append(out, '\n'))
						if err == nil {
							c.bytesSent += int64(n)
							c.messagesSent++
							c.messageWindow.AddEvent()
							c.bytesWindow.Add(int64(n))
						}
						c.mu.Unlock()
					}
				}
			}
			clientsMu.Unlock()
		}
	}
}

// getTotalClients returns the total number of connected stream clients.
func getTotalClients() int {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	return len(clients)
}

// getShardsMissing returns a slice of shard indices with no client connected.
func getShardsMissing() []int {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	used := make(map[int]bool)
	for _, c := range clients {
		for _, s := range c.shards {
			used[s] = true
		}
	}

	missing := make([]int, 0)
	for i := 0; i < *streamShards; i++ {
		if !used[i] {
			missing = append(missing, i)
		}
	}
	return missing
}

// getShardsMultiple returns a map of shard indices to the list of client IPs for shards with >1 client.
func getShardsMultiple() map[int][]string {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	shardMap := make(map[int][]string)
	for _, c := range clients {
		for _, s := range c.shards {
			shardMap[s] = append(shardMap[s], c.ip)
		}
	}

	multiple := make(map[int][]string)
	for s, ips := range shardMap {
		if len(ips) > 1 {
			multiple[s] = ips
		}
	}
	return multiple
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	metricsMu.RLock()
	defer metricsMu.RUnlock()
	w.Header().Set("Content-Type", "application/json")

	// windowed metrics
	windowMsgs := totalCounter.Count(*metricWindowSize)
	windowFailures := failureCounter.Count(*metricWindowSize)
	windowDownsampled := downsampledCounter.Count(*metricWindowSize)
	windowDedup := dedupCounter.Count(*metricWindowSize)
	windowForwarded := forwardedCounter.Count(*metricWindowSize)
	windowBytesReceived := bytesReceivedWindow.Sum(*metricWindowSize)
	windowBytesForwarded := bytesForwardedWindow.Sum(*metricWindowSize)

	// per-client metrics
	clientMetrics := make([]map[string]interface{}, 0)
	clientsMu.Lock()
	for _, c := range clients {
		c.mu.Lock()
		clientMetrics = append(clientMetrics, map[string]interface{}{
			"ip":               c.ip,
			"shards":           c.shards,
			"messages_sent":    c.messagesSent,
			"bytes_sent":       c.bytesSent,
			"window_messages":  int64(c.messageWindow.Count(*metricWindowSize)),
			"window_bytes":     c.bytesWindow.Sum(*metricWindowSize),
		})
		c.mu.Unlock()
	}
	clientsMu.Unlock()

	// extra metrics
	totalClients := getTotalClients()
	shardsMissing := getShardsMissing()
	shardsMultiple := getShardsMultiple()

	// compute ratios
	var ratioTotal, ratioWindow float64
	if totalMessages > 0 {
		ratioTotal = float64(totalForwarded) / float64(totalMessages)
	}
	if windowMsgs > 0 {
		ratioWindow = float64(windowForwarded) / float64(windowMsgs)
	}

	// helper types
	type userStat struct {
		UserID       string           `json:"user_id"`
		Count        int64            `json:"count"`
		PerMessageID map[string]int64 `json:"per_message_id"`
	}
	type sourceStat struct {
		SourceIP string `json:"source_ip"`
		Count    int64  `json:"count"`
	}

	// top‑25 users (all forwarded)
	users := make([]userStat, 0, len(userIDTotals))
	for uid, cnt := range userIDTotals {
		users = append(users, userStat{UserID: uid, Count: cnt, PerMessageID: perUserMessageIDCount[uid]})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Count > users[j].Count })
	if len(users) > 25 {
		users = users[:25]
	}

	// top‑25 downsampled users
	dsUsers := make([]userStat, 0, len(downsampledUserIDTotals))
	for uid, cnt := range downsampledUserIDTotals {
		dsUsers = append(dsUsers, userStat{UserID: uid, Count: cnt, PerMessageID: downsampledPerUserMessageIDCount[uid]})
	}
	sort.Slice(dsUsers, func(i, j int) bool { return dsUsers[i].Count > dsUsers[j].Count })
	if len(dsUsers) > 25 {
		dsUsers = dsUsers[:25]
	}

	// top‑25 deduplicated users
	dupUsers := make([]userStat, 0, len(deduplicatedUserIDTotals))
	for uid, cnt := range deduplicatedUserIDTotals {
		dupUsers = append(dupUsers, userStat{UserID: uid, Count: cnt, PerMessageID: dedupPerUserMessageIDCount[uid]})
	}
	sort.Slice(dupUsers, func(i, j int) bool { return dupUsers[i].Count > dupUsers[j].Count })
	if len(dupUsers) > 25 {
		dupUsers = dupUsers[:25]
	}

	// failures by source
	failures := make([]sourceStat, 0, len(failureSourceTotals))
	for src, cnt := range failureSourceTotals {
		failures = append(failures, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].Count > failures[j].Count })
	if len(failures) > 25 {
		failures = failures[:25]
	}

	// totals by source
	totals := make([]sourceStat, 0, len(totalSourceTotals))
	for src, cnt := range totalSourceTotals {
		totals = append(totals, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Count > totals[j].Count })
	if len(totals) > 25 {
		totals = totals[:25]
	}

	// bytes received by source
	bytesTotals := make([]sourceStat, 0, len(bytesReceivedTotals))
	for src, cnt := range bytesReceivedTotals {
		bytesTotals = append(bytesTotals, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(bytesTotals, func(i, j int) bool { return bytesTotals[i].Count > bytesTotals[j].Count })
	if len(bytesTotals) > 25 {
		bytesTotals = bytesTotals[:25]
	}

	// top‑25 deduplicated by source
	dupSources := make([]sourceStat, 0, len(deduplicatedSourceTotals))
	for src, cnt := range deduplicatedSourceTotals {
		dupSources = append(dupSources, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(dupSources, func(i, j int) bool { return dupSources[i].Count > dupSources[j].Count })
	if len(dupSources) > 25 {
		dupSources = dupSources[:25]
	}

	// shard metrics
	msgs := make(map[int]int64, len(messagesPerShard))
	for s, cnt := range messagesPerShard {
		msgs[s] = cnt
	}
	uids := make(map[int]int, len(userIDsPerShard))
	for s, m := range userIDsPerShard {
		uids[s] = len(m)
	}

	// connected clients + send‑metrics
	type clientInfo struct {
		IP                string `json:"ip"`
		Shards            []int  `json:"shards"`
		Version           string `json:"version"`
		BytesSent         int64  `json:"bytes_sent"`
		MessagesSent      int64  `json:"messages_sent"`
		MessagesPerWindow int64  `json:"messages_per_window"`
		BytesPerWindow    int64  `json:"bytes_per_window"`
	}
	clientsMu.Lock()
	connected := make([]clientInfo, 0, len(clients))
	for _, c := range clients {
		c.mu.Lock()
		connected = append(connected, clientInfo{
			IP:                c.ip,
			Shards:            c.shards,
			Version:           c.version,
			BytesSent:         c.bytesSent,
			MessagesSent:      c.messagesSent,
			MessagesPerWindow: int64(c.messageWindow.Count(*metricWindowSize)),
			BytesPerWindow:    c.bytesWindow.Sum(*metricWindowSize),
		})
		c.mu.Unlock()
	}
	clientsMu.Unlock()

	resp := map[string]interface{}{
		"total_messages":                     totalMessages,
		"total_failures":                     totalFailures,
		"total_downsampled":                  totalDownsampled,
		"total_deduplicated":                 totalDeduplicated,
		"per_message_id":                     messageIDTotals,
		"per_downsampled_message_id":         downsampledMessageTypeTotals,
		"per_deduplicated_user_id":           deduplicatedUserIDTotals,
		"per_deduplicated_source":            deduplicatedSourceTotals,
		"top25_per_user_id":                  users,
		"top25_downsampled_per_user_id":      dsUsers,
		"top25_deduplicated_per_user_id":     dupUsers,
		"failures_by_source":                 failures,
		"totals_by_source":                   totals,
		"bytes_received_by_source":           bytesTotals,
		"top25_deduplicated_per_source":      dupSources,
		"window_messages":                    windowMsgs,
		"window_failures":                    windowFailures,
		"window_downsampled":                 windowDownsampled,
		"window_deduplicated":                windowDedup,
		"window_messages_forwarded":          windowForwarded,
		"bytes_received_window":              windowBytesReceived,
		"bytes_forwarded_window":             windowBytesForwarded,
		"messages_per_shard":                 msgs,
		"user_ids_per_shard":                 uids,
		"connected_clients":                  connected,
		"total_bytes_received":               totalBytesReceived,
		"total_messages_forwarded":           totalForwarded,
		"total_bytes_forwarded":              totalBytesForwarded,
		"ratio_forwarded_to_received":        ratioTotal,
		"window_ratio_forwarded_to_received": ratioWindow,
		"downsample_window_sec":              downsampleWindow.Seconds(),
		"deduplication_window_sec":           dedupWindow.Seconds(),
		"metric_window_size_sec":             (*metricWindowSize).Seconds(),
		"total_clients":                      totalClients,
		"shards_missing":                     shardsMissing,
		"shards_multiple":                    shardsMultiple,
	}

	json.NewEncoder(w).Encode(resp)
}
