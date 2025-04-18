package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
	"strconv"
	"crypto/tls"
	//_ "net/http/pprof"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	"github.com/eclipse/paho.mqtt.golang"
)

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

// UDPPacket holds raw data and source IP
type UDPPacket struct {
	raw      []byte
	sourceIP string
}

// StreamMessage is what we send to TCP clients
type StreamMessage struct {
	Message     interface{} `json:"message"`
	SourceIP    string      `json:"source_ip,omitempty"`
	Timestamp   string      `json:"timestamp"`
	ShardID     int         `json:"shard_id"`
	RawSentence string      `json:"raw_sentence"`
}

// handshakeReq is the JSON clients must send on connect
type handshakeReq struct {
	Shards  []int  `json:"shards"`
	Version string `json:"version"`
}

// StreamClient holds a connected client's state and send-metrics
type StreamClient struct {
	conn          net.Conn
	shards        []int
	version       string
	ip            string
	mu            sync.Mutex
	bytesSent     int64
	messagesSent  int64
	messageWindow *FixedWindowCounter
	bytesWindow   *FixedWindowBytesCounter
}

var (
	udpPort               = flag.Int("udp-listen-port", 8101, "UDP listen port for NMEA sentences")
	destinations          = flag.String("udp-destinations", "", "Comma-delimited list of host:port to forward valid sentences")
	metricWindowSize      = flag.Duration("metric-window-size", time.Minute, "Fixed window size for metrics")
	httpPort              = flag.Int("http-port", 8080, "HTTP port for metrics endpoint")
	numWorkers            = flag.Int("num-workers", 0, "Number of concurrent decode workers (0=CPUs)")
	downsampleWindow      = flag.Duration("downsample-window", 0, "Time window to downsample types 1,2,3,18,19 per UserID")
	deduplicationWindowMs = flag.Int("deduplication-window", 0, "Rolling window length in milliseconds for deduplication. 0 means no deduplication")
	webPath               = flag.String("web-path", "web", "Directory to serve static files from")
	debugFlag 	      = flag.Bool("debug", false, "Enable debug output")
	includeSource 	      = flag.Bool("include-source", false, "Include source_ip in the JSON responses")

	streamPort   = flag.Int("stream-port", 8102, "TCP port to listen on for decoded-sentence stream")
	streamShards = flag.Int("stream-shards", 1, "Number of shards for stream partitioning")

	dedupWindow time.Duration
	windowSize  int

	mqttServer   = flag.String("mqtt-server", "", "MQTT server host:port")
	mqttTLS      = flag.Bool("mqtt-tls", false, "Enable MQTT TLS")
	mqttAuth     = flag.String("mqtt-auth", "", "MQTT authentication (user:pass)")
	mqttTopic    = flag.String("mqtt-topic", "aisDecodes/message", "MQTT topic to publish messages")
)

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
	downsampleTypes = map[string]bool{"1": true, "2": true, "3": true, "18": true, "19": true}
	downMu          sync.Mutex
	lastForward     = map[string]map[string]time.Time{}
)

var (
	dedupMu   sync.Mutex
	lastDedup = map[uint32]time.Time{}
)

var previousPeriodMetrics struct {
	windowMsgs          int64
	windowFailures      int64
	windowDownsampled   int64
	windowDedup         int64
	windowForwarded     int64
	windowBytesReceived int64
	windowBytesForwarded int64
}

func logMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Log memory stats
	log.Printf(
		"Memory Stats: Alloc = %v MB, TotalAlloc = %v MB, Sys = %v MB, HeapAlloc = %v MB, HeapSys = %v MB, NumGC = %v, LastGC = %v",
		m.Alloc/1024/1024, // Allocated memory
		m.TotalAlloc/1024/1024, // Total allocated memory
		m.Sys/1024/1024, // Memory obtained from the OS
		m.HeapAlloc/1024/1024, // Allocated heap memory
		m.HeapSys/1024/1024, // Memory used by the heap
		m.NumGC, // Number of GC cycles
		m.LastGC, // Timestamp of last GC
	)
}

func startMemoryStatsLogging() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		logMemoryStats() // Log memory stats every minute
	}
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

func cleanupDeduplicationState() {
    dedupMu.Lock()
    defer dedupMu.Unlock()

    now := time.Now()

    // Log the number of entries in lastDedup before cleanup
    // log.Printf("Before cleanup: lastDedup size = %d", len(lastDedup))

    // Track and log cleanup of expired entries in lastDedup map
    expiredDedupCount := 0
    notExpiredCount := 0
    for raw, t := range lastDedup {
        if now.Sub(t) > dedupWindow {
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
    if len(lastDedup) > 100000 {  // Arbitrary threshold to indicate if map is getting too large
        log.Printf("WARNING: lastDedup has grown significantly. Total entries: %d", len(lastDedup))
    }
}



// startMetricsReset resets all fixed-window counters every metricWindowSize period.
func startMetricsReset() {
	ticker := time.NewTicker(*metricWindowSize)
	defer ticker.Stop()

	for range ticker.C {
		// Capture current metrics into the previousPeriodMetrics before resetting
		metricsMu.Lock()
		previousPeriodMetrics.windowMsgs = totalCounter.Count()
		previousPeriodMetrics.windowFailures = failureCounter.Count()
		previousPeriodMetrics.windowDownsampled = downsampledCounter.Count()
		previousPeriodMetrics.windowDedup = dedupCounter.Count()
		previousPeriodMetrics.windowForwarded = forwardedCounter.Count()
		previousPeriodMetrics.windowBytesReceived = bytesReceivedWindow.Sum()
		previousPeriodMetrics.windowBytesForwarded = bytesForwardedWindow.Sum()
		metricsMu.Unlock()

		// Reset all counters
		metricsMu.Lock()
		totalCounter.Reset()
		failureCounter.Reset()
		downsampledCounter.Reset()
		dedupCounter.Reset()
		forwardedCounter.Reset()
		bytesReceivedWindow.Reset()
		bytesForwardedWindow.Reset()
		metricsMu.Unlock()

		// Reset client-specific windows
		clientsMu.Lock()
		for _, c := range clients {
			c.messageWindow.Reset()
			c.bytesWindow.Reset()
		}
		clientsMu.Unlock()
	}
}

func main() {

	flag.Parse()

	    if *mqttServer != "" {
        // Set MQTT options
        opts := mqtt.NewClientOptions()
        opts.AddBroker("tcp://" + *mqttServer)

        // Set MQTT TLS if enabled
        if *mqttTLS {
            tlsConfig := &tls.Config{InsecureSkipVerify: true}
            opts.SetTLSConfig(tlsConfig)
        }

        // Set MQTT authentication if provided
        if *mqttAuth != "" {
            authParts := strings.SplitN(*mqttAuth, ":", 2)
            if len(authParts) == 2 {
                opts.SetUsername(authParts[0])
                opts.SetPassword(authParts[1])
            } else {
                log.Fatalf("Invalid MQTT authentication format. Expected user:pass.")
            }
        }

        // Set client ID and clean session
        opts.SetClientID("go-ais-decoder")
        opts.SetCleanSession(true)

        // Connect to the MQTT broker
        mqttClient = mqtt.NewClient(opts)
        if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
            log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
        }

        log.Printf("Connected to MQTT broker: %s", *mqttServer)
    }


	if *streamShards < 1 {
		*streamShards = 1
	}
	dedupWindow = time.Duration(*deduplicationWindowMs) * time.Millisecond
	if *numWorkers <= 0 {
		*numWorkers = runtime.NumCPU()
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
	go startMemoryStatsLogging()

	http.Handle("/", http.FileServer(http.Dir(*webPath)))
	http.HandleFunc("/metrics", metricsHandler)
	go func() {
		addr := fmt.Sprintf(":%d", *httpPort)
		log.Printf("HTTP serving on http://localhost%s/metrics", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	go startStreamListener(*streamPort)

	// Set up UDP connections
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

	// Listen for UDP packets
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

	// Main packet loop
	for {
		buf := bufPool.Get().([]byte)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}
		rawBytes := buf[:n]
		bufPool.Put(buf)
		pkt := packetPool.Get().(*UDPPacket)
		pkt.raw = rawBytes
		pkt.sourceIP = strings.Split(addr.String(), ":")[0]

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
		if s < 0 || s >= *streamShards {
			conn.Close()
			return
		}
	}
	conn.SetReadDeadline(time.Time{})

	client := &StreamClient{
		conn:          conn,
		shards:        req.Shards,
		version:       req.Version,
		ip:            strings.Split(conn.RemoteAddr().String(), ":")[0],
		messageWindow: NewFixedWindowCounter(),
		bytesWindow:   NewFixedWindowBytesCounter(),
	}
	clientsMu.Lock()
	clients = append(clients, client)
	clientsMu.Unlock()
	log.Printf("New stream client %s shards=%v version=%q", client.ip, client.shards, client.version)

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
        rawBytes, srcIP := pkt.raw, pkt.sourceIP
        rawStr := *(*string)(unsafe.Pointer(&rawBytes))

        // Use FNV-1a hash of the raw message for deduplication
        hashedMsg := fnvHash(rawStr)

        // Decode the NMEA sentence
        decoded, err := nmea.ParseSentence(rawStr)
        if err != nil || decoded == nil || decoded.Packet == nil {
            metricsMu.Lock()
            totalFailures++
            failureCounter.AddEvent()
            failureSourceTotals[srcIP]++
            if failureSourceCounters[srcIP] == nil {
                failureSourceCounters[srcIP] = NewFixedWindowCounter()
            }
            failureSourceCounters[srcIP].AddEvent()
            metricsMu.Unlock()
            if *debugFlag {
                log.Printf("[DEBUG] decode failure: %v | raw: %s", err, pkt.raw)
            }
            pkt.raw = nil
            pkt.sourceIP = ""
            packetPool.Put(pkt)
            continue
        }

        hdr := decoded.Packet.GetHeader()
        msgIDCache.RLock()
        mid, ok := msgIDCache.m[hdr.MessageID]
        msgIDCache.RUnlock()
        if !ok {
            mid = strconv.Itoa(int(hdr.MessageID))
            msgIDCache.Lock()
            msgIDCache.m[hdr.MessageID] = mid
            msgIDCache.Unlock()
        }

        userIDCache.RLock()
        uid, ok := userIDCache.m[hdr.UserID]
        userIDCache.RUnlock()
        if !ok {
            uid = strconv.FormatUint(uint64(hdr.UserID), 10)
            userIDCache.Lock()
            userIDCache.m[hdr.UserID] = uid
            userIDCache.Unlock()
        }

        msgID, userID := mid, uid

        // Deduplication check based on FNV-1a hash
        if dedupWindow > 0 {
            dedupMu.Lock()
            now := time.Now()
            if t, seen := lastDedup[hashedMsg]; seen && now.Sub(t) < dedupWindow {
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
                continue
            }
            lastDedup[hashedMsg] = now
            dedupMu.Unlock()
        }

        // Downsample logic
        if *downsampleWindow > 0 && downsampleTypes[msgID] {
            now := time.Now()
            downMu.Lock()
            if lastForward[msgID] == nil {
                lastForward[msgID] = map[string]time.Time{}
            }
            if now.Sub(lastForward[msgID][userID]) < *downsampleWindow {
                downMu.Unlock()
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
                continue
            }
            lastForward[msgID][userID] = now
            downMu.Unlock()
        }

        // Forward to UDP destinations
        for _, conn := range udpConns {
            conn.Write(pkt.raw)
        }

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

        shard := shardForUser(userID)
        metricsMu.Lock()
        messagesPerShard[shard]++
        if userIDsPerShard[shard] == nil {
            userIDsPerShard[shard] = make(map[string]struct{})
        }
        userIDsPerShard[shard][userID] = struct{}{}
        metricsMu.Unlock()

        // Split the raw NMEA sentence by commas and extract the channel (5th field)
	fields := strings.Split(rawStr, ",")
	channel := "Unknown"
	if len(fields) > 5 {
	    channel = string(fields[4][0]) // Extract only the first character of the 5th field (either 'A' or 'B')
	}

        buf := jsonBufPool.Get().(*bytes.Buffer)
        buf.Reset()
        streamObj := StreamMessage{
            Message:     decoded.Packet,
            Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
            ShardID:     shard,
            RawSentence: rawStr,
        }

	if *includeSource { // Check if -include-source is true
	    streamObj.SourceIP = srcIP
	}

        // Add the channel field
        streamObj.Message = map[string]interface{}{
            "message": decoded.Packet,
            "channel": channel,
        }

        json.NewEncoder(buf).Encode(streamObj)
        out := buf.Bytes()
        jsonBufPool.Put(buf)

        if mqttClient != nil && mqttClient.IsConnected() {
            token := mqttClient.Publish(*mqttTopic, 0, false, out)
            token.Wait()
        }

        clientsMu.Lock()
        for _, c := range clients {
            for _, sub := range c.shards {
                if sub == shard {
                    c.mu.Lock()
                    n, err := c.conn.Write(append(out))
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
	for _, c := range clients {
		for _, s := range c.shards {
			used[s] = true
		}
	}
	missing := []int{}
	for i := 0; i < *streamShards; i++ {
		if !used[i] {
			missing = append(missing, i)
		}
	}
	return missing
}

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

	windowMsgs := previousPeriodMetrics.windowMsgs
	windowFailures := previousPeriodMetrics.windowFailures
	windowDownsampled := previousPeriodMetrics.windowDownsampled
	windowDedup := previousPeriodMetrics.windowDedup
	windowForwarded := previousPeriodMetrics.windowForwarded
	windowBytesReceived := previousPeriodMetrics.windowBytesReceived
	windowBytesForwarded := previousPeriodMetrics.windowBytesForwarded

	clientMetrics := []map[string]interface{}{}
	clientsMu.Lock()
	for _, c := range clients {
		c.mu.Lock()
		clientMetrics = append(clientMetrics, map[string]interface{}{
			"ip":              c.ip,
			"shards":          c.shards,
			"messages_sent":   c.messagesSent,
			"bytes_sent":      c.bytesSent,
			"window_messages": c.messageWindow.Count(),
			"window_bytes":    c.bytesWindow.Sum(),
		})
		c.mu.Unlock()
	}
	clientsMu.Unlock()

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

	users := []userStat{}
	for uid, cnt := range userIDTotals {
		users = append(users, userStat{UserID: uid, Count: cnt, PerMessageID: perUserMessageIDCount[uid]})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Count > users[j].Count })
	if len(users) > 25 {
		users = users[:25]
	}

	dsUsers := []userStat{}
	for uid, cnt := range downsampledUserIDTotals {
		dsUsers = append(dsUsers, userStat{UserID: uid, Count: cnt, PerMessageID: downsampledPerUserMessageIDCount[uid]})
	}
	sort.Slice(dsUsers, func(i, j int) bool { return dsUsers[i].Count > dsUsers[j].Count })
	if len(dsUsers) > 25 {
		dsUsers = dsUsers[:25]
	}

	dupUsers := []userStat{}
	for uid, cnt := range deduplicatedUserIDTotals {
		dupUsers = append(dupUsers, userStat{UserID: uid, Count: cnt, PerMessageID: dedupPerUserMessageIDCount[uid]})
	}
	sort.Slice(dupUsers, func(i, j int) bool { return dupUsers[i].Count > dupUsers[j].Count })
	if len(dupUsers) > 25 {
		dupUsers = dupUsers[:25]
	}

	failures := []sourceStat{}
	for src, cnt := range failureSourceTotals {
		failures = append(failures, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].Count > failures[j].Count })
	if len(failures) > 25 {
		failures = failures[:25]
	}

	totals := []sourceStat{}
	for src, cnt := range totalSourceTotals {
		totals = append(totals, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Count > totals[j].Count })
	if len(totals) > 25 {
		totals = totals[:25]
	}

	bytesTotals := []sourceStat{}
	for src, cnt := range bytesReceivedTotals {
		bytesTotals = append(bytesTotals, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(bytesTotals, func(i, j int) bool { return bytesTotals[i].Count > bytesTotals[j].Count })
	if len(bytesTotals) > 25 {
		bytesTotals = bytesTotals[:25]
	}

	dupSources := []sourceStat{}
	for src, cnt := range deduplicatedSourceTotals {
		dupSources = append(dupSources, sourceStat{SourceIP: src, Count: cnt})
	}
	sort.Slice(dupSources, func(i, j int) bool { return dupSources[i].Count > dupSources[j].Count })
	if len(dupSources) > 25 {
		dupSources = dupSources[:25]
	}

	msgs := make(map[int]int64, len(messagesPerShard))
	for s, cnt := range messagesPerShard {
		msgs[s] = cnt
	}
	uids := make(map[int]int, len(userIDsPerShard))
	for s, m := range userIDsPerShard {
		uids[s] = len(m)
	}

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
	connected := []clientInfo{}
	for _, c := range clients {
		c.mu.Lock()
		connected = append(connected, clientInfo{
			IP:                c.ip,
			Shards:            c.shards,
			Version:           c.version,
			BytesSent:         c.bytesSent,
			MessagesSent:      c.messagesSent,
			MessagesPerWindow: c.messageWindow.Count(),
			BytesPerWindow:    c.bytesWindow.Sum(),
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
