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
	"math"
	"os"
        "path/filepath"
        "io/ioutil"
	//_ "net/http/pprof"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	"github.com/eclipse/paho.mqtt.golang"
)

var cfg Config

var (
    startTime = time.Now()
)

var cfgDir string

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
	Shards      []int  `json:"shards"`
	Description string `json:"description"`
}

// StreamClient holds a connected client's state and send-metrics
type StreamClient struct {
	conn          net.Conn
	shards        []int
	description   string
	ip            string
	mu            sync.Mutex
	bytesSent     int64
	messagesSent  int64
	messageWindow *FixedWindowCounter
	bytesWindow   *FixedWindowBytesCounter
}

type UDPDestination struct {
    Host       string `json:"host"`
    Port       int    `json:"port"`
    Shards     []int  `json:"shards"`
    Description string `json:"description"`
}

type UDPDestinationMetrics struct {
    Destination string   `json:"destination"`
    Description string   `json:"description"`
    Shards      []int    `json:"shards"`    // Added field for shard IDs
    MessagesSent int64   `json:"messages_sent"`
    BytesSent    int64   `json:"bytes_sent"`
}

var udpDestinationMetrics = map[string]*UDPDestinationMetrics{}

type Config struct {
    UDPListenPort          int      `json:"udp_listen_port"`
    Destinations           []UDPDestination `json:"udp_destinations"`
    MetricWindowSize       int      `json:"metric_window_size"`
    HTTPPort               int      `json:"http_port"`
    NumWorkers             int      `json:"num_workers"`
    DownsampleWindow       int      `json:"downsample_window"`
    DeduplicationWindowMs  int      `json:"deduplication_window_ms"`
    WebPath                string   `json:"web_path"`
    Debug                  bool     `json:"debug"`
    IncludeSource          bool     `json:"include_source"`
    StreamPort             int      `json:"stream_port"`
    StreamShards           int      `json:"stream_shards"`
    MQTTServer             string   `json:"mqtt_server"`
    MQTTTLS                bool     `json:"mqtt_tls"`
    MQTTAuth               string   `json:"mqtt_auth"`
    MQTTTopic              string   `json:"mqtt_topic"`
    DownsampleMessageTypes []string `json:"downsample_message_types"`
    BlockedIPs 		   []string `json:"blocked_ips"`
}

var (
    udpPort               int
    destinations          string
    metricWindowSize      time.Duration
    downsampleWindow      time.Duration
    deduplicationWindowMs int
    httpPort              int
    numWorkers            int
    webPath               string
    debugFlag             bool
    includeSource         bool
    streamPort            int
    streamShards          int
    mqttServer            string
    mqttTLS               bool
    mqttAuth              string
    mqttTopic             string
    dedupWindow 	  time.Duration
    windowSize  	  int
    blockedIPs 		  map[string]struct{}
)

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

func initializeUDPDestinationMetrics() {
    for _, dest := range cfg.Destinations {
        if len(dest.Shards) == 0 { // Skip destinations with no shards
            log.Printf("Skipping UDP destination %s:%d because it has no shards configured.", dest.Host, dest.Port)
            continue
        }

        destStr := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
        udpDestinationMetrics[destStr] = &UDPDestinationMetrics{
            Destination: destStr,
            Description: dest.Description,
            Shards:      dest.Shards,
            MessagesSent: 0,
            BytesSent:    0,
        }
    }
}

// startMetricsReset resets all fixed-window counters every metricWindowSize period.
func startMetricsReset() {
	ticker := time.NewTicker(metricWindowSize)
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
    metricWindowSize      = time.Duration(cfg.MetricWindowSize) * time.Second
    downsampleWindow      = time.Duration(cfg.DownsampleWindow) * time.Second
    deduplicationWindowMs = cfg.DeduplicationWindowMs
    dedupWindow	      = time.Duration(deduplicationWindowMs) * time.Millisecond
    windowSize            = int(metricWindowSize.Seconds())

    // ints & strings
    udpPort       = cfg.UDPListenPort
    destinations  = strings.Join(destinationStrings, ",")
    httpPort      = cfg.HTTPPort
    numWorkers    = cfg.NumWorkers
    webPath       = cfg.WebPath
    debugFlag     = cfg.Debug
    includeSource = cfg.IncludeSource
    streamPort    = cfg.StreamPort
    streamShards  = cfg.StreamShards

    // MQTT
    mqttServer = cfg.MQTTServer
    mqttTLS    = cfg.MQTTTLS
    mqttAuth   = cfg.MQTTAuth
    mqttTopic  = cfg.MQTTTopic


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

	// Listen for UDP packets
	udpAddrStr := fmt.Sprintf(":%d", udpPort)
	pc, err := net.ListenPacket("udp", udpAddrStr)
	if err != nil {
		log.Fatalf("UDP listen %s: %v", udpAddrStr, err)
	}
	defer pc.Close()

	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	packetChan := make(chan *UDPPacket, 1000)
	for i := 0; i < numWorkers; i++ {
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
		if s < 0 || s >= streamShards {
			conn.Close()
			return
		}
	}
	conn.SetReadDeadline(time.Time{})

	client := &StreamClient{
		conn:          conn,
		shards:        req.Shards,
		description:   req.Description,
		ip:            strings.Split(conn.RemoteAddr().String(), ":")[0],
		messageWindow: NewFixedWindowCounter(),
		bytesWindow:   NewFixedWindowBytesCounter(),
	}

	clientsMu.Lock()
	clients = append(clients, client)
	clientsMu.Unlock()
	log.Printf("New stream client %s shards=%v description=%q", client.ip, client.shards, client.description)

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
        rawStr := *(*string)(unsafe.Pointer(&rawBytes))

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

        metricsMu.Lock()
        totalMessages++
        totalCounter.AddEvent()
        totalSourceTotals[pkt.sourceIP]++
        bytesReceivedTotals[pkt.sourceIP] += int64(len(pkt.raw))
        totalBytesReceived += int64(len(pkt.raw))
        totalSourceCounters.AddEvent()
        metricsMu.Unlock()

        bytesReceivedWindow.Add(int64(len(pkt.raw)))

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
            if debugFlag {
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
        if downsampleWindow > 0 && downsampleTypes[msgID] {
            now := time.Now()
            downMu.Lock()
            if lastForward[msgID] == nil {
                lastForward[msgID] = map[string]time.Time{}
            }
            if now.Sub(lastForward[msgID][userID]) < downsampleWindow {
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

        // Now send the raw message to all matching destinations based on shardID
        // Determine which shard the current packet belongs to (using existing logic)
        shardID := shardForUser(userID) // Sharding logic for this message
        metricsMu.Lock()
        messagesPerShard[shardID]++
        if userIDsPerShard[shardID] == nil {
            userIDsPerShard[shardID] = make(map[string]struct{})
        }
        userIDsPerShard[shardID][userID] = struct{}{}
        metricsMu.Unlock()

        // Forward the raw message to the appropriate destinations based on shard ID
        for _, dest := range cfg.Destinations {
            for _, destShard := range dest.Shards {
                if destShard == shardID { // If this shard matches, forward the message
                    udpAddr := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
                    udpConn, err := net.Dial("udp", udpAddr)
                    if err != nil {
                        log.Printf("Failed to connect to %s: %v", udpAddr, err)
                        continue
                    }
                    _, err = udpConn.Write(pkt.raw) // Send only the raw message
                    if err != nil {
                        log.Printf("Failed to send UDP packet to %s: %v", udpAddr, err)
                    }
                    udpConn.Close()
		    udpDestStr := fmt.Sprintf("%s:%d", dest.Host, dest.Port)
		    udpDestinationMetrics[udpDestStr].MessagesSent++
		    udpDestinationMetrics[udpDestStr].BytesSent += int64(len(pkt.raw))
                    break // After sending to one destination, no need to send further
                }
            }
        }

        // Update metrics
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

        // Split the raw NMEA sentence by commas and extract the channel (5th field)
        fields := strings.Split(rawStr, ",")
        channel := "Unknown"
        if len(fields) > 5 {
            channel = string(fields[4][0])
        }

        buf := jsonBufPool.Get().(*bytes.Buffer)
        buf.Reset()
        streamObj := StreamMessage{
            Message:     decoded.Packet,
            Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
            ShardID:     shardID, // Send the shard ID for transparency
            RawSentence: rawStr,
        }

        if includeSource { // Check if -include-source is true
            streamObj.SourceIP = srcIP
        }

        // Add the channel field
        streamObj.Message = map[string]interface{}{
            "packet": decoded.Packet,
            "channel": channel,
        }

        json.NewEncoder(buf).Encode(streamObj)
        out := buf.Bytes()
        jsonBufPool.Put(buf)

	if mqttClient != nil && mqttClient.IsConnected() {
	    topic := fmt.Sprintf("%s/%d/%s/%s/message", mqttTopic, shardID, userID, msgID)
	    token := mqttClient.Publish(topic, 0, false, out)
	    token.Wait()
	}
	out = append(out, 0)
        clientsMu.Lock()
        for _, c := range clients {
            for _, sub := range c.shards {
                if sub == shardID {
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

func getShardsMultiple() map[int][]string {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	shardMap := make(map[int][]string)

	// Track shards used by clients
	for _, c := range clients {
		for _, s := range c.shards {
			shardMap[s] = append(shardMap[s], c.ip)
		}
	}

	// Track shards used by UDP destinations (include host + port)
	for _, dest := range cfg.Destinations {
		for _, s := range dest.Shards {
			udpDest := fmt.Sprintf("%s:%d", dest.Host, dest.Port) // Include host + port
			shardMap[s] = append(shardMap[s], udpDest)
		}
	}

	// Filter out shards that have multiple sources (either clients or multiple UDP destinations)
	multiple := make(map[int][]string)
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

        udpMetrics := []UDPDestinationMetrics{}
        for _, destMetrics := range udpDestinationMetrics {
             udpMetrics = append(udpMetrics, *destMetrics)
        }

	type clientInfo struct {
		IP                string `json:"ip"`
		Shards            []int  `json:"shards"`
		Description       string `json:"description"`
		BytesSent         int64  `json:"bytes_sent"`
		MessagesSent      int64  `json:"messages_sent"`
		MessagesPerWindow int64  `json:"messages_per_window"`
		BytesPerWindow    int64  `json:"bytes_per_window"`
	}
	blockedIPMetrics := []map[string]interface{}{}
	clientsMu.Lock()
    	for ip, counter := range blockedIPCounters {
        	blockedIPMetrics = append(blockedIPMetrics, map[string]interface{}{
        	    "source_ip": ip,
	            "messages_blocked": counter.Count(),
	        })
	}
	connected := []clientInfo{}
	for _, c := range clients {
		c.mu.Lock()
		connected = append(connected, clientInfo{
			IP:                c.ip,
			Shards:            c.shards,
			Description:       c.description,
			BytesSent:         c.bytesSent,
			MessagesSent:      c.messagesSent,
			MessagesPerWindow: c.messageWindow.Count(),
			BytesPerWindow:    c.bytesWindow.Sum(),
		})
		c.mu.Unlock()
	}
	clientsMu.Unlock()

	resp := map[string]interface{}{
		"uptime_seconds":                     uptime,
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
		"metric_window_size_sec":             (metricWindowSize).Seconds(),
		"total_clients":                      totalClients,
		"shards_missing":                     shardsMissing,
		"shards_multiple":                    shardsMultiple,
		"udp_destinations": 		      udpMetrics,
		"blocked_ip_metrics":                 blockedIPMetrics,
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
