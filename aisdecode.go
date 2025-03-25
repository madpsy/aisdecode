package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io/v2/socket"
)

// Global client list and mutex.
var (
	clients      []*socket.Socket
	clientsMutex sync.Mutex
)

// Global vessel data map and mutex.
var (
	vesselDataMutex sync.Mutex
	// Each key is a vessel's userid, and the value is the current merged state (as a map).
	vesselData = make(map[string]map[string]interface{})
)

var previousVesselData map[string]map[string]interface{}

// New global flag and mutex for change detection.
var (
	changeAvailable bool
	changeMutex     sync.Mutex
)

// Deduplication state: stores messages and their timestamps.
type dedupeState struct {
	message   string
	timestamp time.Time
}

// mergeMaps merges newData into baseData. Values in newData override those in baseData.
func mergeMaps(baseData, newData map[string]interface{}) map[string]interface{} {
	if baseData == nil {
		baseData = make(map[string]interface{})
	}

	for key, value := range newData {
		// Prioritize Latitude, Longitude, and Callsign in the merge
		if key == "Latitude" || key == "Longitude" || key == "CallSign" {
			baseData[key] = value
		} else if baseData[key] == nil {
			baseData[key] = value
		}
	}

	return baseData
}

func filterCompleteVesselData(vesselData map[string]map[string]interface{}) map[string]map[string]interface{} {
	filteredData := make(map[string]map[string]interface{})
	for id, vesselInfo := range vesselData {
		hasLat := vesselInfo["Latitude"] != nil
		hasLon := vesselInfo["Longitude"] != nil
		hasCall := vesselInfo["CallSign"] != nil

		// Only include vessels with all required fields
		if hasLat && hasLon && hasCall {
			filteredData[id] = vesselInfo
		}
	}
	return filteredData
}

// Compare two vessel maps (map[string]interface{}) recursively.
func isInterfaceMapEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, vA := range a {
		vB, ok := b[k]
		if !ok || !compareValues(vA, vB) {
			return false
		}
	}
	return true
}

// Helper function to compare two values.
func compareValues(currentValue, previousValue interface{}) bool {
	switch currentTyped := currentValue.(type) {
	case map[string]interface{}:
		previousTyped, ok := previousValue.(map[string]interface{})
		if !ok {
			return false
		}
		return isInterfaceMapEqual(currentTyped, previousTyped)
	default:
		return currentValue == previousValue
	}
}

// Compare currentData and previousData, which are maps of vessel IDs to vessel data.
func isDataChanged(currentData, previousData map[string]map[string]interface{}) bool {
	if len(currentData) != len(previousData) {
		return true
	}

	for id, currentVessel := range currentData {
		previousVessel, exists := previousData[id]
		if !exists {
			return true
		}
		if !isInterfaceMapEqual(currentVessel, previousVessel) {
			return true
		}
	}
	return false
}

func deepCopyVesselData(original map[string]map[string]interface{}) map[string]map[string]interface{} {
	copy := make(map[string]map[string]interface{})
	for id, vesselInfo := range original {
		newInfo := make(map[string]interface{})
		for k, v := range vesselInfo {
			newInfo[k] = v
		}
		copy[id] = newInfo
	}
	return copy
}

// Check if the message is a duplicate within the deduplication window.
func isDuplicate(message string, dedupeWindow []dedupeState, windowDuration time.Duration) bool {
	message = strings.TrimSpace(message)
	now := time.Now()
	dedupeWindow = filterWindow(dedupeWindow, now.Add(-windowDuration))
	for _, state := range dedupeWindow {
		if state.message == message && now.Sub(state.timestamp) < windowDuration {
			return true // Duplicate found
		}
	}
	return false
}

// Filter the deduplication window to only include messages within the time range.
func filterWindow(window []dedupeState, cutoff time.Time) []dedupeState {
	filtered := []dedupeState{}
	for _, state := range window {
		if state.timestamp.After(cutoff) {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func main() {
	// Command-line flags.
	serialPort := flag.String("serial-port", "", "Serial port device (optional)")
	baud := flag.Int("baud", 38400, "Baud rate (default: 38400), ignored if -serial-port is not specified")
	wsPort := flag.Int("ws-port", 8100, "WebSocket port (default: 8100)")
	webRoot := flag.String("web-root", ".", "Web root directory (default: current directory)")
	debug := flag.Bool("debug", false, "Enable debug output")
	showDecodes := flag.Bool("show-decodes", false, "Output the decoded messages")
	aggregator := flag.String("aggregator", "", "Aggregator host/ip:port (optional)")
	udpListenPort := flag.Int("udp-listen-port", 8101, "UDP listen port for incoming NMEA data (default: 8101)")
	dedupeWindowDuration := flag.Int("dedupe-window", 1000, "Deduplication window in milliseconds (default: 1000, set to 0 to disable deduplication)")
	dumpVesselData := flag.Bool("dump-vessel-data", false, "Log the latest vessel data to the screen whenever it is updated")
	// New update interval flag in seconds, default 2 seconds.
	updateInterval := flag.Int("update-interval", 2, "Update interval in seconds for emitting latest vessel data (default: 2)")
	flag.Parse()

	// Initialize the previous vessel data state.
	previousVesselData = make(map[string]map[string]interface{})

	// --- Setup Socket.IO server ---
	engineServer := types.CreateServer(nil)
	sioServer := socket.NewServer(engineServer, nil)

	sioServer.On("connection", func(args ...any) {
		client := args[0].(*socket.Socket)
		log.Printf("Socket.IO client connected: %s", client.Id())
		clientsMutex.Lock()
		clients = append(clients, client)
		clientsMutex.Unlock()
		client.Join("ais_data")
		client.Join("latest_vessel_data")
		vesselDataMutex.Lock()
		latestData := filterCompleteVesselData(vesselData)
		vesselDataMutex.Unlock()
		latestDataJSON, err := json.Marshal(latestData)
		if err != nil {
			log.Printf("Error marshaling latest vessel data: %v", err)
			return
		}
		if err := client.Emit("latest_vessel_data", string(latestDataJSON)); err != nil {
			log.Printf("Error sending latest vessel data to client %s: %v", client.Id(), err)
		}
		client.On("disconnect", func(args ...any) {
			log.Printf("Socket.IO client disconnected: %s", client.Id())
			clientsMutex.Lock()
			for i, c := range clients {
				if c == client {
					clients = append(clients[:i], clients[i+1:]...)
					break
				}
			}
			clientsMutex.Unlock()
		})
	})

	// --- Setup HTTP server ---
	fs := http.FileServer(http.Dir(*webRoot))
	http.Handle("/", fs)
	http.Handle("/socket.io/", engineServer)
	go func() {
		addr := fmt.Sprintf(":%d", *wsPort)
		log.Printf("Starting HTTP/Socket.IO server on %s, serving web root: %s", addr, filepath.Clean(*webRoot))
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// --- Setup AIS decoder ---
	var port serial.Port
	if *serialPort != "" {
		mode := &serial.Mode{BaudRate: *baud}
		var err error
		port, err = serial.Open(*serialPort, mode)
		if err != nil {
			log.Fatalf("failed to open serial port: %v", err)
		}
		defer port.Close()
	}

	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	// Setup UDP aggregator if needed.
	var udpConn *net.UDPConn
	if *aggregator != "" {
		parts := strings.Split(*aggregator, ":")
		if len(parts) != 2 {
			log.Fatal("Invalid aggregator format. Expected host/ip:port")
		}
		host, portStr := parts[0], parts[1]
		udpPort, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("Invalid port number: %v", err)
		}
		udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, udpPort))
		if err != nil {
			log.Fatalf("Failed to resolve UDP address: %v", err)
		}
		udpConn, err = net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			log.Fatalf("Failed to create UDP connection: %v", err)
		}
		defer udpConn.Close()
		log.Printf("[DEBUG] Connected to aggregator at %s", udpAddr.String())
	}

	var websocketDedupeWindow []dedupeState
	var aggregatorDedupeWindow []dedupeState
	windowDuration := time.Duration(*dedupeWindowDuration) * time.Millisecond

	// --- Start UDP listener for incoming NMEA data ---
	udpAddrStr := fmt.Sprintf(":%d", *udpListenPort)
	udpListener, err := net.ListenPacket("udp", udpAddrStr)
	if err != nil {
		log.Fatalf("Error starting UDP listener: %v", err)
	}
	defer udpListener.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, addr, err := udpListener.ReadFrom(buf)
			if err != nil {
				log.Printf("Error reading UDP message: %v", err)
				continue
			}

			rawNmea := string(buf[:n])
			currentTime := time.Now().Format(time.RFC3339)
			source := addr.String()

			if *debug {
				log.Printf("[DEBUG] Received from UDP (%s) at %s: %s", source, currentTime, rawNmea)
			}

			if *dedupeWindowDuration > 0 && isDuplicate(rawNmea, aggregatorDedupeWindow, windowDuration) {
				if *debug {
					log.Printf("[DEBUG] Dropped duplicate message from %s at %s: %s", source, currentTime, rawNmea)
				}
				continue
			}

			aggregatorDedupeWindow = append(aggregatorDedupeWindow, dedupeState{message: rawNmea, timestamp: time.Now()})

			decoded, err := nmeaCodec.ParseSentence(rawNmea)
			if err != nil {
				log.Printf("Error decoding sentence: %v", err)
				continue
			}
			if decoded == nil || decoded.Packet == nil {
				continue
			}

			var newData map[string]interface{}
			{
				b, err := json.Marshal(decoded.Packet)
				if err != nil {
					log.Printf("Error marshaling AIS packet: %v", err)
					continue
				}
				if err := json.Unmarshal(b, &newData); err != nil {
					log.Printf("Error unmarshaling AIS packet to map: %v", err)
					continue
				}
			}

			out, err := json.MarshalIndent(decoded.Packet, "", "  ")
			if err != nil {
				log.Printf("Error formatting AIS packet: %v", err)
				continue
			}
			typeName := fmt.Sprintf("%T", decoded.Packet)
			typeName = strings.TrimPrefix(typeName, "*")
			message := fmt.Sprintf("%s: %s", typeName, out)

			if *showDecodes {
				log.Println("Decoded AIS Packet:", message)
			}

			clientsMutex.Lock()
			for _, client := range clients {
				go func(c *socket.Socket, msg string) {
					if err := c.Emit("ais_data", msg); err != nil {
						log.Printf("Error sending decoded AIS data to client %s: %v", c.Id(), err)
					}
				}(client, message)
			}
			clientsMutex.Unlock()

			if udpConn != nil {
				if !isDuplicate(rawNmea, aggregatorDedupeWindow, windowDuration) {
					log.Printf("[DEBUG] Sending message to aggregator: %s", rawNmea)
					_, err := udpConn.Write([]byte(rawNmea))
					if err != nil {
						log.Printf("Error sending raw NMEA sentence over UDP: %v", err)
					}
				} else {
					if *debug {
						log.Printf("[DEBUG] Dropped duplicate message to aggregator: %s", rawNmea)
					}
				}
			}

			// Process vessel data update.
			userIDFloat, ok := newData["UserID"].(float64)
			if !ok {
				availableKeys := make([]string, 0, len(newData))
				for key := range newData {
					availableKeys = append(availableKeys, key)
				}
				log.Printf("Vessel packet missing or invalid UserID field. Available keys: %v", availableKeys)
				continue
			}
			vesselID := fmt.Sprintf("%.0f", userIDFloat)
			vesselDataMutex.Lock()
			vesselData[vesselID] = mergeMaps(vesselData[vesselID], newData)
			latestData := filterCompleteVesselData(vesselData)
			vesselDataMutex.Unlock()

			// Instead of immediate emission, set the flag if data has changed.
			if !isDataChanged(latestData, previousVesselData) {
				continue
			}
			changeMutex.Lock()
			changeAvailable = true
			changeMutex.Unlock()
		}
	}()

	// --- Start a ticker routine to emit updates every updateInterval seconds if data has changed ---
	go func() {
		ticker := time.NewTicker(time.Duration(*updateInterval) * time.Second)
		for range ticker.C {
			changeMutex.Lock()
			if changeAvailable {
				changeAvailable = false
				changeMutex.Unlock()

				vesselDataMutex.Lock()
				latestData := filterCompleteVesselData(vesselData)
				vesselDataMutex.Unlock()

				latestDataJSON, err := json.Marshal(latestData)
				if err != nil {
					log.Printf("Error marshaling latest vessel data: %v", err)
					continue
				}

				clientsMutex.Lock()
				for _, client := range clients {
					go func(c *socket.Socket, msg string) {
						if err := c.Emit("latest_vessel_data", msg); err != nil {
							log.Printf("Error sending latest vessel data to client %s: %v", c.Id(), err)
						}
					}(client, string(latestDataJSON))
				}
				clientsMutex.Unlock()

				previousVesselData = deepCopyVesselData(latestData)

				if *dumpVesselData {
					var indentJSON []byte
					indentJSON, err = json.MarshalIndent(latestData, "", "  ")
					if err != nil {
						log.Printf("Error marshaling latest vessel data: %v", err)
					} else {
						log.Printf("Latest vessel data:\n%s", string(indentJSON))
					}
				}
			} else {
				changeMutex.Unlock()
			}
		}
	}()

	// --- Read from serial port line-by-line (if -serial-port is specified) ---
	if *serialPort != "" {
		scanner := bufio.NewScanner(port)
		for scanner.Scan() {
			line := scanner.Text()
			currentTime := time.Now().Format(time.RFC3339)
			source := "Serial"

			if *debug {
				log.Printf("[DEBUG] Received from Serial (%s) at %s: %s", source, currentTime, line)
			}

			if len(line) == 0 || (line[0] != '!' && line[0] != '$') {
				continue
			}

			if *dedupeWindowDuration > 0 && isDuplicate(line, websocketDedupeWindow, windowDuration) {
				if *debug {
					log.Printf("[DEBUG] Dropped duplicate serial message (%s) at %s: %s", source, currentTime, line)
				}
				continue
			}

			websocketDedupeWindow = append(websocketDedupeWindow, dedupeState{message: line, timestamp: time.Now()})
			decoded, err := nmeaCodec.ParseSentence(line)
			if err != nil {
				log.Printf("Error decoding sentence: %v", err)
				continue
			}
			if decoded == nil || decoded.Packet == nil {
				continue
			}

			var newData map[string]interface{}
			{
				b, err := json.Marshal(decoded.Packet)
				if err != nil {
					log.Printf("Error marshaling AIS packet: %v", err)
					continue
				}
				if err := json.Unmarshal(b, &newData); err != nil {
					log.Printf("Error unmarshaling AIS packet to map: %v", err)
					continue
				}
			}

			out, err := json.MarshalIndent(decoded.Packet, "", "  ")
			if err != nil {
				log.Printf("Error formatting AIS packet: %v", err)
				continue
			}
			typeName := fmt.Sprintf("%T", decoded.Packet)
			typeName = strings.TrimPrefix(typeName, "*")
			message := fmt.Sprintf("%s: %s", typeName, out)

			if *showDecodes {
				log.Println("Decoded AIS Packet:", message)
			}

			clientsMutex.Lock()
			for _, client := range clients {
				go func(c *socket.Socket, msg string) {
					if err := c.Emit("ais_data", msg); err != nil {
						log.Printf("Error sending decoded AIS data to client %s: %v", c.Id(), err)
					}
				}(client, message)
			}
			clientsMutex.Unlock()

			if udpConn != nil {
				if !isDuplicate(line, aggregatorDedupeWindow, windowDuration) {
					log.Printf("[DEBUG] Sending message to aggregator: %s", line)
					_, err := udpConn.Write([]byte(line))
					if err != nil {
						log.Printf("Error sending raw NMEA sentence over UDP: %v", err)
					}
				} else {
					if *debug {
						log.Printf("[DEBUG] Dropped duplicate message to aggregator: %s", line)
					}
				}
			}

			userIDFloat, ok := newData["UserID"].(float64)
			if !ok {
				availableKeys := make([]string, 0, len(newData))
				for key := range newData {
					availableKeys = append(availableKeys, key)
				}
				log.Printf("Vessel packet missing or invalid UserID field. Available keys: %v", availableKeys)
				continue
			}

			vesselID := fmt.Sprintf("%.0f", userIDFloat)
			vesselDataMutex.Lock()
			vesselData[vesselID] = mergeMaps(vesselData[vesselID], newData)
			latestData := filterCompleteVesselData(vesselData)
			vesselDataMutex.Unlock()

			// Instead of immediate emission, set the flag if data has changed.
			if !isDataChanged(latestData, previousVesselData) {
				continue
			}
			changeMutex.Lock()
			changeAvailable = true
			changeMutex.Unlock()
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Error reading from serial port: %v", err)
		}
	}

	// Wait forever.
	select {}
}
