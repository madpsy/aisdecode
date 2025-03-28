package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
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

// AISMessage represents the structured JSON message sent to the ais_data room.
type AISMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp string      `json:"timestamp"`
}

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

// Global flag and mutex for change detection.
var (
	changeAvailable bool
	changeMutex     sync.Mutex
)

// Deduplication state: stores messages and their timestamps.
type dedupeState struct {
	message   string
	timestamp time.Time
}

var (
	websocketDedupeWindow  []dedupeState
	websocketDedupeMutex   sync.Mutex

	aggregatorDedupeWindow []dedupeState
	aggregatorDedupeMutex  sync.Mutex
)

// isDuplicateWithLock checks for duplicates while holding the given mutex.
func isDuplicateWithLock(message string, window *[]dedupeState, mutex *sync.Mutex, duration time.Duration) bool {
	mutex.Lock()
	defer mutex.Unlock()
	return isDuplicate(message, *window, duration)
}

// appendToWindowWithLock appends a deduplication entry while holding the given mutex.
func appendToWindowWithLock(message string, window *[]dedupeState, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()
	*window = append(*window, dedupeState{message: message, timestamp: time.Now()})
}

// mergeMaps merges newData into baseData. Values in newData override those in baseData.
func mergeMaps(baseData, newData map[string]interface{}) map[string]interface{} {
    if baseData == nil {
        baseData = make(map[string]interface{})
    }
    // Merge all top-level keys from newData.
    for key, value := range newData {
        baseData[key] = value
    }

    // Elevate selected fields from ReportA while keeping the nested structure.
    if reportA, ok := newData["ReportA"].(map[string]interface{}); ok {
        // Elevate Name from ReportA.
        if name, ok := reportA["Name"].(string); ok && strings.TrimSpace(name) != "" {
            baseData["Name"] = name
        }
    }

    // Elevate selected fields from ReportB while keeping the nested structure.
    if reportB, ok := newData["ReportB"].(map[string]interface{}); ok {
        // Elevate CallSign from ReportB.
        if cs, ok := reportB["CallSign"].(string); ok && strings.TrimSpace(cs) != "" {
            baseData["CallSign"] = cs
        }
        // Elevate Dimension.
        if dim, ok := reportB["Dimension"]; ok {
            baseData["Dimension"] = dim
        }
        // Elevate FixType.
        if fixType, ok := reportB["FixType"]; ok {
            baseData["FixType"] = fixType
        }
        // Elevate ShipType as Type.
        if shipType, ok := reportB["ShipType"]; ok {
            baseData["Type"] = shipType
        }
        // Mark AISType as "B" because ReportB is present.
        baseData["AISType"] = "B"
    }
    
    // Default AISType to "A" if it has not been set yet.
    if _, ok := baseData["AISType"]; !ok {
        baseData["AISType"] = "A"
    }
    
    return baseData
}

// filterCompleteVesselData filters vessels that have all required fields.
func filterCompleteVesselData(vesselData map[string]map[string]interface{}) map[string]map[string]interface{} {
    filteredData := make(map[string]map[string]interface{})
    for id, vesselInfo := range vesselData {
        // Check if Latitude and Longitude exist and are valid numbers.
        lat, latOk := vesselInfo["Latitude"].(float64)
        lon, lonOk := vesselInfo["Longitude"].(float64)
        if !latOk || !lonOk {
            continue
        }
        // As an extra precaution, ensure the numbers fall within valid ranges.
        if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
            continue
        }
        // Set default for CallSign if missing.
        if vesselInfo["CallSign"] == nil {
            vesselInfo["CallSign"] = "NOCALL"
        }
        // Set default for Name if missing or empty.
        if name, ok := vesselInfo["Name"].(string); !ok || strings.TrimSpace(name) == "" {
            vesselInfo["Name"] = "NONAME"
        }
        filteredData[id] = vesselInfo
    }
    return filteredData
}

// isInterfaceMapEqual compares two maps recursively.
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

// compareValues helps compare two interface{} values.
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

// isDataChanged compares currentData and previousData.
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

// isDuplicate checks if a message is a duplicate within the deduplication window.
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

// filterWindow filters deduplication states to those newer than cutoff.
func filterWindow(window []dedupeState, cutoff time.Time) []dedupeState {
	filtered := []dedupeState{}
	for _, state := range window {
		if state.timestamp.After(cutoff) {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func cleanInvalidData(data map[string]interface{}) {
    // Clean TrueHeading: remove if set to 511.
    if th, ok := data["TrueHeading"].(float64); ok && th == 511 {
        delete(data, "TrueHeading")
    }
    // Clean Cog: remove if set to 360.
    if cog, ok := data["Cog"].(float64); ok && cog == 360 {
        delete(data, "Cog")
    }
    // Validate Latitude.
    if latVal, exists := data["Latitude"]; exists {
        if lat, ok := latVal.(float64); ok {
            // Remove if latitude is outside the valid range.
            if lat < -90 || lat > 90 {
                delete(data, "Latitude")
            }
        } else {
            // If not a valid float (for example an empty string), remove it.
            delete(data, "Latitude")
        }
    }
    // Validate Longitude.
    if lonVal, exists := data["Longitude"]; exists {
        if lon, ok := lonVal.(float64); ok {
            // Remove if longitude is outside the valid range.
            if lon < -180 || lon > 180 {
                delete(data, "Longitude")
            }
        } else {
            // If not a valid float (for example an empty string), remove it.
            delete(data, "Longitude")
        }
    }
}



func filterVesselSummary(vessels map[string]map[string]interface{}) map[string]map[string]interface{} {
	summary := make(map[string]map[string]interface{})
	// List of keys to include in the summary.
	for id, v := range vessels {
		summary[id] = map[string]interface{}{
			"UserID":               v["UserID"],
			"Name":                 v["Name"],
			"CallSign":             v["CallSign"],
			"LastUpdated":          v["LastUpdated"],
			"NumMessages":          v["NumMessages"],
			"Destination":          v["Destination"],
			"Sog":                  v["Sog"],
			"Type":                 v["Type"],
			"MaximumStaticDraught": v["MaximumStaticDraught"],
			"NavigationalStatus":   v["NavigationalStatus"],
			"Latitude":   		v["Latitude"],
			"Longitude":  		v["Longitude"],
			"TrueHeading":  	v["TrueHeading"],
		}
	}
	return summary
}

func main() {
	// Command-line flags.
	serialPort := flag.String("serial-port", "", "Serial port device (optional)")
	baud := flag.Int("baud", 38400, "Baud rate (default: 38400), ignored if -serial-port is not specified")
	wsPort := flag.Int("ws-port", 8100, "WebSocket port (default: 8100)")
	webRoot := flag.String("web-root", ".", "Web root directory (default: current directory)")
	debug := flag.Bool("debug", false, "Enable debug output")
	showDecodes := flag.Bool("show-decodes", false, "Output the decoded messages")
	aggregator := flag.String("aggregator", "", "Comma delimited list of aggregator host/ip:port (optional)")
	udpListenPort := flag.Int("udp-listen-port", 8101, "UDP listen port for incoming NMEA data (default: 8101)")
	dedupeWindowDuration := flag.Int("dedupe-window", 1000, "Deduplication window in milliseconds (default: 1000, set to 0 to disable deduplication)")
	dumpVesselData := flag.Bool("dump-vessel-data", false, "Log the latest vessel data to the screen whenever it is updated")
	updateInterval := flag.Int("update-interval", 10, "Update interval in seconds for emitting latest vessel data (default: 10)")
	expireAfter := flag.Duration("expire-after", 60*time.Minute, "Expire vessel data if no update is received within this duration (default: 60m)")
	noState := flag.Bool("no-state", false, "When specified, do not save or load the state (default: false)")
	stateFile := flag.String("state-file", "", "Path to state file (optional). Overrides the default location of web-root/state.json")

	flag.Parse()

	// Determine the state file path within the web root.
	statePath := filepath.Join(*webRoot, "state.json")
	if *stateFile != "" {
	    statePath = *stateFile
	}

	if !*noState {
	    // Try opening (or creating) the state file to ensure it is writable.
	    f, err := os.OpenFile(statePath, os.O_WRONLY|os.O_CREATE, 0644)
	    if err != nil {
	        log.Fatalf("Cannot write to state file %s: %v", statePath, err)
	    }
	    f.Close()
	}

	// Initialize previous vessel data.
	previousVesselData = make(map[string]map[string]interface{})

	// Load state from statePath unless state persistence is disabled.
	if !*noState {
		if _, err := os.Stat(statePath); err == nil {
			data, err := os.ReadFile(statePath)
			if err != nil {
				log.Printf("Error reading state file %s: %v", statePath, err)
			} else {
				var loadedData map[string]map[string]interface{}
				if err := json.Unmarshal(data, &loadedData); err != nil {
					log.Printf("Invalid JSON in state file %s: %v", statePath, err)
				} else {
					vesselDataMutex.Lock()
					vesselData = loadedData
					vesselDataMutex.Unlock()
					log.Printf("Loaded vessel state from %s", statePath)
				}
			}
		} else if !os.IsNotExist(err) {
			log.Printf("Error accessing state file %s: %v", statePath, err)
		}
	}

	// --- Setup Socket.IO server ---
	engineServer := types.CreateServer(nil)
	sioServer := socket.NewServer(engineServer, nil)
	sioServer.On("connection", func(args ...any) {
		client := args[0].(*socket.Socket)
		log.Printf("Socket.IO client connected: %s", client.Id())
		clientsMutex.Lock()
		clients = append(clients, client)
		clientsMutex.Unlock()

		// Force clients to join the latest vessel summary room.
		client.Join(socket.Room("latest_vessel_summary"))
		log.Printf("Client %s joined room latest_vessel_summary", client.Id())

		// Listen for subscription events to join other rooms.
		client.On("subscribe", func(args ...any) {
			if len(args) < 1 {
				return
			}
			roomName, ok := args[0].(string)
			if !ok {
				return
			}
			client.Join(socket.Room(roomName))
			log.Printf("Client %s subscribed to room %s", client.Id(), roomName)
		})

		client.On("unsubscribe", func(args ...any) {
			if len(args) < 1 {
				log.Printf("Client %s sent unsubscribe with no room specified", client.Id())
				return
			}
			roomName, ok := args[0].(string)
			if !ok {
				log.Printf("Client %s sent unsubscribe with non-string room value", client.Id())
				return
			}
			client.Leave(socket.Room(roomName))
			log.Printf("Client %s unsubscribed from room %s", client.Id(), roomName)
		})

		vesselDataMutex.Lock()
		completeData := filterCompleteVesselData(vesselData)
		summaryData := filterVesselSummary(completeData)
		vesselDataMutex.Unlock()
		summaryJSON, err := json.Marshal(summaryData)
		if err != nil {
			log.Printf("Error marshaling latest vessel summary: %v", err)
			return
		}
		if err := client.Emit("latest_vessel_summary", string(summaryJSON)); err != nil {
			log.Printf("Error sending latest vessel summary to client %s: %v", client.Id(), err)
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

	// Add HTTP endpoint for vessel state.
	http.HandleFunc("/state/", func(w http.ResponseWriter, r *http.Request) {
		// Extract the vessel userid from the URL path.
		userID := strings.TrimPrefix(r.URL.Path, "/state/")
		vesselDataMutex.Lock()
		defer vesselDataMutex.Unlock()

		// If no specific userID is provided, return all complete vessels.
		if userID == "" {
			latestData := filterCompleteVesselData(vesselData)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(latestData); err != nil {
				http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
			}
			return
		}

		// Lookup the vessel data for the specified userID.
		vessel, exists := vesselData[userID]
		if !exists {
			http.Error(w, "Vessel not found", http.StatusNotFound)
			return
		}

		// Return the JSON state for the specified vessel.
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(vessel); err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		}
	})

	http.HandleFunc("/summary/", func(w http.ResponseWriter, r *http.Request) {
	    // Extract the vessel userid from the URL path.
	    userID := strings.TrimPrefix(r.URL.Path, "/summary/")
	    vesselDataMutex.Lock()
	    defer vesselDataMutex.Unlock()

	    // If no specific userID is provided, return summary data for all complete vessels.
	    if userID == "" {
	        completeData := filterCompleteVesselData(vesselData)
	        summaryData := filterVesselSummary(completeData)
	        w.Header().Set("Content-Type", "application/json")
	        if err := json.NewEncoder(w).Encode(summaryData); err != nil {
	            http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
	        }
	        return
	    }

	    // Lookup the vessel data for the specified userID.
	    vessel, exists := vesselData[userID]
	    if !exists {
	        http.Error(w, "Vessel not found", http.StatusNotFound)
	        return
	    }

	    // Create a summary for the specific vessel.
	    vesselSummary := filterVesselSummary(map[string]map[string]interface{}{userID: vessel})
	    w.Header().Set("Content-Type", "application/json")
	    if err := json.NewEncoder(w).Encode(vesselSummary[userID]); err != nil {
	        http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
	    }
	})


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
	var aggregatorConns []*net.UDPConn
	if *aggregator != "" {
	    // Split the aggregator argument by comma.
	    aggregatorList := strings.Split(*aggregator, ",")
	    for _, addrStr := range aggregatorList {
	        addrStr = strings.TrimSpace(addrStr)
	        parts := strings.Split(addrStr, ":")
	        if len(parts) != 2 {
	            log.Fatalf("Invalid aggregator format for '%s'. Expected host/ip:port", addrStr)
	        }
	        host, portStr := parts[0], parts[1]
	        udpPort, err := strconv.Atoi(portStr)
	        if err != nil {
	            log.Fatalf("Invalid port number in aggregator '%s': %v", addrStr, err)
	        }
	        udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, udpPort))
	        if err != nil {
	            log.Fatalf("Failed to resolve UDP address for '%s': %v", addrStr, err)
	        }
	        conn, err := net.DialUDP("udp", nil, udpAddr)
	        if err != nil {
	            log.Fatalf("Failed to create UDP connection for '%s': %v", addrStr, err)
	        }
	        aggregatorConns = append(aggregatorConns, conn)
	        log.Printf("[DEBUG] Connected to aggregator at %s", udpAddr.String())
	    }
	    // Defer closing all aggregator connections.
	    defer func() {
	        for _, conn := range aggregatorConns {
	            conn.Close()
	        }
	    }()
	}

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
		currentTime := time.Now().UTC().Format(time.RFC3339Nano)
		source := addr.String()
		if *debug {
			log.Printf("[DEBUG] Received from UDP (%s) at %s: %s", source, currentTime, rawNmea)
		}
		// Check deduplication before processing.
		if *dedupeWindowDuration > 0 && isDuplicateWithLock(rawNmea, &aggregatorDedupeWindow, &aggregatorDedupeMutex, windowDuration) {
			if *debug {
				log.Printf("[DEBUG] Dropped duplicate message from %s at %s: %s", source, currentTime, rawNmea)
			}
			continue
		}
		appendToWindowWithLock(rawNmea, &aggregatorDedupeWindow, &aggregatorDedupeMutex)

		decoded, err := nmeaCodec.ParseSentence(rawNmea)
		if err != nil {
		    log.Printf("Error decoding sentence: %v", err)
		    continue
		}
		if decoded == nil || decoded.Packet == nil {
		    continue
		}

		// Convert the decoded packet to a map for cleaning.
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

		// Clean the invalid data before constructing the final message.
		cleanInvalidData(newData)

		// Build the AIS message with cleaned data.
		typeName := "ais.PositionReport" // Or derive this as needed.
		aisMsg := AISMessage{
		    Type:      typeName,
		    Data:      newData,
		    Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
		finalMsg, err := json.Marshal(aisMsg)
		if err != nil {
		    log.Printf("Error marshaling AISMessage: %v", err)
		    continue
		}
		if *showDecodes {
		    log.Println("Decoded AIS Packet:", string(finalMsg))
		}

		// Use the same newData to extract the UserID.
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
		roomName := "ais_data/" + vesselID
		if err := sioServer.To(socket.Room(roomName)).Emit("ais_data", string(finalMsg)); err != nil {
		    log.Printf("Error sending decoded AIS data to room %s: %v", roomName, err)
		}


			// Forward to aggregator if enabled.
			if len(aggregatorConns) > 0 {
			    for _, conn := range aggregatorConns {
			        if _, err := conn.Write([]byte(rawNmea)); err != nil {
 				   if *debug {
				        log.Printf("[DEBUG] Error sending raw NMEA sentence over UDP to aggregator: %v", err)
				    }
				}
			    }
			}

			// Now record the message in the aggregator deduplication window.
			aggregatorDedupeWindow = append(aggregatorDedupeWindow, dedupeState{message: rawNmea, timestamp: time.Now()})

			// Process vessel data update.
			vesselDataMutex.Lock()

			cleanInvalidData(newData)

			merged := mergeMaps(vesselData[vesselID], newData)
			merged["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
			if count, ok := merged["NumMessages"].(float64); ok {
				merged["NumMessages"] = count + 1
			} else {
				merged["NumMessages"] = 1.0
			}
			vesselData[vesselID] = merged
			vesselDataMutex.Unlock()
			vesselDataMutex.Lock()
			latestData := filterCompleteVesselData(vesselData)
			vesselDataMutex.Unlock()
			if !isDataChanged(latestData, previousVesselData) {
				continue
			}
			changeMutex.Lock()
			changeAvailable = true
			changeMutex.Unlock()
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(*updateInterval) * time.Second)
		for range ticker.C {
			// Remove vessels that haven't updated within expireAfter.
			vesselDataMutex.Lock()
			now := time.Now().UTC()
			for id, vessel := range vesselData {
				lastUpdatedStr, ok := vessel["LastUpdated"].(string)
				if !ok {
					delete(vesselData, id)
					continue
				}
				t, err := time.Parse(time.RFC3339Nano, lastUpdatedStr)
				if err != nil || now.Sub(t) > *expireAfter {
					delete(vesselData, id)
				}
			}
			latestData := filterCompleteVesselData(vesselData)
			vesselDataMutex.Unlock()
	
			changeMutex.Lock()
			if changeAvailable {
				changeAvailable = false
				changeMutex.Unlock()
				
				// Create the summary data payload for clients.
				summaryData := filterVesselSummary(latestData)
				summaryJSON, err := json.Marshal(summaryData)
				if err != nil {
					log.Printf("Error marshaling latest vessel summary: %v", err)
					continue
				}
				clientsMutex.Lock()
				for _, client := range clients {
					go func(c *socket.Socket, msg string) {
						if err := c.Emit("latest_vessel_summary", msg); err != nil {
							log.Printf("Error sending latest vessel summary to client %s: %v", c.Id(), err)
						}
					}(client, string(summaryJSON))
				}
				clientsMutex.Unlock()
			
				// Save the complete vessel data to state file.
				latestDataJSON, err := json.Marshal(latestData)
				if err != nil {
					log.Printf("Error marshaling complete vessel data for state file: %v", err)
				} else if !*noState {
					if err := os.WriteFile(statePath, latestDataJSON, 0644); err != nil {
						log.Printf("Error writing state file %s: %v", statePath, err)
					}
				}
			
				previousVesselData = deepCopyVesselData(latestData)
				if *dumpVesselData {
					indentJSON, err := json.MarshalIndent(latestData, "", "  ")
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
		currentTime := time.Now().UTC().Format(time.RFC3339Nano)
		source := "Serial"
		if *debug {
			log.Printf("[DEBUG] Received from Serial (%s) at %s: %s", source, currentTime, line)
		}
		if len(line) == 0 || (line[0] != '!' && line[0] != '$') {
			continue
		}
		// Check deduplication for the serial data.
		if *dedupeWindowDuration > 0 && isDuplicateWithLock(line, &websocketDedupeWindow, &websocketDedupeMutex, windowDuration) {
			if *debug {
				log.Printf("[DEBUG] Dropped duplicate serial message (%s) at %s: %s", source, currentTime, line)
			}
			continue
		}
		appendToWindowWithLock(line, &websocketDedupeWindow, &websocketDedupeMutex)
		// Also record in the aggregator dedupe window.
		appendToWindowWithLock(line, &aggregatorDedupeWindow, &aggregatorDedupeMutex)

       		 if len(aggregatorConns) > 0 {
       		     for _, conn := range aggregatorConns {
      		          if _, err := conn.Write([]byte(line)); err != nil {
		                    if *debug {
		                        log.Printf("[DEBUG] Error sending raw NMEA sentence over UDP to aggregator: %v", err)
		                    }
		                } else {
		                    if *debug {
		                        log.Printf("[DEBUG] Forwarded raw NMEA sentence over UDP to aggregator: %s", line)
		                    }
		                }
		            }
		        }

		decoded, err := nmeaCodec.ParseSentence(line)
		if err != nil {
			log.Printf("Error decoding sentence: %v", err)
			continue
		}
			if decoded == nil || decoded.Packet == nil {
				continue
			}

			// Convert the decoded packet to a map so we can clean it.
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

			// Clean the invalid data.
			cleanInvalidData(newData)
			
			// Build the AIS message with cleaned data.
			typeName := "ais.PositionReport"
			aisMsg := AISMessage{
			    Type:      typeName,
			    Data:      newData,
			    Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			}
			finalMsg, err := json.Marshal(aisMsg)
			if err != nil {
			    log.Printf("Error marshaling AISMessage: %v", err)
			    continue
			}
			if *showDecodes {
			    log.Println("Decoded AIS Packet:", string(finalMsg))
			}
		
			// Use the same 	newData to extract the UserID.
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
			roomName := "ais_data/" + vesselID
			if err := sioServer.To(socket.Room(roomName)).Emit("ais_data", string(finalMsg)); err != nil {
			    log.Printf("Error sending decoded AIS data to room %s: %v", roomName, err)
			}

			// Update vessel state using the same newData.
			vesselDataMutex.Lock()
			merged := mergeMaps(vesselData[vesselID], newData)
			merged["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
			if count, ok := merged["NumMessages"].(float64); ok {
			    merged["NumMessages"] = count + 1
			} else {
			    merged["NumMessages"] = 1.0
			}
			vesselData[vesselID] = merged
			vesselDataMutex.Unlock()
			vesselDataMutex.Lock()
			latestData := filterCompleteVesselData(vesselData)
			vesselDataMutex.Unlock()
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
