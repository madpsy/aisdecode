package main

import (
	"bytes"
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
	"math"
	"reflect"
	"net/url"
        "io"

	"go.bug.st/serial"
	"github.com/google/uuid"
	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io/v2/socket"
)

var startTime = time.Now()

type SlidingWindowCounter struct {
    mu     sync.Mutex
    events []time.Time
}

type Metrics struct {
	SerialMessagesPerSec    float64 `json:"serial_messages_per_sec"`
	UDPMessagesPerSec       float64 `json:"udp_messages_per_sec"`
	TotalMessages           int     `json:"total_messages"`
	SerialMessagesPerMin    float64 `json:"serial_messages_per_min"`
	UDPMessagesPerMin       float64 `json:"udp_messages_per_min"`
	TotalDeduplications     int     `json:"total_deduplications"`
	ActiveWebSockets        int     `json:"active_websockets"`
	ActiveWebSocketRooms    map[string]int `json:"active_websocket_rooms"`
	NumVesselsClassA        int     `json:"num_vessels_class_a"`
	NumVesselsClassB        int     `json:"num_vessels_class_b"`
	NumVesselsAtoN          int     `json:"num_vessels_aton"`
	NumVesselsBaseStation   int     `json:"num_vessels_base_station"`
	NumVesselsSAR           int     `json:"num_vessels_sar"`
	TotalKnownVessels       int     `json:"total_known_vessels"`
	UptimeSeconds           int     `json:"uptime_seconds"`
}

type TopVessel struct {
	UserID       string `json:"user_id"`
	NumMessages  int    `json:"num_messages"`
}

var (
    	serialCounter 	SlidingWindowCounter
    	udpCounter    	SlidingWindowCounter
	totalMessages   int
	dedupeMessages  int
	activeClients   int
	activeRooms     = make(map[string]int)
	vesselCounts    = make(map[string]int) // tracks vessels per type (Class A, B, etc.)
	newVessels      int
	topVessels      []TopVessel
)

var (
    roomsMutex  sync.Mutex
    clientRooms = make(map[socket.SocketId][]string)
)

// AISMessage represents the structured JSON message sent to the ais_data room.
type AISMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp string      `json:"timestamp"`
}

type Port struct {
    City    string  `json:"CITY"`
    State   string  `json:"STATE"`
    Country string  `json:"COUNTRY"`
    Latitude  float64 `json:"LATITUDE"`
    Longitude float64 `json:"LONGITUDE"`
}

var ports []Port

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

// Global map to track message timestamps per vessel.
var vesselMsgTimestamps = make(map[string][]time.Time)
var vesselMsgTimestampsMutex sync.Mutex

// Global flag and mutex for change detection.
var (
	changeAvailable bool
	changeMutex     sync.Mutex
)

var receiversMutex sync.Mutex

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

var vesselHistoryMutex sync.Mutex
var vesselLastCoordinates = make(map[string]struct{ lat, lon float64 })

var (
    pendingVesselDataMutex sync.Mutex
    pendingVesselData      = make(map[string]map[string]interface{})
)

func (sw *SlidingWindowCounter) AddEvent() {
    sw.mu.Lock()
    defer sw.mu.Unlock()
    sw.events = append(sw.events, time.Now())
}

func calculateVesselCounts() map[string]int {
    // Initialize counts for each type.
    counts := map[string]int{
        "Class A":      0,
        "Class B":      0,
        "AtoN":         0,
        "Base Station": 0,
        "SAR":          0,
    }
    // Lock vesselData to safely iterate over it.
    vesselDataMutex.Lock()
    defer vesselDataMutex.Unlock()
    for _, vessel := range vesselData {
        if cls, ok := vessel["AISClass"].(string); ok {
            switch cls {
            case "A":
                counts["Class A"]++
            case "B":
                counts["Class B"]++
            case "AtoN":
                counts["AtoN"]++
            case "Base Station":
                counts["Base Station"]++
            case "SAR":
                counts["SAR"]++
            }
        }
    }
    return counts
}

func updateReceiver(payload map[string]string, stateDir string) error {
	receiversPath := filepath.Join(stateDir, "receivers.json")
	// Ensure the payload has a non-empty "uuid"
	uuidStr, ok := payload["uuid"]
	if !ok || strings.TrimSpace(uuidStr) == "" {
		return fmt.Errorf("missing uuid in payload")
	}

	// Load existing receivers from the file.
        receivers, err := loadReceivers(receiversPath)
        if err != nil {
            if os.IsNotExist(err) {
                receivers = make(map[string]map[string]string)
            } else {
                return fmt.Errorf("failed to load receivers: %w", err)
            }
        }

	// Look for an existing receiver with the same UUID.
	var foundKey string
	for key, rec := range receivers {
		if rec["uuid"] == uuidStr {
			foundKey = key
			break
		}
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	if foundKey != "" {
		// Update the existing receiver.
		rec := receivers[foundKey]
		rec["name"] = payload["name"]
		rec["description"] = payload["description"]
		rec["latitude"] = payload["latitude"]
		rec["longitude"] = payload["longitude"]
		rec["url"] = payload["url"]
		rec["LastUpdated"] = nowStr
		receivers[foundKey] = rec
	} else {
		// Create a new receiver entry.
		newKey := getNextKey(receivers)
		receivers[newKey] = map[string]string{
			"uuid":        uuidStr,
			"name":        payload["name"],
			"description": payload["description"],
			"latitude":    payload["latitude"],
			"longitude":   payload["longitude"],
			"url":         payload["url"],
			"LastUpdated": nowStr,
		}
	}

        if err := saveReceivers(receiversPath, receivers); err != nil {
            return fmt.Errorf("failed to save receivers: %w", err)
        }
        return nil
}

// getNextKey returns the next incrementing key based on the current keys in receivers.
// It iterates through the keys (which are expected to be numeric strings),
// finds the maximum, and returns max+1 as a string.
func getNextKey(receivers map[string]map[string]string) string {
	max := 0
	for key := range receivers {
		if id, err := strconv.Atoi(key); err == nil && id > max {
			max = id
		}
	}
	return strconv.Itoa(max + 1)
}

// loadReceivers reads the receivers from the specified JSON file and unmarshals it into a map.
// The keys of the map are string representations of numeric IDs.
func loadReceivers(path string) (map[string]map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var receivers map[string]map[string]string
	if err := json.Unmarshal(data, &receivers); err != nil {
		return nil, err
	}
	return receivers, nil
}

// saveReceivers writes the receivers map to the specified JSON file in an indented format.
func saveReceivers(path string, receivers map[string]map[string]string) error {
	data, err := json.MarshalIndent(receivers, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (sw *SlidingWindowCounter) Count(duration time.Duration) int {
    cutoff := time.Now().Add(-duration)
    sw.mu.Lock()
    defer sw.mu.Unlock()
    // Make a copy of the events slice
    eventsCopy := append([]time.Time(nil), sw.events...)
    // Purge outdated events from the copy
    i := 0
    for i < len(eventsCopy) && eventsCopy[i].Before(cutoff) {
        i++
    }
    return len(eventsCopy) - i
}

func cleanDedupeWindow(window *[]dedupeState, mutex *sync.Mutex, duration time.Duration) {
    mutex.Lock()
    defer mutex.Unlock()
    *window = filterWindow(*window, time.Now().Add(-duration))
}

func isValidURL(urlStr string) bool {
    u, err := url.ParseRequestURI(urlStr)
    return err == nil && u.Scheme != "" && u.Host != ""
}

func loadPorts(webRoot string) error {
    filePath := filepath.Join(webRoot, "ports.json")
    log.Printf("Looking for ports.json at: %s", filePath)
    data, err := os.ReadFile(filePath)
    if err != nil {
        log.Fatalf("failed to read ports.json: %v", err)
    }

    if err := json.Unmarshal(data, &ports); err != nil {
        log.Fatalf("failed to parse ports.json: %v", err)
    }
    return nil
}

// Get all ports within a given radius of lat/lon
func getPortsWithinRadius(lat, lon, radius float64) []Port {
    var result []Port
    for _, port := range ports {
        dist := haversine(lat, lon, port.Latitude, port.Longitude)
        if dist <= radius {
            result = append(result, port)
        }
    }
    return result
}

// Get the closest port to a given lat/lon
func getClosestPort(lat, lon float64) (Port, float64) {
    var closestPort Port
    minDistance := math.MaxFloat64
    for _, port := range ports {
        dist := haversine(lat, lon, port.Latitude, port.Longitude)
        if dist < minDistance {
            minDistance = dist
            closestPort = port
        }
    }
    return closestPort, minDistance
}

// Get all ports that match a given country
func getPortsByCountry(country string) []Port {
    var result []Port
    for _, port := range ports {
        if strings.EqualFold(port.Country, country) {
            result = append(result, port)
        }
    }
    return result
}

// Helper function to validate a receiver map.
func isValidReceiver(rec map[string]string) bool {
	// Check "uuid"
	uuidStr, ok := rec["uuid"]
	if !ok || strings.TrimSpace(uuidStr) == "" {
		return false
	}
	if _, err := uuid.Parse(uuidStr); err != nil {
		return false
	}
	// Check "name"
	name, ok := rec["name"]
	if !ok || strings.TrimSpace(name) == "" {
		return false
	}
	// Check "description"
	desc, ok := rec["description"]
	if !ok || strings.TrimSpace(desc) == "" {
		return false
	}
	// Check "latitude"
	latStr, ok := rec["latitude"]
	if !ok || strings.TrimSpace(latStr) == "" {
		return false
	}
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil || lat < -90 || lat > 90 {
		return false
	}
	// Check "longitude"
	lonStr, ok := rec["longitude"]
	if !ok || strings.TrimSpace(lonStr) == "" {
		return false
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil || lon < -180 || lon > 180 {
		return false
	}
	urlStr, ok := rec["url"]
	if !ok {
		return false
	}
	urlStr = strings.TrimSpace(urlStr)
	if urlStr != "" && !isValidURL(urlStr) {
		return false
	}
	return true
}

// cleanupHistoryFiles scans the "history" directory under baseDir and removes
// any records older than expireAfter. It writes the valid records to a temporary file
// and then replaces the original file. If no valid records remain, the file is deleted.
func cleanupHistoryFiles(baseDir string, expireAfter time.Duration) {
	historyDir := filepath.Join(baseDir, "history")
	files, err := os.ReadDir(historyDir)
	if err != nil {
		log.Printf("Error reading history directory %s: %v", historyDir, err)
		return
	}

	cutoffTime := time.Now().UTC().Add(-expireAfter)

	for _, file := range files {
		// Process only CSV files (skip directories and non-CSV files)
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}
		filePath := filepath.Join(historyDir, file.Name())

		// Open the original file for reading.
		origFile, err := os.Open(filePath)
		if err != nil {
			log.Printf("Error opening file %s: %v", filePath, err)
			continue
		}
		scanner := bufio.NewScanner(origFile)
		var validLines []string

		// Read the file line by line and keep only records newer than cutoffTime.
		for scanner.Scan() {
			line := scanner.Text()
			if strings.TrimSpace(line) == "" {
				continue
			}
			// Assume CSV format: timestamp,latitude,longitude,... etc.
			fields := strings.Split(line, ",")
			if len(fields) < 1 {
				continue
			}
			ts, err := time.Parse(time.RFC3339Nano, fields[0])
			if err != nil {
				// If the timestamp doesn't parse, skip this record.
				continue
			}
			if ts.After(cutoffTime) || ts.Equal(cutoffTime) {
				validLines = append(validLines, line)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading file %s: %v", filePath, err)
		}
		origFile.Close()

		// Write valid lines to a temporary file.
		tempFilePath := filePath + ".tmp"
		tempFile, err := os.Create(tempFilePath)
		if err != nil {
			log.Printf("Error creating temp file for %s: %v", filePath, err)
			continue
		}
		for _, line := range validLines {
			if _, err := tempFile.WriteString(line + "\n"); err != nil {
				log.Printf("Error writing to temp file %s: %v", tempFilePath, err)
				break
			}
		}
		tempFile.Close()

		// Check if the temporary file is empty.
		info, err := os.Stat(tempFilePath)
		if err != nil {
			log.Printf("Error stating temp file %s: %v", tempFilePath, err)
			continue
		}
		if info.Size() == 0 {
			// If no records remain, remove both the original and temp file.
			if err := os.Remove(filePath); err != nil {
				log.Printf("Error removing file %s: %v", filePath, err)
			}
			os.Remove(tempFilePath)
		} else {
			// Atomically replace the original file with the temporary file.
			if err := os.Rename(tempFilePath, filePath); err != nil {
				log.Printf("Error renaming temp file %s to %s: %v", tempFilePath, filePath, err)
			}
		}
	}
}

func scheduleDailyCleanup(historyBase string, expireAfter time.Duration) {
	now := time.Now()
	// Calculate next midnight: create a time value for midnight of the next day.
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	durationUntilMidnight := nextMidnight.Sub(now)
	
	// Wait until midnight.
	time.AfterFunc(durationUntilMidnight, func() {
		log.Println("Running scheduled daily history cleanup at midnight.")
		cleanupHistoryFiles(historyBase, expireAfter)
		
		// After the first cleanup at midnight, schedule it to run every 24 hours.
		ticker := time.NewTicker(24 * time.Hour)
		for range ticker.C {
			log.Println("Running scheduled daily history cleanup at midnight.")
			cleanupHistoryFiles(historyBase, expireAfter)
		}
	})
}

func fallbackNameForMessageType(msgType string) (string, bool) {
    switch msgType {
    case "AidsToNavigationReport":
        return "AtoN", true
    case "BaseStationReport":
        return "Base Station", true
    case "StandardSearchAndRescueAircraftReport":
        return "SAR Aircraft", true
    default:
        return "", false
    }
}

// addMessageType adds the message type from the decoded packet to the vessel state.
// It ensures the message type is only added once.
func addMessageType(vessel map[string]interface{}, packet interface{}) {
    msgType := getMessageTypeName(packet)
    var mtypes []string
    // Try to retrieve any existing MessageTypes.
    if current, ok := vessel["MessageTypes"]; ok {
        // It might be stored as []string or []interface{}
        switch arr := current.(type) {
        case []string:
            mtypes = arr
        case []interface{}:
            for _, v := range arr {
                if s, ok := v.(string); ok {
                    mtypes = append(mtypes, s)
                }
            }
        }
    }
    // Only add if not already present.
    for _, mt := range mtypes {
        if mt == msgType {
            return
        }
    }
    mtypes = append(mtypes, msgType)
    vessel["MessageTypes"] = mtypes
}

func getMessageTypeName(packet interface{}) string {
    t := reflect.TypeOf(packet)
    if t.Kind() == reflect.Ptr {
        t = t.Elem()
    }
    fullName := t.String() // e.g., "ais.PositionReport"
    parts := strings.Split(fullName, ".")
    if len(parts) > 0 {
        return parts[len(parts)-1] // e.g., "PositionReport"
    }
    return fullName
}

func logDecodedMessage(packet interface{}, logDir string) {
    msgType := getMessageTypeName(packet)
    filePath := filepath.Join(logDir, msgType+".json")

    // Marshal the packet to JSON.
    b, err := json.Marshal(packet)
    if err != nil {
        log.Printf("Error marshaling packet for logging: %v", err)
        return
    }

    // Open the file in append mode (or create it if necessary).
    f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Printf("Error opening log file %s: %v", filePath, err)
        return
    }
    defer f.Close()

    // Append the JSON data with a newline.
    if _, err := f.Write(append(b, '\n')); err != nil {
        log.Printf("Error writing to log file %s: %v", filePath, err)
    }
}

func externalLookupCall(vesselID string, lookupURL string) {
	// Prepare JSON body: {"MMSI": vesselID}
	reqBody, err := json.Marshal(map[string]string{"MMSI": vesselID})
	if err != nil {
		log.Printf("Error marshaling JSON for external lookup for vessel %s: %v", vesselID, err)
		return
	}

	// Create an HTTP client with a 1-second timeout.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(lookupURL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	// Decode the response.
	var respData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		log.Printf("Error decoding external lookup response for vessel %s: %v", vesselID, err)
		return
	}

	// Check that the response contains an MMSI field matching the vesselID.
	mmsiVal, ok := respData["MMSI"]
	if !ok || fmt.Sprintf("%v", mmsiVal) != vesselID {
		log.Printf("External lookup response MMSI mismatch for vessel %s", vesselID)
		return
	}

	// Check for a valid Name field.
	nameVal, ok := respData["Name"]
	if !ok {
		log.Printf("External lookup response missing Name for vessel %s", vesselID)
		return
	}
	nameStr, ok := nameVal.(string)
	if !ok || strings.TrimSpace(nameStr) == "" {
		log.Printf("External lookup response has invalid Name for vessel %s", vesselID)
		return
	}

	// Optionally, get CallSign if it exists.
	var callSignStr string
	if cs, ok := respData["CallSign"]; ok {
		if csStr, ok := cs.(string); ok && strings.TrimSpace(csStr) != "" {
			callSignStr = csStr
		}
	}

        // Optionally extract and validate ImageURL if available.
        var imageURLStr string
        if img, ok := respData["ImageURL"]; ok {
            if imgStr, ok := img.(string); ok && strings.TrimSpace(imgStr) != "" && isValidURL(imgStr) {
                imageURLStr = imgStr
            }
        }

	// Update vessel state with the lookup results.
	vesselDataMutex.Lock()
	defer vesselDataMutex.Unlock()
	if vessel, exists := vesselData[vesselID]; exists {
		vessel["Name"] = nameStr
		if callSignStr != "" {
			vessel["CallSign"] = callSignStr
		}
		if imageURLStr != "" {
	        	vessel["ImageURL"] = imageURLStr
        	}
		// Optionally update LastUpdated.
		vessel["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
		log.Printf("External lookup updated vessel %s: Name=%s, CallSign=%s, ImageURL=%s", vesselID, nameStr, callSignStr, imageURLStr)
	}
}

func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // Earth radius in meters.
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	lat1Rad := lat1 * math.Pi / 180.0
	lat2Rad := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

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
func mergeMaps(baseData, newData map[string]interface{}, msgType string) map[string]interface{} {
    if baseData == nil {
        baseData = make(map[string]interface{})
    }
    // Merge all top-level keys from newData, with special logic for "Name".
    for key, value := range newData {
        if key == "Name" {
            incomingName, ok := value.(string)
            if !ok || strings.TrimSpace(incomingName) == "" {
     	      continue
    	    }
            incomingName = strings.TrimSpace(incomingName)
            
            // Determine the effective current name by stripping any NameExtension.
            effectiveCurrentName := ""
            if existingName, exists := baseData["Name"].(string); exists && strings.TrimSpace(existingName) != "" {
                effectiveCurrentName = strings.TrimSpace(existingName)
                var ext string
                // Check for a NameExtension in baseData.
                if e, ok := baseData["NameExtension"].(string); ok && strings.TrimSpace(e) != "" {
                    ext = strings.TrimSpace(e)
                }
                // Also check if newData carries a NameExtension.
                if e, ok := newData["NameExtension"].(string); ok && strings.TrimSpace(e) != "" {
                    ext = strings.TrimSpace(e)
                }
                if ext != "" && strings.HasSuffix(effectiveCurrentName, ext) {
                    effectiveCurrentName = strings.TrimSuffix(effectiveCurrentName, ext)
                    effectiveCurrentName = strings.TrimSpace(effectiveCurrentName)
                }
            }
            // If there's an effective current name that is valid (not "NO NAME")
            // and it matches the incoming name, then skip updating.
            if effectiveCurrentName != "" && strings.ToUpper(effectiveCurrentName) != "NO NAME" && effectiveCurrentName == incomingName {
                continue
            }
            baseData["Name"] = incomingName
        } else {
            baseData[key] = value
        }
    }

    // Elevate selected fields from ReportA while keeping the nested structure.
    if reportA, ok := newData["ReportA"].(map[string]interface{}); ok {
        if name, ok := reportA["Name"].(string); ok && strings.TrimSpace(name) != "" {
            if existingName, exists := baseData["Name"].(string); !exists ||
                strings.TrimSpace(existingName) == "" ||
                strings.ToUpper(strings.TrimSpace(existingName)) == "NO NAME" ||
                strings.TrimSpace(existingName) != strings.TrimSpace(name) {
                baseData["Name"] = strings.TrimSpace(name)
            }
        }
    }

    // Continue with other merging logic (e.g. ReportB, AISClass, etc.).
    if reportB, ok := newData["ReportB"].(map[string]interface{}); ok {
        if cs, ok := reportB["CallSign"].(string); ok && strings.TrimSpace(cs) != "" {
            baseData["CallSign"] = cs
        }
        if dim, ok := reportB["Dimension"]; ok {
            baseData["Dimension"] = dim
        }
        if fixType, ok := reportB["FixType"]; ok {
            baseData["FixType"] = fixType
        }
        if shipType, ok := reportB["ShipType"]; ok {
            if shipTypeFloat, ok := shipType.(float64); ok {
                if shipTypeFloat != 0 || baseData["Type"] == nil {
                    baseData["Type"] = shipType
                }
            } else if shipTypeStr, ok := shipType.(string); ok {
                if shipTypeStr != "0" || baseData["Type"] == nil {
                    baseData["Type"] = shipType
                }
            } else {
                baseData["Type"] = shipType
            }
        }
    }
    
    // Set AISClass based on message type.
    switch msgType {
    case "ShipStaticData", "PositionReport":
        // Class A messages: mark vessel as Class A.
        baseData["AISClass"] = "A"
    case "AidsToNavigationReport":
        baseData["AISClass"] = "AtoN"
    case "BaseStationReport":
        baseData["AISClass"] = "Base Station"
    case "StandardSearchAndRescueAircraftReport":
        baseData["AISClass"] = "SAR"
    default:
        // Default to Class B if no class has been set.
        if _, ok := baseData["AISClass"]; !ok {
            baseData["AISClass"] = "B"
        }
    }

    // Handle NameExtension: if present, append it if not already there.
    if ext, ok := baseData["NameExtension"].(string); ok && strings.TrimSpace(ext) != "" {
        ext = strings.TrimSpace(ext)
        if name, ok := baseData["Name"].(string); ok && strings.TrimSpace(name) != "" {
            if !strings.HasSuffix(name, ext) {
                baseData["Name"] = name + ext
            }
        } else {
            baseData["Name"] = ext
        }
        delete(baseData, "NameExtension")
    }
    
    // Fallback logic: if current Name is "NO NAME", then use fallback value based on msgType.
    if name, ok := baseData["Name"].(string); ok && strings.ToUpper(strings.TrimSpace(name)) == "NO NAME" {
        if fallback, valid := fallbackNameForMessageType(msgType); valid {
            baseData["Name"] = fallback
        }
    }
    
    return baseData
}

// filterCompleteVesselData filters vessels that have all required fields.
func filterCompleteVesselData(vesselData map[string]map[string]interface{}) map[string]map[string]interface{} {
    filteredData := make(map[string]map[string]interface{})
    for id, vesselInfo := range vesselData {
        // Create a deep copy of the nested vesselInfo map.
        copyInfo := make(map[string]interface{})
        for k, v := range vesselInfo {
            copyInfo[k] = v
        }
        // Validate Latitude and Longitude from the copied map.
        lat, latOk := copyInfo["Latitude"].(float64)
        lon, lonOk := copyInfo["Longitude"].(float64)
        if !latOk || !lonOk || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
            continue
        }
        // Set defaults if missing.
        if copyInfo["CallSign"] == nil {
            copyInfo["CallSign"] = "NO CALL"
        }
        if name, ok := copyInfo["Name"].(string); !ok || strings.TrimSpace(name) == "" {
            copyInfo["Name"] = "NO NAME"
        }
        filteredData[id] = copyInfo
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
// compareValues helps compare two interface{} values.
func compareValues(currentValue, previousValue interface{}) bool {
	switch currentTyped := currentValue.(type) {
	case map[string]interface{}:
		previousTyped, ok := previousValue.(map[string]interface{})
		if !ok {
			return false
		}
		return isInterfaceMapEqual(currentTyped, previousTyped)
	case []interface{}:
		previousTyped, ok := previousValue.([]interface{})
		if !ok {
			return false
		}
		if len(currentTyped) != len(previousTyped) {
			return false
		}
		for i := range currentTyped {
			if !compareValues(currentTyped[i], previousTyped[i]) {
				return false
			}
		}
		return true
	case []string:
		previousTyped, ok := previousValue.([]string)
		if !ok {
			return false
		}
		if len(currentTyped) != len(previousTyped) {
			return false
		}
		for i := range currentTyped {
			if currentTyped[i] != previousTyped[i] {
				return false
			}
		}
		return true
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

// appendHistory appends a new history record for the given vessel.
func appendHistory(baseDir, userID string, lat, lon float64, sog, cog, trueHeading, timestamp string) error {
	historyDir := filepath.Join(baseDir, "history")
	// Create the history directory if it doesn't exist.
	if err := os.MkdirAll(historyDir, 0755); err != nil {
		return err
	}
	filePath := filepath.Join(historyDir, userID+".csv")
	// Open the file in append mode (or create it if it doesn't exist).
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write a CSV line: timestamp,latitude,longitude,SOG,COG,TrueHeading.
	line := fmt.Sprintf("%s,%.6f,%.6f,%s,%s,%s\n", timestamp, lat, lon, sog, cog, trueHeading)
	if _, err := f.WriteString(line); err != nil {
		return err
	}
	return nil
}

func pushReceiverFiles(stateDir, aggregatorPublicURL string) {
	// Only execute if aggregatorPublicURL is provided.
	if aggregatorPublicURL == "" {
		return
	}

	// Determine the path to your myinfo file.
	myinfoPath := filepath.Join(stateDir, "myinfo.json")
	
	// Load myinfo.json to extract the receiver's UUID.
	var myinfo map[string]string
	data, err := os.ReadFile(myinfoPath)
	if err != nil {
		log.Printf("Error reading myinfo file for push: %v", err)
		return
	}

	if err := json.Unmarshal(data, &myinfo); err != nil {
		log.Printf("Error unmarshaling myinfo for push: %v", err)
		return
	}

	// Delete the "password" field, if it exists.
	delete(myinfo, "password")

	receiverUUID, ok := myinfo["uuid"]
	if !ok || strings.TrimSpace(receiverUUID) == "" {
		log.Printf("No valid UUID found in myinfo.json")
		return
	}

	stateFilePath := filepath.Join(stateDir, "state.json")
	metricsFilePath := filepath.Join(stateDir, "metrics.json")

	// Helper function to perform the PUT request for each file.
	pushFile := func(action, filePath string) {
		// Read the file content.
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("File %s not found, skipping push for %s", filePath, action)
			return
		}

		// Construct the target URL.
		targetURL := strings.TrimRight(aggregatorPublicURL, "/") + "/receivers/" + receiverUUID + "/" + action

		req, err := http.NewRequest("PUT", targetURL, bytes.NewReader(fileData))
		if err != nil {
			log.Printf("Error creating PUT request for %s: %v", action, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error sending PUT request for %s: %v", action, err)
			return
		}
		defer resp.Body.Close()

		// Optionally, you can read the response body if needed.
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Printf("Unexpected status code %d when pushing %s: %s", resp.StatusCode, action, string(body))
		} else {
			log.Printf("Successfully pushed %s for receiver %s", action, receiverUUID)
		}
	}

	// Push both state and metrics files.
	pushFile("state", stateFilePath)
	pushFile("metrics", metricsFilePath)
}

func filterVesselSummary(vessels map[string]map[string]interface{}) map[string]map[string]interface{} {
	summary := make(map[string]map[string]interface{})
	// List of keys to include in the summary.
	for id, v := range vessels {
		summary[id] = map[string]interface{}{
			"UserID":               v["UserID"],
			"Name":                 v["Name"],
			"CallSign":             v["CallSign"],
			"ImageURL":		v["ImageURL"],
			"LastUpdated":          v["LastUpdated"],
			"NumMessages":          v["NumMessages"],
			"Destination":          v["Destination"],
			"Sog":                  v["Sog"],
			"Cog":                  v["Cog"],
			"Type":                 v["Type"],
			"Dimension": 		v["Dimension"],
			"MaximumStaticDraught": v["MaximumStaticDraught"],
			"NavigationalStatus":   v["NavigationalStatus"],
			"Latitude":   		v["Latitude"],
			"Longitude":  		v["Longitude"],
			"TrueHeading":  	v["TrueHeading"],
			"AISClass":             v["AISClass"],
			"MID":                  v["MID"],
			"MessageTypes":		v["MessageTypes"],
		}
	}
	return summary
}

func main() {
	startTime := time.Now()
	// Command-line flags.
	serialPort := flag.String("serial-port", "", "Serial port device (optional)")
	baud := flag.Int("baud", 38400, "Baud rate (default: 38400), ignored if -serial-port is not specified")
	wsPort := flag.Int("ws-port", 8100, "WebSocket port (default: 8100)")
	webRoot := flag.String("web-root", "web", "Web root directory (default: web)")
	debug := flag.Bool("debug", false, "Enable debug output")
	showDecodes := flag.Bool("show-decodes", false, "Output the decoded messages")
	aggregator := flag.String("aggregator", "", "Comma delimited list of aggregator host/ip:port (optional)")
	udpListenPort := flag.Int("udp-listen-port", 8101, "UDP listen port for incoming NMEA data (default: 8101)")
	dedupeWindowDuration := flag.Int("dedupe-window", 1000, "Deduplication window in milliseconds (default: 1000, set to 0 to disable deduplication)")
	dumpVesselData := flag.Bool("dump-vessel-data", false, "Log the latest vessel data to the screen whenever it is updated")
	updateInterval := flag.Int("update-interval", 10, "Update interval in seconds for emitting latest vessel data (default: 10)")
	expireAfter := flag.Duration("expire-after", 24*time.Hour, "Expire vessel data if no update is received within this duration (default: 24h)")
	noState := flag.Bool("no-state", false, "When specified, do not save or load the state (default: false)")
	stateDir := flag.String("state-dir", "state", "Directory to store state (default: state)")
	externalLookupURL := flag.String("external-lookup", "", "URL for external lookup endpoint (if specified, enables lookups for vessels missing Name)")
	aggregatorPublicURL := flag.String("aggregator-public-url", "", "Public aggregator URL to push myinfo.json to on startup (optional)")
	allowAllUUIDs := flag.Bool("allow-all-uuids", false, "If specified, allows all receiver UUIDs (by default, UUIDs are restricted via allowed list)")
	logAllDecodesDir := flag.String("log-all-decodes", "", "Directory path to log every decoded message (optional)")
	aggregatorUploadPeriod := flag.Int("aggregator-upload-period", 15, "Aggregator upload period in minutes (default: 15, 0 disables periodic uploads)")

	flag.Parse()
	
	if *stateDir != "" {
  	   if err := os.MkdirAll(*stateDir, 0755); err != nil {
	       log.Fatalf("Failed to create state directory %s: %v", *stateDir, err)
	   }

	   allowedUUIDsFilePath := filepath.Join(*stateDir, "allowed-uuids.json")
	   if _, err := os.Stat(allowedUUIDsFilePath); os.IsNotExist(err) {
           // File does not exist, so create it with an empty list.
               emptyList := []string{}
               data, err := json.MarshalIndent(emptyList, "", "  ")
               if err != nil {
                   log.Fatalf("Error marshaling empty allowed UUID list: %v", err)
               }
               if err := os.WriteFile(allowedUUIDsFilePath, data, 0644); err != nil {
                   log.Fatalf("Error creating allowed UUIDs file: %v", err)
               }
               log.Printf("Created %s with an empty list", allowedUUIDsFilePath)
           }
        }

	var statePath string
        statePath = filepath.Join(*stateDir, "state.json")

         if err := loadPorts(*webRoot); err != nil {
	    log.Fatalf("Failed to load ports: %v", err)
	 }

	if !*noState {
	    // Try opening (or creating) the state file to ensure it is writable.
	    f, err := os.OpenFile(statePath, os.O_WRONLY|os.O_CREATE, 0644)
	    if err != nil {
	        log.Fatalf("Cannot write to state file %s: %v", statePath, err)
	    }
	    f.Close()
	}

	if *logAllDecodesDir != "" {
	    if err := os.MkdirAll(*logAllDecodesDir, 0755); err != nil {
	        log.Fatalf("Failed to create log directory %s: %v", *logAllDecodesDir, err)
	    }
	    log.Printf("Logging all decodes to directory: %s", *logAllDecodesDir)
	}

	var historyBase string
	if *stateDir != "" {
  	    historyBase = *stateDir
	} else {
	    historyBase = *webRoot
	}

	if !*noState {
		cleanupHistoryFiles(historyBase, *expireAfter)
		scheduleDailyCleanup(historyBase, *expireAfter)
	}

	if !*noState {
	    var myInfoPath string
	    if *stateDir != "" {
	        myInfoPath = filepath.Join(*stateDir, "myinfo.json")
	    } else {
	        myInfoPath = filepath.Join(*webRoot, "myinfo.json")
	    }
	    // myInfo holds the fields we want.
	    myInfo := make(map[string]string)
	    // Attempt to read the file if it exists.
	    data, err := os.ReadFile(myInfoPath)
	    if err == nil {
	        if err := json.Unmarshal(data, &myInfo); err != nil {
	            log.Printf("Error unmarshaling %s: %v", myInfoPath, err)
	        }
	    } else if !os.IsNotExist(err) {
	        log.Printf("Error reading %s: %v", myInfoPath, err)
	    }
	
	    // Check the uuid field. Generate a new one if missing or invalid.
	    if uuidStr, ok := myInfo["uuid"]; !ok || strings.TrimSpace(uuidStr) == "" {
	        myInfo["uuid"] = uuid.NewString()
	    } else {
	        if _, err := uuid.Parse(uuidStr); err != nil {
	            myInfo["uuid"] = uuid.NewString()
	        }
	    }
	    // Ensure other fields exist.
	    if _, ok := myInfo["name"]; !ok {
	        myInfo["name"] = ""
	    }
	    if _, ok := myInfo["description"]; !ok {
	        myInfo["description"] = ""
	    }
	    if _, ok := myInfo["latitude"]; !ok {
	        myInfo["latitude"] = ""
	    }
	    if _, ok := myInfo["longitude"]; !ok {
	        myInfo["longitude"] = ""
	    }
	    if _, ok := myInfo["url"]; !ok {
	        myInfo["url"] = ""
	    }
	    if _, ok := myInfo["password"]; !ok {
	        myInfo["password"] = ""
	    }
	    // Write the updated myinfo.json back.
	    b, err := json.MarshalIndent(myInfo, "", "  ")
	    if err != nil {
	        log.Printf("Error marshaling myinfo data: %v", err)
	    } else {
	        err = os.WriteFile(myInfoPath, b, 0644)
	        if err != nil {
	            log.Printf("Error writing myinfo file %s: %v", myInfoPath, err)
	        }
	    }

		if *aggregatorPublicURL != "" {
		    // Build the endpoint URL by ensuring no trailing slash.
		    aggregatorEndpoint := strings.TrimRight(*aggregatorPublicURL, "/") + "/receivers"
		    myinfoData, err := os.ReadFile(myInfoPath)
		    if err != nil {
		        log.Printf("Aggregator push: could not read myinfo.json: %v", err)
		    } else {
		        req, err := http.NewRequest("PUT", aggregatorEndpoint, bytes.NewReader(myinfoData))
		        if err != nil {
		            log.Printf("Aggregator push: failed to create PUT request: %v", err)
		        } else {
		            req.Header.Set("Content-Type", "application/json")
		            client := &http.Client{Timeout: 5 * time.Second}
		            resp, err := client.Do(req)
		            if err != nil {
		                log.Printf("Aggregator push: PUT request failed: %v", err)
		            } else {
		                if resp.StatusCode == http.StatusOK {
		                    log.Printf("Aggregator push: successfully pushed myinfo.json to %s", aggregatorEndpoint)
		                } else {
		                    log.Printf("Aggregator push: received status code %d when pushing myinfo.json to %s", resp.StatusCode, aggregatorEndpoint)
		                }
		                resp.Body.Close()
		            }
		        }
		    }
		}
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

	var metricsStateDir string
	   if *stateDir != "" {
	       metricsStateDir = *stateDir
	   }

	StartMetrics(metricsStateDir, *noState)

	go pushReceiverFiles(*stateDir, *aggregatorPublicURL)
	go func() {
		ticker := time.NewTicker(time.Duration(*aggregatorUploadPeriod) * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			pushReceiverFiles(*stateDir, *aggregatorPublicURL)
		}
	}()
	
	// --- Setup Socket.IO server ---
	engineServer := types.CreateServer(nil)
	sioServer := socket.NewServer(engineServer, nil)
	sioServer.On("connection", func(args ...any) {
		client := args[0].(*socket.Socket)
		log.Printf("Socket.IO client connected: %s", client.Id())
		clientsMutex.Lock()
		clients = append(clients, client)
		clientsMutex.Unlock()

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
			roomsMutex.Lock()
			activeRooms[roomName]++
			clientRooms[client.Id()] = append(clientRooms[client.Id()], roomName)
			roomsMutex.Unlock()
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
			
			roomsMutex.Lock()
		        if count, exists := activeRooms[roomName]; exists && count > 0 {
			        activeRooms[roomName]--
			        if activeRooms[roomName] == 0 {
			            delete(activeRooms, roomName) // Remove room if no users left.
			        }
			}
			if rooms, exists := clientRooms[client.Id()]; exists {
     		        	for i, r := range rooms {
		        	        if r == roomName {
				                clientRooms[client.Id()] = append(rooms[:i], rooms[i+1:]...)
                				break
            				}
        			}
    			}
			roomsMutex.Unlock()
		})

		client.On("subscribeMetrics", func(args ...any) {
			    client.Join("metrics")
			    roomsMutex.Lock()
			    activeRooms["metrics"]++
			    clientRooms[client.Id()] = append(clientRooms[client.Id()], "metrics")
			    roomsMutex.Unlock()
			    log.Printf("Client %s subscribed to metrics room", client.Id())
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

		    // Remove the client from the global clients slice.
		    clientsMutex.Lock()
		    for i, c := range clients {
		        if c == client {
		            clients = append(clients[:i], clients[i+1:]...)
		            break
		        }
		    }
		    clientsMutex.Unlock()

		    // Remove the client from all subscribed rooms and update activeRooms.
		        roomsMutex.Lock()
		        if rooms, exists := clientRooms[client.Id()]; exists {
		            for _, roomName := range rooms {
			            if count, exists := activeRooms[roomName]; exists && count > 0 {
			                activeRooms[roomName]--
				                if activeRooms[roomName] == 0 {
				                    delete(activeRooms, roomName)
				                }
            		   }
        		}
		        // Remove the client's entry.
		        delete(clientRooms, client.Id())
		    }
		    roomsMutex.Unlock()
		})


		client.On("requestState", func(args ...any) {
		    log.Printf("Client %s requested latest vessel state", client.Id())
    
		    vesselDataMutex.Lock()
		    completeData := filterCompleteVesselData(vesselData)
		    vesselDataMutex.Unlock()

		    stateJSON, err := json.Marshal(completeData)
		    if err != nil {
		        log.Printf("Error marshaling latest vessel state: %v", err)
		        return
		    }
		
		    if err := sioServer.To(socket.Room("latest_vessel_state")).Emit("latest_vessel_state", string(stateJSON)); err != nil {
		        log.Printf("Error sending latest vessel state to client %s: %v", client.Id(), err)
		    }
		})

		client.On("requestSummary", func(args ...any) {
		    log.Printf("Client %s requested vessel summary", client.Id())
		
		    vesselDataMutex.Lock()
		    completeData := filterCompleteVesselData(vesselData)
		    summaryData := filterVesselSummary(completeData)
		    vesselDataMutex.Unlock()

		    summaryJSON, err := json.Marshal(summaryData)
		    if err != nil {
		        log.Printf("Error marshaling latest vessel summary: %v", err)
		        return
		    }
		
		    if err := sioServer.To(socket.Room("latest_vessel_summary")).Emit("latest_vessel_summary", string(summaryJSON)); err != nil {
		        log.Printf("Error sending latest vessel state to client %s: %v", client.Id(), err)
		    }
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

	http.HandleFunc("/history/", func(w http.ResponseWriter, r *http.Request) {
	    // URL should be /history/<userid>/<hours>
	    path := strings.TrimPrefix(r.URL.Path, "/history/")
	    parts := strings.Split(path, "/")
	    if len(parts) != 2 {
	        http.Error(w, "Invalid URL. Expected format: /history/<userid>/<hours>", http.StatusBadRequest)
	        return
	    }
	    userID := parts[0]
	    hoursStr := parts[1]
	    hours, err := strconv.Atoi(hoursStr)
	    if err != nil {
	        http.Error(w, "Invalid hours parameter", http.StatusBadRequest)
	        return
	    }
	    cutoffTime := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)

	    // Build the file path to the vessel's history CSV.
	    filePath := filepath.Join(historyBase, "history", userID+".csv")
	    f, err := os.Open(filePath)
	    if err != nil {
	        http.Error(w, "History file not found", http.StatusNotFound)
	        return
	    }
	    defer f.Close()

	    w.Header().Set("Content-Type", "text/csv")
	    scanner := bufio.NewScanner(f)
	    for scanner.Scan() {
	        line := scanner.Text()
	        if strings.TrimSpace(line) == "" {
	            continue
	        }
	        // Split the line by commas.
	        fields := strings.Split(line, ",")
	        if len(fields) < 3 {
	            // Not enough data, skip.
	            continue
	        }

	        // Parse the timestamp.
	        ts, err := time.Parse(time.RFC3339Nano, fields[0])
	        if err != nil {
	            continue
	        }
	        if ts.After(cutoffTime) || ts.Equal(cutoffTime) {
	            // Check how many fields are present.
	            if len(fields) == 3 {
	                // Old data: append empty fields for SOG, COG, TrueHeading.
	                fmt.Fprintf(w, "%s,%s,%s,,,\n", fields[0], fields[1], fields[2])
        	    } else if len(fields) >= 6 {
	                // New data: output first six fields.
	                fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s\n", fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
	            } else {
	                // If you have a mix (or more fields than expected), you could either handle them
	                // or skip them. Here we choose to skip.
	                continue
	            }
	        }
	    }
	    if err := scanner.Err(); err != nil {
	        http.Error(w, "Error reading history file", http.StatusInternalServerError)
	        return
	    }
	})

	http.HandleFunc("/receivers", func(w http.ResponseWriter, r *http.Request) {
	    // Ensure state persistence is enabled.
	    if *noState {
	        http.Error(w, "State persistence disabled", http.StatusForbidden)
	        return
	    }

	    stateDirectory := *stateDir
	    receiversPath := filepath.Join(stateDirectory, "receivers.json")

	    switch r.Method {
	    case "PUT":
	        // Decode incoming JSON payload.
	        var rec map[string]string
	        if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
	            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
	            return
	        }
	        defer r.Body.Close()

		delete(rec, "password")
	
	        // If allowed UUIDs are enforced, check against the allowed list.
	        if !*allowAllUUIDs {
	            allowedFilePath := filepath.Join(stateDirectory, "allowed-uuids.json")
	            allowedData, err := os.ReadFile(allowedFilePath)
	            if err != nil {
	                http.Error(w, "Not allowed: allowed UUIDs file not found", http.StatusForbidden)
	                return
	            }
	            var allowedList []string
	            if err := json.Unmarshal(allowedData, &allowedList); err != nil {
	                http.Error(w, "Not allowed: invalid allowed UUIDs file", http.StatusForbidden)
	                return
	            }
	            uuidFound := false
	            for _, allowed := range allowedList {
	                if rec["uuid"] == allowed {
	                    uuidFound = true
	                    break
	                }
	            }
	            if !uuidFound {
	                http.Error(w, "Not allowed: receiver UUID not in allowed list", http.StatusForbidden)
	                return
	            }
	        }

	        // Validate receiver fields.
	        if !isValidReceiver(rec) {
	            http.Error(w, "Invalid receiver fields", http.StatusBadRequest)
	            return
	        }

	        // Set LastUpdated to current time.
	        rec["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
	
	        // Update receiver state using the helper function.
		if err := updateReceiver(rec, *stateDir); err != nil {
		    http.Error(w, "Error updating receiver: " + err.Error(), http.StatusInternalServerError)
		    return
		}
        
	        w.WriteHeader(http.StatusOK)
	        w.Write([]byte("Receiver info saved successfully"))
	    
		case "GET":
			// Load receivers from the receivers.json file.
			receivers, err := loadReceivers(receiversPath)
			if err != nil && !os.IsNotExist(err) {
				http.Error(w, "Error reading receivers", http.StatusInternalServerError)
				return
			}
			if receivers == nil {
				receivers = make(map[string]map[string]string)
			}

			// Build the output array.
			var out []map[string]interface{}

			// Process the local myinfo.json first.
			myinfoPath := filepath.Join(stateDirectory, "myinfo.json")
			if data, err := os.ReadFile(myinfoPath); err == nil {
				var localReceiver map[string]interface{}
				if err := json.Unmarshal(data, &localReceiver); err == nil {
					// Remove internal fields.
					delete(localReceiver, "uuid")
					delete(localReceiver, "password")
					// Force local attributes.
					localReceiver["local"] = true
					localReceiver["id"] = 0

					// Add LastUpdated field based on the file's modification time.
					localReceiver["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
	
					out = append(out, localReceiver)
				} else {
					log.Printf("Error unmarshaling myinfo.json: %v", err)
				}
			} else {
				log.Printf("No myinfo.json found at %s, skipping local receiver", myinfoPath)
			}
	
			// Process each receiver from receivers.json.
			for id, rec := range receivers {
				recCopy := make(map[string]interface{})
				for k, v := range rec {
					recCopy[k] = v
				}
				// Remove internal field "uuid".
				delete(recCopy, "uuid")
				// Remove the password field.
				delete(recCopy, "password")
				// Attempt to convert the key to a number.
				if numID, err := strconv.Atoi(id); err == nil {
					recCopy["id"] = numID
				} else {
					recCopy["id"] = id
				}
				recCopy["local"] = false
				out = append(out, recCopy)
			}
	
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(out); err != nil {
				http.Error(w, "Error encoding response", http.StatusInternalServerError)
				return
			}
	
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		})

		http.HandleFunc("/receivers/", func(w http.ResponseWriter, r *http.Request) {
		// Trim the prefix "/receivers/".
		    path := strings.TrimPrefix(r.URL.Path, "/receivers/")
		
		    // --- Handle GET requests for endpoints ending with ".json" ---
		    if r.Method == http.MethodGet &&
		        (strings.HasSuffix(path, "state.json") || strings.HasSuffix(path, "metrics.json")) {
		        // Expected URL format: /receivers/<internalID>/state.json or /receivers/<internalID>/metrics.json
		        parts := strings.Split(path, "/")
		        if len(parts) != 2 {
		            http.Error(w, "Invalid URL format. Expected /receivers/<id>/(state.json|metrics.json)", http.StatusBadRequest)
		            return
		        }
		        internalID := parts[0]
		        action := parts[1] // "state.json" or "metrics.json"

		        // Load receivers from the state directory.
		        receiversPath := filepath.Join(*stateDir, "receivers.json")
		        receivers, err := loadReceivers(receiversPath)
		        if err != nil {
		            http.Error(w, "Error reading receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        receiver, exists := receivers[internalID]
		        if !exists {
		            http.Error(w, "Receiver not found", http.StatusNotFound)
		            return
		        }
		        recUUID, ok := receiver["uuid"]
		        if !ok || strings.TrimSpace(recUUID) == "" {
		            http.Error(w, "Receiver missing UUID", http.StatusInternalServerError)
		            return
		        }

		        // Choose the proper filename based on the action.
		        var filename string
		        if action == "state.json" {
		            filename = "state.json"
		        } else if action == "metrics.json" {
		            filename = "metrics.json"
		        } else {
		            http.Error(w, "Invalid endpoint: use state.json or metrics.json", http.StatusBadRequest)
		            return
		        }
		        // Build the full file path.
		        filePath := filepath.Join(*stateDir, "receivers", recUUID, filename)
		        data, err := os.ReadFile(filePath)
		        if err != nil {
		            http.Error(w, "File not found", http.StatusNotFound)
		            return
		        }
		        w.Header().Set("Content-Type", "application/json")
		        w.Write(data)
		        return
		    }

		    // --- Handle PUT requests for updating receiver state/metrics using UUID ---
		    if r.Method == http.MethodPut {
		        // Expect URL format: /receivers/<uuid>/state  OR  /receivers/<uuid>/metrics
		        parts := strings.Split(path, "/")
		        if len(parts) != 2 {
		            http.Error(w, "Invalid URL format. Expected /receivers/<uuid>/(state|metrics)", http.StatusBadRequest)
		            return
		        }
		        receiverUUID := parts[0]
		        action := parts[1] // "state" or "metrics"

		        // Validate the provided UUID.
		        if strings.TrimSpace(receiverUUID) == "" {
		            http.Error(w, "Missing UUID in URL", http.StatusBadRequest)
		            return
		        }
		        if _, err := uuid.Parse(receiverUUID); err != nil {
		            http.Error(w, "Invalid UUID format", http.StatusBadRequest)
		            return
		        }

		        // If your application enforces allowed UUIDs, perform that check here.
		        // (For example, read allowed UUIDs from allowed-uuids.json and ensure receiverUUID is one of them.)
		        if !*allowAllUUIDs {
		            allowedFilePath := filepath.Join(*stateDir, "allowed-uuids.json")
		            allowedData, err := os.ReadFile(allowedFilePath)
		            if err != nil {
		                http.Error(w, "Not allowed: allowed UUIDs file not found", http.StatusForbidden)
		                return
		            }
		            var allowedList []string
		            if err := json.Unmarshal(allowedData, &allowedList); err != nil {
		                http.Error(w, "Not allowed: invalid allowed UUIDs file", http.StatusForbidden)
		                return
		            }
		            uuidFound := false
		            for _, allowed := range allowedList {
		                if receiverUUID == allowed {
		                    uuidFound = true
		                    break
		                }
		            }
		            if !uuidFound {
		                http.Error(w, "Not allowed: receiver UUID not in allowed list", http.StatusForbidden)
		                return
		            }
		        }
		
		        // Ensure that the directory to store the receiver’s files exists.
		        saveDir := filepath.Join(*stateDir, "receivers", receiverUUID)
		        if err := os.MkdirAll(saveDir, 0755); err != nil {
		            http.Error(w, fmt.Sprintf("Error creating directory: %v", err), http.StatusInternalServerError)
		            return
		        }
		
		        // Based on the action, determine the output filename.
		        var filename string
		        switch action {
		        case "state":
		            filename = "state.json"
		        case "metrics":
		            filename = "metrics.json"
		        default:
		            http.Error(w, "Invalid action. Use state or metrics.", http.StatusBadRequest)
		            return
		        }
		        filePath := filepath.Join(saveDir, filename)
		
		        // Read and validate the JSON payload.
		        body, err := io.ReadAll(r.Body)
		        if err != nil {
		            http.Error(w, "Error reading request body", http.StatusInternalServerError)
		            return
		        }
		        defer r.Body.Close()

		        var js json.RawMessage
		        if err := json.Unmarshal(body, &js); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }

		        // Write the JSON payload to the target file.
		        if err := os.WriteFile(filePath, body, 0644); err != nil {
		            http.Error(w, fmt.Sprintf("Error writing to file: %v", err), http.StatusInternalServerError)
		            return
		        }

			receiversPath := filepath.Join(*stateDir, "receivers.json")
			receivers, err := loadReceivers(receiversPath)
			if err != nil && !os.IsNotExist(err) {
				http.Error(w, "Error reading receivers data", http.StatusInternalServerError)
				return
			}
			// If there are no receivers yet, initialize the map.
			if receivers == nil {
				receivers = make(map[string]map[string]string)
			}
			// Find the receiver record by UUID.
			for key, rec := range receivers {
				if rec["uuid"] == receiverUUID {
					rec["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
					receivers[key] = rec
					break
				}
			}

			if err := saveReceivers(receiversPath, receivers); err != nil {
				http.Error(w, "Error updating receivers data: "+err.Error(), http.StatusInternalServerError)
				return
			}

		        // Respond with a success message.
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte(fmt.Sprintf("Receiver %s %s saved successfully", receiverUUID, action)))
		        return
		    }

		    // Method not allowed for any other HTTP methods.
		    http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		})


		// Handle /ports endpoint
		http.HandleFunc("/ports", func(w http.ResponseWriter, r *http.Request) {
		    if r.Method != http.MethodPost {
		        http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		        return
		    }

		    var requestData struct {
		        Action    string  `json:"action"`
		        Latitude  float64 `json:"latitude"`
		        Longitude float64 `json:"longitude"`
		        Radius    float64 `json:"radius"`
		        Country   string  `json:"country"`
		    }

		    if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		        http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		        return
		    }

		    var response []Port
		
		    switch requestData.Action {
		    case "within_radius":
		        response = getPortsWithinRadius(requestData.Latitude, requestData.Longitude, requestData.Radius)
		        if len(response) == 0 {
		            http.Error(w, "No ports found within the specified radius", http.StatusNotFound)  // Return 404 if no ports found
		            return
		        }
		    case "closest_port":
		        closestPort, distance := getClosestPort(requestData.Latitude, requestData.Longitude)
		        if (closestPort == Port{}) {  // Check if the closest port is empty
		            http.Error(w, "No closest port found", http.StatusNotFound)  // Return 404 if no closest port found
		            return
		        }
		        response = append(response, closestPort)
		        log.Printf("Closest port is %s, %s, %s with distance: %.2f meters", closestPort.City, closestPort.Country, closestPort.State, distance)
		    case "by_country":
		        response = getPortsByCountry(requestData.Country)
		        if len(response) == 0 {
		            http.Error(w, "No ports found for the given country", http.StatusNotFound)  // Return 404 if no ports found
		            return
		        }
		    default:
		        http.Error(w, "Invalid action", http.StatusBadRequest)
		        return
		    }

		    w.Header().Set("Content-Type", "application/json")
		    if err := json.NewEncoder(w).Encode(response); err != nil {
		        http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		    }
		})

		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
	        	// Determine which directory to use for state.
 	        	var metricsFilePath string
			if *stateDir != "" {
			        metricsFilePath = filepath.Join(*stateDir, "metrics.json")
		        }

		        // Read the file.
		        data, err := os.ReadFile(metricsFilePath)
		        if err != nil {
			        http.Error(w, "Error reading metrics file: "+err.Error(), http.StatusInternalServerError)
		                return
			}

		        w.Header().Set("Content-Type", "application/json")
		        w.Write(data)
	       })
		http.HandleFunc("/myinfo", func(w http.ResponseWriter, r *http.Request) {
		    switch r.Method {
		    case http.MethodGet:
		        // Require basic auth for GET.
		        username, password, ok := r.BasicAuth()
		        if !ok || username != "admin" {
		            w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		            http.Error(w, "Unauthorized", http.StatusUnauthorized)
		            return
		        }
		        
		        // Read the current myinfo file.
		        myinfoPath := filepath.Join(*stateDir, "myinfo.json")
		        data, err := os.ReadFile(myinfoPath)
		        if err != nil {
		            http.Error(w, "Error reading myinfo file", http.StatusInternalServerError)
		            return
		        }
		        var myinfo map[string]interface{}
		        if err := json.Unmarshal(data, &myinfo); err != nil {
		            http.Error(w, "Error parsing myinfo file", http.StatusInternalServerError)
		            return
		        }
        
		        // If a password is stored, verify that it matches the provided password.
		        if pwVal, exists := myinfo["password"]; exists {
		            if pwStr, ok := pwVal.(string); ok && strings.TrimSpace(pwStr) != "" {
		                if password != pwStr {
		                    w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		                    http.Error(w, "Unauthorized", http.StatusUnauthorized)
		                    return
		                }
		            }
		        }
		        
		        // Remove the password field before returning the response.
		        delete(myinfo, "password")
		        w.Header().Set("Content-Type", "application/json")
		        if err := json.NewEncoder(w).Encode(myinfo); err != nil {
		            http.Error(w, "Error encoding myinfo", http.StatusInternalServerError)
		            return
		        }
		    case http.MethodPut:
		        myinfoPath := filepath.Join(*stateDir, "myinfo.json")
		        
		        // Read the existing myinfo file (if it exists) to see if a password is already set.
		        var existing map[string]interface{}
		        if data, err := os.ReadFile(myinfoPath); err == nil {
		            json.Unmarshal(data, &existing)
		        }
		        // If there is a non-empty stored password, require basic auth for this PUT.
		        if pwVal, exists := existing["password"]; exists {
		            if pwStr, ok := pwVal.(string); ok && strings.TrimSpace(pwStr) != "" {
		                username, password, ok := r.BasicAuth()
		                if !ok || username != "admin" || password != pwStr {
		                    w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		                    http.Error(w, "Unauthorized", http.StatusUnauthorized)
		                    return
		                }
		            }
		        }
		        
		        // Decode the incoming JSON update.
		        var updates map[string]string
		        if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()
		        
		        // Validate non-empty values for all fields except "url".
		        for key, value := range updates {
		            if key != "url" && strings.TrimSpace(value) == "" {
		                http.Error(w, fmt.Sprintf("Field %q cannot be empty", key), http.StatusBadRequest)
		                return
		            }
		        }
		        // Validate latitude and longitude if provided.
		        if latStr, ok := updates["latitude"]; ok {
		            if lat, err := strconv.ParseFloat(latStr, 64); err != nil || lat < -90 || lat > 90 {
		                http.Error(w, "Invalid latitude value", http.StatusBadRequest)
		                return
		            }
		        }
		        if lonStr, ok := updates["longitude"]; ok {
		            if lon, err := strconv.ParseFloat(lonStr, 64); err != nil || lon < -180 || lon > 180 {
		                http.Error(w, "Invalid longitude value", http.StatusBadRequest)
		                return
		            }
		        }
		        // Validate the uuid field if provided.
		        if uuidStr, ok := updates["uuid"]; ok {
		            if _, err := uuid.Parse(uuidStr); err != nil {
		                http.Error(w, "Invalid UUID format", http.StatusBadRequest)
		                return
		            }
		        }
		        // Validate that password field is at least 8 characters long (if provided).
		        if newPassword, ok := updates["password"]; ok {
		            if len(newPassword) < 8 {
		                http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		                return
		            }
		        }
		        
		        // Read the current myinfo data (if any). Use an empty map if the file does not exist.
		        var myinfo map[string]string
		        if data, err := os.ReadFile(myinfoPath); err == nil {
		            if err := json.Unmarshal(data, &myinfo); err != nil {
		                myinfo = make(map[string]string)
		            }
		        } else {
		            myinfo = make(map[string]string)
		        }
        
		        // Merge the updates into the current myinfo.
		        for key, value := range updates {
		            myinfo[key] = value
		        }
		        myinfo["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
		        
		        output, err := json.MarshalIndent(myinfo, "", "  ")
		        if err != nil {
		            http.Error(w, "Error encoding updated data", http.StatusInternalServerError)
		            return
		        }
		        if err := os.WriteFile(myinfoPath, output, 0644); err != nil {
		            http.Error(w, "Error saving updated data", http.StatusInternalServerError)
		            return
		        }
        
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("myinfo updated successfully"))
		    default:
		        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		    }
		})
		http.HandleFunc("/alloweduuids", func(w http.ResponseWriter, r *http.Request) {
		    // --- Basic Auth Logic ---
		    username, password, ok := r.BasicAuth()
		    if !ok || username != "admin" {
		        w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		        http.Error(w, "Unauthorized", http.StatusUnauthorized)
		        return
		    }
		    // Read the local myinfo file to check if a password is set.
		    myinfoPath := filepath.Join(*stateDir, "myinfo.json")
		    if data, err := os.ReadFile(myinfoPath); err == nil {
		        var myinfo map[string]interface{}
		        if err := json.Unmarshal(data, &myinfo); err == nil {
		            if pw, exists := myinfo["password"].(string); exists && strings.TrimSpace(pw) != "" {
		                if password != pw {
		                    w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		                    http.Error(w, "Unauthorized", http.StatusUnauthorized)
		                    return
		                }
		            }
		        }
		    }
		    
		    allowedFilePath := filepath.Join(*stateDir, "allowed-uuids.json")
		
		    switch r.Method {
		    case http.MethodGet:
		        // Read the allowed-uuids.json file. Return an empty list if the file doesn't exist.
		        var allowedList []string
		        if data, err := os.ReadFile(allowedFilePath); err == nil {
		            if err := json.Unmarshal(data, &allowedList); err != nil {
		                http.Error(w, "Error parsing allowed UUIDs file", http.StatusInternalServerError)
		                return
		            }
		        }
		        w.Header().Set("Content-Type", "application/json")
		        if err := json.NewEncoder(w).Encode(allowedList); err != nil {
		            http.Error(w, "Error encoding response", http.StatusInternalServerError)
		            return
		        }
        
		    case http.MethodPut:
		        // Decode the JSON payload to get a new UUID.
		        var payload struct {
		            UUID string `json:"uuid"`
		        }
		        if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()

		        // Validate the UUID.
		        if _, err := uuid.Parse(payload.UUID); err != nil {
		            http.Error(w, "Invalid UUID format", http.StatusBadRequest)
		            return
		        }

		        // Load the current allowed UUID list, if any.
		        var allowedList []string
		        if data, err := os.ReadFile(allowedFilePath); err == nil {
		            if err := json.Unmarshal(data, &allowedList); err != nil {
		                http.Error(w, "Error parsing allowed UUIDs file", http.StatusInternalServerError)
		                return
		            }
		        }
		        // Add the new UUID only if it is not already in the list.
		        exists := false
		        for _, u := range allowedList {
		            if u == payload.UUID {
		                exists = true
		                break
		            }
		        }
		        if !exists {
		            allowedList = append(allowedList, payload.UUID)
		            newData, err := json.MarshalIndent(allowedList, "", "  ")
		            if err != nil {
		                http.Error(w, "Error marshaling allowed UUIDs", http.StatusInternalServerError)
		                return
		            }
		            if err := os.WriteFile(allowedFilePath, newData, 0644); err != nil {
		                http.Error(w, "Error saving allowed UUIDs", http.StatusInternalServerError)
		                return
		            }
		        }
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("UUID added successfully"))
		        
		    case http.MethodDelete:
		        // Decode the JSON payload to get the UUID to be removed.
		        var payload struct {
		            UUID string `json:"uuid"`
		        }
		        if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()

		        // Validate the UUID.
		        if _, err := uuid.Parse(payload.UUID); err != nil {
		            http.Error(w, "Invalid UUID format", http.StatusBadRequest)
		            return
		        }

		        // Load the current allowed UUID list.
		        var allowedList []string
		        if data, err := os.ReadFile(allowedFilePath); err == nil {
		            if err := json.Unmarshal(data, &allowedList); err != nil {
		                http.Error(w, "Error parsing allowed UUIDs file", http.StatusInternalServerError)
		                return
		            }
		        }
		        // Remove the given UUID if it exists.
		        newList := []string{}
		        removed := false
		        for _, u := range allowedList {
		            if u == payload.UUID {
		                removed = true
		                continue
		            }
		            newList = append(newList, u)
		        }
		        if !removed {
		            http.Error(w, "UUID not found", http.StatusNotFound)
		            return
		        }
		        newData, err := json.MarshalIndent(newList, "", "  ")
		        if err != nil {
		            http.Error(w, "Error marshaling allowed UUIDs", http.StatusInternalServerError)
		            return
		        }
		        if err := os.WriteFile(allowedFilePath, newData, 0644); err != nil {
		            http.Error(w, "Error saving allowed UUIDs", http.StatusInternalServerError)
		            return
		        }
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("UUID removed successfully"))
		        
		    default:
		        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		    }
		})

		http.HandleFunc("/managereceivers", func(w http.ResponseWriter, r *http.Request) {
		    // --- Authentication (same as used in /alloweduuids) ---
		    username, password, ok := r.BasicAuth()
		    if !ok || username != "admin" {
		        w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		        http.Error(w, "Unauthorized", http.StatusUnauthorized)
		        return
		    }
		    myinfoPath := filepath.Join(*stateDir, "myinfo.json")
		    if data, err := os.ReadFile(myinfoPath); err == nil {
		        var myinfo map[string]interface{}
		        if err := json.Unmarshal(data, &myinfo); err == nil {
		            if pw, exists := myinfo["password"].(string); exists && strings.TrimSpace(pw) != "" {
		                if password != pw {
		                    w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		                    http.Error(w, "Unauthorized", http.StatusUnauthorized)
		                    return
		                }
		            }
		        }
		    }

		    receiversPath := filepath.Join(*stateDir, "receivers.json")
		    switch r.Method {
		    case http.MethodGet:
		        // Load and return all receivers.
		        receivers, err := loadReceivers(receiversPath)
		        if err != nil && !os.IsNotExist(err) {
		            http.Error(w, "Error reading receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        var out []map[string]interface{}
		        if receivers != nil {
		            for key, rec := range receivers {
		                recCopy := make(map[string]interface{})
		                for k, v := range rec {
		                    recCopy[k] = v
		                }
		                // Optionally, convert the map key (numeric string) to an ID value.
		                if id, err := strconv.Atoi(key); err == nil {
		                    recCopy["id"] = id
		                } else {
		                    recCopy["id"] = key
		                }
		                out = append(out, recCopy)
		            }
		        }
		        w.Header().Set("Content-Type", "application/json")
		        if err := json.NewEncoder(w).Encode(out); err != nil {
		            http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
		            return
		        }
		    case http.MethodPut:
		        // Expect a full receiver record.
		        var rec map[string]string
		        if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()
		        // Remove any password field if present.
		        delete(rec, "password")
		        if !isValidReceiver(rec) {
		            http.Error(w, "Invalid receiver fields", http.StatusBadRequest)
		            return
		        }
		        rec["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
		        if err := updateReceiver(rec, *stateDir); err != nil {
		            http.Error(w, "Error updating receiver: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("Receiver info saved successfully"))
		    case http.MethodPatch:
		        // Partial update: update only the provided fields of an existing receiver.
		        var updates map[string]string
		        if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()
		        uuidStr, ok := updates["uuid"]
		        if !ok || strings.TrimSpace(uuidStr) == "" {
		            http.Error(w, "Missing uuid in payload", http.StatusBadRequest)
		            return
		        }
		        receivers, err := loadReceivers(receiversPath)
		        if err != nil && !os.IsNotExist(err) {
		            http.Error(w, "Error loading receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        // Find the receiver with the matching uuid.
		        var foundKey string
		        for key, rec := range receivers {
		            if rec["uuid"] == uuidStr {
		                foundKey = key
		                break
		            }
		        }
		        if foundKey == "" {
		            http.Error(w, "Receiver not found", http.StatusNotFound)
		            return
		        }
		        // Only update the fields provided (except "uuid").
		        for field, value := range updates {
		            if field == "uuid" {
		                continue
		            }
		            receivers[foundKey][field] = value
		        }
		        // Update the LastUpdated field.
		        receivers[foundKey]["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
		        if err := saveReceivers(receiversPath, receivers); err != nil {
		            http.Error(w, "Error saving receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("Receiver info patched successfully"))
		    case http.MethodDelete:
		        // Delete a receiver: the payload must include the receiver's UUID.
		        var payload struct {
		            UUID string `json:"uuid"`
		        }
		        if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		            return
		        }
		        defer r.Body.Close()
		        if strings.TrimSpace(payload.UUID) == "" {
		            http.Error(w, "Missing uuid in payload", http.StatusBadRequest)
		            return
		        }
		        receivers, err := loadReceivers(receiversPath)
		        if err != nil && !os.IsNotExist(err) {
		            http.Error(w, "Error loading receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        var foundKey string
		        for key, rec := range receivers {
		            if rec["uuid"] == payload.UUID {
		                foundKey = key
		                break
		            }
		        }
		        if foundKey == "" {
		            http.Error(w, "Receiver not found", http.StatusNotFound)
		            return
		        }
		        delete(receivers, foundKey)
		        if err := saveReceivers(receiversPath, receivers); err != nil {
		            http.Error(w, "Error saving receivers: "+err.Error(), http.StatusInternalServerError)
		            return
		        }
		        w.WriteHeader(http.StatusOK)
		        w.Write([]byte("Receiver deleted successfully"))
		    default:
		        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
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
	        log.Printf("Connected to aggregator at %s", udpAddr.String())
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
		udpCounter.AddEvent()
		totalMessages++
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
			dedupeMessages++
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

		if *logAllDecodesDir != "" {
		    logDecodedMessage(decoded.Packet, *logAllDecodesDir)
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
		typeName := getMessageTypeName(decoded.Packet)
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
		var MID int
		if len(vesselID) >= 3 {
		    MID, _ = strconv.Atoi(vesselID[:3])
		} else {
		    MID, _ = strconv.Atoi(vesselID)
		}
		newData["MID"] = MID

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

			msgType := getMessageTypeName(decoded.Packet)
			merged := mergeMaps(vesselData[vesselID], newData, msgType)
			merged["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
			addMessageType(merged, decoded.Packet)
			// Get current time
			now := time.Now().UTC()

			// Lock the timestamp map for the current vessel.
			vesselMsgTimestampsMutex.Lock()
			vesselMsgTimestamps[vesselID] = append(vesselMsgTimestamps[vesselID], now)
			
			// Remove timestamps older than expire-after.
			cutoff := now.Add(-*expireAfter)
			validTimestamps := vesselMsgTimestamps[vesselID][:0]
			for _, t := range vesselMsgTimestamps[vesselID] {
			    if t.After(cutoff) {
			        validTimestamps = append(validTimestamps, t)
			    }
			}
			vesselMsgTimestamps[vesselID] = validTimestamps
			vesselMsgTimestampsMutex.Unlock()

			// Set rolling total for NumMessages.
			merged["NumMessages"] = float64(len(validTimestamps))
			vesselData[vesselID] = merged
			vesselDataMutex.Unlock()

			if *externalLookupURL != "" {
			    vesselDataMutex.Lock()
			    _, exists := vesselData[vesselID]
			    vesselDataMutex.Unlock()
			
			    if !exists {
			        go externalLookupCall(vesselID, *externalLookupURL)
			    } else {
			        name, ok := merged["Name"].(string)
			        if !ok || strings.TrimSpace(name) == "" || name == "NO NAME" {
			            go externalLookupCall(vesselID, *externalLookupURL)
			        }
			    }
			}

			// Append to vessel history only if lat/lon have changed by an acceptable amount.
			if lat, ok := merged["Latitude"].(float64); ok {
   			 if lon, ok := merged["Longitude"].(float64); ok {
			        vesselHistoryMutex.Lock()
			        last, exists := vesselLastCoordinates[vesselID]
			        distance := 0.0
			        if exists {
			            distance = haversine(last.lat, last.lon, lat, lon)
			        }
			
			        // Check for spurious jump: if the distance is greater than 10 km.
			        if exists && distance > 10000.0 {
			            pendingVesselDataMutex.Lock()
			            if pending, found := pendingVesselData[vesselID]; found {
			                // Compare new reading to the already pending one.
			                pLat, ok1 := pending["Latitude"].(float64)
			                pLon, ok2 := pending["Longitude"].(float64)
			                if ok1 && ok2 {
			                    pendingDistance := haversine(pLat, pLon, lat, lon)
			                    if pendingDistance <= 10000.0 {
			                        // The new reading is close enough to the pending update.
			                        // Commit the pending update to the vessel's current state.
			                        vesselDataMutex.Lock()
			                        vesselData[vesselID] = pending
			                        vesselDataMutex.Unlock()
			                        // Update the baseline coordinate.
			                        vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{pLat, pLon}
			
			                        // Append the pending update to history.
			                        ts := pending["LastUpdated"].(string)
			                        var sogStr, cogStr, trueHeadingStr string
			                        if sog, ok := pending["Sog"].(float64); ok {
			                            sogStr = fmt.Sprintf("%.2f", sog)
				                        }
			                        if cog, ok := pending["Cog"].(float64); ok {
			                            cogStr = fmt.Sprintf("%.2f", cog)
			                        }
			                        if th, ok := pending["TrueHeading"].(float64); ok {
			                            trueHeadingStr = fmt.Sprintf("%.2f", th)
			                        }
			                        if !*noState {
			                            if err := appendHistory(historyBase, vesselID, pLat, pLon, sogStr, cogStr, trueHeadingStr, ts); err != nil {
			                                log.Printf("Error appending history for vessel %s: %v", vesselID, err)
			                            }
			                        }
			                        // Remove the pending update.
			                        delete(pendingVesselData, vesselID)
			                    } else {
			                        // The new update is still far from the pending one; update the pending update.
			                        pendingVesselData[vesselID] = merged
			                    }
			                }
			            } else {
			                // No pending update exists yet—store this spurious reading.
			                pendingVesselData[vesselID] = merged
			            }
			            pendingVesselDataMutex.Unlock()
			            vesselHistoryMutex.Unlock()
			            // Do not update the current state with this spurious reading.
			            continue
			        }
			
			        // For very small movements (<10 m), keep the current behavior.
			        if exists && distance < 10.0 {
			            vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{lat, lon}
			            vesselHistoryMutex.Unlock()
			            continue
			        }
			
			        // Otherwise, the update is within acceptable bounds.
			        // Commit the update normally.
			        ts := merged["LastUpdated"].(string)
			        var sogStr, cogStr, trueHeadingStr string
			        if sog, ok := merged["Sog"].(float64); ok {
			            sogStr = fmt.Sprintf("%.2f", sog)
			        }
			        if cog, ok := merged["Cog"].(float64); ok {
			            cogStr = fmt.Sprintf("%.2f", cog)
			        }
			        if th, ok := merged["TrueHeading"].(float64); ok {
			            trueHeadingStr = fmt.Sprintf("%.2f", th)
			        }
			
			        // Append the valid update to history.
			        if !*noState {
			            if err := appendHistory(historyBase, vesselID, lat, lon, sogStr, cogStr, trueHeadingStr, ts); err != nil {
			                log.Printf("Error appending history for vessel %s: %v", vesselID, err)
			            }
			        }
			        // Update the baseline coordinate for future comparisons.
			        vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{lat, lon}
			        vesselHistoryMutex.Unlock()
			    }
			}
						
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



	go func() {
	    ticker := time.NewTicker(1 * time.Minute)
	    defer ticker.Stop()
	    for range ticker.C {
	        cleanDedupeWindow(&aggregatorDedupeWindow, &aggregatorDedupeMutex, windowDuration)
	        cleanDedupeWindow(&websocketDedupeWindow, &websocketDedupeMutex, windowDuration)
	    }
	}()

	go func() {
	    ticker := time.NewTicker(1 * time.Second)
	    defer ticker.Stop()
	  
	    for range ticker.C {
		vesselDataMutex.Lock()
	        totalKnown := len(vesselData)
        	vesselDataMutex.Unlock()
		counts := calculateVesselCounts()
		
		roomsMutex.Lock()
	        roomsCopy := make(map[string]int)
	    	for room, count := range activeRooms {
	           roomsCopy[room] = count
		}
	        roomsMutex.Unlock()
		uptimeSeconds := int(time.Since(startTime).Seconds())

	        metrics := Metrics{
            	    SerialMessagesPerSec:  float64(serialCounter.Count(1 * time.Second)),
	            SerialMessagesPerMin:  float64(serialCounter.Count(1 * time.Minute)),
        	    UDPMessagesPerSec:     float64(udpCounter.Count(1 * time.Second)),
        	    UDPMessagesPerMin:     float64(udpCounter.Count(1 * time.Minute)),
		    TotalMessages:         totalMessages,
	            TotalDeduplications:   dedupeMessages,
	            ActiveWebSockets:      len(clients),
	            ActiveWebSocketRooms:  roomsCopy,
           	    NumVesselsClassA:      counts["Class A"],
	            NumVesselsClassB:      counts["Class B"],
	            NumVesselsAtoN:        counts["AtoN"],
        	    NumVesselsBaseStation: counts["Base Station"],
        	    NumVesselsSAR:         counts["SAR"],
		    TotalKnownVessels:     totalKnown,
		    UptimeSeconds:         uptimeSeconds,
	        }

	        metricsJSON, err := json.Marshal(metrics)
	        if err != nil {
	            log.Printf("Error marshaling metrics: %v", err)
	            continue
	        }

	        // Emit asynchronously so a slow client won't block the ticker loop.
	        go func(msg string) {
	            if err := sioServer.To("metrics").Emit("metrics_update", msg); err != nil {
	                log.Printf("Error emitting metrics: %v", err)
	            }
	        }(string(metricsJSON))
	    }
	}()

	go func() {
	    ticker := time.NewTicker(time.Duration(*updateInterval) * time.Second)
	    for range ticker.C {
	        now := time.Now().UTC()
	        cutoff := now.Add(-*expireAfter)
        
	        // Step 1: Clean timestamps and calculate counts while holding vesselMsgTimestampsMutex.
	        validCounts := make(map[string]int)
	        vesselMsgTimestampsMutex.Lock()
	        for vesselID, timestamps := range vesselMsgTimestamps {
	            var valid []time.Time
	            for _, t := range timestamps {
	                if t.After(cutoff) {
	                    valid = append(valid, t)
	                }
	            }
	            vesselMsgTimestamps[vesselID] = valid
	            validCounts[vesselID] = len(valid)
	        }
	        vesselMsgTimestampsMutex.Unlock()
        
	        // Step 2: Update vesselData with the computed counts.
	        vesselDataMutex.Lock()
	        for vesselID, count := range validCounts {
	            if vessel, exists := vesselData[vesselID]; exists {
	                vessel["NumMessages"] = float64(count)
	            }
	        }
	        vesselDataMutex.Unlock()
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
		serialCounter.AddEvent()
		totalMessages++
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

			if *logAllDecodesDir != "" {
			    logDecodedMessage(decoded.Packet, *logAllDecodesDir)
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
			typeName := getMessageTypeName(decoded.Packet)
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
			var MID int
			if len(vesselID) >= 3 {
			    MID, _ = strconv.Atoi(vesselID[:3])
			} else {
			    MID, _ = strconv.Atoi(vesselID)
			}
			newData["MID"] = MID
	
			roomName := "ais_data/" + vesselID
			if err := sioServer.To(socket.Room(roomName)).Emit("ais_data", string(finalMsg)); err != nil {
			    log.Printf("Error sending decoded AIS data to room %s: %v", roomName, err)
			}

			// Update vessel state using the same newData.
			vesselDataMutex.Lock()
			msgType := getMessageTypeName(decoded.Packet)
			merged := mergeMaps(vesselData[vesselID], newData, msgType)
			merged["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
			addMessageType(merged, decoded.Packet)
			// Get current time
			now := time.Now().UTC()

			// Lock the timestamp map for the current vessel.
			vesselMsgTimestampsMutex.Lock()
			vesselMsgTimestamps[vesselID] = append(vesselMsgTimestamps[vesselID], now)

			// Remove timestamps older than expire-after.
			cutoff := now.Add(-*expireAfter)
			validTimestamps := vesselMsgTimestamps[vesselID][:0]
			for _, t := range vesselMsgTimestamps[vesselID] {
			    if t.After(cutoff) {
			        validTimestamps = append(validTimestamps, t)
			    }
			}
			vesselMsgTimestamps[vesselID] = validTimestamps
			vesselMsgTimestampsMutex.Unlock()

			// Set rolling total for NumMessages.
			merged["NumMessages"] = float64(len(validTimestamps))

			vesselData[vesselID] = merged
			vesselDataMutex.Unlock()

			if *externalLookupURL != "" {
			    vesselDataMutex.Lock()
			    _, exists := vesselData[vesselID]
			    vesselDataMutex.Unlock()
			
			    if !exists {
			        go externalLookupCall(vesselID, *externalLookupURL)
			    } else {
			        name, ok := merged["Name"].(string)
			        if !ok || strings.TrimSpace(name) == "" || name == "NO NAME" {
			            go externalLookupCall(vesselID, *externalLookupURL)
			        }
			    }
			}

			// Append to vessel history only if lat/lon have changed by an acceptable amount.
			if lat, ok := merged["Latitude"].(float64); ok {
    			if lon, ok := merged["Longitude"].(float64); ok {
			        vesselHistoryMutex.Lock()
			        last, exists := vesselLastCoordinates[vesselID]
			        distance := 0.0
			        if exists {
			            distance = haversine(last.lat, last.lon, lat, lon)
			        }
			
			        // Check for spurious jump: if the distance is greater than 10 km.
			        if exists && distance > 10000.0 {
			            pendingVesselDataMutex.Lock()
			            if pending, found := pendingVesselData[vesselID]; found {
			                // Compare new reading to the already pending one.
			                pLat, ok1 := pending["Latitude"].(float64)
			                pLon, ok2 := pending["Longitude"].(float64)
			                if ok1 && ok2 {
			                    pendingDistance := haversine(pLat, pLon, lat, lon)
			                    if pendingDistance <= 10000.0 {
			                        // The new reading is close enough to the pending update.
			                        // Commit the pending update to the vessel's current state.
			                        vesselDataMutex.Lock()
			                        vesselData[vesselID] = pending
			                        vesselDataMutex.Unlock()
			                        // Update the baseline coordinate.
			                        vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{pLat, pLon}
			
			                        // Append the pending update to history.
			                        ts := pending["LastUpdated"].(string)
			                        var sogStr, cogStr, trueHeadingStr string
			                        if sog, ok := pending["Sog"].(float64); ok {
			                            sogStr = fmt.Sprintf("%.2f", sog)
			                        }
			                        if cog, ok := pending["Cog"].(float64); ok {
			                            cogStr = fmt.Sprintf("%.2f", cog)
			                        }
			                        if th, ok := pending["TrueHeading"].(float64); ok {
			                            trueHeadingStr = fmt.Sprintf("%.2f", th)
			                        }
			                        if !*noState {
			                            if err := appendHistory(historyBase, vesselID, pLat, pLon, sogStr, cogStr, trueHeadingStr, ts); err != nil {
			                                log.Printf("Error appending history for vessel %s: %v", vesselID, err)
			                            }
			                        }
			                        // Remove the pending update.
			                        delete(pendingVesselData, vesselID)
			                    } else {
			                        // The new update is still far from the pending one; update the pending update.
			                        pendingVesselData[vesselID] = merged
			                    }
			                }
			            } else {
			                // No pending update exists yet—store this spurious reading.
			                pendingVesselData[vesselID] = merged
			            }
			            pendingVesselDataMutex.Unlock()
			            vesselHistoryMutex.Unlock()
			            // Do not update the current state with this spurious reading.
			            continue			
			        }

			        // For very small movements (<10 m), keep the current behavior.
			        if exists && distance < 10.0 {
			            vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{lat, lon}
			            vesselHistoryMutex.Unlock()
			            continue
			        }
			
			        // Otherwise, the update is within acceptable bounds.
			        // Commit the update normally.
			        ts := merged["LastUpdated"].(string)
			        var sogStr, cogStr, trueHeadingStr string
			        if sog, ok := merged["Sog"].(float64); ok {
			            sogStr = fmt.Sprintf("%.2f", sog)
			        }
			        if cog, ok := merged["Cog"].(float64); ok {
			            cogStr = fmt.Sprintf("%.2f", cog)
			        }
			        if th, ok := merged["TrueHeading"].(float64); ok {
			            trueHeadingStr = fmt.Sprintf("%.2f", th)
			        }
			
			        // Append the valid update to history.
			        if !*noState {
			            if err := appendHistory(historyBase, vesselID, lat, lon, sogStr, cogStr, trueHeadingStr, ts); err != nil {
			                log.Printf("Error appending history for vessel %s: %v", vesselID, err)
			            }
			        }
			        // Update the baseline coordinate for future comparisons.
			        vesselLastCoordinates[vesselID] = struct{ lat, lon float64 }{lat, lon}
			        vesselHistoryMutex.Unlock()
			    }
			}

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
