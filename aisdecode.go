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

	"go.bug.st/serial"
	"github.com/google/uuid"
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

func cleanDedupeWindow(window *[]dedupeState, mutex *sync.Mutex, duration time.Duration) {
    mutex.Lock()
    defer mutex.Unlock()
    *window = filterWindow(*window, time.Now().Add(-duration))
}

func isValidURL(urlStr string) bool {
    u, err := url.ParseRequestURI(urlStr)
    return err == nil && u.Scheme != "" && u.Host != ""
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
	client := &http.Client{Timeout: 1 * time.Second}
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
              baseData["Name"] = "NO NAME"
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
            vesselInfo["CallSign"] = "NO CALL"
        }
        // Set default for Name if missing or empty.
        if name, ok := vesselInfo["Name"].(string); !ok || strings.TrimSpace(name) == "" {
            vesselInfo["Name"] = "NO NAME"
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
	expireAfter := flag.Duration("expire-after", 24*time.Hour, "Expire vessel data if no update is received within this duration (default: 24h)")
	noState := flag.Bool("no-state", false, "When specified, do not save or load the state (default: false)")
	stateDir := flag.String("state-dir", "", "Directory to store state (optional). Overrides the default location of web-root")
	externalLookupURL := flag.String("external-lookup", "", "URL for external lookup endpoint (if specified, enables lookups for vessels missing Name)")
	aggregatorPublicURL := flag.String("aggregator-public-url", "", "Public aggregator URL to push myinfo.json to on startup (optional)")
	restrictUUIDsFlag := flag.Bool("restrict-uuids", false, "If specified, restricts which receiver UUIDs can be sent to us. Expects a JSON file at <state dir>/allowed-uuids.json with a list of allowed UUIDs")
	logAllDecodesDir := flag.String("log-all-decodes", "", "Directory path to log every decoded message (optional)")

	flag.Parse()

	// Determine the state file path within the web root.
	var statePath string
	if *stateDir != "" {
	    statePath = filepath.Join(*stateDir, "state.json")
	} else {
	    statePath = filepath.Join(*webRoot, "state.json")
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
	    if *noState {
	        http.Error(w, "State persistence disabled", http.StatusForbidden)
	        return
	    }

	    // Determine the directory in which state is stored.
	    var stateDirectory string
	    if *stateDir != "" {
	        stateDirectory = *stateDir
	    } else {
	        stateDirectory = *webRoot
	    }
	    receiversPath := filepath.Join(stateDirectory, "receivers.json")

	    switch r.Method {
	    case "PUT":
	        // Decode the incoming JSON payload.
	        var rec map[string]string
	        if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
	            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
	            return
	        }

        	if *restrictUUIDsFlag {
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

	        // Use the helper function to validate the payload.
	        if !isValidReceiver(rec) {
	            http.Error(w, "Invalid receiver fields", http.StatusBadRequest)
	            return
	        }

	        // Set the LastUpdated field.
	        rec["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)

	        // Read, update (or append), and write back the receivers file.
	        receiversMutex.Lock()
	        defer receiversMutex.Unlock()

	        var receivers []map[string]string
	        data, err := os.ReadFile(receiversPath)
	        if err == nil {
	            if err := json.Unmarshal(data, &receivers); err != nil {
	                receivers = []map[string]string{}
	            }
	        } else if !os.IsNotExist(err) {
	            http.Error(w, "Error reading receivers state", http.StatusInternalServerError)
	            return
	        }

	        // If an entry with the same UUID exists, update it.
	        updated := false
	        for i, rcv := range receivers {
	            if rcv["uuid"] == rec["uuid"] {
	                receivers[i] = rec
	                updated = true
	                break
	            }
	        }
	        if !updated {
	            receivers = append(receivers, rec)
	        }

	        newData, err := json.MarshalIndent(receivers, "", "  ")
	        if err != nil {
	            http.Error(w, "Error marshaling receivers state", http.StatusInternalServerError)
	            return
	        }
	        if err := os.WriteFile(receiversPath, newData, 0644); err != nil {
	            http.Error(w, "Error writing receivers state", http.StatusInternalServerError)
	            return
	        }
	        w.WriteHeader(http.StatusOK)
	        w.Write([]byte("Receiver info saved successfully"))
        
	    case "GET":
	        // Load our own info from myinfo.json.
	        var myinfoPath string
	        if *stateDir != "" {
	            myinfoPath = filepath.Join(*stateDir, "myinfo.json")
	        } else {
	            myinfoPath = filepath.Join(*webRoot, "myinfo.json")
	        }
	        var myinfo map[string]string
	        data, err := os.ReadFile(myinfoPath)
	        if err == nil {
	            if err := json.Unmarshal(data, &myinfo); err != nil {
	                myinfo = make(map[string]string)
	            }
	        } else if os.IsNotExist(err) {
	            myinfo = make(map[string]string)
	        } else {
	            http.Error(w, "Error reading myinfo", http.StatusInternalServerError)
	            return
	        }

	        // Add LastUpdated based on file modification time.
	        if fileInfo, err := os.Stat(myinfoPath); err == nil {
	            myinfo["LastUpdated"] = fileInfo.ModTime().UTC().Format(time.RFC3339Nano)
	        } else {
	            myinfo["LastUpdated"] = time.Now().UTC().Format(time.RFC3339Nano)
	        }
	        // Force local to true for myinfo.json.
	        myinfo["local"] = "true"

	        // Validate myinfo and only include it if it passes the checks.
	        validResponse := []map[string]string{}
	        if isValidReceiver(myinfo) {
	            validResponse = append(validResponse, myinfo)
	        } else {
	            log.Printf("myinfo.json did not pass validation and will be omitted from /receivers response")
	        }

	        // Load receivers info.
	        receiversMutex.Lock()
	        data, err = os.ReadFile(receiversPath)
	        receiversMutex.Unlock()
	        var receivers []map[string]string
	        if err == nil {
	            if err := json.Unmarshal(data, &receivers); err != nil {
	                receivers = []map[string]string{}
	            }
	        } else if os.IsNotExist(err) {
	            receivers = []map[string]string{}
	        } else {
	            http.Error(w, "Error reading receivers", http.StatusInternalServerError)
	            return
	        }

	        // Filter out any receivers that do not pass validation.
	        for _, rec := range receivers {
	            // Override "local" to false for all receivers other than myinfo.json.
	            rec["local"] = "false"
	            if isValidReceiver(rec) {
	                validResponse = append(validResponse, rec)
	            } else {
	                log.Printf("Skipping invalid receiver with uuid: %s", rec["uuid"])
	            }
	        }

	        for _, rec := range validResponse {
	            delete(rec, "uuid")
	        }

	        w.Header().Set("Content-Type", "application/json")
	        if err := json.NewEncoder(w).Encode(validResponse); err != nil {
	            http.Error(w, "Error encoding response", http.StatusInternalServerError)
	        }

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
    	go func() {
        	ticker := time.NewTicker(time.Duration(*updateInterval) * time.Second)
	        for range ticker.C {
	            now := time.Now().UTC()
	            cutoff := now.Add(-*expireAfter)
	            vesselMsgTimestampsMutex.Lock()
	            for vesselID, timestamps := range vesselMsgTimestamps {
	                valid := timestamps[:0]
	                for _, t := range timestamps {
	                    if t.After(cutoff) {
	                        valid = append(valid, t)
	                    }
	                }
	                vesselMsgTimestamps[vesselID] = valid

        	        // Also update the vesselData if it exists.
	                vesselDataMutex.Lock()
	                if vessel, exists := vesselData[vesselID]; exists {
	                    vessel["NumMessages"] = float64(len(valid))
	                }
	                vesselDataMutex.Unlock()
	            }
	            vesselMsgTimestampsMutex.Unlock()
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

	// Wait forever.
	select {}
}
