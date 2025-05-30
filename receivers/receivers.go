package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/pbkdf2"
)

type Settings struct {
	DbHost                   string  `json:"db_host"`
	DbPort                   int     `json:"db_port"`
	DbUser                   string  `json:"db_user"`
	DbPass                   string  `json:"db_pass"`
	DbName                   string  `json:"db_name"`
	ListenPort               int     `json:"listen_port"`
	Debug                    bool    `json:"debug"`
	MetricsBaseURL           string  `json:"metrics_base_url"`
	IngestHost               string  `json:"ingest_host"`
	IngestHTTPPort           int     `json:"ingest_http_port"`
	PublicAddReceiverEnabled bool    `json:"public_add_receiver_enabled"`
	IPAddressTimeoutMinutes  int     `json:"ip_address_timeout_minutes"`
	PortMetricsHours         int     `json:"port_metrics_hours"`
	WebhookURL               string  `json:"webhook_url,omitempty"`
	BaseURL                  string  `json:"base_url"`                   // Base URL for password reset links
	CleanupIntervalMinutes   int     `json:"cleanup_interval_minutes"`   // Interval for cleaning up expired IP blocks
	MaxFailedSignupAttempts  int     `json:"max_failed_signup_attempts"` // Maximum number of failed signup attempts before blocking
	ReceiverOfflineHours     float64 `json:"receiver_offline_hours"`     // Hours before a receiver is considered offline
}

type Receiver struct {
	ID               int                    `json:"id"`
	LastUpdated      time.Time              `json:"lastupdated"`
	Description      string                 `json:"description"`
	Latitude         float64                `json:"latitude"`
	Longitude        float64                `json:"longitude"`
	Name             string                 `json:"name"`
	URL              *string                `json:"url,omitempty"`
	IPAddress        string                 `json:"ip_address,omitempty"` // Computed from port metrics
	Email            string                 `json:"email"`                // Added email field
	Notifications    bool                   `json:"notifications"`        // Flag for notifications
	Password         string                 `json:"-"`                    // Plain text password for temporary use only
	PasswordHash     string                 `json:"-"`                    // Hashed password for storage
	PasswordSalt     string                 `json:"-"`                    // Salt for password hashing
	Messages         int                    `json:"messages"`
	UDPPort          *int                   `json:"udp_port,omitempty"`
	MessageStats     map[string]MessageStat `json:"message_stats"`                // Added for admin endpoints
	RequestIPAddress string                 `json:"request_ip_address,omitempty"` // IP address of who added the receiver
	CustomFields     map[string]interface{} `json:"custom_fields,omitempty"`      // For additional fields like reset tokens
	lastSeenTime     *time.Time             `json:"-"`                            // Not directly exposed in JSON but used when converting to map
}

// PublicReceiver is used for public API responses without sensitive fields
type PublicReceiver struct {
	ID            int       `json:"id"`
	LastUpdated   time.Time `json:"lastupdated"`
	Description   string    `json:"description"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	Name          string    `json:"name"`
	URL           *string   `json:"url,omitempty"`
	Notifications bool      `json:"notifications"` // Flag for notifications
	Messages      int       `json:"messages"`
	// Email is intentionally not exposed in the public API
	Email string `json:"-"`
	// UDPPort is intentionally not exposed in the public API
	UDPPort *int `json:"-"`
	// lastSeenTime is not directly exposed in JSON but used when converting to map
	lastSeenTime *time.Time `json:"-"`
}

type ReceiverInput struct {
	Description   string  `json:"description"`
	Latitude      float64 `json:"latitude"`
	Longitude     float64 `json:"longitude"`
	Name          string  `json:"name"`
	URL           *string `json:"url,omitempty"`
	Email         string  `json:"email"`
	Notifications bool    `json:"notifications"`
	Password      *string `json:"password,omitempty"`
}

type ReceiverPatch struct {
	Description   *string  `json:"description,omitempty"`
	Latitude      *float64 `json:"latitude,omitempty"`
	Longitude     *float64 `json:"longitude,omitempty"`
	Name          *string  `json:"name,omitempty"`
	URL           *string  `json:"url,omitempty"`
	Email         *string  `json:"email,omitempty"`
	Notifications *bool    `json:"notifications,omitempty"`
	Password      *string  `json:"password,omitempty"`
}

// Event types for receiver_events table
const (
	ReceiverOffline = "RECEIVER_OFFLINE"
	ReceiverOnline  = "RECEIVER_ONLINE"
)

var (
	db                *sql.DB
	settings          Settings
	udpDedicatedPorts string
	availablePorts    []int

	// New variables for collector tracking
	collectorsMutex  sync.RWMutex
	collectors       []Collector
	portMetricsMutex sync.RWMutex
	portMetricsMap   map[string]map[int]PortMetric // Map of IP address to map of UDP port to PortMetric

	// New map to track the most recent last seen time for each port
	portLastSeenMutex sync.RWMutex
	portLastSeenMap   map[int]time.Time // Map of UDP port to last seen time

	// Track when the ingester was last successfully contacted
	lastIngestContactMutex sync.RWMutex
	lastIngestContactTime  time.Time
	ingestWasDown          bool
)

// New types for collector tracking
type Collector struct {
	Description string `json:"description"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Shards      []int  `json:"shards"`
}

type CollectorsResponse struct {
	Clients          []Collector `json:"clients"`
	ConfiguredShards int         `json:"configured_shards"`
}

type PortMetric struct {
	IPAddress    string    `json:"ip_address"`
	UDPPort      int       `json:"udp_port"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int       `json:"message_count"`
}

// PortMetricsResponse represents the new JSON format returned by the collector
type PortMetricsResponse struct {
	Metrics  []PortMetric         `json:"metrics"`
	LastSeen map[string]time.Time `json:"lastseen"` // Port number as string -> last seen time
}

// MessageStat is a simplified version of PortMetric without redundant fields
type MessageStat struct {
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int       `json:"message_count"`
}

// IngestSettings represents the settings returned by the ingester
type IngestSettings struct {
	UDPListenPort          int           `json:"udp_listen_port"`
	UDPDedicatedPorts      string        `json:"udp_dedicated_ports"`
	UDPDestinations        []interface{} `json:"udp_destinations"`
	MetricWindowSize       int           `json:"metric_window_size"`
	HTTPPort               int           `json:"http_port"`
	NumWorkers             int           `json:"num_workers"`
	DownsampleWindow       int           `json:"downsample_window"`
	DeduplicationWindow    int           `json:"deduplication_window_ms"`
	WebPath                string        `json:"web_path"`
	Debug                  bool          `json:"debug"`
	IncludeSource          bool          `json:"include_source"`
	StreamPort             int           `json:"stream_port"`
	StreamShards           int           `json:"stream_shards"`
	MQTTServer             string        `json:"mqtt_server"`
	MQTTTLS                bool          `json:"mqtt_tls"`
	MQTTAuth               string        `json:"mqtt_auth"`
	MQTTTopic              string        `json:"mqtt_topic"`
	DownsampleMessageTypes []string      `json:"downsample_message_types"`
	BlockedIPs             []string      `json:"blocked_ips"`
	FailedDecodeLog        string        `json:"failed_decode_log"`
}

func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// take only the first IP in the list
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	// fall back to RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// fetchIngestSettings fetches settings from the ingester
func fetchIngestSettings() (*IngestSettings, error) {
	url := fmt.Sprintf("http://%s:%d/settings", settings.IngestHost, settings.IngestHTTPPort)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch settings from ingester: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK response from ingester: %v", resp.Status)
	}

	var ingestSettings IngestSettings
	if err := json.NewDecoder(resp.Body).Decode(&ingestSettings); err != nil {
		return nil, fmt.Errorf("error decoding ingester settings: %v", err)
	}

	return &ingestSettings, nil
}

func notifyWebhook(rec Receiver) {
	notifyWebhookWithType(rec, "receiver_added")
}

// notifyWebhookWithClientIP is used when we need to include the client IP address
func notifyWebhookWithClientIP(rec Receiver, clientIP string) {
	// Store the request IP address in the receiver object for the webhook
	rec.RequestIPAddress = clientIP

	// Initialize CustomFields if needed
	if rec.CustomFields == nil {
		rec.CustomFields = make(map[string]interface{})
	}

	notifyWebhookWithType(rec, "receiver_added")
}

func notifyWebhookDelete(rec Receiver, clientIP string, isAdminAction bool) {
	// Store the request IP address in the receiver object for the webhook
	rec.RequestIPAddress = clientIP

	// Add information about whether this was an admin action
	if rec.CustomFields == nil {
		rec.CustomFields = make(map[string]interface{})
	}
	rec.CustomFields["is_admin_action"] = isAdminAction

	notifyWebhookWithType(rec, "receiver_deleted")
}

func notifyWebhookUpdate(rec Receiver, clientIP string, changedFields map[string]interface{}, isAdminAction bool) {
	// Store the request IP address in the receiver object for the webhook
	rec.RequestIPAddress = clientIP

	// Add the changed fields to the receiver's custom fields
	if rec.CustomFields == nil {
		rec.CustomFields = make(map[string]interface{})
	}
	rec.CustomFields["changed_fields"] = changedFields
	rec.CustomFields["is_admin_action"] = isAdminAction

	notifyWebhookWithType(rec, "receiver_updated")
}

func notifyWebhookWithType(rec Receiver, alertType string) {
	if settings.WebhookURL == "" {
		return
	}
	// Build the alert envelope
	envelope := map[string]interface{}{
		"alert_type": alertType,
		"receiver": map[string]interface{}{
			"id":            rec.ID,
			"lastupdated":   rec.LastUpdated,
			"description":   rec.Description,
			"latitude":      rec.Latitude,
			"longitude":     rec.Longitude,
			"name":          rec.Name,
			"email":         rec.Email,         // Include the email field for notifications
			"notifications": rec.Notifications, // Include the notifications flag
		},
	}

	// Add the LastSeen field directly to the receiver object
	// This is needed for the alerts service to properly format the time
	if rec.CustomFields != nil && rec.CustomFields["lastseen"] != nil {
		envelope["receiver"].(map[string]interface{})["lastseen"] = rec.CustomFields["lastseen"]
	}
	if rec.URL != nil {
		envelope["receiver"].(map[string]interface{})["url"] = rec.URL
	}
	if rec.UDPPort != nil {
		envelope["receiver"].(map[string]interface{})["udp_port"] = rec.UDPPort
	}
	if rec.RequestIPAddress != "" {
		envelope["receiver"].(map[string]interface{})["request_ip_address"] = rec.RequestIPAddress
	}
	if rec.CustomFields != nil && len(rec.CustomFields) > 0 {
		envelope["receiver"].(map[string]interface{})["custom_fields"] = rec.CustomFields
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("notifyWebhook: failed to marshal envelope: %v", err)
		return
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(settings.WebhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("notifyWebhook: POST webhook error: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("notifyWebhook: non-2xx status from webhook: %s", resp.Status)
	}
}

// parsePortRange parses a port range string like "9000-9999" into a slice of integers
func parsePortRange(portRange string) ([]int, error) {
	parts := strings.Split(portRange, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid port range format: %s", portRange)
	}

	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid start port: %v", err)
	}

	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid end port: %v", err)
	}

	if start > end {
		return nil, fmt.Errorf("start port cannot be greater than end port")
	}

	var ports []int
	for i := start; i <= end; i++ {
		ports = append(ports, i)
	}

	return ports, nil
}

// allocatePort allocates a random unused port from the available ports
func allocatePort(receiverID int) (int, error) {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		return 0, err
	}

	// Get an available port that's not already allocated
	var selectedPort int
	err := db.QueryRow(`
        SELECT udp_port FROM receiver_ports
        WHERE receiver_id IS NULL
        ORDER BY RANDOM()
        LIMIT 1
    `).Scan(&selectedPort)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("no available ports")
	} else if err != nil {
		return 0, fmt.Errorf("failed to query available port: %v", err)
	}

	// Update the receiver_ports table
	_, err = db.Exec(`
        UPDATE receiver_ports
        SET receiver_id = $1, last_updated = NOW()
        WHERE udp_port = $2
    `, receiverID, selectedPort)
	if err != nil {
		return 0, fmt.Errorf("failed to update receiver_ports: %v", err)
	}

	return selectedPort, nil
}

// allocatePortTx allocates a random unused port from the available ports within a transaction
func allocatePortTx(tx *sql.Tx, receiverID int) (int, error) {
	// Get an available port that's not already allocated
	var selectedPort int
	err := tx.QueryRow(`
        SELECT udp_port FROM receiver_ports
        WHERE receiver_id IS NULL
        ORDER BY RANDOM()
        LIMIT 1
    `).Scan(&selectedPort)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("no available ports")
	} else if err != nil {
		return 0, fmt.Errorf("failed to query available port: %v", err)
	}

	// Update the receiver_ports table within the transaction
	_, err = tx.Exec(`
        UPDATE receiver_ports
        SET receiver_id = $1, last_updated = NOW()
        WHERE udp_port = $2
    `, receiverID, selectedPort)
	if err != nil {
		return 0, fmt.Errorf("failed to update receiver_ports: %v", err)
	}

	return selectedPort, nil
}

// fetchCollectors fetches the list of collectors from the ingester
func fetchCollectors() ([]Collector, error) {
	url := fmt.Sprintf("http://%s:%d/clients", settings.IngestHost, settings.IngestHTTPPort)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch collectors from ingester: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-OK response from ingester: %v", resp.Status)
	}

	var collectorsResponse CollectorsResponse
	if err := json.NewDecoder(resp.Body).Decode(&collectorsResponse); err != nil {
		return nil, fmt.Errorf("error decoding collectors response: %v", err)
	}

	return collectorsResponse.Clients, nil
}

// fetchPortMetrics fetches port metrics from a collector
func fetchPortMetrics(collector Collector) ([]PortMetric, map[int]time.Time, error) {
	baseURL := fmt.Sprintf("http://%s:%d/portmetrics", collector.IP, collector.Port)

	// Add the time query parameter using the PortMetricsHours setting
	urlWithParams := fmt.Sprintf("%s?time=%d", baseURL, settings.PortMetricsHours)

	resp, err := http.Get(urlWithParams)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch port metrics from collector %s:%d: %v", collector.IP, collector.Port, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("received non-OK response from collector %s:%d: %v", collector.IP, collector.Port, resp.Status)
	}

	// Parse the new JSON format
	var portMetricsResponse PortMetricsResponse
	if err := json.NewDecoder(resp.Body).Decode(&portMetricsResponse); err != nil {
		return nil, nil, fmt.Errorf("error decoding port metrics from collector %s:%d: %v", collector.IP, collector.Port, err)
	}

	// Convert the string port keys to integers
	portLastSeen := make(map[int]time.Time)
	for portStr, lastSeen := range portMetricsResponse.LastSeen {
		portNum, err := strconv.Atoi(portStr)
		if err != nil {
			log.Printf("Warning: Invalid port number in lastseen map: %s", portStr)
			continue
		}
		portLastSeen[portNum] = lastSeen
	}

	return portMetricsResponse.Metrics, portLastSeen, nil
}

// startCollectorTracking starts the background goroutine to track collectors
func startCollectorTracking() {
	// Initialize the port metrics map
	portMetricsMap = make(map[string]map[int]PortMetric)

	// Initialize the port last seen map
	portLastSeenMap = make(map[int]time.Time)

	// Start the collector tracking goroutine
	go func() {
		// Track when the ingester came back online
		var ingestRecoveryTime time.Time
		// Define a grace period (60 seconds) after ingester recovery before checking status changes
		const ingestRecoveryGracePeriod = 60 * time.Second

		for {
			// Fetch collectors
			newCollectors, err := fetchCollectors()
			if err != nil {
				log.Printf("Error fetching collectors: %v", err)
				// Mark that the ingester was down
				lastIngestContactMutex.Lock()
				ingestWasDown = true
				lastIngestContactMutex.Unlock()

				// Sleep and continue if we can't fetch collectors
				// Skip status checks when ingester is unreachable to avoid false offline alerts
				time.Sleep(5 * time.Second)
				continue
			}

			// Update the last successful contact time
			lastIngestContactMutex.Lock()
			wasDown := ingestWasDown
			ingestWasDown = false
			lastIngestContactTime = time.Now()
			lastIngestContactMutex.Unlock()

			// If the ingester was previously down, log this and record the recovery time
			if wasDown {
				ingestRecoveryTime = time.Now()
				log.Printf("Ingester is back online, entering grace period of %v to avoid false alerts", ingestRecoveryGracePeriod)

				// Clear the port last seen map to avoid false offline/online transitions
				portLastSeenMutex.Lock()
				portLastSeenMap = make(map[int]time.Time)
				portLastSeenMutex.Unlock()

				// Skip the status check for one cycle after the ingester comes back
				time.Sleep(5 * time.Second)
				continue
			}

			// Check if we're still within the grace period after ingester recovery
			if !ingestRecoveryTime.IsZero() && time.Since(ingestRecoveryTime) < ingestRecoveryGracePeriod {
				log.Printf("Still in ingester recovery grace period (%v remaining), skipping status checks",
					ingestRecoveryGracePeriod-time.Since(ingestRecoveryTime))
				time.Sleep(5 * time.Second)
				continue
			}

			// If we were in a grace period but now it's over, reset the recovery time
			if !ingestRecoveryTime.IsZero() && time.Since(ingestRecoveryTime) >= ingestRecoveryGracePeriod {
				log.Printf("Ingester recovery grace period ended, resuming normal status checks")
				ingestRecoveryTime = time.Time{} // Reset to zero time
			}

			// Update collectors list
			collectorsMutex.Lock()
			collectors = newCollectors
			collectorsMutex.Unlock()

			// Store previous port last seen map for comparison
			prevPortLastSeenMap := make(map[int]time.Time)
			portLastSeenMutex.RLock()
			for port, lastSeen := range portLastSeenMap {
				prevPortLastSeenMap[port] = lastSeen
			}
			portLastSeenMutex.RUnlock()

			// Create new maps to store the updated data
			newPortMetricsMap := make(map[string]map[int]PortMetric)
			newPortLastSeenMap := make(map[int]time.Time)

			// Use a WaitGroup to wait for all collector goroutines to complete
			var wg sync.WaitGroup

			// For each collector, fetch port metrics
			for _, collector := range newCollectors {
				wg.Add(1)
				go func(c Collector) {
					defer wg.Done()

					portMetrics, portLastSeen, err := fetchPortMetrics(c)
					if err != nil {
						log.Printf("Error fetching port metrics from collector %s:%d: %v", c.IP, c.Port, err)
						return
					}

					// Update new port metrics map
					portMetricsMutex.Lock()
					for _, metric := range portMetrics {
						// Create map for IP address if it doesn't exist
						if _, ok := newPortMetricsMap[metric.IPAddress]; !ok {
							newPortMetricsMap[metric.IPAddress] = make(map[int]PortMetric)
						}

						// Always use the latest metric data from the collector
						newPortMetricsMap[metric.IPAddress][metric.UDPPort] = metric
					}
					portMetricsMutex.Unlock()

					// Update new port last seen map with the most recent last seen time
					if portLastSeen != nil {
						portLastSeenMutex.Lock()
						for port, lastSeen := range portLastSeen {
							// Only update if this is more recent than what we already have
							if existing, ok := newPortLastSeenMap[port]; !ok || lastSeen.After(existing) {
								newPortLastSeenMap[port] = lastSeen
							}
						}
						portLastSeenMutex.Unlock()
					}
				}(collector)
			}

			// Wait for all collector goroutines to complete
			wg.Wait()

			// Only update the global maps after all collectors have reported
			portMetricsMutex.Lock()
			portMetricsMap = newPortMetricsMap
			portMetricsMutex.Unlock()

			portLastSeenMutex.Lock()
			portLastSeenMap = newPortLastSeenMap
			portLastSeenMutex.Unlock()

			// After all collectors have been processed and maps updated, check for receiver status changes
			// Skip status checks if we're in the grace period
			if ingestRecoveryTime.IsZero() || time.Since(ingestRecoveryTime) >= ingestRecoveryGracePeriod {
				checkReceiverStatusChanges(prevPortLastSeenMap)
			}

			// Sleep for 5 seconds
			time.Sleep(5 * time.Second)
		}
	}()
}

// logReceiverEvent logs an event to the receiver_events table
func logReceiverEvent(receiverID int, eventType string) error {
	if err := ensureConnection(); err != nil {
		return fmt.Errorf("database connection error: %v", err)
	}

	_, err := db.Exec(`
		INSERT INTO receiver_events (receiver_id, event_type, timestamp)
		VALUES ($1, $2, NOW())
	`, receiverID, eventType)

	if err != nil {
		return fmt.Errorf("failed to log receiver event: %v", err)
	}

	return nil
}

// getLastReceiverEvent gets the most recent event for a receiver
func getLastReceiverEvent(receiverID int) (string, time.Time, error) {
	if err := ensureConnection(); err != nil {
		return "", time.Time{}, fmt.Errorf("database connection error: %v", err)
	}

	var eventType string
	var timestamp time.Time

	err := db.QueryRow(`
		SELECT event_type, timestamp
		FROM receiver_events
		WHERE receiver_id = $1
		ORDER BY timestamp DESC
		LIMIT 1
	`, receiverID).Scan(&eventType, &timestamp)

	if err == sql.ErrNoRows {
		return "", time.Time{}, nil // No events found
	} else if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to get last receiver event: %v", err)
	}

	return eventType, timestamp, nil
}

// getReceiverState returns the current state of a receiver (online/offline)
// based on its last event in the receiver_events table
func getReceiverState(receiverID int) (bool, error) {
	// Skip the dummy receiver with ID 0
	if receiverID == 0 {
		return false, nil
	}

	// Get the last event for this receiver
	eventType, _, err := getLastReceiverEvent(receiverID)
	if err != nil {
		return false, fmt.Errorf("failed to get receiver state: %v", err)
	}

	// If no events found, default to offline
	if eventType == "" {
		return false, nil
	}

	// If the last event is RECEIVER_OFFLINE, return false
	if eventType == ReceiverOffline {
		return false, nil
	}

	// If the last event is RECEIVER_ONLINE, check if the receiver's last seen time is beyond the offline threshold
	// Get the UDP port for this receiver
	var udpPort int
	err = db.QueryRow(`
		SELECT udp_port FROM receiver_ports
		WHERE receiver_id = $1
	`, receiverID).Scan(&udpPort)

	if err == sql.ErrNoRows {
		// No UDP port assigned, consider offline
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get UDP port: %v", err)
	}

	// Check if the receiver has a last seen time in the portLastSeenMap
	portLastSeenMutex.RLock()
	lastSeen, ok := portLastSeenMap[udpPort]
	portLastSeenMutex.RUnlock()

	if !ok {
		// No last seen time, consider offline
		return false, nil
	}

	// Calculate the offline threshold
	offlineMinutes := settings.ReceiverOfflineHours * 60
	offlineThreshold := time.Duration(offlineMinutes) * time.Minute

	// Check if the receiver is beyond the offline threshold
	if time.Since(lastSeen) > offlineThreshold {
		// Beyond threshold, consider offline
		return false, nil
	}

	// Last event is RECEIVER_ONLINE and last seen time is within threshold
	return true, nil
}

// calculateUptimePercentage calculates the uptime percentage for a receiver
// over the past 7 days based on the events in the receiver_events table
func calculateUptimePercentage(receiverID int) (float64, error) {
	// Skip the dummy receiver with ID 0
	if receiverID == 0 {
		return 0, nil
	}

	if err := ensureConnection(); err != nil {
		return 0, fmt.Errorf("database connection error: %v", err)
	}

	// Get the current time and the time 7 days ago
	now := time.Now()
	sevenDaysAgo := now.AddDate(0, 0, -7)

	// Get all events for the receiver from the past 7 days
	rows, err := db.Query(`
		SELECT event_type, timestamp
		FROM receiver_events
		WHERE receiver_id = $1 AND timestamp >= $2
		ORDER BY timestamp ASC
	`, receiverID, sevenDaysAgo)

	if err != nil {
		return 0, fmt.Errorf("failed to query receiver events: %v", err)
	}
	defer rows.Close()

	// If there are no events, check if the receiver exists and return 0% uptime
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM receivers WHERE id = $1`, receiverID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to check if receiver exists: %v", err)
	}
	if count == 0 {
		return 0, fmt.Errorf("receiver not found")
	}

	// Process the events to calculate uptime
	var events []struct {
		EventType string
		Timestamp time.Time
	}

	for rows.Next() {
		var eventType string
		var timestamp time.Time
		if err := rows.Scan(&eventType, &timestamp); err != nil {
			return 0, fmt.Errorf("failed to scan event row: %v", err)
		}
		events = append(events, struct {
			EventType string
			Timestamp time.Time
		}{EventType: eventType, Timestamp: timestamp})
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error iterating through events: %v", err)
	}

	// If there are no events in the past 7 days, get the last event before that period
	if len(events) == 0 {
		var eventType string
		var timestamp time.Time
		err := db.QueryRow(`
			SELECT event_type, timestamp
			FROM receiver_events
			WHERE receiver_id = $1 AND timestamp < $2
			ORDER BY timestamp DESC
			LIMIT 1
		`, receiverID, sevenDaysAgo).Scan(&eventType, &timestamp)

		if err == sql.ErrNoRows {
			// No events at all, assume offline for the entire period
			return 0, nil
		} else if err != nil {
			return 0, fmt.Errorf("failed to get last event before period: %v", err)
		}

		// If the last event was RECEIVER_ONLINE, the receiver was online for the entire period
		if eventType == ReceiverOnline {
			return 100.0, nil
		}
		// If the last event was RECEIVER_OFFLINE, the receiver was offline for the entire period
		return 0, nil
	}

	// Calculate the total time the receiver was online
	var onlineTime time.Duration
	var lastEventType string
	var lastEventTime time.Time

	// If the first event in our period is not at the start of the period,
	// we need to check the last event before the period to determine the initial state
	if !events[0].Timestamp.Equal(sevenDaysAgo) && !events[0].Timestamp.Before(sevenDaysAgo) {
		var eventType string
		err := db.QueryRow(`
			SELECT event_type
			FROM receiver_events
			WHERE receiver_id = $1 AND timestamp < $2
			ORDER BY timestamp DESC
			LIMIT 1
		`, receiverID, sevenDaysAgo).Scan(&eventType)

		if err == sql.ErrNoRows {
			// No events before the period, assume the initial state is offline
			lastEventType = ReceiverOffline
		} else if err != nil {
			return 0, fmt.Errorf("failed to get last event before period: %v", err)
		} else {
			lastEventType = eventType
		}
		lastEventTime = sevenDaysAgo
	} else {
		// The first event is at the start of the period
		lastEventType = events[0].EventType
		lastEventTime = events[0].Timestamp
	}

	// Process each event
	for i := 0; i < len(events); i++ {
		event := events[i]

		// If the last event was RECEIVER_ONLINE, add the time until this event to the online time
		if lastEventType == ReceiverOnline {
			onlineTime += event.Timestamp.Sub(lastEventTime)
		}

		lastEventType = event.EventType
		lastEventTime = event.Timestamp
	}

	// Add the time from the last event to now
	if lastEventType == ReceiverOnline {
		onlineTime += now.Sub(lastEventTime)
	}

	// Calculate the total time period
	totalPeriod := now.Sub(sevenDaysAgo)

	// Calculate the percentage
	uptimePercentage := float64(onlineTime) / float64(totalPeriod) * 100.0

	// Ensure the percentage is between 0 and 100
	if uptimePercentage < 0 {
		uptimePercentage = 0
	} else if uptimePercentage > 100 {
		uptimePercentage = 100
	}

	return uptimePercentage, nil
}

// checkReceiverStatusChanges checks for receivers that have gone offline or come back online
func checkReceiverStatusChanges(prevPortLastSeenMap map[int]time.Time) {
	// Wait a bit to allow all collector goroutines to update the portLastSeenMap
	time.Sleep(2 * time.Second)

	// Get the current port last seen map
	portLastSeenMutex.RLock()
	currentPortLastSeenMap := make(map[int]time.Time)
	for port, lastSeen := range portLastSeenMap {
		currentPortLastSeenMap[port] = lastSeen
	}
	portLastSeenMutex.RUnlock()

	// Track receivers that have already had status changes logged in this cycle
	// to prevent duplicate alerts
	statusChangedReceivers := make(map[int]bool)

	// Get all receivers with their ports
	if err := ensureConnection(); err != nil {
		log.Printf("Error connecting to database: %v", err)
		return
	}

	rows, err := db.Query(`
		SELECT r.id, rp.udp_port
		FROM receivers r
		JOIN receiver_ports rp ON r.id = rp.receiver_id
		WHERE r.id > 0 -- Skip the anonymous receiver
	`)
	if err != nil {
		log.Printf("Error querying receivers: %v", err)
		return
	}
	defer rows.Close()

	now := time.Now()

	// Convert hours to minutes first to avoid precision loss with small values
	offlineMinutes := settings.ReceiverOfflineHours * 60
	offlineThreshold := time.Duration(offlineMinutes) * time.Minute

	// Log the actual threshold being used
	// log.Printf("Using offline threshold of %v (from %f hours)", offlineThreshold, settings.ReceiverOfflineHours)

	// Process each receiver
	for rows.Next() {
		var receiverID int
		var udpPort int
		if err := rows.Scan(&receiverID, &udpPort); err != nil {
			log.Printf("Error scanning receiver row: %v", err)
			continue
		}

		// Check if the receiver has a current last seen time
		currentLastSeen, hasCurrent := currentPortLastSeenMap[udpPort]
		prevLastSeen, hasPrev := prevPortLastSeenMap[udpPort]

		// On first run, initialize receiver status in the events table
		if !hasPrev && hasCurrent {
			// Check if the receiver has any previous events
			lastEventType, _, err := getLastReceiverEvent(receiverID)
			if err != nil {
				log.Printf("Error getting last event for receiver %d: %v", receiverID, err)
				continue
			}

			// If no previous events, always initialize as offline
			// This ensures proper state transition when it comes online
			if lastEventType == "" {
				// Always mark as offline initially, regardless of current status
				if err := logReceiverEvent(receiverID, ReceiverOffline); err != nil {
					log.Printf("Error logging initial OFFLINE event for receiver %d: %v", receiverID, err)
				} else {
					log.Printf("Logged initial OFFLINE event for receiver %d (no alert sent)", receiverID)
				}

				// Let the normal transition logic handle the change to online if appropriate
				continue
			}
		}

		// MODIFIED: Check for online transition even without previous data
		// This ensures that when a receiver sends data for the first time, it will go online
		if hasCurrent && !statusChangedReceivers[receiverID] {
			// Check if the receiver is currently online based on the threshold
			isOnline := now.Sub(currentLastSeen) <= offlineThreshold

			// Check the last event for this receiver
			lastEventType, _, err := getLastReceiverEvent(receiverID)
			if err != nil {
				log.Printf("Error getting last event for receiver %d: %v", receiverID, err)
			} else if lastEventType == ReceiverOffline && isOnline {
				// If the last event was OFFLINE and the receiver is now online, trigger an online event

				// Log the online event
				if err := logReceiverEvent(receiverID, ReceiverOnline); err != nil {
					log.Printf("Error logging ONLINE event for receiver %d: %v", receiverID, err)
				} else {
					log.Printf("Receiver %d came back ONLINE (last seen: %v)", receiverID, currentLastSeen)

					// Mark this receiver as having had a status change in this cycle
					statusChangedReceivers[receiverID] = true

					// Send webhook notification for online event
					go func(recID int) {
						// Get receiver details
						var rec Receiver
						err := db.QueryRow(`
						SELECT r.id, r.name, r.description, r.latitude, r.longitude, r.email, r.notifications,
						       r.lastupdated, rp.udp_port, r.url
						FROM receivers r
						LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
						WHERE r.id = $1
					`, recID).Scan(
							&rec.ID, &rec.Name, &rec.Description, &rec.Latitude, &rec.Longitude,
							&rec.Email, &rec.Notifications, &rec.LastUpdated, &rec.UDPPort, &rec.URL,
						)

						if err != nil {
							log.Printf("Error fetching receiver %d for online webhook: %v", recID, err)
							return
						}

						// Add the last seen time to custom fields
						if rec.CustomFields == nil {
							rec.CustomFields = make(map[string]interface{})
						}

						// Store the last seen time in RFC3339 format
						lastSeenStr := currentLastSeen.Format(time.RFC3339)
						rec.CustomFields["lastseen"] = lastSeenStr

						// Send the webhook notification
						notifyWebhookWithType(rec, ReceiverOnline)
					}(receiverID)
				}
			}
		}

		// Original check for status changes when we have previous data
		if hasPrev {
			// Check if the receiver was previously online
			wasOnline := now.Sub(prevLastSeen) <= offlineThreshold

			// For brief ingester restarts, we should be more tolerant
			// If the receiver was recently seen (within 5 minutes) in the previous check,
			// consider it online even if it's not in the current map
			shortOfflineThreshold := 5 * time.Minute

			// If the receiver is not in the current map but was recently seen, still consider it online
			isOnline := (hasCurrent && now.Sub(currentLastSeen) <= offlineThreshold) ||
				(!hasCurrent && now.Sub(prevLastSeen) <= shortOfflineThreshold)

			// Debug logging removed

			// Force offline detection if the last seen time hasn't changed in more than the threshold period
			// This handles cases where collectors report the same timestamp repeatedly
			if isOnline && hasPrev && hasCurrent && currentLastSeen.Equal(prevLastSeen) &&
				now.Sub(currentLastSeen) > offlineThreshold {
				isOnline = false
				log.Printf("Forcing offline detection for receiver %d - last seen time hasn't changed in %v",
					receiverID, now.Sub(currentLastSeen))
			}

			// Force offline check removed - now handled by the general case

			// Check if we need to force an offline event for this receiver
			needsOfflineEvent := false

			// Case 1: Status changed from online to offline
			if wasOnline && !isOnline {
				needsOfflineEvent = true
			}

			// Case 2: Receiver is offline but its last event is ONLINE
			if !isOnline && hasCurrent {
				// Check the last event for this receiver
				lastEventType, _, err := getLastReceiverEvent(receiverID)
				if err != nil {
					log.Printf("Error getting last event for receiver %d: %v", receiverID, err)
				} else if lastEventType == ReceiverOnline {
					needsOfflineEvent = true
					// No debug logging needed here
				}
			}

			// If we need to log an offline event
			if needsOfflineEvent {
				// Log the offline event
				if err := logReceiverEvent(receiverID, ReceiverOffline); err != nil {
					log.Printf("Error logging OFFLINE event for receiver %d: %v", receiverID, err)
				} else {
					// Use the previous last seen time if current is not available
					lastSeenToReport := prevLastSeen
					if hasCurrent {
						lastSeenToReport = currentLastSeen
					}

					log.Printf("Receiver %d went OFFLINE (last seen: %v)", receiverID, lastSeenToReport)

					// Send webhook notification for offline event
					go func(recID int) {
						// Get receiver details
						var rec Receiver
						err := db.QueryRow(`
						SELECT r.id, r.name, r.description, r.latitude, r.longitude, r.email, r.notifications,
						       r.lastupdated, rp.udp_port, r.url
						FROM receivers r
						LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
						WHERE r.id = $1
					`, recID).Scan(
							&rec.ID, &rec.Name, &rec.Description, &rec.Latitude, &rec.Longitude,
							&rec.Email, &rec.Notifications, &rec.LastUpdated, &rec.UDPPort, &rec.URL,
						)

						if err != nil {
							log.Printf("Error fetching receiver %d for offline webhook: %v", recID, err)
							return
						}

						// Add the last seen time to custom fields
						if rec.CustomFields == nil {
							rec.CustomFields = make(map[string]interface{})
						}

						// Store the last seen time in RFC3339 format
						// Use the previous last seen time if current is not available
						lastSeenToReport := prevLastSeen
						if hasCurrent {
							lastSeenToReport = currentLastSeen
						}
						lastSeenStr := lastSeenToReport.Format(time.RFC3339)
						rec.CustomFields["lastseen"] = lastSeenStr

						// Send the webhook notification
						notifyWebhookWithType(rec, ReceiverOffline)
					}(receiverID)
				}
			} else if !wasOnline && isOnline && !statusChangedReceivers[receiverID] { // If status changed from offline to online
				// Log the online event
				if err := logReceiverEvent(receiverID, ReceiverOnline); err != nil {
					log.Printf("Error logging ONLINE event for receiver %d: %v", receiverID, err)
				} else {
					// Use the current last seen time which must exist for an online transition
					log.Printf("Receiver %d came back ONLINE (last seen: %v)", receiverID, currentLastSeen)

					// Mark this receiver as having had a status change in this cycle
					statusChangedReceivers[receiverID] = true

					// Send webhook notification for online event
					go func(recID int) {
						// Get receiver details
						var rec Receiver
						err := db.QueryRow(`
						SELECT r.id, r.name, r.description, r.latitude, r.longitude, r.email, r.notifications,
						       r.lastupdated, rp.udp_port, r.url
						FROM receivers r
						LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
						WHERE r.id = $1
					`, recID).Scan(
							&rec.ID, &rec.Name, &rec.Description, &rec.Latitude, &rec.Longitude,
							&rec.Email, &rec.Notifications, &rec.LastUpdated, &rec.UDPPort, &rec.URL,
						)

						if err != nil {
							log.Printf("Error fetching receiver %d for online webhook: %v", recID, err)
							return
						}

						// Add the last seen time to custom fields
						if rec.CustomFields == nil {
							rec.CustomFields = make(map[string]interface{})
						}

						// Store the last seen time in RFC3339 format
						lastSeenStr := currentLastSeen.Format(time.RFC3339)
						rec.CustomFields["lastseen"] = lastSeenStr

						// Send the webhook notification
						notifyWebhookWithType(rec, ReceiverOnline)
					}(receiverID)
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating through receivers: %v", err)
	}
}

// getMessagesByPort gets the message count for a specific UDP port from the port metrics map
// This ignores the receiver's IP address and only filters by UDP port
func getMessagesByPort(udpPort *int) (int, map[string]MessageStat) {
	portMetricsMutex.RLock()
	defer portMetricsMutex.RUnlock()

	totalMessages := 0
	messageStats := make(map[string]MessageStat)

	// If no UDP port is specified, return 0 and empty map
	if udpPort == nil {
		return 0, messageStats
	}

	// Iterate through all IP addresses and find metrics for the specified UDP port
	for ipAddress, portMap := range portMetricsMap {
		// Check if we have metrics for this UDP port
		if metric, ok := portMap[*udpPort]; ok {
			totalMessages += metric.MessageCount
			// Convert PortMetric to MessageStat to remove redundant fields
			messageStat := MessageStat{
				FirstSeen:    metric.FirstSeen,
				LastSeen:     metric.LastSeen,
				MessageCount: metric.MessageCount,
			}
			// Use just the IP address as the key
			messageStats[ipAddress] = messageStat
		}
	}

	return totalMessages, messageStats
}

func main() {
	// Load settings.json
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}

	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}

	// Set default values for settings if not specified
	// This maintains backward compatibility with existing settings files
	if settings.PublicAddReceiverEnabled == false && !strings.Contains(string(data), "public_add_receiver_enabled") {
		settings.PublicAddReceiverEnabled = true
		log.Printf("PublicAddReceiverEnabled not specified in settings.json, defaulting to true")
	}

	// Default IP address timeout to 60 minutes (1 hour) if not specified
	if settings.IPAddressTimeoutMinutes == 0 && !strings.Contains(string(data), "ip_address_timeout_minutes") {
		settings.IPAddressTimeoutMinutes = 60
		log.Printf("IPAddressTimeoutMinutes not specified in settings.json, defaulting to 60 minutes (1 hour)")
	}

	// Default port metrics hours to 1 if not specified
	if settings.PortMetricsHours == 0 && !strings.Contains(string(data), "port_metrics_hours") {
		settings.PortMetricsHours = 1
		log.Printf("PortMetricsHours not specified in settings.json, defaulting to 1 hour")
	}

	// Default receiver offline hours to 0.25 (15 minutes) if not specified
	if settings.ReceiverOfflineHours == 0 {
		// Check if the setting is actually in the file
		if !strings.Contains(string(data), "receiver_offline_hours") {
			settings.ReceiverOfflineHours = 0.25 // 15 minutes in hours
			log.Printf("ReceiverOfflineHours not specified in settings.json, defaulting to 0.25 hours (15 minutes)")
		} else {
			// The setting is in the file but parsed as 0
			log.Printf("WARNING: ReceiverOfflineHours is in settings.json but parsed as 0, setting to default of 0.25 hours (15 minutes)")
			settings.ReceiverOfflineHours = 0.25
		}
	}

	// Connect to Postgres
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName,
	)
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Ensure connection is alive initially
	if err = ensureConnection(); err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}

	// Fetch settings from ingester with retry
	var ingestSettings *IngestSettings
	for {
		ingestSettings, err = fetchIngestSettings()
		if err == nil {
			break
		}
		log.Printf("Failed to fetch settings from ingester: %v. Retrying in 5 seconds...", err)
		time.Sleep(5 * time.Second)
	}

	// Parse UDP dedicated ports
	udpDedicatedPorts = ingestSettings.UDPDedicatedPorts
	log.Printf("UDP dedicated ports: %s", udpDedicatedPorts)

	// Verify it's a range of ports
	var err2 error
	availablePorts, err2 = parsePortRange(udpDedicatedPorts)
	if err2 != nil {
		log.Fatalf("Invalid UDP dedicated ports format: %v", err2)
	}

	createSchema()

	// Start collector tracking
	startCollectorTracking()

	// Start the cleanup routine for expired blocks and old failed signup attempts
	startCleanupRoutine()

	// Public API: GET /receivers and POST /addreceiver
	http.HandleFunc("/receivers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// public list: no IP
		handleListReceiversPublic(w, r)
	})

	// Public endpoint to add a new receiver
	http.HandleFunc("/addreceiver", handleAddReceiver)
	// Public endpoint to edit a receiver
	http.HandleFunc("/editreceiver", handleEditReceiver)

	// Public endpoint to delete a receiver
	http.HandleFunc("/deletereceiver", handleDeleteReceiverPublic)

	// Public endpoint for password reset
	http.HandleFunc("/password-reset", handlePasswordReset)

	// Public endpoint to update receiver IP address - removed

	// Public endpoint to get UDP port with authentication
	http.HandleFunc("/getudpport", handleGetUDPPort)

	http.HandleFunc("/admin/getip", adminGetIPHandler)

	// Admin API: full CRUD under /admin/receivers
	http.HandleFunc("/admin/receivers", adminReceiversHandler)
	http.HandleFunc("/admin/receivers/", adminReceiverHandler)
	http.HandleFunc("/admin/receivers/regenerate-password/", adminRegeneratePasswordHandler)
	http.HandleFunc("/admin/receiver-events", handleReceiverEvents)

	// Serve static files at /admin/
	http.Handle(
		"/admin/",
		http.StripPrefix("/admin/", http.FileServer(http.Dir("web"))),
	)

	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func createSchema() error {
	// Check if we need to add password_hash and password_salt columns
	var columnCount int
	err := db.QueryRow(`
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = 'receivers'
        AND column_name IN ('password_hash', 'password_salt')
    `).Scan(&columnCount)

	if err != nil {
		log.Printf("Error checking for password columns: %v", err)
	} else if columnCount < 2 {
		// Add the new columns if they don't exist
		_, err = db.Exec(`
            ALTER TABLE receivers
            ADD COLUMN IF NOT EXISTS password_hash VARCHAR(64),
            ADD COLUMN IF NOT EXISTS password_salt VARCHAR(32)
        `)

		if err != nil {
			log.Printf("Error adding password columns: %v", err)
		} else {
			log.Printf("Added password_hash and password_salt columns to receivers table")

			// Migrate existing passwords to the new hashed format
			rows, err := db.Query(`
                SELECT id, password FROM receivers
                WHERE password IS NOT NULL AND password != ''
            `)

			if err != nil {
				log.Printf("Error querying receivers for password migration: %v", err)
			} else {
				defer rows.Close()

				for rows.Next() {
					var id int
					var password string

					if err := rows.Scan(&id, &password); err != nil {
						log.Printf("Error scanning receiver row: %v", err)
						continue
					}

					// Generate salt
					saltBytes := make([]byte, 16)
					if _, err := rand.Read(saltBytes); err != nil {
						log.Printf("Error generating salt for receiver %d: %v", id, err)
						continue
					}

					salt := base64.StdEncoding.EncodeToString(saltBytes)
					hash := hashPassword(password, salt)

					// Update the receiver with the hashed password
					_, err = db.Exec(`
                        UPDATE receivers
                        SET password_hash = $1, password_salt = $2
                        WHERE id = $3
                    `, hash, salt, id)

					if err != nil {
						log.Printf("Error updating receiver %d with hashed password: %v", id, err)
					}
				}

				if err := rows.Err(); err != nil {
					log.Printf("Error iterating through receivers: %v", err)
				}
			}
		}
	}

	// Password column is already nullable in the table definition

	// Check if email uniqueness constraint exists and remove it if it does
	var constraintExists bool
	err = db.QueryRow(`
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'receivers_email_key'
        )
    `).Scan(&constraintExists)

	if err != nil {
		log.Printf("Error checking for email uniqueness constraint: %v", err)
	} else if constraintExists {
		// Remove unique constraint from email column
		_, err = db.Exec(`
            ALTER TABLE receivers
            DROP CONSTRAINT receivers_email_key;
        `)

		if err != nil {
			log.Printf("Error removing unique constraint from email column: %v", err)
		} else {
			log.Printf("Removed unique constraint from email column")
		}
	}
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS receivers (
            id SERIAL PRIMARY KEY,
            lastupdated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            description VARCHAR(30) NOT NULL,
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL,
            name VARCHAR(15) NOT NULL UNIQUE,
            url TEXT,
            email VARCHAR(100) NOT NULL,
            notifications BOOLEAN NOT NULL DEFAULT TRUE,
            password VARCHAR(20),
            password_hash VARCHAR(64) NOT NULL,
            password_salt VARCHAR(32) NOT NULL,
            request_ip_address TEXT NOT NULL DEFAULT ''
        );
    `)
	if err != nil {
		log.Fatalf("Error creating receivers table: %v", err)
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receivers_id ON receivers(id);`)
	if err != nil {
		log.Fatalf("Error creating index: %v", err)
	}
	// Create a unique index on the name column if it doesn't exist
	_, err = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_receivers_name ON receivers(name);`)
	if err != nil {
		log.Fatalf("Error creating unique index on name: %v", err)
	}
	// Sync the SERIAL sequence to start at 1 to ensure the first receiver has ID 1
	_, err = db.Exec(`
        SELECT setval(
          pg_get_serial_sequence('receivers','id'),
          1,
          false
        );
    `)
	if err != nil {
		log.Fatalf("Error syncing receivers_id_seq: %v", err)
	}

	// Create receiver_ports table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS receiver_ports (
            udp_port INT PRIMARY KEY,
            receiver_id INT REFERENCES receivers(id) ON DELETE SET NULL,
            last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    `)
	if err != nil {
		log.Fatalf("Error creating receiver_ports table: %v", err)
	}

	// Create index on receiver_id
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receiver_ports_receiver_id ON receiver_ports(receiver_id);`)
	if err != nil {
		return fmt.Errorf("error creating index on receiver_id: %v", err)
	}

	// Populate receiver_ports table with available ports
	if len(availablePorts) > 0 {
		// First, check which ports already exist in the table
		rows, err := db.Query(`SELECT udp_port FROM receiver_ports`)
		if err != nil {
			log.Fatalf("Error querying receiver_ports: %v", err)
		}
		defer rows.Close()

		existingPorts := make(map[int]bool)
		for rows.Next() {
			var port int
			if err := rows.Scan(&port); err != nil {
				log.Fatalf("Error scanning port: %v", err)
			}
			existingPorts[port] = true
		}

		// Insert any ports that don't already exist
		for _, port := range availablePorts {
			if !existingPorts[port] {
				_, err := db.Exec(`
                    INSERT INTO receiver_ports (udp_port)
                    VALUES ($1)
                `, port)
				if err != nil {
					log.Fatalf("Error inserting port %d: %v", port, err)
				}
			}
		}
	}

	// Create password_reset_tokens table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            token VARCHAR(64) PRIMARY KEY,
            receiver_id INT NOT NULL REFERENCES receivers(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            used BOOLEAN NOT NULL DEFAULT FALSE
        );
    `)
	if err != nil {
		return fmt.Errorf("error creating password_reset_tokens table: %v", err)
	}

	// Create index on receiver_id
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_password_reset_tokens_receiver_id ON password_reset_tokens(receiver_id);`)
	if err != nil {
		return fmt.Errorf("error creating index on receiver_id: %v", err)
	}

	// Create blocked_signup_ips table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS blocked_signup_ips (
            id SERIAL PRIMARY KEY,
            ip_address VARCHAR(45) NOT NULL UNIQUE,
            blocked_at TIMESTAMP NOT NULL DEFAULT NOW(),
            unblock_at TIMESTAMP NOT NULL,
            reason TEXT NOT NULL,
            attempts INT NOT NULL DEFAULT 1,
            last_attempt_email VARCHAR(255),
            last_attempt_name VARCHAR(255),
            last_attempt_description TEXT,
            last_attempt_latitude DOUBLE PRECISION,
            last_attempt_longitude DOUBLE PRECISION
        );
    `)
	if err != nil {
		return fmt.Errorf("error creating blocked_signup_ips table: %v", err)
	}

	// Create index on ip_address for better performance
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_blocked_signup_ips_ip_address ON blocked_signup_ips(ip_address);`)
	if err != nil {
		return fmt.Errorf("error creating index on blocked_signup_ips.ip_address: %v", err)
	}

	// Create failed_signup_attempts table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS failed_signup_attempts (
            id SERIAL PRIMARY KEY,
            ip_address VARCHAR(45) NOT NULL,
            attempt_time TIMESTAMP NOT NULL DEFAULT NOW(),
            reason TEXT NOT NULL,
            email VARCHAR(255),
            name VARCHAR(255),
            description TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    `)
	if err != nil {
		return fmt.Errorf("error creating failed_signup_attempts table: %v", err)
	}

	// Create indexes for better performance on failed_signup_attempts
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_failed_signup_attempts_ip_address ON failed_signup_attempts(ip_address);`)
	if err != nil {
		return fmt.Errorf("error creating index on failed_signup_attempts.ip_address: %v", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_failed_signup_attempts_attempt_time ON failed_signup_attempts(attempt_time);`)
	if err != nil {
		return fmt.Errorf("error creating index on failed_signup_attempts.attempt_time: %v", err)
	}

	// Create receiver_events table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS receiver_events (
            id SERIAL PRIMARY KEY,
            receiver_id INTEGER NOT NULL REFERENCES receivers(id) ON DELETE CASCADE,
            event_type VARCHAR(20) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    `)
	if err != nil {
		return fmt.Errorf("error creating receiver_events table: %v", err)
	}

	// Create index on receiver_id for better performance
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receiver_events_receiver_id ON receiver_events(receiver_id);`)
	if err != nil {
		return fmt.Errorf("error creating index on receiver_events.receiver_id: %v", err)
	}

	// Create index on timestamp for better performance
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_receiver_events_timestamp ON receiver_events(timestamp);`)
	if err != nil {
		return fmt.Errorf("error creating index on receiver_events.timestamp: %v", err)
	}

	return nil
}

// Ensure connection is alive, attempt to reconnect if needed
func ensureConnection() error {
	if err := db.Ping(); err != nil {
		log.Println("Database connection lost, attempting to reconnect...")
		db, err = sql.Open("postgres", fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName,
		))
		if err != nil {
			return fmt.Errorf("failed to reconnect to database: %v", err)
		}
		if err := db.Ping(); err != nil {
			return fmt.Errorf("unable to verify reconnected database: %v", err)
		}
		log.Println("Reconnected to the database successfully.")
	}
	return nil
}

// Helper function to build the query and fetch filtered receivers based on id and ip_address.
func getFilteredReceivers(w http.ResponseWriter, filters map[string]string) ([]Receiver, error) {
	if err := ensureConnection(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return []Receiver{}, nil // Return empty slice here instead of `nil`
	}

	// Extract parameters
	idParam := filters["id"]
	// We no longer use ipParam since we filter by IP using port metrics

	// Base query
	baseQuery := `
        SELECT r.id,
               r.lastupdated,
               r.description,
               r.latitude,
               r.longitude,
               r.name,
               r.url,
               r.email,
               r.notifications,
               r.password_hash,
               r.password_salt,
               rp.udp_port
          FROM receivers r
          LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id`

	var (
		conditions []string
		args       []interface{}
	)
	idx := 1
	if idParam != "" {
		// Validate and add id filter
		idVal, err := strconv.Atoi(idParam)
		if err != nil {
			return nil, fmt.Errorf("invalid id parameter")
		}
		conditions = append(conditions, fmt.Sprintf("id = $%d", idx))
		args = append(args, idVal)
		idx++
	}
	// We no longer filter by ip_address in the SQL query
	// Instead, we'll filter by IP address using port metrics after fetching the receivers
	// This comment is kept to document the change

	// Add WHERE clause if filters are provided
	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}
	baseQuery += " ORDER BY id"

	// Execute query
	rows, err := db.Query(baseQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Scan results
	var list []Receiver
	for rows.Next() {
		var rec Receiver
		// Create a nullable int for UDP port
		var udpPort sql.NullInt64

		if err := rows.Scan(
			&rec.ID,
			&rec.LastUpdated,
			&rec.Description,
			&rec.Latitude,
			&rec.Longitude,
			&rec.Name,
			&rec.URL,
			&rec.Email,
			&rec.Notifications,
			&rec.PasswordHash,
			&rec.PasswordSalt,
			&udpPort, // Scan into nullable int
		); err != nil {
			return nil, err
		}

		// Only set UDPPort if it's not null
		if udpPort.Valid {
			port := int(udpPort.Int64)
			rec.UDPPort = &port

			// Use port metrics to determine the IP address
			portMetricsMutex.RLock()
			var lastSeenIP string
			var lastSeenTime time.Time

			// Find the most recent IP address that sent messages to this port
			for ipAddress, portMap := range portMetricsMap {
				if metric, ok := portMap[port]; ok {
					if lastSeenIP == "" || metric.LastSeen.After(lastSeenTime) {
						lastSeenIP = ipAddress
						lastSeenTime = metric.LastSeen
					}
				}
			}
			portMetricsMutex.RUnlock()

			// Use the IP from port metrics if available
			rec.IPAddress = lastSeenIP
		}

		list = append(list, rec)
	}

	// Return empty list if no receivers are found
	if len(list) == 0 {
		return []Receiver{}, nil // Return empty slice explicitly
	}

	return list, nil
}

// adminGetIPHandler handles GET /admin/getip?id=<id>
func adminGetIPHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 1) Parse and validate 'id' query parameter
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "`id` query parameter is required", http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idStr)
	if err != nil || id < 1 {
		http.Error(w, "`id` must be a positive integer", http.StatusBadRequest)
		return
	}

	// 2) Ensure DB connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	// 3) First check if the receiver exists
	var exists bool
	err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM receivers WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	if !exists {
		// Receiver doesn't exist - return 404 Not Found
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	}

	// 4) Get the UDP port for this receiver
	var udpPort sql.NullInt64
	err = db.QueryRow(`SELECT udp_port FROM receiver_ports WHERE receiver_id = $1`, id).Scan(&udpPort)
	if err != nil && err != sql.ErrNoRows {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	// 5) If we have a UDP port, look for the most recent IP in port metrics
	var addr string
	if udpPort.Valid {
		port := int(udpPort.Int64)

		// Lock the port metrics map to safely read from it
		portMetricsMutex.RLock()

		var lastSeenIP string
		var lastSeenTime time.Time

		// Find the most recent IP address that sent messages to this port
		for ipAddress, portMap := range portMetricsMap {
			if metric, ok := portMap[port]; ok {
				if lastSeenIP == "" || metric.LastSeen.After(lastSeenTime) {
					lastSeenIP = ipAddress
					lastSeenTime = metric.LastSeen
				}
			}
		}

		portMetricsMutex.RUnlock()

		addr = lastSeenIP
	}

	// 6) Write response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"ip_address": addr,
	})
}

// Function to get the message count for a given IP address from the metrics API.
type SimpleMetrics struct {
	Messages int `json:"messages"` // Messages field inside simple_metrics
}

type MetricsResponse struct {
	SimpleMetrics SimpleMetrics `json:"simple_metrics"` // This is where the simple_metrics field is mapped
}

func getMessagesByIP(ipAddress string, udpPort *int) (int, error) {
	// Use the new port metrics tracking system instead of the metrics API
	// We only care about the UDP port, not the IP address
	messages, _ := getMessagesByPort(udpPort)
	return messages, nil
}

// --- Public listing only ---
func handleListReceivers(w http.ResponseWriter, r *http.Request) {
	// Ensure the connection is alive before performing the query
	if err := ensureConnection(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := db.Query(`
        SELECT id, lastupdated, description, latitude, longitude, name, url
        FROM receivers ORDER BY id
    `)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var list []Receiver
	for rows.Next() {
		var rec Receiver
		if err := rows.Scan(
			&rec.ID, &rec.LastUpdated, &rec.Description,
			&rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL,
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		list = append(list, rec)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// --- Admin handlers (full CRUD) ---
func adminReceiversHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleListReceiversAdmin(w, r)
	case http.MethodPost:
		handleCreateReceiver(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// getReceiverEvents retrieves events for a specific receiver
func getReceiverEvents(receiverID int, limit int) ([]map[string]interface{}, error) {
	if err := ensureConnection(); err != nil {
		return nil, fmt.Errorf("database connection error: %v", err)
	}

	// Default limit to 100 if not specified or negative
	if limit <= 0 {
		limit = 100
	}

	rows, err := db.Query(`
		SELECT id, event_type, timestamp
		FROM receiver_events
		WHERE receiver_id = $1
		ORDER BY timestamp DESC
		LIMIT $2
	`, receiverID, limit)

	if err != nil {
		return nil, fmt.Errorf("failed to query receiver events: %v", err)
	}
	defer rows.Close()

	var events []map[string]interface{}
	for rows.Next() {
		var id int
		var eventType string
		var timestamp time.Time

		if err := rows.Scan(&id, &eventType, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %v", err)
		}

		event := map[string]interface{}{
			"id":         id,
			"event_type": eventType,
			"timestamp":  timestamp,
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through events: %v", err)
	}

	return events, nil
}

// Public list: excludes ip_address and password
func handleListReceiversPublic(w http.ResponseWriter, r *http.Request) {
	// Ensure the connection is alive before performing the query
	if err := ensureConnection(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Extract query parameters
	idParam := r.URL.Query().Get("id")
	ipParam := r.URL.Query().Get("ip_address")
	maxAgeParam := r.URL.Query().Get("maxage")

	// Only apply maxage filtering if no specific receiver ID is requested
	var maxAgeTime *time.Time
	if idParam == "" {
		// Parse maxage parameter (in hours)
		// Default to 7 days (168 hours) if not specified
		// Maximum allowed value is 90 days (2160 hours)
		var maxAgeHours float64 = 168 // Default to 7 days

		if maxAgeParam != "" {
			var err error
			maxAgeHours, err = strconv.ParseFloat(maxAgeParam, 64)
			if err != nil || maxAgeHours < 0 {
				http.Error(w, "Invalid maxage parameter: must be a positive number of hours", http.StatusBadRequest)
				return
			}

			// Cap at maximum allowed value (90 days = 2160 hours)
			if maxAgeHours > 2160 {
				maxAgeHours = 2160
			}
		}

		// Calculate the cutoff time
		cutoffTime := time.Now().Add(-time.Duration(maxAgeHours * float64(time.Hour)))
		maxAgeTime = &cutoffTime
	}

	// Build the query based on filters - only filter by ID, not by IP address
	baseQuery := `
        SELECT r.id, r.lastupdated, r.description, r.latitude, r.longitude, r.name, r.url, r.email, r.notifications, rp.udp_port
        FROM receivers r
        LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
        WHERE 1=1
    `

	var conditions []string
	var args []interface{}
	idx := 1

	if idParam != "" {
		// Validate and add id filter
		idVal, err := strconv.Atoi(idParam)
		if err != nil {
			http.Error(w, "Invalid id parameter", http.StatusBadRequest)
			return
		}
		conditions = append(conditions, fmt.Sprintf("id = $%d", idx))
		args = append(args, idVal)
		idx++
	}

	if len(conditions) > 0 {
		baseQuery += " AND " + strings.Join(conditions, " AND ")
	}

	baseQuery += " ORDER BY id"

	// Execute the query with the parameters
	rows, err := db.Query(baseQuery, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// If we're filtering by IP address, we'll need to use the portMetricsMap
	var ipAddressFilter string
	if ipParam != "" {
		ipAddressFilter = ipParam
	}

	// Map to track receivers by port for IP address filtering
	receiversByPort := make(map[int]PublicReceiver)
	// Map to track last_seen time for each port
	lastSeenByPort := make(map[int]time.Time)

	var list []PublicReceiver
	for rows.Next() {
		var rec Receiver
		var udpPort sql.NullInt64
		if err := rows.Scan(
			&rec.ID, &rec.LastUpdated, &rec.Description,
			&rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.Email, &rec.Notifications, &udpPort,
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Only set UDPPort if it's not null
		if udpPort.Valid {
			port := int(udpPort.Int64)
			rec.UDPPort = &port

			// Use port metrics to determine the IP address
			portMetricsMutex.RLock()
			var lastSeenIP string
			var lastSeenTime time.Time

			// Find the most recent IP address that sent messages to this port
			for ipAddress, portMap := range portMetricsMap {
				if metric, ok := portMap[port]; ok {
					if lastSeenIP == "" || metric.LastSeen.After(lastSeenTime) {
						lastSeenIP = ipAddress
						lastSeenTime = metric.LastSeen
					}
				}
			}
			portMetricsMutex.RUnlock()

			// Use the IP from port metrics if available
			rec.IPAddress = lastSeenIP
		}

		// Fetch message count (0 on error)
		// Note: getMessagesByIP now only uses the UDP port, not the IP address
		msgs, err := getMessagesByIP("", rec.UDPPort) // Pass empty string for IP address
		if err != nil {
			msgs = 0
		}

		// Get the last seen time for this port from the portLastSeenMap
		var lastSeen *time.Time
		if rec.UDPPort != nil {
			portLastSeenMutex.RLock()
			if ls, ok := portLastSeenMap[*rec.UDPPort]; ok {
				lastSeen = &ls
			}
			portLastSeenMutex.RUnlock()
		}

		// Convert to PublicReceiver (which doesn't include password or IP address)
		publicRec := PublicReceiver{
			ID:            rec.ID,
			LastUpdated:   rec.LastUpdated,
			Description:   rec.Description,
			Latitude:      rec.Latitude,
			Longitude:     rec.Longitude,
			Name:          rec.Name,
			URL:           rec.URL,
			Notifications: rec.Notifications, // Include notifications in public API
			Email:         rec.Email,         // Store email but don't expose in JSON
			Messages:      msgs,
			UDPPort:       rec.UDPPort, // Store UDPPort for filtering but don't expose in JSON
		}

		// Store the lastSeen value to use later when converting to map
		publicRec.lastSeenTime = lastSeen

		// If we're filtering by IP address, store this receiver by its port
		if ipAddressFilter != "" && rec.UDPPort != nil {
			// We'll check the portMetricsMap later
			receiversByPort[*rec.UDPPort] = publicRec
		} else {
			// If not filtering by IP or receiver has no port, add to the list directly
			list = append(list, publicRec)
		}
	}

	// If we're filtering by IP address, check the portMetricsMap
	if ipAddressFilter != "" {
		// Fetch the primary UDP port from the ingester settings
		ingestSettings, err := fetchIngestSettings()
		if err != nil {
			log.Printf("Error fetching ingester settings: %v", err)
			// Continue without filtering if we can't get the primary port
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(list)
			return
		}

		// The primary UDP port is the UDP listen port from the ingester settings
		primaryUDPPort := ingestSettings.UDPListenPort

		// Lock the map for reading
		portMetricsMutex.RLock()

		// Check if we have metrics for this IP address
		if portMap, ok := portMetricsMap[ipAddressFilter]; ok {
			// For each port this IP has been seen on
			for port, metric := range portMap {
				// Skip the primary UDP port - we only want receivers on non-primary ports
				if port == primaryUDPPort {
					continue
				}

				// If we have a receiver for this port
				if _, ok := receiversByPort[port]; ok {
					// Store the last_seen time for this port
					lastSeenByPort[port] = metric.LastSeen
				}
			}
		}

		portMetricsMutex.RUnlock()

		// Find the port with the most recent last_seen time
		var mostRecentPort int
		var mostRecentTime time.Time
		for port, lastSeen := range lastSeenByPort {
			if lastSeen.After(mostRecentTime) {
				mostRecentPort = port
				mostRecentTime = lastSeen
			}
		}

		// If we found a matching non-primary port, add its receiver to the list
		if !mostRecentTime.IsZero() {
			list = append(list, receiversByPort[mostRecentPort])
		}
	}

	// Create a response that will include receivers
	var response []interface{}

	// Only add the anonymous receiver if no specific ID was requested and no IP filter was provided
	if idParam == "" && ipAddressFilter == "" {
		// Fetch the UDP listen port from the ingester settings
		ingestSettings, err := fetchIngestSettings()
		var udpListenPort int
		if err != nil {
			log.Printf("Error fetching ingester settings: %v", err)
			udpListenPort = 0 // Default to 0 if we can't fetch the settings
		} else {
			udpListenPort = ingestSettings.UDPListenPort
		}

		// Create a map for the anonymous receiver
		anonymousReceiver := map[string]interface{}{
			"id":          0,
			"name":        "Anonymous",
			"description": "Anonymous",
			// Don't include notifications field for dummy receiver
		}

		// Add the lastseen field if available
		portLastSeenMutex.RLock()
		if lastSeen, ok := portLastSeenMap[udpListenPort]; ok {
			anonymousReceiver["lastseen"] = lastSeen
		}
		portLastSeenMutex.RUnlock()

		// Always add the anonymous receiver (ID 0) regardless of maxage criteria
		// or if a specific receiver is requested by ID
		response = append(response, anonymousReceiver)
	}

	// Add all the regular receivers that meet the maxage criteria (if applicable)
	for _, rec := range list {
		// Skip receivers that don't meet the maxage criteria, but only if maxAgeTime is set
		// (which means we're not requesting a specific receiver by ID)
		if maxAgeTime != nil && (rec.lastSeenTime == nil || rec.lastSeenTime.Before(*maxAgeTime)) {
			continue
		}

		// Convert PublicReceiver to map to ensure all fields are included in the JSON
		recBytes, _ := json.Marshal(rec)
		var recMap map[string]interface{}
		json.Unmarshal(recBytes, &recMap)

		// Add the lastseen field if available
		if rec.lastSeenTime != nil {
			recMap["lastseen"] = rec.lastSeenTime
		}

		// Add the state field (online/offline) if this is not the dummy receiver
		if rec.ID > 0 {
			state, err := getReceiverState(rec.ID)
			if err != nil {
				log.Printf("Error getting state for receiver %d: %v", rec.ID, err)
			} else {
				recMap["state"] = state
			}

			// Add the uptime percentage for the past 7 days
			uptimePct, err := calculateUptimePercentage(rec.ID)
			if err != nil {
				log.Printf("Error calculating uptime percentage for receiver %d: %v", rec.ID, err)
			} else {
				// Round to 2 decimal places
				recMap["uptime_pct_week"] = math.Round(uptimePct*100) / 100
			}
		}

		response = append(response, recMap)
	}

	// If no receivers are found, return an empty array instead of null
	if len(response) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]")) // Send empty array explicitly
		return
	}

	// Return the combined list in JSON format
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Admin list: same as public but includes ip_address (computed from port metrics) and password
func handleListReceiversAdmin(w http.ResponseWriter, r *http.Request) {
	// Parse filters from query parameters (can be the same as public)
	filters := map[string]string{
		"id":         r.URL.Query().Get("id"),
		"ip_address": r.URL.Query().Get("ip_address"),
	}

	// Call the helper function to get the filtered receivers
	list, err := getFilteredReceivers(w, filters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// For each receiver in the list, fetch messages based on the ip_address
	for i, rec := range list {
		// Fetch messages count and message stats for each receiver based on udp_port
		// Note: We don't need the IP address for this since getMessagesByPort only uses UDP port
		msgs, messageStats := getMessagesByPort(rec.UDPPort)

		// Set the messages field and message stats
		list[i].Messages = msgs
		list[i].MessageStats = messageStats

		// Store the last seen time for this port to use when converting to map
		if rec.UDPPort != nil {
			portLastSeenMutex.RLock()
			if lastSeen, ok := portLastSeenMap[*rec.UDPPort]; ok {
				// Store the last seen time in a field that we'll access when converting to map
				list[i].lastSeenTime = &lastSeen
			}
			portLastSeenMutex.RUnlock()
		}
	}

	// Add dummy entry for receiver ID 0
	// Fetch the UDP listen port from the ingester settings
	ingestSettings, err := fetchIngestSettings()
	var udpListenPort int
	if err != nil {
		log.Printf("Error fetching ingester settings: %v", err)
		udpListenPort = 0 // Default to 0 if we can't fetch the settings
	} else {
		udpListenPort = ingestSettings.UDPListenPort
	}

	// Create dummy receiver
	dummyReceiver := Receiver{
		ID:            0,
		LastUpdated:   time.Now(),
		Description:   "Anonymous",
		Latitude:      0,
		Longitude:     0,
		Name:          "Anonymous",
		URL:           nil,
		Email:         "",
		Notifications: true,
		Password:      "",
		Messages:      0,
		MessageStats:  make(map[string]MessageStat),
	}

	// Set the UDP port
	dummyReceiver.UDPPort = &udpListenPort

	// Fetch message stats for the dummy receiver based on the UDP listen port
	// This will aggregate all messages for this UDP port across all IP addresses
	msgs, messageStats := getMessagesByPort(&udpListenPort)
	dummyReceiver.Messages = msgs
	dummyReceiver.MessageStats = messageStats

	// Store the last seen time for the dummy receiver
	portLastSeenMutex.RLock()
	if lastSeen, ok := portLastSeenMap[udpListenPort]; ok {
		dummyReceiver.lastSeenTime = &lastSeen
	}
	portLastSeenMutex.RUnlock()

	// Add the dummy receiver to the beginning of the list
	list = append([]Receiver{dummyReceiver}, list...)

	// Convert receivers to maps to ensure all fields are included in the JSON
	var responseList []map[string]interface{}
	for _, rec := range list {
		// Convert Receiver to map but exclude password fields
		recMap := map[string]interface{}{
			"id":            rec.ID,
			"lastupdated":   rec.LastUpdated,
			"description":   rec.Description,
			"latitude":      rec.Latitude,
			"longitude":     rec.Longitude,
			"name":          rec.Name,
			"email":         rec.Email,
			"notifications": rec.Notifications,
			"messages":      rec.Messages,
			"message_stats": rec.MessageStats,
			// password field removed as it will no longer be plain text
		}

		// Add optional fields
		if rec.URL != nil {
			recMap["url"] = rec.URL
		}
		if rec.UDPPort != nil {
			recMap["udp_port"] = rec.UDPPort
		}
		if rec.IPAddress != "" {
			recMap["ip_address"] = rec.IPAddress
		}

		// Add the lastseen field if available
		if rec.lastSeenTime != nil {
			recMap["lastseen"] = rec.lastSeenTime
		}

		// Add the state field (online/offline) if this is not the dummy receiver
		if rec.ID > 0 {
			state, err := getReceiverState(rec.ID)
			if err != nil {
				log.Printf("Error getting state for receiver %d: %v", rec.ID, err)
			} else {
				recMap["state"] = state
			}

			// Add the uptime percentage for the past 7 days
			uptimePct, err := calculateUptimePercentage(rec.ID)
			if err != nil {
				log.Printf("Error calculating uptime percentage for receiver %d: %v", rec.ID, err)
			} else {
				// Round to 2 decimal places
				recMap["uptime_pct_week"] = math.Round(uptimePct*100) / 100
			}
		}

		responseList = append(responseList, recMap)
	}

	// Return the list of receivers in JSON format, including ip_address, messages, message stats, and lastseen
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responseList)
}

func adminReceiverHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(parts[3])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		handleGetReceiver(w, r, id)
	case http.MethodPut:
		handlePutReceiver(w, r, id)
	case http.MethodPatch:
		// Set isAdminAction flag in the handler
		handlePatchReceiver(w, r, id)
	case http.MethodDelete:
		// Set isAdminAction flag in the handler
		handleDeleteReceiver(w, r, id)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleReceiverEvents handles GET requests for receiver events
func handleReceiverEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse receiver ID from query parameter
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "Missing required 'id' parameter", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid receiver ID", http.StatusBadRequest)
		return
	}

	// Parse limit parameter (optional)
	limitStr := r.URL.Query().Get("limit")
	limit := 100 // Default limit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	// Get events for the receiver
	events, err := getReceiverEvents(id, limit)
	if err != nil {
		http.Error(w, "Failed to get receiver events: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return events as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// generateRandomPassword generates a random password and returns both the plain text password
// and the hashed password with salt for secure storage
func generateRandomPassword() (string, string, string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+"
	password := make([]byte, 12) // Increased from 8 to 12 characters

	for i := range password {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", "", "", err
		}
		password[i] = charset[n.Int64()]
	}

	plainPassword := string(password)

	// Generate a random salt
	salt := make([]byte, 16)
	_, err := rand.Read(salt)
	if err != nil {
		return "", "", "", err
	}
	saltStr := base64.StdEncoding.EncodeToString(salt)

	// Hash the password with the salt using a more secure method
	hashedPassword := hashPassword(plainPassword, saltStr)

	return plainPassword, hashedPassword, saltStr, nil
}

// hashPassword hashes a password with a given salt using PBKDF2
func hashPassword(password, salt string) string {
	// Use PBKDF2 with HMAC-SHA256, 10000 iterations, and 32-byte output
	dk := pbkdf2.Key([]byte(password), []byte(salt), 10000, 32, sha256.New)
	return base64.StdEncoding.EncodeToString(dk)
}

// verifyPassword checks if a plain text password matches the stored hash
func verifyPassword(plainPassword, storedHash, storedSalt string) bool {
	// Hash the provided password with the stored salt
	calculatedHash := hashPassword(plainPassword, storedSalt)
	return calculatedHash == storedHash
}

func handleCreateReceiver(w http.ResponseWriter, r *http.Request) {
	// 1) decode JSON body
	var input ReceiverInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// 2) Generate a random password if not provided
	var plainPassword string
	var hashedPassword string
	var saltStr string

	if input.Password != nil {
		plainPassword = *input.Password
	} else {
		var err error
		plainPassword, hashedPassword, saltStr, err = generateRandomPassword()
		if err != nil {
			http.Error(w, "Failed to generate password", http.StatusInternalServerError)
			return
		}
	}

	// 4) build Receiver and validate
	rec := Receiver{
		Description:   input.Description,
		Latitude:      input.Latitude,
		Longitude:     input.Longitude,
		Name:          strings.ToUpper(input.Name),
		URL:           input.URL,
		Email:         input.Email,
		Notifications: input.Notifications,
		Password:      plainPassword, // Store plain password temporarily for validation
		PasswordHash:  hashedPassword,
		PasswordSalt:  saltStr,
		// IPAddress is no longer used - we get IP from port metrics
	}

	// Get the client's IP address for request tracking
	clientIP := getClientIP(r)
	if err := validateReceiver(rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 5) Start a transaction to ensure atomicity
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Failed to start transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Lock the receivers table to prevent concurrent inserts
	_, err = tx.Exec(`LOCK TABLE receivers IN EXCLUSIVE MODE`)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to lock table: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the current value of the sequence
	var currSeqVal int
	err = tx.QueryRow(`SELECT last_value FROM receivers_id_seq`).Scan(&currSeqVal)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get sequence value: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Find the maximum ID ever used (including deleted receivers)
	var maxID int
	err = tx.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM receivers`).Scan(&maxID)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get max ID: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If the max ID is greater than the current sequence value, update the sequence
	if maxID >= currSeqVal {
		_, err = tx.Exec(`SELECT setval('receivers_id_seq', $1)`, maxID+1)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to update sequence: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Get a new ID from the sequence (which is now guaranteed to be higher than any ID ever used)
	var newID int
	err = tx.QueryRow(`SELECT nextval('receivers_id_seq')`).Scan(&newID)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get new ID: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// INSERT including password and request_ip_address with explicit ID
	// Note: ip_address is no longer used
	// If password was provided by the user but not hashed yet, hash it now
	if input.Password != nil && rec.PasswordHash == "" {
		// Generate a salt
		saltBytes := make([]byte, 16)
		_, err := rand.Read(saltBytes)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to generate salt", http.StatusInternalServerError)
			return
		}
		saltStr = base64.StdEncoding.EncodeToString(saltBytes)
		rec.PasswordSalt = saltStr
		rec.PasswordHash = hashPassword(rec.Password, saltStr)
	}

	err = tx.QueryRow(`
        INSERT INTO receivers (
            id,
            description,
            latitude,
            longitude,
            name,
            url,
            email,
            notifications,
            password_hash,
            password_salt,
            request_ip_address
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        RETURNING id, lastupdated
    `, newID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.Email, rec.Notifications, rec.PasswordHash, rec.PasswordSalt, clientIP).
		Scan(&rec.ID, &rec.LastUpdated)
	if err != nil {
		tx.Rollback()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 6) Allocate a UDP port for this receiver within the transaction
	udpPort, err := allocatePortTx(tx, rec.ID)
	if err != nil {
		tx.Rollback() // Rollback the transaction on error

		if err.Error() == "no available ports" {
			log.Printf("ERROR: No available UDP ports found for receiver ID %d", rec.ID)
			http.Error(w, "Failed to create receiver: No available UDP ports", http.StatusServiceUnavailable)
		} else {
			log.Printf("ERROR: Failed to allocate UDP port for receiver ID %d: %v", rec.ID, err)
			http.Error(w, fmt.Sprintf("Failed to create receiver: UDP port allocation error: %v", err), http.StatusInternalServerError)
		}

		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		log.Printf("ERROR: Failed to commit transaction for receiver ID %d: %v", rec.ID, err)
		http.Error(w, "Failed to create receiver: Transaction commit error", http.StatusInternalServerError)
		return
	}

	rec.UDPPort = &udpPort

	// 7) send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(rec)
	// Notify webhook for admin-created receiver
	if settings.WebhookURL != "" {
		go notifyWebhookWithClientIP(rec, clientIP)
	}

	// Log initial OFFLINE event for the new receiver
	// It will be marked as ONLINE when we get a lastseen time for it
	if err := logReceiverEvent(rec.ID, ReceiverOffline); err != nil {
		log.Printf("Error logging initial OFFLINE event for new receiver %d: %v", rec.ID, err)
	} else {
		log.Printf("Logged initial OFFLINE event for new receiver %d", rec.ID)
	}
}

func handleGetReceiver(w http.ResponseWriter, r *http.Request, id int) {
	var rec Receiver
	// Query the receiver from the database.
	// Create a nullable int for UDP port
	var udpPort sql.NullInt64

	err := db.QueryRow(`
        SELECT r.id, r.lastupdated, r.description, r.latitude, r.longitude, r.name, r.url, r.email, r.notifications,
               r.password_hash, r.password_salt, rp.udp_port
        FROM receivers r
        LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
        WHERE r.id = $1
    `, id).Scan(
		&rec.ID, &rec.LastUpdated, &rec.Description,
		&rec.Latitude, &rec.Longitude, &rec.Name, &rec.URL, &rec.Email, &rec.Notifications,
		&rec.PasswordHash, &rec.PasswordSalt, &udpPort,
	)

	// Only set UDPPort if it's not null
	if udpPort.Valid {
		port := int(udpPort.Int64)
		rec.UDPPort = &port

		// Use port metrics to determine the IP address
		portMetricsMutex.RLock()
		var lastSeenIP string
		var lastSeenTime time.Time

		// Find the most recent IP address that sent messages to this port
		for ipAddress, portMap := range portMetricsMap {
			if metric, ok := portMap[port]; ok {
				if lastSeenIP == "" || metric.LastSeen.After(lastSeenTime) {
					lastSeenIP = ipAddress
					lastSeenTime = metric.LastSeen
				}
			}
		}
		portMetricsMutex.RUnlock()

		// Use the IP from port metrics if available
		rec.IPAddress = lastSeenIP
	}
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Now, fetch the number of messages and message stats for the receiver's UDP port.
	messages, messageStats := getMessagesByPort(rec.UDPPort)

	// Add the messages field and message stats to the receiver struct.
	rec.Messages = messages
	rec.MessageStats = messageStats

	// Send the full receiver response with messages count and message map.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)
}

func handlePutReceiver(w http.ResponseWriter, r *http.Request, id int) {
	// Get the client's IP address
	clientIP := getClientIP(r)

	// 1) decode JSON body
	var input ReceiverInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// 2) Get current password hash and salt if not provided in the input
	var passwordHash, passwordSalt string
	var plainPassword string

	if input.Password != nil {
		// Generate a new salt and hash the password
		saltBytes := make([]byte, 16)
		if _, err := rand.Read(saltBytes); err != nil {
			http.Error(w, "Failed to generate salt", http.StatusInternalServerError)
			return
		}
		passwordSalt = base64.StdEncoding.EncodeToString(saltBytes)
		plainPassword = *input.Password
		passwordHash = hashPassword(plainPassword, passwordSalt)
	} else {
		// Fetch the current password hash and salt from the database
		err := db.QueryRow(`SELECT password_hash, password_salt FROM receivers WHERE id = $1`, id).Scan(&passwordHash, &passwordSalt)
		if err != nil && err != sql.ErrNoRows {
			http.Error(w, "Failed to retrieve current password data", http.StatusInternalServerError)
			return
		}
	}

	// No longer update the IP address field

	// Fetch the existing receiver to track changes
	var originalRec Receiver
	err := db.QueryRow(`
        SELECT id, description, latitude, longitude, name, url, email, notifications
        FROM receivers WHERE id = $1
    `, id).Scan(
		&originalRec.ID,
		&originalRec.Description,
		&originalRec.Latitude,
		&originalRec.Longitude,
		&originalRec.Name,
		&originalRec.URL,
		&originalRec.Email,
		&originalRec.Notifications,
	)

	// If the receiver doesn't exist yet, that's fine - we're creating a new one
	if err != nil && err != sql.ErrNoRows {
		http.Error(w, fmt.Sprintf("Failed to fetch existing receiver: %v", err), http.StatusInternalServerError)
		return
	}

	// 3) build Receiver and validate
	rec := Receiver{
		ID:            id,
		Description:   input.Description,
		Latitude:      input.Latitude,
		Longitude:     input.Longitude,
		Name:          strings.ToUpper(input.Name),
		URL:           input.URL,
		Email:         input.Email,
		Notifications: input.Notifications,
		PasswordHash:  passwordHash,
		PasswordSalt:  passwordSalt,
	}

	// Set the password field for validation
	if input.Password != nil {
		rec.Password = *input.Password // Use the provided password
	} else {
		// Set a dummy password for validation when not changing the password
		// This is needed because validateReceiver checks the password length
		rec.Password = "dummy_password_12345" // 19 chars, within 8-20 limit
	}
	if err := validateReceiver(rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 4) UPSERT excluding ip_address
	err = db.QueryRow(`
        INSERT INTO receivers (
            id,
            description,
            latitude,
            longitude,
            name,
            url,
            email,
            notifications,
            password_hash,
            password_salt
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (id) DO UPDATE
          SET description   = EXCLUDED.description,
              latitude      = EXCLUDED.latitude,
              longitude     = EXCLUDED.longitude,
              name          = EXCLUDED.name,
              url           = EXCLUDED.url,
              email         = EXCLUDED.email,
              notifications = EXCLUDED.notifications,
              password_hash = EXCLUDED.password_hash,
              password_salt = EXCLUDED.password_salt,
              lastupdated   = NOW()
        RETURNING lastupdated
    `, rec.ID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.Email, rec.Notifications, rec.PasswordHash, rec.PasswordSalt).
		Scan(&rec.LastUpdated)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 6) send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)

	// Track what fields were changed
	changedFields := make(map[string]interface{})

	// Only track changes if the receiver existed before
	if err != sql.ErrNoRows {
		if rec.Description != originalRec.Description {
			changedFields["description"] = map[string]interface{}{
				"old": originalRec.Description,
				"new": rec.Description,
			}
		}

		if rec.Latitude != originalRec.Latitude {
			changedFields["latitude"] = map[string]interface{}{
				"old": originalRec.Latitude,
				"new": rec.Latitude,
			}
		}

		if rec.Longitude != originalRec.Longitude {
			changedFields["longitude"] = map[string]interface{}{
				"old": originalRec.Longitude,
				"new": rec.Longitude,
			}
		}

		if rec.Name != originalRec.Name {
			changedFields["name"] = map[string]interface{}{
				"old": originalRec.Name,
				"new": rec.Name,
			}
		}

		// For URL, we need to handle nil pointers
		urlChanged := false
		if (originalRec.URL == nil && rec.URL != nil) ||
			(originalRec.URL != nil && rec.URL == nil) {
			urlChanged = true
		} else if originalRec.URL != nil && rec.URL != nil && *originalRec.URL != *rec.URL {
			urlChanged = true
		}

		if urlChanged {
			var oldURL, newURL string
			if originalRec.URL != nil {
				oldURL = *originalRec.URL
			}
			if rec.URL != nil {
				newURL = *rec.URL
			}
			changedFields["url"] = map[string]interface{}{
				"old": oldURL,
				"new": newURL,
			}
		}

		if rec.Email != originalRec.Email {
			changedFields["email"] = map[string]interface{}{
				"old": originalRec.Email,
				"new": rec.Email,
			}
		}

		if rec.Notifications != originalRec.Notifications {
			changedFields["notifications"] = map[string]interface{}{
				"old": originalRec.Notifications,
				"new": rec.Notifications,
			}
		}

		if input.Password != nil {
			// Don't include the actual password values, just indicate it was changed
			changedFields["password"] = map[string]interface{}{
				"changed": true,
			}
		}

		// Only send webhook if fields were actually changed
		if len(changedFields) > 0 && settings.WebhookURL != "" {
			go notifyWebhookUpdate(rec, clientIP, changedFields, false) // Not an admin action
		}
	} else {
		// This is a new receiver being created with PUT, so notify as an add
		if settings.WebhookURL != "" {
			go notifyWebhookWithClientIP(rec, clientIP)
		}
	}
}

func handlePatchReceiver(w http.ResponseWriter, r *http.Request, id int) {
	// Check if this is an admin action based on the request path
	isAdminAction := strings.Contains(r.URL.Path, "/admin/")

	// Get the client's IP address
	clientIP := getClientIP(r)

	// 1) decode JSON body
	var patch ReceiverPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// 3) load existing record
	var rec Receiver
	err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, email, notifications,
               password_hash, password_salt
        FROM receivers WHERE id = $1
    `, id).Scan(
		&rec.ID,
		&rec.LastUpdated,
		&rec.Description,
		&rec.Latitude,
		&rec.Longitude,
		&rec.Name,
		&rec.URL,
		&rec.Email,
		&rec.Notifications,
		&rec.PasswordHash,
		&rec.PasswordSalt,
	)
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store original values for comparison
	originalRec := Receiver{
		ID:            rec.ID,
		Description:   rec.Description,
		Latitude:      rec.Latitude,
		Longitude:     rec.Longitude,
		Name:          rec.Name,
		URL:           rec.URL,
		Email:         rec.Email,
		Notifications: rec.Notifications,
	}

	// 4) apply patch
	if patch.Description != nil {
		rec.Description = *patch.Description
	}
	if patch.Latitude != nil {
		rec.Latitude = *patch.Latitude
	}
	if patch.Longitude != nil {
		rec.Longitude = *patch.Longitude
	}

	if patch.Name != nil {
		rec.Name = strings.ToUpper(*patch.Name)
	}
	if patch.URL != nil {
		rec.URL = patch.URL
	}
	if patch.Email != nil {
		rec.Email = *patch.Email
	}
	if patch.Notifications != nil {
		rec.Notifications = *patch.Notifications
	}
	if patch.Password != nil {
		// Generate a new salt and hash the new password
		saltBytes := make([]byte, 16)
		if _, err := rand.Read(saltBytes); err != nil {
			http.Error(w, "Failed to generate salt", http.StatusInternalServerError)
			return
		}
		rec.PasswordSalt = base64.StdEncoding.EncodeToString(saltBytes)
		rec.PasswordHash = hashPassword(*patch.Password, rec.PasswordSalt)
		rec.Password = *patch.Password // Store temporarily for validation
	} else {
		// Set a dummy password for validation when not changing the password
		// This is needed because validateReceiver checks the password length
		rec.Password = "dummy_password_12345" // 19 chars, within 8-20 limit
	}
	// No longer update the IP address field

	// 5) validate updated rec
	if err := validateReceiver(rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 6) perform UPDATE (excluding ip_address)
	err = db.QueryRow(`
        UPDATE receivers
           SET description   = $1,
               latitude      = $2,
               longitude     = $3,
               name          = $4,
               url           = $5,
               email         = $6,
               notifications = $7,
               password_hash = $8,
               password_salt = $9,
               lastupdated   = NOW()
         WHERE id = $10
         RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.Email, rec.Notifications,
		rec.PasswordHash, rec.PasswordSalt, rec.ID).
		Scan(&rec.LastUpdated)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 8) send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)

	// Track what fields were changed
	changedFields := make(map[string]interface{})

	if patch.Description != nil && rec.Description != originalRec.Description {
		changedFields["description"] = map[string]interface{}{
			"old": originalRec.Description,
			"new": rec.Description,
		}
	}

	if patch.Latitude != nil && rec.Latitude != originalRec.Latitude {
		changedFields["latitude"] = map[string]interface{}{
			"old": originalRec.Latitude,
			"new": rec.Latitude,
		}
	}

	if patch.Longitude != nil && rec.Longitude != originalRec.Longitude {
		changedFields["longitude"] = map[string]interface{}{
			"old": originalRec.Longitude,
			"new": rec.Longitude,
		}
	}

	if patch.Name != nil && rec.Name != originalRec.Name {
		changedFields["name"] = map[string]interface{}{
			"old": originalRec.Name,
			"new": rec.Name,
		}
	}

	// For URL, we need to handle nil pointers
	urlChanged := false
	if (originalRec.URL == nil && rec.URL != nil) ||
		(originalRec.URL != nil && rec.URL == nil) {
		urlChanged = true
	} else if originalRec.URL != nil && rec.URL != nil && *originalRec.URL != *rec.URL {
		urlChanged = true
	}

	if urlChanged {
		var oldURL, newURL string
		if originalRec.URL != nil {
			oldURL = *originalRec.URL
		}
		if rec.URL != nil {
			newURL = *rec.URL
		}
		changedFields["url"] = map[string]interface{}{
			"old": oldURL,
			"new": newURL,
		}
	}

	if patch.Email != nil && rec.Email != originalRec.Email {
		changedFields["email"] = map[string]interface{}{
			"old": originalRec.Email,
			"new": rec.Email,
		}
	}

	if patch.Notifications != nil && rec.Notifications != originalRec.Notifications {
		changedFields["notifications"] = map[string]interface{}{
			"old": originalRec.Notifications,
			"new": rec.Notifications,
		}
	}

	if patch.Password != nil {
		// Don't include the actual password values, just indicate it was changed
		changedFields["password"] = map[string]interface{}{
			"changed": true,
		}
	}

	// Only send webhook if fields were actually changed
	if len(changedFields) > 0 && settings.WebhookURL != "" {
		go notifyWebhookUpdate(rec, clientIP, changedFields, isAdminAction)
	}
}

func handleDeleteReceiver(w http.ResponseWriter, r *http.Request, id int) {
	// Check if this is an admin action based on the request path
	isAdminAction := strings.Contains(r.URL.Path, "/admin/")

	// Get the client's IP address
	clientIP := getClientIP(r)

	// Fetch the receiver details before deletion for the webhook
	var rec Receiver
	// Use a temporary string variable to hold the lastupdated text
	var lastUpdatedStr string
	err := db.QueryRow(`
        SELECT id, name, description, latitude, longitude, url, email, notifications,
               lastupdated::text
        FROM receivers WHERE id = $1`, id).
		Scan(&rec.ID, &rec.Name, &rec.Description, &rec.Latitude, &rec.Longitude,
			&rec.URL, &rec.Email, &rec.Notifications, &lastUpdatedStr)

	// Parse the lastUpdatedStr into a time.Time if the query was successful
	if err == nil {
		parsedTime, parseErr := time.Parse(time.RFC3339, lastUpdatedStr)
		if parseErr != nil {
			log.Printf("Error parsing lastupdated time: %v", parseErr)
		} else {
			rec.LastUpdated = parsedTime
		}
	}

	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("Failed to fetch receiver: %v", err), http.StatusInternalServerError)
		return
	}

	// Get UDP port for the receiver
	var udpPort sql.NullInt64
	err = db.QueryRow(`
        SELECT udp_port FROM receiver_ports WHERE receiver_id = $1
    `, id).Scan(&udpPort)

	if err == nil && udpPort.Valid {
		port := int(udpPort.Int64)
		rec.UDPPort = &port
	}

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() // Rollback if not committed

	// First, unallocate the UDP port by setting receiver_id to NULL
	_, err = tx.Exec(`
        UPDATE receiver_ports
        SET receiver_id = NULL, last_updated = NOW()
        WHERE receiver_id = $1
    `, id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to unallocate UDP port: %v", err), http.StatusInternalServerError)
		return
	}

	// Then delete the receiver
	res, err := tx.Exec(`DELETE FROM receivers WHERE id = $1`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)

	// Send webhook notification about the deletion
	if settings.WebhookURL != "" {
		go notifyWebhookDelete(rec, clientIP, isAdminAction)
	}
}

func validateReceiver(r Receiver) error {
	if len(r.Description) > 30 {
		return fmt.Errorf("description must be ≤30 characters")
	}
	if len(r.Name) > 15 {
		return fmt.Errorf("name must be ≤15 characters")
	}
	if r.Latitude < -90 || r.Latitude > 90 {
		return fmt.Errorf("latitude must be between -90 and 90")
	}
	if r.Longitude < -180 || r.Longitude > 180 {
		return fmt.Errorf("longitude must be between -180 and 180")
	}
	if r.URL != nil && *r.URL != "" {
		if _, err := url.ParseRequestURI(*r.URL); err != nil {
			return fmt.Errorf("invalid URL")
		}
	}

	// Validate email format
	if r.Email == "" {
		return fmt.Errorf("email is required")
	}
	if !isValidEmail(r.Email) {
		return fmt.Errorf("invalid email format")
	}

	// Validate password length
	if len(r.Password) < 8 {
		return fmt.Errorf("password must be at least 8 characters")
	}
	if len(r.Password) > 20 {
		return fmt.Errorf("password must be no more than 20 characters")
	}

	// Check if the name is already in use by another receiver
	var count int
	query := `SELECT COUNT(*) FROM receivers WHERE name = $1`
	args := []interface{}{r.Name}

	// If we're updating an existing receiver, exclude it from the check
	if r.ID > 0 {
		query += ` AND id != $2`
		args = append(args, r.ID)
	}

	err := db.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return fmt.Errorf("database error while checking name uniqueness: %v", err)
	}

	if count > 0 {
		return fmt.Errorf("name '%s' is already in use by another receiver", r.Name)
	}

	// Email uniqueness check removed as per requirements

	return nil
}

// isValidEmail validates email format using a simple regex pattern
func isValidEmail(email string) bool {
	// More robust email validation regex that properly handles domain parts
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9\-]+(\.[a-zA-Z0-9\-]+)*(\.[a-zA-Z]{2,})$`)
	return emailRegex.MatchString(email)
}

// adminRegeneratePasswordHandler handles POST /admin/receivers/regenerate-password/{id}
func adminRegeneratePasswordHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Extract receiver ID from URL
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid URL format", http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(parts[4])
	if err != nil {
		http.Error(w, "Invalid receiver ID", http.StatusBadRequest)
		return
	}

	// Generate a new random password
	newPassword, hashedPassword, salt, err := generateRandomPassword()
	if err != nil {
		http.Error(w, "Failed to generate password", http.StatusInternalServerError)
		return
	}

	// Fetch the current receiver to validate with the new password
	var rec Receiver
	err = db.QueryRow(`
        SELECT id, description, latitude, longitude, name, url, email, notifications
        FROM receivers WHERE id = $1
    `, id).Scan(
		&rec.ID,
		&rec.Description,
		&rec.Latitude,
		&rec.Longitude,
		&rec.Name,
		&rec.URL,
		&rec.Email,
		&rec.Notifications,
	)

	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Set the new password and validate
	rec.Password = newPassword
	if err := validateReceiver(rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update the password in the database
	var lastUpdated time.Time
	err = db.QueryRow(`
        UPDATE receivers
        SET password_hash = $1, password_salt = $2, lastupdated = NOW()
        WHERE id = $3
        RETURNING lastupdated
    `, hashedPassword, salt, id).Scan(&lastUpdated)

	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Return the new password (plain text only for display to user)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"password": newPassword,
	})
}

// handleReceiverIP endpoint removed - no longer needed with automatic collector tracking

// handleEditReceiver handles POST /editreceiver
// This is a public endpoint that allows editing an existing receiver
// It requires id and password for authentication
// Users can update all fields including their own password
func handleEditReceiver(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON request body
	var input struct {
		ID            int      `json:"id"`
		Password      string   `json:"password"`
		Description   *string  `json:"description,omitempty"`
		Latitude      *float64 `json:"latitude,omitempty"`
		Longitude     *float64 `json:"longitude,omitempty"`
		Name          *string  `json:"name,omitempty"`
		URL           *string  `json:"url,omitempty"`
		Email         *string  `json:"email,omitempty"`
		Notifications *bool    `json:"notifications,omitempty"`
		NewPassword   *string  `json:"new_password,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if input.ID <= 0 {
		http.Error(w, "id is required and must be positive", http.StatusBadRequest)
		return
	}
	if input.Password == "" {
		http.Error(w, "password is required", http.StatusBadRequest)
		return
	}

	// Ensure database connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch the current receiver to verify password and get existing values
	var rec Receiver
	err := db.QueryRow(`
        SELECT id, lastupdated, description, latitude, longitude, name, url, email, notifications, password_hash, password_salt
        FROM receivers WHERE id = $1
    `, input.ID).Scan(
		&rec.ID,
		&rec.LastUpdated,
		&rec.Description,
		&rec.Latitude,
		&rec.Longitude,
		&rec.Name,
		&rec.URL,
		&rec.Email,
		&rec.Notifications,
		&rec.PasswordHash,
		&rec.PasswordSalt,
	)
	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if password matches using the hash
	if !verifyPassword(input.Password, rec.PasswordHash, rec.PasswordSalt) {
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Store original values for comparison before applying updates
	originalRec := Receiver{
		ID:            rec.ID,
		Description:   rec.Description,
		Latitude:      rec.Latitude,
		Longitude:     rec.Longitude,
		Name:          rec.Name,
		URL:           rec.URL,
		Email:         rec.Email,
		Notifications: rec.Notifications,
	}

	// Apply updates to the receiver
	if input.Description != nil {
		rec.Description = *input.Description
	}
	if input.Latitude != nil {
		rec.Latitude = *input.Latitude
	}
	if input.Longitude != nil {
		rec.Longitude = *input.Longitude
	}
	if input.Name != nil {
		rec.Name = strings.ToUpper(*input.Name)
	}
	if input.URL != nil {
		rec.URL = input.URL
	}
	if input.Email != nil {
		rec.Email = *input.Email
	}
	if input.Notifications != nil {
		rec.Notifications = *input.Notifications
	}
	// No longer update the IP address field
	if input.NewPassword != nil {
		// Generate new salt and hash for the new password
		saltBytes := make([]byte, 16)
		_, err := rand.Read(saltBytes)
		if err != nil {
			http.Error(w, "Failed to generate salt", http.StatusInternalServerError)
			return
		}
		rec.PasswordSalt = base64.StdEncoding.EncodeToString(saltBytes)
		rec.PasswordHash = hashPassword(*input.NewPassword, rec.PasswordSalt)
	}

	// Set the password field for validation
	// This is needed because validateReceiver checks the password length
	// but we're not actually changing the password unless NewPassword is provided
	rec.Password = input.Password

	// Validate the updated receiver
	if err := validateReceiver(rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get the client's IP address for tracking
	clientIP := getClientIP(r)

	// Update the receiver in the database
	err = db.QueryRow(`
        UPDATE receivers
        SET description      = $1,
            latitude         = $2,
            longitude        = $3,
            name             = $4,
            url              = $5,
            email            = $6,
            notifications    = $7,
            password_hash    = $8,
            password_salt    = $9,
            request_ip_address = $10,
            lastupdated      = NOW()
        WHERE id = $11
        RETURNING lastupdated
    `, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.Email, rec.Notifications, rec.PasswordHash, rec.PasswordSalt, clientIP, rec.ID).
		Scan(&rec.LastUpdated)

	if err != nil {
		// Check for uniqueness violations
		if strings.Contains(err.Error(), "unique constraint") {
			if strings.Contains(err.Error(), "idx_receivers_name") {
				http.Error(w, fmt.Sprintf("name '%s' is already in use by another receiver", rec.Name), http.StatusBadRequest)
				return
			}
			if strings.Contains(err.Error(), "receivers_email_key") {
				http.Error(w, fmt.Sprintf("email '%s' is already in use by another receiver", rec.Email), http.StatusBadRequest)
				return
			}
		}
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get message count and message stats for the updated receiver
	messages, messageStats := getMessagesByPort(rec.UDPPort)
	rec.Messages = messages
	rec.MessageStats = messageStats

	// Return the updated receiver
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec)

	// Track what fields were changed
	changedFields := make(map[string]interface{})

	if rec.Description != originalRec.Description {
		changedFields["description"] = map[string]interface{}{
			"old": originalRec.Description,
			"new": rec.Description,
		}
	}

	if rec.Latitude != originalRec.Latitude {
		changedFields["latitude"] = map[string]interface{}{
			"old": originalRec.Latitude,
			"new": rec.Latitude,
		}
	}

	if rec.Longitude != originalRec.Longitude {
		changedFields["longitude"] = map[string]interface{}{
			"old": originalRec.Longitude,
			"new": rec.Longitude,
		}
	}

	if rec.Name != originalRec.Name {
		changedFields["name"] = map[string]interface{}{
			"old": originalRec.Name,
			"new": rec.Name,
		}
	}

	// For URL, we need to handle nil pointers
	urlChanged := false
	if (originalRec.URL == nil && rec.URL != nil) ||
		(originalRec.URL != nil && rec.URL == nil) {
		urlChanged = true
	} else if originalRec.URL != nil && rec.URL != nil && *originalRec.URL != *rec.URL {
		urlChanged = true
	}

	if urlChanged {
		var oldURL, newURL string
		if originalRec.URL != nil {
			oldURL = *originalRec.URL
		}
		if rec.URL != nil {
			newURL = *rec.URL
		}
		changedFields["url"] = map[string]interface{}{
			"old": oldURL,
			"new": newURL,
		}
	}

	if rec.Email != originalRec.Email {
		changedFields["email"] = map[string]interface{}{
			"old": originalRec.Email,
			"new": rec.Email,
		}
	}

	if rec.Notifications != originalRec.Notifications {
		changedFields["notifications"] = map[string]interface{}{
			"old": originalRec.Notifications,
			"new": rec.Notifications,
		}
	}

	if input.NewPassword != nil {
		// Don't include the actual password values, just indicate it was changed
		changedFields["password"] = map[string]interface{}{
			"changed": true,
		}
	}

	// Only send webhook if fields were actually changed
	if len(changedFields) > 0 && settings.WebhookURL != "" {
		go notifyWebhookUpdate(rec, clientIP, changedFields, false) // Not an admin action
	}
}

// handleAddReceiver handles POST /addreceiver
// This is a public endpoint that allows adding a new receiver
// It requires name, description, lat, long
// URL is optional, and password is automatically generated
// cleanupExpiredBlocks removes expired entries from the blocked_signup_ips table
func cleanupExpiredBlocks() {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		log.Printf("Database connection error when cleaning up expired blocks: %v", err)
		return
	}

	// Delete expired blocks
	result, err := db.Exec(`
        DELETE FROM blocked_signup_ips
        WHERE unblock_at < NOW()
    `)

	if err != nil {
		log.Printf("Failed to clean up expired IP blocks: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		log.Printf("Cleaned up %d expired IP blocks", rowsAffected)
	}
}

// cleanupOldFailedAttempts removes old entries from the failed_signup_attempts table
func cleanupOldFailedAttempts() {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		log.Printf("Database connection error when cleaning up old failed attempts: %v", err)
		return
	}

	// Delete old failed attempts
	result, err := db.Exec(`
        DELETE FROM failed_signup_attempts
        WHERE attempt_time < NOW() - INTERVAL '1 hour'
    `)

	if err != nil {
		log.Printf("Failed to clean up old failed signup attempts: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		log.Printf("Cleaned up %d old failed signup attempts", rowsAffected)
	}
}

// startCleanupRoutine starts a background goroutine that periodically cleans up
// expired blocks and old failed signup attempts
func startCleanupRoutine() {
	go func() {
		// Run cleanup immediately on startup
		cleanupExpiredBlocks()
		cleanupOldFailedAttempts()

		// Then run every 15 minutes
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cleanupExpiredBlocks()
				cleanupOldFailedAttempts()
			}
		}
	}()

	log.Printf("Started background cleanup routine for IP blocks and failed signup attempts")
}

// trackFailedSignupAttempt records a failed signup attempt in the database
func trackFailedSignupAttempt(clientIP, reason, email, name, description string, latitude, longitude float64) {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		log.Printf("Database connection error when tracking failed signup: %v", err)
		return
	}

	// Insert the failed attempt
	_, err := db.Exec(`
        INSERT INTO failed_signup_attempts (
            ip_address, attempt_time, reason, email, name, description, latitude, longitude
        ) VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7)
    `, clientIP, reason, email, name, description, latitude, longitude)

	if err != nil {
		log.Printf("Failed to record failed signup attempt: %v", err)
		return
	}

	// Clean up old failed attempts and expired blocks
	go cleanupOldFailedAttempts()
	go cleanupExpiredBlocks()

	// Check if this IP has exceeded the maximum number of failed attempts
	var count int
	err = db.QueryRow(`
        SELECT COUNT(*) FROM failed_signup_attempts
        WHERE ip_address = $1
        AND attempt_time > NOW() - INTERVAL '1 hour'
    `, clientIP).Scan(&count)

	if err != nil {
		log.Printf("Failed to count failed signup attempts: %v", err)
		return
	}

	// If the count exceeds the maximum, block the IP
	if count >= settings.MaxFailedSignupAttempts {
		// Block the IP for the configured timeout period
		timeoutDuration := time.Duration(settings.IPAddressTimeoutMinutes) * time.Minute
		unblockTime := time.Now().Add(timeoutDuration)

		// Check if this IP is already blocked
		var isAlreadyBlocked bool
		var currentAttempts int
		err = db.QueryRow(`
            SELECT EXISTS (
                SELECT 1 FROM blocked_signup_ips
                WHERE ip_address = $1
            ),
            COALESCE((SELECT attempts FROM blocked_signup_ips WHERE ip_address = $1), 0)
        `, clientIP).Scan(&isAlreadyBlocked, &currentAttempts)

		if err != nil {
			log.Printf("Failed to check if IP is already blocked: %v", err)
			isAlreadyBlocked = false // Assume not blocked if we can't check
			currentAttempts = 0
		}

		// Insert or update the blocked IP
		_, err = db.Exec(`
            INSERT INTO blocked_signup_ips (
                ip_address, blocked_at, unblock_at, reason,
                last_attempt_email, last_attempt_name, last_attempt_description,
                last_attempt_latitude, last_attempt_longitude
            )
            VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (ip_address)
            DO UPDATE SET
                blocked_at = NOW(),
                unblock_at = $2,
                reason = $3,
                attempts = blocked_signup_ips.attempts + 1,
                last_attempt_email = $4,
                last_attempt_name = $5,
                last_attempt_description = $6,
                last_attempt_latitude = $7,
                last_attempt_longitude = $8
        `, clientIP, unblockTime, "Too many failed signup attempts",
			email, name, description, latitude, longitude)

		if err != nil {
			log.Printf("Failed to block IP after failed signup attempts: %v", err)
			return
		}

		if !isAlreadyBlocked {
			// This is a new block - send an alert
			alertRec := Receiver{
				Name:             "BLOCKED_IP_FAILED_ATTEMPTS",
				Description:      fmt.Sprintf("IP: %s blocked for too many failed signup attempts", clientIP),
				RequestIPAddress: clientIP,
				CustomFields: map[string]interface{}{
					"reason":                "Too many failed signup attempts",
					"unblock_at":            unblockTime.Format(time.RFC3339),
					"attempts":              count,
					"attempted_email":       email,
					"attempted_name":        name,
					"attempted_description": description,
					"attempted_latitude":    latitude,
					"attempted_longitude":   longitude,
				},
			}

			// Send an alert about the blocked IP
			if settings.WebhookURL != "" {
				go notifyWebhookWithType(alertRec, "blocked_ip_attempt")
			}

			log.Printf("IP %s blocked for too many failed signup attempts (%d in the last hour)", clientIP, count)
		} else {
			log.Printf("IP %s remains blocked after additional failed signup attempts (total: %d in the last hour)", clientIP, count)
		}
	}
}

func handleAddReceiver(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Check if public registration is enabled
	if !settings.PublicAddReceiverEnabled {
		http.Error(w, "Public registration disabled", http.StatusForbidden)
		return
	}

	// Parse JSON request body
	var input struct {
		Name          string  `json:"name"`
		Description   string  `json:"description"`
		Latitude      float64 `json:"latitude"`
		Longitude     float64 `json:"longitude"`
		Email         string  `json:"email"`
		Notifications bool    `json:"notifications"`
		URL           *string `json:"url,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if input.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if input.Description == "" {
		http.Error(w, "description is required", http.StatusBadRequest)
		return
	}
	if input.Email == "" {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}

	// Generate a random password
	password, hashedPassword, salt, err := generateRandomPassword()
	if err != nil {
		http.Error(w, "Failed to generate password: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create receiver object
	// Get the client's IP address for verification and tracking
	clientIP := getClientIP(r)

	// Use the client IP for validation purposes
	ipToCheck := clientIP

	// Check if this client IP is in the blocked_signup_ips table
	var blockID int
	var unblockAt time.Time
	var reason string
	var attempts int
	err = db.QueryRow(`
        SELECT id, unblock_at, reason, attempts FROM blocked_signup_ips
        WHERE ip_address = $1
    `, clientIP).Scan(&blockID, &unblockAt, &reason, &attempts)

	// If we found this IP in the blocked_signup_ips table
	if err == nil {
		// Check if it's still within the block period
		if time.Now().Before(unblockAt) {
			// Check if this is the first attempt after being blocked
			var firstAttemptAfterBlock bool
			if attempts == 1 {
				firstAttemptAfterBlock = true
			}

			// IP is still blocked, increment the attempts counter and store signup details
			_, err = db.Exec(`
                UPDATE blocked_signup_ips
                SET attempts = attempts + 1,
                    last_attempt_email = $2,
                    last_attempt_name = $3,
                    last_attempt_description = $4,
                    last_attempt_latitude = $5,
                    last_attempt_longitude = $6
                WHERE id = $1
            `, blockID, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
			if err != nil {
				log.Printf("Failed to update blocked IP attempts: %v", err)
			}

			// Only send an alert if this is the first attempt after being blocked
			if firstAttemptAfterBlock && settings.WebhookURL != "" {
				// Create a receiver object with the necessary information for the alert
				alertRec := Receiver{
					Name:             "BLOCKED_IP_SIGNUP_ATTEMPT",
					Description:      fmt.Sprintf("IP: %s tried to sign up while blocked", clientIP),
					RequestIPAddress: clientIP,
					CustomFields: map[string]interface{}{
						"reason":                reason,
						"unblock_at":            unblockAt.Format(time.RFC3339),
						"attempts":              attempts + 1,
						"attempted_email":       input.Email,
						"attempted_name":        input.Name,
						"attempted_description": input.Description,
						"attempted_latitude":    input.Latitude,
						"attempted_longitude":   input.Longitude,
					},
				}
				go notifyWebhookWithType(alertRec, "blocked_ip_attempt")
				log.Printf("Sent alert for first attempt by blocked IP %s (attempt #%d)", clientIP, attempts+1)
			} else {
				log.Printf("Blocked IP %s attempted to sign up again (attempt #%d)", clientIP, attempts+1)
			}

			// Return an error to the client
			timeoutMessage := fmt.Sprintf("This IP address is temporarily blocked from creating new receivers")
			http.Error(w, timeoutMessage, http.StatusForbidden)
			return
		} else {
			// Block period has expired, remove from blocked_signup_ips table
			_, err = db.Exec(`
                DELETE FROM blocked_signup_ips
                WHERE id = $1
            `, blockID)
			if err != nil {
				log.Printf("Failed to remove expired IP block: %v", err)
			}
		}
	} else if err != sql.ErrNoRows {
		// If there was an error other than "no rows", return it
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if this client IP has already added a receiver recently
	var existingID int
	var existingLastUpdated time.Time
	err = db.QueryRow(`
        SELECT id, lastupdated FROM receivers
        WHERE request_ip_address = $1
        ORDER BY lastupdated DESC
        LIMIT 1
    `, clientIP).Scan(&existingID, &existingLastUpdated)

	// If we found a receiver with this client IP
	if err == nil {
		// Check if it was updated within the configured timeout period
		timeoutDuration := time.Duration(settings.IPAddressTimeoutMinutes) * time.Minute
		if time.Since(existingLastUpdated) < timeoutDuration {
			// Check if this IP is already blocked
			var isAlreadyBlocked bool
			var currentAttempts int
			err = db.QueryRow(`
                SELECT EXISTS (
                    SELECT 1 FROM blocked_signup_ips
                    WHERE ip_address = $1
                ),
                COALESCE((SELECT attempts FROM blocked_signup_ips WHERE ip_address = $1), 0)
            `, clientIP).Scan(&isAlreadyBlocked, &currentAttempts)

			if err != nil {
				log.Printf("Failed to check if IP is already blocked: %v", err)
				isAlreadyBlocked = false // Assume not blocked if we can't check
				currentAttempts = 0
			}

			// Add this IP to the blocked_signup_ips table
			unblockTime := time.Now().Add(timeoutDuration)
			_, err = db.Exec(`
                INSERT INTO blocked_signup_ips (
                    ip_address, blocked_at, unblock_at, reason,
                    last_attempt_email, last_attempt_name, last_attempt_description,
                    last_attempt_latitude, last_attempt_longitude
                )
                VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (ip_address)
                DO UPDATE SET
                    blocked_at = NOW(),
                    unblock_at = $2,
                    reason = $3,
                    attempts = blocked_signup_ips.attempts + 1,
                    last_attempt_email = $4,
                    last_attempt_name = $5,
                    last_attempt_description = $6,
                    last_attempt_latitude = $7,
                    last_attempt_longitude = $8
            `, clientIP, unblockTime, "Rate limit: Added receiver too recently",
				input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
			if err != nil {
				log.Printf("Failed to add IP to blocked_signup_ips table: %v", err)
			}

			// Only send an alert if this is a new block (not already blocked)
			if !isAlreadyBlocked && settings.WebhookURL != "" {
				// Create a receiver object with the necessary information for the alert
				alertRec := Receiver{
					Name:             "BLOCKED_IP_RATE_LIMIT",
					Description:      fmt.Sprintf("IP: %s blocked for adding receivers too frequently", clientIP),
					RequestIPAddress: clientIP,
					CustomFields: map[string]interface{}{
						"reason":                 "Rate limit: Added receiver too recently",
						"unblock_at":             unblockTime.Format(time.RFC3339),
						"attempts":               1,
						"attempted_email":        input.Email,
						"attempted_name":         input.Name,
						"attempted_description":  input.Description,
						"attempted_latitude":     input.Latitude,
						"attempted_longitude":    input.Longitude,
						"previous_receiver_id":   existingID,
						"previous_receiver_time": existingLastUpdated.Format(time.RFC3339),
					},
				}

				// Send an alert about the blocked IP
				go notifyWebhookWithType(alertRec, "blocked_ip_attempt")

				log.Printf("IP %s blocked for adding receivers too frequently", clientIP)
			} else {
				log.Printf("IP %s remains blocked after attempting to add another receiver too soon", clientIP)
			}

			timeoutMessage := fmt.Sprintf("Client IP address has already added a receiver recently")
			http.Error(w, timeoutMessage, http.StatusForbidden)
			return
		}
	} else if err != sql.ErrNoRows {
		// If there was an error other than "no rows", return it
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rec := Receiver{
		Description:   input.Description,
		Latitude:      input.Latitude,
		Longitude:     input.Longitude,
		Name:          strings.ToUpper(input.Name),
		URL:           input.URL,
		Email:         input.Email,
		Notifications: input.Notifications,
		Password:      password,
		PasswordHash:  hashedPassword,
		PasswordSalt:  salt,
	}

	// Validate the receiver
	if err := validateReceiver(rec); err != nil {
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, err.Error(), input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Ensure database connection
	if err := ensureConnection(); err != nil {
		errorMsg := "Database connection error: " + err.Error()
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	// Fetch the UDP listen port from the ingester settings
	ingestSettings, err := fetchIngestSettings()
	if err != nil {
		errorMsg := fmt.Sprintf("Failed to fetch ingester settings: %v", err)
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	// The primary UDP port is the UDP listen port from the ingester settings
	primaryUDPPort := ingestSettings.UDPListenPort

	// Check if the IP address is sending data to the primary UDP port and not to any non-primary port with a receiver
	portMetricsMutex.RLock()
	sendingToPrimaryPort := false
	nonPrimaryPorts := make([]int, 0)

	if portMap, ok := portMetricsMap[ipToCheck]; ok {
		for port, metric := range portMap {
			// Skip ports with no messages
			if metric.MessageCount <= 0 {
				continue
			}

			if port == primaryUDPPort {
				sendingToPrimaryPort = true
			} else {
				nonPrimaryPorts = append(nonPrimaryPorts, port)
			}
		}
	}
	portMetricsMutex.RUnlock()

	// Check if the IP address is sending data to the primary UDP port
	if !sendingToPrimaryPort {
		errorMsg := fmt.Sprintf("The IP address '%s' is not sending data to the primary UDP port (%d)", ipToCheck, primaryUDPPort)
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusBadRequest)
		return
	}

	// Check if the IP address is sending data to any non-primary port that has a receiver assigned
	if len(nonPrimaryPorts) > 0 {
		// Check each non-primary port to see if it has a receiver assigned
		for _, port := range nonPrimaryPorts {
			var receiverID sql.NullInt64
			err := db.QueryRow(`
                SELECT receiver_id FROM receiver_ports
                WHERE udp_port = $1 AND receiver_id IS NOT NULL
            `, port).Scan(&receiverID)

			// If we found a receiver assigned to this port, block the new receiver
			if err == nil && receiverID.Valid {
				// Check if this IP is already blocked
				var isAlreadyBlocked bool
				var currentAttempts int
				err = db.QueryRow(`
                    SELECT EXISTS (
                        SELECT 1 FROM blocked_signup_ips
                        WHERE ip_address = $1
                    ),
                    COALESCE((SELECT attempts FROM blocked_signup_ips WHERE ip_address = $1), 0)
                `, clientIP).Scan(&isAlreadyBlocked, &currentAttempts)

				if err != nil {
					log.Printf("Failed to check if IP is already blocked: %v", err)
					isAlreadyBlocked = false // Assume not blocked if we can't check
					currentAttempts = 0
				}

				// Add this IP to the blocked_signup_ips table
				timeoutDuration := time.Duration(settings.IPAddressTimeoutMinutes) * time.Minute
				unblockTime := time.Now().Add(timeoutDuration)
				_, err = db.Exec(`
                    INSERT INTO blocked_signup_ips (
                        ip_address, blocked_at, unblock_at, reason,
                        last_attempt_email, last_attempt_name, last_attempt_description,
                        last_attempt_latitude, last_attempt_longitude
                    )
                    VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (ip_address)
                    DO UPDATE SET
                        blocked_at = NOW(),
                        unblock_at = $2,
                        reason = $3,
                        attempts = blocked_signup_ips.attempts + 1,
                        last_attempt_email = $4,
                        last_attempt_name = $5,
                        last_attempt_description = $6,
                        last_attempt_latitude = $7,
                        last_attempt_longitude = $8
                `, clientIP, unblockTime, "Already sending data to a non-primary port with an assigned receiver",
					input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
				if err != nil {
					log.Printf("Failed to add IP to blocked_signup_ips table: %v", err)
				}

				// Only send an alert if this is a new block (not already blocked)
				if !isAlreadyBlocked && settings.WebhookURL != "" {
					// Create a receiver object with the necessary information for the alert
					alertRec := Receiver{
						Name:             "BLOCKED_IP_PORT_CONFLICT",
						Description:      fmt.Sprintf("IP: %s blocked for port conflict", clientIP),
						RequestIPAddress: clientIP,
						CustomFields: map[string]interface{}{
							"reason":                  "Already sending data to a non-primary port with an assigned receiver",
							"unblock_at":              unblockTime.Format(time.RFC3339),
							"attempts":                1,
							"attempted_email":         input.Email,
							"attempted_name":          input.Name,
							"attempted_description":   input.Description,
							"attempted_latitude":      input.Latitude,
							"attempted_longitude":     input.Longitude,
							"conflicting_port":        port,
							"conflicting_receiver_id": receiverID.Int64,
						},
					}

					// Send an alert about the blocked IP
					go notifyWebhookWithType(alertRec, "blocked_ip_attempt")

					log.Printf("IP %s blocked for sending data to port %d which already has receiver ID %d",
						clientIP, port, receiverID.Int64)
				} else {
					log.Printf("IP %s remains blocked after attempting to sign up with port conflict", clientIP)
				}

				errorMsg := fmt.Sprintf("The IP address '%s' is already sending data to a non-primary port with an assigned receiver", ipToCheck)
				// We don't need to track this as a failed attempt since it's already being added to blocked_signup_ips
				http.Error(w, errorMsg, http.StatusForbidden)
				return
			}
		}
	}

	// Start a transaction to ensure atomicity
	tx, err := db.Begin()
	if err != nil {
		errorMsg := "Failed to start transaction: " + err.Error()
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	// Lock the receivers table to prevent concurrent inserts
	_, err = tx.Exec(`LOCK TABLE receivers IN EXCLUSIVE MODE`)
	if err != nil {
		tx.Rollback()
		errorMsg := "Failed to lock table: " + err.Error()
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	// Get the current value of the sequence
	var currSeqVal int
	err = tx.QueryRow(`SELECT last_value FROM receivers_id_seq`).Scan(&currSeqVal)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get sequence value: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Find the maximum ID ever used (including deleted receivers)
	var maxID int
	err = tx.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM receivers`).Scan(&maxID)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get max ID: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If the max ID is greater than the current sequence value, update the sequence
	if maxID >= currSeqVal {
		_, err = tx.Exec(`SELECT setval('receivers_id_seq', $1)`, maxID+1)
		if err != nil {
			tx.Rollback()
			http.Error(w, "Failed to update sequence: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Get a new ID from the sequence (which is now guaranteed to be higher than any ID ever used)
	var newID int
	err = tx.QueryRow(`SELECT nextval('receivers_id_seq')`).Scan(&newID)
	if err != nil {
		tx.Rollback()
		http.Error(w, "Failed to get new ID: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert the new receiver with the explicit ID and client IP
	err = tx.QueryRow(`
        INSERT INTO receivers (
            id,
            description,
            latitude,
            longitude,
            name,
            url,
            email,
            notifications,
            password_hash,
            password_salt,
            request_ip_address
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        RETURNING id, lastupdated`,
		newID, rec.Description, rec.Latitude, rec.Longitude, rec.Name, rec.URL, rec.Email, rec.Notifications, rec.PasswordHash, rec.PasswordSalt, clientIP).
		Scan(&rec.ID, &rec.LastUpdated)

	// Store the request IP address in the receiver object for the webhook
	rec.RequestIPAddress = clientIP

	if err != nil {
		tx.Rollback() // Rollback the transaction on error

		// Check for uniqueness violations
		if strings.Contains(err.Error(), "unique constraint") {
			if strings.Contains(err.Error(), "idx_receivers_name") {
				errorMsg := fmt.Sprintf("name '%s' is already in use by another receiver", rec.Name)
				// Track the failed signup attempt
				go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
				http.Error(w, errorMsg, http.StatusBadRequest)
				return
			}
			if strings.Contains(err.Error(), "receivers_email_key") {
				errorMsg := fmt.Sprintf("email '%s' is already in use by another receiver", rec.Email)
				// Track the failed signup attempt
				go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
				http.Error(w, errorMsg, http.StatusBadRequest)
				return
			}
		}
		errorMsg := "Database error: " + err.Error()
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	// Allocate a UDP port for this receiver within the transaction
	udpPort, err := allocatePortTx(tx, rec.ID)
	if err != nil {
		tx.Rollback() // Rollback the transaction on error

		if err.Error() == "no available ports" {
			log.Printf("ERROR: No available UDP ports found for receiver ID %d", rec.ID)
			errorMsg := "Failed to create receiver: No available UDP ports"
			// Track the failed signup attempt
			go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
			http.Error(w, errorMsg, http.StatusServiceUnavailable)
		} else {
			log.Printf("ERROR: Failed to allocate UDP port for receiver ID %d: %v", rec.ID, err)
			errorMsg := fmt.Sprintf("Failed to create receiver: UDP port allocation error: %v", err)
			// Track the failed signup attempt
			go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
			http.Error(w, errorMsg, http.StatusInternalServerError)
		}
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		log.Printf("ERROR: Failed to commit transaction for receiver ID %d: %v", rec.ID, err)
		errorMsg := "Failed to create receiver: Transaction commit error"
		// Track the failed signup attempt
		go trackFailedSignupAttempt(clientIP, errorMsg, input.Email, input.Name, input.Description, input.Latitude, input.Longitude)
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return
	}

	rec.UDPPort = &udpPort

	// Create a response object that includes the password
	type AddReceiverResponse struct {
		ID            int       `json:"id"`
		LastUpdated   time.Time `json:"lastupdated"`
		Description   string    `json:"description"`
		Latitude      float64   `json:"latitude"`
		Longitude     float64   `json:"longitude"`
		Name          string    `json:"name"`
		URL           *string   `json:"url,omitempty"`
		Email         string    `json:"email"`
		Notifications bool      `json:"notifications"`
		Password      string    `json:"password"` // Include password in response
		UDPPort       *int      `json:"udp_port,omitempty"`
	}

	// Create the response with the password included
	response := AddReceiverResponse{
		ID:            rec.ID,
		LastUpdated:   rec.LastUpdated,
		Description:   rec.Description,
		Latitude:      rec.Latitude,
		Longitude:     rec.Longitude,
		Name:          rec.Name,
		URL:           rec.URL,
		Email:         rec.Email,
		Notifications: rec.Notifications,
		Password:      rec.Password, // Include the plain text password
		UDPPort:       rec.UDPPort,
	}

	// Return the response object with the password included
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
	if settings.WebhookURL != "" {
		go notifyWebhookWithClientIP(rec, clientIP)
	}

	// Log initial OFFLINE event for the new receiver
	// It will be marked as ONLINE when we get a lastseen time for it
	if err := logReceiverEvent(rec.ID, ReceiverOffline); err != nil {
		log.Printf("Error logging initial OFFLINE event for new receiver %d: %v", rec.ID, err)
	} else {
		log.Printf("Logged initial OFFLINE event for new receiver %d", rec.ID)
	}
}

// handleDeleteReceiverPublic handles POST /deletereceiver
// This is a public endpoint that allows deleting an existing receiver
// It requires id and password for authentication
func handleDeleteReceiverPublic(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get the client's IP address
	clientIP := getClientIP(r)

	// Parse JSON request body
	var input struct {
		ID       int    `json:"id"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if input.ID <= 0 {
		http.Error(w, "id is required and must be positive", http.StatusBadRequest)
		return
	}
	if input.Password == "" {
		http.Error(w, "password is required", http.StatusBadRequest)
		return
	}

	// Ensure database connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch the current receiver to verify password and for the webhook
	var rec Receiver
	var passwordHash, passwordSalt string
	// Use a temporary string variable to hold the lastupdated text
	var lastUpdatedStr string
	err := db.QueryRow(`
        SELECT id, name, description, latitude, longitude, url, email, notifications,
               password_hash, password_salt, lastupdated::text
        FROM receivers WHERE id = $1`, input.ID).
		Scan(&rec.ID, &rec.Name, &rec.Description, &rec.Latitude, &rec.Longitude,
			&rec.URL, &rec.Email, &rec.Notifications, &passwordHash, &passwordSalt, &lastUpdatedStr)

	// Parse the lastUpdatedStr into a time.Time if the query was successful
	if err == nil {
		parsedTime, parseErr := time.Parse(time.RFC3339, lastUpdatedStr)
		if parseErr != nil {
			log.Printf("Error parsing lastupdated time: %v", parseErr)
		} else {
			rec.LastUpdated = parsedTime
		}
	}
	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get UDP port for the receiver
	var udpPort sql.NullInt64
	err = db.QueryRow(`
        SELECT udp_port FROM receiver_ports WHERE receiver_id = $1
    `, input.ID).Scan(&udpPort)

	if err == nil && udpPort.Valid {
		port := int(udpPort.Int64)
		rec.UDPPort = &port
	}

	// Check if password matches using the hash
	if !verifyPassword(input.Password, passwordHash, passwordSalt) {
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() // Rollback if not committed

	// First, unallocate the UDP port by setting receiver_id to NULL
	_, err = tx.Exec(`
        UPDATE receiver_ports
        SET receiver_id = NULL, last_updated = NOW()
        WHERE receiver_id = $1
    `, input.ID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to unallocate UDP port: %v", err), http.StatusInternalServerError)
		return
	}

	// Then delete the receiver
	res, err := tx.Exec(`DELETE FROM receivers WHERE id = $1`, input.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit transaction: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Receiver deleted successfully",
	})

	// Send webhook notification about the deletion (not an admin action)
	if settings.WebhookURL != "" {
		go notifyWebhookDelete(rec, clientIP, false) // Public endpoint, not an admin action
	}
}

// generateResetToken generates a secure random token for password reset
func generateResetToken() (string, error) {
	// Generate 32 random bytes (256 bits)
	tokenBytes := make([]byte, 32)
	_, err := rand.Read(tokenBytes)
	if err != nil {
		return "", err
	}

	// Encode as base64 for URL safety
	token := base64.URLEncoding.EncodeToString(tokenBytes)
	return token, nil
}

// handlePasswordReset handles POST /password-reset
// This endpoint handles both requesting a password reset and resetting the password with a token
func handlePasswordReset(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON request body
	var requestBody map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Determine if this is a reset request or a password reset
	if email, ok := requestBody["email"].(string); ok && email != "" {
		// Check if ID is also provided for reset request
		var receiverID int
		if idFloat, ok := requestBody["id"].(float64); ok {
			receiverID = int(idFloat)
		} else {
			http.Error(w, "Both ID and email are required for password reset", http.StatusBadRequest)
			return
		}

		// This is a request for a password reset
		handlePasswordResetRequest(w, email, receiverID)
		return
	} else if token, ok := requestBody["token"].(string); ok && token != "" {
		// This is a password reset with token
		newPassword, ok := requestBody["new_password"].(string)
		if !ok || newPassword == "" {
			http.Error(w, "New password is required", http.StatusBadRequest)
			return
		}
		handlePasswordResetWithToken(w, token, newPassword)
		return
	} else {
		// Invalid request
		http.Error(w, "Either id+email or token+new_password is required", http.StatusBadRequest)
		return
	}
}

// handlePasswordResetRequest handles the first part of the password reset process
// It generates a token and sends an email with a reset link
// Now requires both ID and email for better security
func handlePasswordResetRequest(w http.ResponseWriter, email string, receiverID int) {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Find receiver by both ID and email
	var receiverName string
	err := db.QueryRow(`
        SELECT name FROM receivers WHERE id = $1 AND email = $2
    `, receiverID, email).Scan(&receiverName)

	// Always return success even if receiver not found to prevent enumeration attacks
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "If your account details are correct, you will receive a password reset link",
		})
		return
	} else if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if a reset has been requested in the last hour
	var lastResetTime time.Time
	err = db.QueryRow(`
        SELECT created_at FROM password_reset_tokens
        WHERE receiver_id = $1
        ORDER BY created_at DESC
        LIMIT 1
    `, receiverID).Scan(&lastResetTime)

	if err == nil && time.Since(lastResetTime) < time.Hour {
		// A reset was requested less than an hour ago, but don't reveal this
		// Return success to prevent timing attacks
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "If your account details are correct, you will receive a password reset link",
		})
		return
	}

	// Generate a secure token
	token, err := generateResetToken()
	if err != nil {
		http.Error(w, "Failed to generate reset token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Calculate expiration time (24 hours from now)
	expiresAt := time.Now().Add(24 * time.Hour)

	// Delete any existing tokens for this receiver
	_, err = db.Exec(`
        DELETE FROM password_reset_tokens WHERE receiver_id = $1
    `, receiverID)
	if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Store the token in the database
	_, err = db.Exec(`
        INSERT INTO password_reset_tokens (token, receiver_id, expires_at)
        VALUES ($1, $2, $3)
    `, token, receiverID, expiresAt)
	if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Send email with reset link via webhook
	if settings.WebhookURL != "" {
		// Create a custom alert envelope with reset token
		envelope := map[string]interface{}{
			"alert_type": "password_reset",
			"receiver": map[string]interface{}{
				"id":          receiverID,
				"name":        receiverName,
				"description": "Password Reset",
			},
			"custom": map[string]interface{}{
				"reset_token": token,
				"email":       email,
				"id":          receiverID, // Include ID explicitly in custom fields
			},
		}

		// Send the webhook
		payload, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("Failed to marshal password reset envelope: %v", err)
			http.Error(w, "Failed to send password reset email", http.StatusInternalServerError)
			return
		}

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Post(settings.WebhookURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("POST webhook error for password reset: %v", err)
			http.Error(w, "Failed to send password reset email", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("Non-2xx status from webhook for password reset: %s", resp.Status)
			http.Error(w, "Failed to send password reset email", http.StatusInternalServerError)
			return
		}
	}

	// Return success response - same message whether account exists or not
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "If your account details are correct, you will receive a password reset link",
	})
}

// handlePasswordResetWithToken handles the second part of the password reset process
// It verifies the token and updates the password
func handlePasswordResetWithToken(w http.ResponseWriter, token, newPassword string) {
	// Ensure database connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "Database connection error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Validate password length
	if len(newPassword) < 8 {
		http.Error(w, "Password must be at least 8 characters", http.StatusBadRequest)
		return
	}
	if len(newPassword) > 20 {
		http.Error(w, "Password must be no more than 20 characters", http.StatusBadRequest)
		return
	}

	// Find the token in the database
	var receiverID int
	var expiresAt time.Time
	var used bool
	err := db.QueryRow(`
        SELECT receiver_id, expires_at, used FROM password_reset_tokens WHERE token = $1
    `, token).Scan(&receiverID, &expiresAt, &used)

	if err == sql.ErrNoRows {
		http.Error(w, "Invalid or expired token", http.StatusBadRequest)
		return
	} else if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if token is expired
	if time.Now().After(expiresAt) {
		http.Error(w, "Token has expired", http.StatusBadRequest)
		return
	}

	// Check if token has already been used
	if used {
		http.Error(w, "Token has already been used", http.StatusBadRequest)
		return
	}

	// Get receiver information - we don't need to verify email here since tokens are already tied to receiver ID
	var rec Receiver
	err = db.QueryRow(`
        SELECT id, name, email FROM receivers WHERE id = $1
    `, receiverID).Scan(&rec.ID, &rec.Name, &rec.Email)

	if err == sql.ErrNoRows {
		http.Error(w, "Receiver not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Generate a new salt and hash the password
	saltBytes := make([]byte, 16)
	_, err = rand.Read(saltBytes)
	if err != nil {
		http.Error(w, "Failed to generate salt", http.StatusInternalServerError)
		return
	}
	salt := base64.StdEncoding.EncodeToString(saltBytes)
	hash := hashPassword(newPassword, salt)

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Failed to start transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() // Rollback if not committed

	// Update the password
	_, err = tx.Exec(`
        UPDATE receivers
        SET password_hash = $1, password_salt = $2, lastupdated = NOW()
        WHERE id = $3
    `, hash, salt, receiverID)

	if err != nil {
		http.Error(w, "Failed to update password: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Mark the token as used
	_, err = tx.Exec(`
        UPDATE password_reset_tokens
        SET used = true
        WHERE token = $1
    `, token)

	if err != nil {
		http.Error(w, "Failed to mark token as used: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Delete ALL tokens for this receiver to completely reset the rate limiting
	// This allows the user to request another password reset immediately if needed
	_, err = tx.Exec(`
        DELETE FROM password_reset_tokens
        WHERE receiver_id = $1
    `, receiverID)

	if err != nil {
		http.Error(w, "Failed to reset password reset limit: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success response
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Password has been reset successfully",
	})
}

// handleGetUDPPort handles requests to get a receiver's UDP port by authenticating with ID and password
func handleGetUDPPort(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var input struct {
		ID       int    `json:"id"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Validate input
	if input.ID <= 0 {
		http.Error(w, "Invalid receiver ID", http.StatusBadRequest)
		return
	}

	if input.Password == "" {
		http.Error(w, "Password is required", http.StatusBadRequest)
		return
	}

	// Ensure database connection
	if err := ensureConnection(); err != nil {
		http.Error(w, "Database connection error", http.StatusInternalServerError)
		return
	}

	// Query the database for the receiver's password hash and salt
	var passwordHash, passwordSalt string
	var udpPort sql.NullInt64

	err := db.QueryRow(`
        SELECT r.password_hash, r.password_salt, rp.udp_port
        FROM receivers r
        LEFT JOIN receiver_ports rp ON r.id = rp.receiver_id
        WHERE r.id = $1
    `, input.ID).Scan(&passwordHash, &passwordSalt, &udpPort)

	if err == sql.ErrNoRows {
		// Don't reveal whether the receiver exists or not
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	} else if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Verify the password
	if !verifyPassword(input.Password, passwordHash, passwordSalt) {
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	// Check if UDP port is assigned
	if !udpPort.Valid {
		http.Error(w, "No UDP port assigned to this receiver", http.StatusNotFound)
		return
	}

	// Return the UDP port in JSON format
	response := struct {
		ID      int `json:"id"`
		UDPPort int `json:"udp_port"`
	}{
		ID:      input.ID,
		UDPPort: int(udpPort.Int64),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// generateResetToken generates a secure random token for password reset
