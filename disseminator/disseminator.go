package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lib/pq"
	"github.com/zishang520/engine.io-client-go/transports"
	"github.com/zishang520/engine.io/v2/types"
	clientSocket "github.com/zishang520/socket.io-client-go/socket"
	serverSocket "github.com/zishang520/socket.io/v2/socket"
)

type filterKey struct {
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Radius     float64 `json:"radius"`
	MaxResults int     `json:"maxResults"`
	MaxAge     int     `json:"maxAge"`
	MinSpeed   float64 `json:"minSpeed"`
	UserID     int64   `json:"userID"`
	Types      string  `json:"types"`
	TypeGroups string  `json:"typeGroups"`
	Classes    string  `json:"classes"`
	ReceiverID int64   `json:"receiverID"`
}

type FilterParams struct {
	Latitude     float64
	Longitude    float64
	Radius       float64
	MaxResults   int
	MaxAge       int
	MinSpeed     float64
	UpdatePeriod int
	UserID       int64
	LastUpdated  time.Time
	Types        string
	TypeGroups   string
	Classes      string
	ReceiverID   int64
}

type Settings struct {
	IngestHost               string `json:"ingester_host"`
	IngestPort               int    `json:"ingester_port"`
	ListenPort               int    `json:"listen_port"`
	Debug                    bool   `json:"debug"`
	PollInterval             int    `json:"poll_interval"`
	ReceiversBaseURL         string `json:"receivers_base_url"`
	MetricsBaseURL           string `json:"metrics_base_url"`
	StatisticsBaseURL        string `json:"statistics_base_url"`
	RedisHost                string `json:"redis_host"`
	RedisPort                int    `json:"redis_port"`
	CacheTime                int    `json:"cache_time"`
	PublicMQTTWebsocketsHost string `json:"public_mqtt_websockets_host"`
	PublicMQTTPort           int    `json:"public_mqtt_port"`
	PublicMQTTTopic          string `json:"public_mqtt_topic"`
}

type ClientDatabaseSettings struct {
	DbHost string `json:"db_host"`
	DbPort int    `json:"db_port"`
	DbUser string `json:"db_user"`
	DbPass string `json:"db_pass"`
	DbName string `json:"db_name"`
}

type Client struct {
	Description string `json:"description"`
	Ip          string `json:"ip"`
	Port        int    `json:"port"`
	Shards      []int  `json:"shards"`
}

var conf *Settings

var (
	redisClient *redis.Client
	redisCtx    = context.Background()
)

// typeGroupsMap maps group IDs to their corresponding vessel type values
var typeGroupsMap = map[int][]int{
	0:  {0},
	1:  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 38, 39},
	2:  {20, 21, 22, 23, 24, 25, 26, 27, 28, 29},
	3:  {30},
	4:  {31},
	5:  {32},
	6:  {33},
	7:  {34},
	8:  {35},
	9:  {36},
	10: {37},
	11: {40, 41, 42, 43, 44, 45, 46, 47, 48, 49},
	12: {50},
	13: {51},
	14: {52},
	15: {53},
	16: {54},
	17: {55},
	18: {56, 57},
	19: {58},
	20: {59},
	21: {60, 61, 62, 63, 64, 65, 66, 67, 68, 69},
	22: {70, 71, 72, 73, 74, 75, 76, 77, 78, 79},
	23: {80, 81, 82, 83, 84, 85, 86, 87, 88, 89},
	24: {90, 91, 92, 93, 94, 95, 96, 97, 98, 99},
}

var clientConnections map[string]*ClientConnection
var clientConnectionsMu sync.RWMutex
var streamShards int // Global variable to store the total number of shards

var ongoingBysourceRequests = make(map[string]chan map[string]interface{})

var (
	clientMetricsRequests   = make(map[string]map[string]string)
	clientMetricsRequestsMu sync.Mutex
)

var (
	clientSubscriptionsMu sync.RWMutex
	wsHandlersMu          sync.RWMutex
)

var (
	lastReqMu   sync.Mutex
	lastReqTime = make(map[serverSocket.SocketId]time.Time)
	minInterval = 250 * time.Millisecond
)

type ClientConnection struct {
	Db         *sql.DB
	DbHost     string
	DbPort     int
	DbUser     string
	DbPass     string
	DbName     string
	Shards     []int
	WSHost     string
	WSPort     int
	wsClient   *clientSocket.Socket
	wsClientMu sync.Mutex
}

var ioServer *serverSocket.Server
var connectedClients = make(map[serverSocket.SocketId]*serverSocket.Socket)
var connectedClientsMu sync.RWMutex
var clientSummaryFilters = make(map[serverSocket.SocketId]FilterParams)
var clientSummaryMu sync.RWMutex
var clientSubscriptions = make(map[serverSocket.SocketId]map[string]struct{})
var wsHandlersRegistered = make(map[*clientSocket.Socket]bool)

var (
	clientMetricsBysourceTickers = make(map[string]*time.Ticker)                      // Map to track tickers for 'metrics/bysource' by id/ipaddress
	cachedMetricsData            = make(map[string]map[string]interface{})            // Cache to store response for id/ipaddress
	ongoingRequests              = make(map[string]*sync.WaitGroup)                   // Track ongoing requests for each id/ipaddress
	activeClients                = make(map[string]map[*serverSocket.Socket]struct{}) // Track active clients for each id/ipaddress
	tickerMu                     sync.Mutex                                           // Mutex to protect the map when accessing tickers
	clientLastQueryValue         = make(map[*serverSocket.Socket]string)
)

// AIS spec: TrueHeading == 511 means “not available”
const NoTrueHeading = 511.0

func shardForUser(userID string) int {
	h := fnv.New32a()
	h.Write([]byte(userID))
	return int(h.Sum32()) % streamShards
}

func keyForFilter(p FilterParams) string {
	k := filterKey{
		Latitude:   p.Latitude,
		Longitude:  p.Longitude,
		Radius:     p.Radius,
		MaxResults: p.MaxResults,
		MaxAge:     p.MaxAge,
		MinSpeed:   p.MinSpeed,
		UserID:     p.UserID,
		Types:      p.Types,
		TypeGroups: p.TypeGroups,
		Classes:    p.Classes,
		ReceiverID: p.ReceiverID,
	}
	raw, _ := json.Marshal(k)
	h := fnv.New64a()
	h.Write(raw)
	return fmt.Sprintf("summary:%x", h.Sum64())
}

// getTypesFromGroups converts a comma-separated list of group IDs to a list of individual type values
// Invalid group IDs are logged and ignored
func getTypesFromGroups(groupsStr string) []int {
	if groupsStr == "" {
		return nil
	}

	groupsList := strings.Split(groupsStr, ",")
	var allTypes []int
	var invalidGroups []string

	for _, g := range groupsList {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}

		groupID, err := strconv.Atoi(g)
		if err != nil {
			invalidGroups = append(invalidGroups, g)
			continue
		}

		if types, exists := typeGroupsMap[groupID]; exists {
			allTypes = append(allTypes, types...)
		} else {
			invalidGroups = append(invalidGroups, g)
		}
	}

	if len(invalidGroups) > 0 {
		log.Printf("Warning: Ignoring invalid type groups: %s", strings.Join(invalidGroups, ", "))
	}

	return allTypes
}

func getSummaryJSON(p FilterParams, ttl int) ([]byte, error) {
	key := keyForFilter(p)

	// 1) Try to GET the raw JSON from Redis
	if blob, err := redisClient.Get(redisCtx, key).Bytes(); err == nil {
		return blob, nil
	}

	// 2) Cache miss → compute the summary as Go map
	summary, err := getSummaryResults(
		p.Latitude, p.Longitude, p.Radius,
		p.MaxResults, p.MaxAge, p.MinSpeed, p.UserID, p.Types, p.TypeGroups, p.Classes,
		p.ReceiverID,
	)
	if err != nil {
		return nil, err
	}

	// 3) Marshal it exactly once
	blob, err := json.Marshal(summary)
	if err != nil {
		return nil, err
	}

	// 4) Store asynchronously
	go func() {
		_ = redisClient.
			Set(redisCtx, key, blob, time.Duration(ttl)*time.Second).
			Err()
	}()

	return blob, nil
}

// Returns true if at least one connected client still has userID in its set
func anySubscriberExists(userID string) bool {
	clientSubscriptionsMu.RLock()
	defer clientSubscriptionsMu.RUnlock()

	for _, subs := range clientSubscriptions {
		if _, still := subs[userID]; still {
			return true
		}
	}
	return false
}

func getSummaryWithRedisCache(p FilterParams, ttl int) (map[string]interface{}, error) {
	key := keyForFilter(p)

	// Try GET
	if blob, err := redisClient.Get(redisCtx, key).Bytes(); err == nil {
		var cached map[string]interface{}
		if err := json.Unmarshal(blob, &cached); err == nil {
			return cached, nil
		}
		log.Printf("⚠️ Redis unmarshal failed for %s: %v", key, err)
	}

	// Miss → compute
	summary, err := getSummaryResults(
		p.Latitude, p.Longitude, p.Radius,
		p.MaxResults, p.MaxAge, p.MinSpeed, p.UserID, p.Types, p.TypeGroups, p.Classes,
		p.ReceiverID,
	)
	if err != nil {
		return nil, err
	}

	// Async SET so we don’t block
	go func() {
		blob, _ := json.Marshal(summary)
		if err := redisClient.Set(redisCtx, key, blob, time.Duration(ttl)*time.Second).Err(); err != nil {
			log.Printf("⚠️ Redis SET failed for %s: %v", key, err)
		}
	}()

	return summary, nil
}

func (cc *ClientConnection) ensureDB() error {
	if err := cc.Db.Ping(); err != nil {
		// try reconnect
		newDb, err2 := connectToDatabase(&ClientDatabaseSettings{
			DbHost: cc.DbHost, DbPort: cc.DbPort,
			DbUser: cc.DbUser, DbPass: cc.DbPass,
			DbName: cc.DbName,
		})
		if err2 != nil {
			return fmt.Errorf("reconnect failed: %v (ping err: %v)", err2, err)
		}
		cc.Db.Close()
		cc.Db = newDb
	}
	return nil
}

func handleMetricsBysource(client *serverSocket.Socket, data map[string]interface{}) {
	var queryParam, queryValue string

	// Check if the 'id' or 'ipaddress'/'ip_address' parameter is present
	if id, ok := data["id"].(float64); ok {
		idInt := int(id)
		log.Printf("Client %s requested by id: %d", client.Id(), idInt)
		queryParam = "id"
		queryValue = strconv.Itoa(idInt)
	} else if ip, ok := data["ipaddress"].(string); ok {
		log.Printf("Client %s requested by ipaddress: %s", client.Id(), ip)
		queryParam = "ipaddress"
		queryValue = ip

		// Add debug logging to verify the IP address being used
		log.Printf("Using IP address %s for metrics/bysource WebSocket request", ip)
	} else if ip, ok := data["ip_address"].(string); ok {
		// Handle the case where the client sends 'ip_address' instead of 'ipaddress'
		log.Printf("Client %s requested by ip_address: %s", client.Id(), ip)
		queryParam = "ipaddress" // Convert to the format expected by the HTTP endpoint
		queryValue = ip

		log.Printf("Using IP address %s for metrics/bysource WebSocket request (from ip_address field)", ip)
	} else {
		log.Printf("Invalid or missing ipaddress/id in data: %v", data)
		return
	}

	log.Printf("Received %s: %s from client %s", queryParam, queryValue, client.Id())

	// Locking for active clients to avoid race conditions
	tickerMu.Lock()

	// Track the client's old query value (id or ipaddress) if it exists
	oldQueryValue := clientLastQueryValue[client]

	// If the client was previously requesting a different query value, clean it up
	if oldQueryValue != "" && oldQueryValue != queryValue {
		// Remove client from the old query value's activeClients map
		if currentClients, exists := activeClients[oldQueryValue]; exists {
			delete(currentClients, client)
			// If no clients remain for the old query value, stop the ticker
			if len(currentClients) == 0 {
				if ticker, exists := clientMetricsBysourceTickers[oldQueryValue]; exists {
					ticker.Stop()
					delete(clientMetricsBysourceTickers, oldQueryValue)
					delete(cachedMetricsData, oldQueryValue)
					log.Printf("Stopped ticker for query value %s as no clients remain.", oldQueryValue)
				}
			}
		}
	}

	// Register the client to receive updates for the new query value (ipaddress/id)
	if activeClients[queryValue] == nil {
		activeClients[queryValue] = make(map[*serverSocket.Socket]struct{})
	}
	activeClients[queryValue][client] = struct{}{}

	// Store the current query value for this client (id or ipaddress)
	clientLastQueryValue[client] = queryValue

	// Check if there is already an ongoing ticker for this queryValue
	if _, exists := clientMetricsBysourceTickers[queryValue]; !exists {
		// No ticker exists for this queryValue, so create a new one
		wg := &sync.WaitGroup{}
		ongoingRequests[queryValue] = wg
		wg.Add(1)

		metricsBysourceTicker := time.NewTicker(1 * time.Second)
		clientMetricsBysourceTickers[queryValue] = metricsBysourceTicker
		tickerMu.Unlock()

		go func() {
			defer wg.Done()

			for range metricsBysourceTicker.C {
				url := fmt.Sprintf("%s/metrics/bysource?%s=%s", conf.MetricsBaseURL, queryParam, queryValue)
				resp, err := http.Get(url)
				if err != nil {
					log.Printf("Error making GET request to %s: %v", url, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					log.Printf("Received non-OK response from external API: %d", resp.StatusCode)
					return
				}

				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Printf("Error reading response body: %v", err)
					return
				}

				var apiResponse map[string]interface{}
				if err := json.Unmarshal(body, &apiResponse); err != nil {
					log.Printf("Error unmarshalling response body: %v", err)
					return
				}

				// Cache the response
				cachedMetricsData[queryValue] = apiResponse

				// Emit the response to all connected clients for this queryValue
				tickerMu.Lock()
				for client := range activeClients[queryValue] {
					if err := client.Emit("metrics/bysource", apiResponse); err != nil {
						log.Printf("Error emitting response to client %s: %v", client.Id(), err)
					}
				}
				tickerMu.Unlock()
			}
		}()
	} else {
		tickerMu.Unlock()
	}

	// Handle client disconnection
	client.On("disconnect", func(...any) {
		tickerMu.Lock()
		if clients, exists := activeClients[queryValue]; exists {
			delete(clients, client)
			if len(clients) == 0 {
				if ticker, exists := clientMetricsBysourceTickers[queryValue]; exists {
					ticker.Stop()
					delete(clientMetricsBysourceTickers, queryValue)
					delete(cachedMetricsData, queryValue)
				}
			}
		}
		// Also remove the client's last query value record (id or ipaddress)
		delete(clientLastQueryValue, client)
		tickerMu.Unlock()
	})
}

// QueryDatabaseForUser looks up which collector shard handles the given userID,
// ensures its DB connection is alive, and runs the provided SQL query.
func QueryDatabaseForUser(userID, query string) (*sql.Rows, error) {
	shardID := shardForUser(userID)
	var clientDescription string

	// Find the ClientConnection for this shard
	clientConnectionsMu.RLock()
	var cc *ClientConnection
	for _, conn := range clientConnections {
		for _, shard := range conn.Shards {
			if shard == shardID {
				clientDescription = conn.DbHost
				cc = conn
				break
			}
		}
		if cc != nil {
			break
		}
	}
	clientConnectionsMu.RUnlock()

	if cc == nil {
		return nil, fmt.Errorf("no client found handling shard %d", shardID)
	}

	// Ensure the DB is alive (auto-reconnect if needed)
	if err := cc.ensureDB(); err != nil {
		return nil, fmt.Errorf("failed to ensure DB for client %s: %v", clientDescription, err)
	}

	// Execute the query
	rows, err := cc.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query on database for client %s: %v", clientDescription, err)
	}

	return rows, nil
}

func formatMsg(parts ...any) string {
	b, _ := json.Marshal(parts)
	return string(b)
}

// QueryDatabasesForAllShards queries all shard databases with the same query,
// and uses ensureDB() to auto-reconnect closed or stale connections.
func QueryDatabasesForAllShards(query string) (map[string][]map[string]interface{}, error) {
	results := make(map[string][]map[string]interface{})

	clientConnectionsMu.RLock()
	conns := make([]*ClientConnection, 0, len(clientConnections))
	for _, cc := range clientConnections {
		conns = append(conns, cc)
	}
	clientConnectionsMu.RUnlock()

	for _, cc := range conns {
		// Ensure the DB is alive (auto-reconnect if needed)
		if err := cc.ensureDB(); err != nil {
			return nil, fmt.Errorf("failed to ensure DB for client %s: %v", cc.DbHost, err)
		}

		rows, err := cc.Db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query on database for client %s: %v", cc.DbHost, err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for client %s: %v", cc.DbHost, err)
		}

		for rows.Next() {
			vals := make([]interface{}, len(columns))
			for i := range vals {
				vals[i] = new(interface{})
			}
			if err := rows.Scan(vals...); err != nil {
				log.Printf("Error scanning result for shard %s: %v", cc.DbHost, err)
				continue
			}

			rowMap := make(map[string]interface{}, len(columns))
			for i, col := range columns {
				rowMap[col] = *(vals[i].(*interface{}))
			}

			results[cc.DbHost] = append(results[cc.DbHost], rowMap)
		}
	}

	return results, nil
}

// Helper function to handle receiver IDs
func handleReceiverIDs(receiverIDs []int64, receiverID int64, summary map[string]interface{}, userIDStr string) {
	// If a specific receiver ID was requested, only include that one
	if receiverID > 0 {
		// Check if the requested receiver is in the list
		for _, id := range receiverIDs {
			if id == receiverID {
				summary["CurrentReceivers"] = []int64{receiverID}
				// log.Printf("Adding CurrentReceivers with single ID %d for vessel %s", receiverID, userIDStr)
				break
			}
		}
	} else {
		// Otherwise include all receivers
		summary["CurrentReceivers"] = receiverIDs
		// log.Printf("Adding CurrentReceivers with %d IDs for vessel %s", len(receiverIDs), userIDStr)
	}
}

func getSummaryResults(lat, lon, radius float64, limit int, maxAge int, minSpeed float64, userid int64, types string, typeGroups string, classes string, receiverID int64) (map[string]interface{}, error) {
	query := `
	      SELECT s.user_id
	           , s.packet
	           , s.timestamp
	           , s.ais_class
	           , s.count
	           , s.name
	           , vr.receiver_ids
	      FROM state s
	      LEFT JOIN vessel_receivers vr ON s.user_id = vr.user_id
	  `

	whereAdded := false

	// If UserID is provided, filter by user_id (UserID)
	if userid > 0 {
		query += fmt.Sprintf(" WHERE s.user_id = %d", userid)
		whereAdded = true
	}

	// If ReceiverID is provided, filter using vessel_receivers table
	if receiverID > 0 {ID)
		if whereAdded {
			query += fmt.Sprintf(" AND s.user_id IN (SELECT user_id FROM vessel_receivers WHERE %d = ANY(receiver_ids))", receiverID)
		} else {
			query += fmt.Sprintf(" WHERE s.user_id IN (SELECT user_id FROM vessel_receivers WHERE %d = ANY(receiver_ids))", receiverID)
			whereAdded = true
		}
	}

	// If lat, lon, and radius are specified, filter by distance
	if lat != 0 && lon != 0 && radius != 0 {
		if whereAdded {
			query += fmt.Sprintf(`
                AND ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (s.packet->>'Longitude')::float,
                        (s.packet->>'Latitude')::float
                    ), 4326),
                    ST_SetSRID(ST_Point(%f, %f), 4326)
                ) <= %f
            `, lon, lat, radius)
		} else {
			query += fmt.Sprintf(`
                WHERE ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (s.packet->>'Longitude')::float,
                        (s.packet->>'Latitude')::float
                    ), 4326),
                    ST_SetSRID(ST_Point(%f, %f), 4326)
                ) <= %f
            `, lon, lat, radius)
			whereAdded = true
		}
	}

	if maxAge > 0 {
		cutoff := time.Now().Add(-time.Duration(maxAge) * time.Hour).UTC().Format(time.RFC3339Nano)
		if whereAdded {
			query += fmt.Sprintf(" AND s.timestamp >= '%s'", cutoff)
		} else {
			query += fmt.Sprintf(" WHERE s.timestamp >= '%s'", cutoff)
			whereAdded = true
		}
	}

	if minSpeed > 0 {
		if whereAdded {
			query += fmt.Sprintf(
				" AND (s.packet->>'Sog')::float >= %f",
				minSpeed,
			)
		} else {
			query += fmt.Sprintf(
				" WHERE (s.packet->>'Sog')::float >= %f",
				minSpeed,
			)
			whereAdded = true
		}
	}

	// Process individual types and type groups
	// When both filters are provided, they are combined with OR logic
	// (a vessel will match if it has any of the types specified in either filter)
	var typeConditions []string

	// Filter by Type if specified
	if types != "" {
		typesList := strings.Split(types, ",")
		for _, t := range typesList {
			t = strings.TrimSpace(t)
			if t != "" {
				typeVal, err := strconv.ParseFloat(t, 64)
				if err == nil {
					typeConditions = append(typeConditions, fmt.Sprintf("(s.packet->>'Type')::float = %f", typeVal))
				} else {
					log.Printf("Warning: Ignoring invalid type value: %s", t)
				}
			}
		}
	}

	// Filter by TypeGroups if specified
	if typeGroups != "" {
		groupTypes := getTypesFromGroups(typeGroups)
		for _, typeVal := range groupTypes {
			// Special handling for type 0: include vessels with no Type field
			if typeVal == 0 {
				typeConditions = append(typeConditions, fmt.Sprintf("((s.packet->>'Type')::float = 0 OR s.packet->>'Type' IS NULL)"))
			} else {
				typeConditions = append(typeConditions, fmt.Sprintf("(s.packet->>'Type')::float = %d", typeVal))
			}
		}
	}

	// Apply type conditions if any exist
	if len(typeConditions) > 0 {
		typeFilter := strings.Join(typeConditions, " OR ")

		// Special handling for AtoN, BASE, and SAR vessels
		// These should always be included regardless of type filtering
		specialClassesFilter := "s.ais_class IN ('AtoN', 'BASE', 'SAR')"

		// Combine with OR logic to include both vessels matching type conditions
		// AND special class vessels regardless of their type
		combinedFilter := fmt.Sprintf("(%s) OR (%s)", typeFilter, specialClassesFilter)

		if whereAdded {
			query += fmt.Sprintf(" AND (%s)", combinedFilter)
		} else {
			query += fmt.Sprintf(" WHERE (%s)", combinedFilter)
			whereAdded = true
		}
	}

	// Filter by AIS Class if specified
	if classes != "" {
		var classConditions []string
		classesList := strings.Split(classes, ",")

		for _, c := range classesList {
			c = strings.TrimSpace(c)
			if c != "" {
				// Valid values are A, B, BASE, SAR and AtoN
				switch c {
				case "A", "B", "BASE", "SAR", "AtoN":
					classConditions = append(classConditions, fmt.Sprintf("s.ais_class = '%s'", c))
				default:
					log.Printf("Warning: Ignoring invalid AIS class value: %s", c)
				}
			}
		}

		if len(classConditions) > 0 {
			classFilter := strings.Join(classConditions, " OR ")
			if whereAdded {
				query += fmt.Sprintf(" AND (%s)", classFilter)
			} else {
				query += fmt.Sprintf(" WHERE (%s)", classFilter)
				whereAdded = true
			}
		}
	}

	// Finalizing the query
	query += " ORDER BY s.timestamp ASC"

	// Apply LIMIT only if it's greater than 0
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	// Query the database
	results, err := QueryDatabasesForAllShards(query)
	if err != nil {
		return nil, fmt.Errorf("Error querying database: %v", err)
	}

	// Process the results and create the summary
	summarizedResults := make(map[string]interface{})
	for _, shardData := range results {
		for _, row := range shardData {
			userID, ok := row["user_id"]
			if !ok {
				continue
			}

			var userIDStr string
			switch v := userID.(type) {
			case int:
				userIDStr = fmt.Sprintf("%d", v)
			case int64:
				userIDStr = fmt.Sprintf("%d", v)
			default:
				continue
			}

			packetData, ok := row["packet"].([]byte)
			if !ok {
				continue
			}

			packetStr := string(packetData)
			var packetMap map[string]interface{}
			if err := json.Unmarshal([]byte(packetStr), &packetMap); err != nil {
				continue
			}

			summary := make(map[string]interface{})
			summary["UserID"] = getFieldFloat(packetMap, "UserID")
			summary["CallSign"] = getFieldString(packetMap, "CallSign")
			summary["Cog"] = getFieldFloat(packetMap, "Cog")
			summary["Destination"] = getFieldString(packetMap, "Destination")
			summary["Dimension"] = getFieldJSON(packetMap, "Dimension")
			summary["MaximumStaticDraught"] = getNullableFloat(packetMap, "MaximumStaticDraught")
			summary["NavigationalStatus"] = getNullableFloat(packetMap, "NavigationalStatus")
			summary["Sog"] = getFieldFloat(packetMap, "Sog")
			summary["Type"] = getFieldFloat(packetMap, "Type")

			name := getFieldString(packetMap, "Name")
			ext := getFieldString(packetMap, "NameExtension")
			if ext != "" {
				name = name + ext
			}
			summary["Name"] = name

			// pull out the raw floats
			lat := getFieldFloat(packetMap, "Latitude")
			lon := getFieldFloat(packetMap, "Longitude")
			trueheading := getFieldFloat(packetMap, "TrueHeading")

			// garbage sentinel, skip this row entirely
			if isNull(lat) || isNull(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
				continue
			}

			// only then insert them into summary
			summary["Latitude"] = lat
			summary["Longitude"] = lon

			if trueheading != NoTrueHeading {
				summary["TrueHeading"] = trueheading
			}

			if aisClass, ok := row["ais_class"].(string); ok {
				summary["AISClass"] = aisClass

			}
			if timestamp, ok := row["timestamp"].(time.Time); ok {
				summary["LastUpdated"] = timestamp.UTC().Format(time.RFC3339Nano)
			}

			if count, ok := row["count"].(int64); ok {
				summary["NumMessages"] = count
			}

			// override Name from DB only if the name column is non-NULL and non-empty
			if rawNameVal, exists := row["name"]; exists && rawNameVal != nil {
				var nameStr string
				switch v := rawNameVal.(type) {
				case string:
					nameStr = v
				default:
					log.Printf("debug: unexpected type for DB name column: %T (value=%#v)", rawNameVal, rawNameVal)
				}
				if nameStr != "" {
					summary["Name"] = nameStr
				}
			}
			// default Name to AISClass + " Class" if empty
			if nameVal, ok := summary["Name"].(string); !ok || nameVal == "" {
				if classVal, ok := summary["AISClass"].(string); ok {
					summary["Name"] = fmt.Sprintf("%s (%s)", classVal, userIDStr)
				}
			}

			// Add CurrentReceivers field from vessel_receivers table
			if receiverIDsRaw, ok := row["receiver_ids"]; ok {
				// Try to handle different types that might come from the database
				switch receiverIDs := receiverIDsRaw.(type) {
				case []int64:
					// Direct array of int64
					handleReceiverIDs(receiverIDs, receiverID, summary, userIDStr)
				case pq.Int64Array:
					// PostgreSQL int64 array
					handleReceiverIDs([]int64(receiverIDs), receiverID, summary, userIDStr)
				case []uint8:
					// Raw binary data - try to parse as PostgreSQL array format
					// PostgreSQL array format is typically: {1,2,3}
					arrayStr := string(receiverIDs)
					if len(arrayStr) > 2 && arrayStr[0] == '{' && arrayStr[len(arrayStr)-1] == '}' {
						// Remove the braces and split by comma
						numStrs := strings.Split(arrayStr[1:len(arrayStr)-1], ",")
						var ids []int64
						for _, numStr := range numStrs {
							if id, err := strconv.ParseInt(numStr, 10, 64); err == nil {
								ids = append(ids, id)
							}
						}
						handleReceiverIDs(ids, receiverID, summary, userIDStr)
					} else {
						log.Printf("Unexpected array format for vessel %s: %s", userIDStr, arrayStr)
					}
				default:
					// log.Printf("Unexpected type for receiver_ids for vessel %s: %T", userIDStr, receiverIDsRaw)
				}
			} else {
				log.Printf("No receiver_ids found for vessel %s", userIDStr)
			}

			summarizedResults[userIDStr] = summary
		}
	}

	return summarizedResults, nil
}

func getSummaryHistoryResults(lat, lon, radius float64, limit int, minSpeed float64, userid int64, types string, typeGroups string, classes string, receiverID int64, fromTime, toTime time.Time) (map[string]interface{}, error) {
	// Step 1: Find messages within the time period and area, and get the one closest to fromTime for each user_id
	query := fmt.Sprintf(`
	       WITH ranked_messages AS (
	           SELECT
	               user_id,
	               packet,
	               timestamp,
	               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - '%s'::timestamp)))) as rn
	           FROM messages
	           WHERE timestamp BETWEEN '%s' AND '%s'
	           AND message_id IN (1,2,3,9,18,19)
	           AND (receiver_id_duplicated IS NULL)
    `, fromTime.Format(time.RFC3339Nano), fromTime.Format(time.RFC3339Nano), toTime.Format(time.RFC3339Nano))

	// If lat, lon, and radius are specified, filter by distance
	if lat != 0 && lon != 0 && radius != 0 {
		query += fmt.Sprintf(`
            AND ST_DistanceSphere(
                ST_SetSRID(ST_Point(
                    (packet->>'Longitude')::float,
                    (packet->>'Latitude')::float
                ), 4326),
                ST_SetSRID(ST_Point(%f, %f), 4326)
            ) <= %f
        `, lon, lat, radius)
	}

	// If UserID is provided, filter by user_id
	if userid > 0 {
		query += fmt.Sprintf(" AND user_id = '%d'", userid)
	}

	// If ReceiverID is provided, filter directly on receiver_id column
	if receiverID > 0 {
		log.Printf("History: Filtering by receiver ID %d", receiverID)
		query += fmt.Sprintf(" AND receiver_id = %d", receiverID)
	}

	// If minSpeed is provided, filter by speed
	if minSpeed > 0 {
		query += fmt.Sprintf(" AND (packet->>'Sog')::float >= %f", minSpeed)
	}

	// Process individual types and type groups
	var typeConditions []string

	// Filter by Type if specified
	if types != "" {
		typesList := strings.Split(types, ",")
		for _, t := range typesList {
			t = strings.TrimSpace(t)
			if t != "" {
				typeVal, err := strconv.ParseFloat(t, 64)
				if err == nil {
					typeConditions = append(typeConditions, fmt.Sprintf("(packet->>'Type')::float = %f", typeVal))
				} else {
					log.Printf("Warning: Ignoring invalid type value: %s", t)
				}
			}
		}
	}

	// Filter by TypeGroups if specified
	if typeGroups != "" {
		groupTypes := getTypesFromGroups(typeGroups)
		for _, typeVal := range groupTypes {
			typeConditions = append(typeConditions, fmt.Sprintf("(packet->>'Type')::float = %d", typeVal))
		}
	}

	// Apply type conditions if any exist
	if len(typeConditions) > 0 {
		typeFilter := strings.Join(typeConditions, " OR ")
		query += fmt.Sprintf(" AND (%s)", typeFilter)
	}

	// Filter by AIS Class if specified
	if classes != "" {
		classConditions := []string{}
		classesList := strings.Split(classes, ",")
		for _, c := range classesList {
			c = strings.TrimSpace(c)
			if c != "" {
				// We'll need to join with the state table to filter by ais_class
				// This will be handled in the state query after the initial results
				classConditions = append(classConditions, fmt.Sprintf("'%s'", c))
			}
		}

		if len(classConditions) > 0 {
			// Note: We don't filter here because ais_class is in the state table
			// The filtering will be done when querying the state table
		}
	}

	// Close the CTE and select only the closest message for each user_id
	query += `
        )
        SELECT user_id, packet, timestamp
        FROM ranked_messages
        WHERE rn = 1
    `

	// Apply LIMIT only if it's greater than 0
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	// Log the final query when filtering by receiver ID
	if receiverID > 0 {
		log.Printf("Final SQL history query with receiver filter: %s", query)
	}

	// Execute the query on all shards
	summarizedResults := make(map[string]interface{})

	clientConnectionsMu.RLock()
	conns := make([]*ClientConnection, 0, len(clientConnections))
	for _, cc := range clientConnections {
		conns = append(conns, cc)
	}
	clientConnectionsMu.RUnlock()

	for _, cc := range conns {
		// Ensure the DB is alive
		if err := cc.ensureDB(); err != nil {
			log.Printf("Failed to ensure DB for client %s: %v", cc.DbHost, err)
			continue
		}

		// Execute the query
		rows, err := cc.Db.Query(query)
		if err != nil {
			log.Printf("Failed to execute query on database for client %s: %v", cc.DbHost, err)
			continue
		}

		// Process the results
		for rows.Next() {
			var (
				userID     string
				packetData []byte
				timestamp  time.Time
			)

			if err := rows.Scan(&userID, &packetData, &timestamp); err != nil {
				log.Printf("Error scanning message row: %v", err)
				continue
			}

			// Parse the packet JSON from the message (closest to fromTime)
			var messagePacket map[string]interface{}
			if err := json.Unmarshal(packetData, &messagePacket); err != nil {
				log.Printf("Error unmarshalling message packet: %v", err)
				continue
			}

			// Get position data from the message
			lat := getFieldFloat(messagePacket, "Latitude")
			lon := getFieldFloat(messagePacket, "Longitude")

			// Skip invalid positions
			if isNull(lat) || isNull(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
				continue
			}

			// Now get additional vessel data from the state table and count positions
			stateQuery := fmt.Sprintf(`
                SELECT
                    s.packet,
                    s.ais_class,
                    s.count,
                    s.name,
                    (SELECT COUNT(*) FROM messages
                     WHERE user_id = '%s'
                     AND timestamp BETWEEN '%s' AND '%s'
                     AND message_id IN (1,2,3,9,18,19)
                     AND (receiver_id_duplicated IS NULL)) as position_count
                FROM state s
                WHERE s.user_id = '%s'
            `, userID, fromTime.Format(time.RFC3339Nano), toTime.Format(time.RFC3339Nano), userID)

			// Filter by AIS Class if specified
			if classes != "" {
				classesList := strings.Split(classes, ",")
				classConditions := []string{}

				for _, c := range classesList {
					c = strings.TrimSpace(c)
					if c != "" {
						classConditions = append(classConditions, fmt.Sprintf("s.ais_class = '%s'", c))
					}
				}

				if len(classConditions) > 0 {
					classFilter := strings.Join(classConditions, " OR ")
					stateQuery += fmt.Sprintf(" AND (%s)", classFilter)
				}
			}

			stateRows, err := QueryDatabaseForUser(userID, stateQuery)
			if err != nil {
				log.Printf("Error querying state for user %s: %v", userID, err)
				continue
			}

			if !stateRows.Next() {
				stateRows.Close()
				continue
			}

			var (
				statePacketData []byte
				aisClass        string
				count           int64
				name            sql.NullString
				positionCount   int64
			)

			if err := stateRows.Scan(&statePacketData, &aisClass, &count, &name, &positionCount); err != nil {
				log.Printf("Error scanning state row: %v", err)
				stateRows.Close()
				continue
			}
			stateRows.Close()

			// Parse the packet JSON from the state table
			var statePacket map[string]interface{}
			if err := json.Unmarshal(statePacketData, &statePacket); err != nil {
				log.Printf("Error unmarshalling state packet: %v", err)
				continue
			}

			// Create the summary for this vessel
			summary := make(map[string]interface{})

			// Use position data and navigational status from the message (closest to fromTime)
			summary["Latitude"] = lat
			summary["Longitude"] = lon
			summary["Cog"] = getFieldFloat(messagePacket, "Cog")
			summary["Sog"] = getFieldFloat(messagePacket, "Sog")
			summary["NavigationalStatus"] = getNullableFloat(messagePacket, "NavigationalStatus")

			trueheading := getFieldFloat(messagePacket, "TrueHeading")
			if trueheading != NoTrueHeading {
				summary["TrueHeading"] = trueheading
			}

			// Use other vessel data from the state table
			summary["UserID"] = getFieldFloat(statePacket, "UserID")
			summary["CallSign"] = getFieldString(statePacket, "CallSign")
			summary["Destination"] = getFieldString(statePacket, "Destination")
			summary["Dimension"] = getFieldJSON(statePacket, "Dimension")
			summary["MaximumStaticDraught"] = getNullableFloat(statePacket, "MaximumStaticDraught")
			summary["Type"] = getFieldFloat(statePacket, "Type")

			vesselName := getFieldString(statePacket, "Name")
			ext := getFieldString(statePacket, "NameExtension")
			if ext != "" {
				vesselName = vesselName + ext
			}
			summary["Name"] = vesselName

			summary["AISClass"] = aisClass
			summary["LastUpdated"] = timestamp.UTC().Format(time.RFC3339Nano)
			summary["NumMessages"] = count
			summary["Positions"] = positionCount

			// Override Name from DB if available
			if name.Valid && name.String != "" {
				summary["Name"] = name.String
			}

			// Default Name to AISClass + " Class" if empty
			if nameVal, ok := summary["Name"].(string); !ok || nameVal == "" {
				if classVal, ok := summary["AISClass"].(string); ok {
					summary["Name"] = fmt.Sprintf("%s (%s)", classVal, userID)
				}
			}

			// Add CurrentReceivers field from vessel_receivers table
			// Query vessel_receivers table to get receiver_ids for this user_id
			receiverQuery := fmt.Sprintf(`
				SELECT receiver_ids FROM vessel_receivers WHERE user_id = '%s'
			`, userID)

			receiverRows, err := QueryDatabaseForUser(userID, receiverQuery)
			if err == nil {
				if receiverRows.Next() {
					var receiverIDsRaw interface{}
					if err := receiverRows.Scan(&receiverIDsRaw); err == nil {
						// Try to handle different types that might come from the database
						switch receiverIDs := receiverIDsRaw.(type) {
						case []int64:
							// Direct array of int64
							handleReceiverIDs(receiverIDs, receiverID, summary, userID)
						case pq.Int64Array:
							// PostgreSQL int64 array
							handleReceiverIDs([]int64(receiverIDs), receiverID, summary, userID)
						case []uint8:
							// Raw binary data - try to parse as PostgreSQL array format
							// PostgreSQL array format is typically: {1,2,3}
							arrayStr := string(receiverIDs)
							if len(arrayStr) > 2 && arrayStr[0] == '{' && arrayStr[len(arrayStr)-1] == '}' {
								// Remove the braces and split by comma
								numStrs := strings.Split(arrayStr[1:len(arrayStr)-1], ",")
								var ids []int64
								for _, numStr := range numStrs {
									if id, err := strconv.ParseInt(numStr, 10, 64); err == nil {
										ids = append(ids, id)
									}
								}
								handleReceiverIDs(ids, receiverID, summary, userID)
							} else {
								log.Printf("History: Unexpected array format for vessel %s: %s", userID, arrayStr)
							}
						default:
							log.Printf("History: Unexpected type for receiver_ids for vessel %s: %T", userID, receiverIDsRaw)
						}
					} else {
						log.Printf("History: Error scanning receiver_ids for vessel %s: %v", userID, err)
					}
				}
				receiverRows.Close()
			}

			summarizedResults[userID] = summary
		}
		rows.Close()
	}

	return summarizedResults, nil
}

func getHistoryResults(userID string, hours int) ([]map[string]interface{}, error) {
	// 1) Compute cutoff timestamp
	pastTime := time.Now().
		Add(-time.Duration(hours) * time.Hour).
		UTC().
		Format(time.RFC3339)

	// 2) History from messages for selected movement types
	historyQuery := fmt.Sprintf(`
SELECT
	   timestamp,
	   (packet->>'Latitude')::double precision AS latitude,
	   (packet->>'Longitude')::double precision AS longitude,
	   (packet->>'Sog')::double precision AS sog,
	   (packet->>'Cog')::double precision AS cog,
	   (packet->>'TrueHeading')::double precision AS trueHeading
FROM messages
WHERE user_id   = '%[1]s'
	 AND timestamp >= '%[2]s'
	 AND message_id IN (1,2,3,9,18,19)
	 AND packet->>'Latitude'  IS NOT NULL
	 AND packet->>'Longitude' IS NOT NULL
	 AND (receiver_id_duplicated IS NULL)
ORDER BY timestamp
LIMIT 2000;
`, userID, pastTime)

	rows, err := QueryDatabaseForUser(userID, historyQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying history for user %s: %v", userID, err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var (
			ts  time.Time
			lat sql.NullFloat64
			lon sql.NullFloat64
			sog sql.NullFloat64
			cog sql.NullFloat64
			th  sql.NullFloat64
		)
		if err := rows.Scan(&ts, &lat, &lon, &sog, &cog, &th); err != nil {
			return nil, fmt.Errorf("error scanning history row: %v", err)
		}

		entry := map[string]interface{}{
			"timestamp": ts,
			"latitude":  lat.Float64,
			"longitude": lon.Float64,
		}
		if sog.Valid {
			entry["sog"] = sog.Float64
		}
		if cog.Valid {
			entry["cog"] = cog.Float64
		}
		if th.Valid && th.Float64 != NoTrueHeading {
			entry["trueHeading"] = th.Float64
		}

		results = append(results, entry)
	}

	// 3) Now fetch the absolute latest from the state table
	stateQuery := fmt.Sprintf(`
SELECT
    timestamp,
    (packet->>'Latitude')::double precision AS latitude,
    (packet->>'Longitude')::double precision AS longitude,
    (packet->>'Sog')::double precision AS sog,
    (packet->>'Cog')::double precision AS cog,
    (packet->>'TrueHeading')::double precision AS trueHeading
FROM state
WHERE user_id = '%s'
LIMIT 1;
`, userID)

	stRows, err := QueryDatabaseForUser(userID, stateQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying state for user %s: %v", userID, err)
	}
	defer stRows.Close()

	if stRows.Next() {
		var (
			ts2  time.Time
			lat2 sql.NullFloat64
			lon2 sql.NullFloat64
			sog2 sql.NullFloat64
			cog2 sql.NullFloat64
			th2  sql.NullFloat64
		)
		if err := stRows.Scan(&ts2, &lat2, &lon2, &sog2, &cog2, &th2); err != nil {
			return nil, fmt.Errorf("error scanning state row: %v", err)
		}

		stateEntry := map[string]interface{}{
			"timestamp": ts2,
			"latitude":  lat2.Float64,
			"longitude": lon2.Float64,
		}
		if sog2.Valid {
			stateEntry["sog"] = sog2.Float64
		}
		if cog2.Valid {
			stateEntry["cog"] = cog2.Float64
		}
		if th2.Valid && th2.Float64 != NoTrueHeading {
			stateEntry["trueHeading"] = th2.Float64
		}

		// 4) Avoid duplicate timestamps: append only if it's not already in results
		if len(results) == 0 || !results[len(results)-1]["timestamp"].(time.Time).Equal(ts2) {
			results = append(results, stateEntry)
		}
	}

	return results, nil
}

// Helper function to check if a value is "NULL" (in Go's float64 representation)
func isNull(value float64) bool {
	return value == 0.0 // Assuming 0.0 means NULL in your database; adjust if needed
}

func (cc *ClientConnection) getWSClient() (*clientSocket.Socket, error) {
	cc.wsClientMu.Lock()
	defer cc.wsClientMu.Unlock()

	// If we already have one, but it's not connected anymore, throw it away
	if cc.wsClient != nil && cc.wsClient.Connected() {
		return cc.wsClient, nil
	}
	// (Optionally) clean up old one
	cc.wsClient = nil

	// … now dial a fresh one …
	opts := clientSocket.DefaultOptions()
	opts.SetTransports(types.NewSet(transports.Polling, transports.WebSocket))
	manager := clientSocket.NewManager(
		fmt.Sprintf("http://%s:%d", cc.WSHost, cc.WSPort),
		opts,
	)
	cli := manager.Socket("/", opts)

	// as soon as we see a disconnect, zero it out so next getWSClient() redials
	cli.On("disconnect", func(...any) {
		cc.wsClientMu.Lock()
		defer cc.wsClientMu.Unlock()
		cc.wsClient = nil
	})

	// propagate manager errors, and also clear wsClient on fatal error
	manager.On("error", func(errs ...any) {
		log.Printf("collector WS manager error: %v", errs)
		cc.wsClientMu.Lock()
		cc.wsClient = nil
		cc.wsClientMu.Unlock()
	})

	cc.wsClient = cli
	return cli, nil
}

func latestMessagesHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// 1) Parse & validate UserID
	userIDStr := q.Get("UserID")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil || userID <= 0 {
		http.Error(w, "Invalid or missing UserID", http.StatusBadRequest)
		return
	}

	// 2) Optional MessageID(s), DAC, FI, ReceiverID
	var (
		messageIDs []int64
		dac, fi    int
		receiverID int64
	)
	if v := q.Get("MessageID"); v != "" {
		// Support comma-separated list of message IDs
		idStrings := strings.Split(v, ",")
		for _, idStr := range idStrings {
			idStr = strings.TrimSpace(idStr)
			if idStr == "" {
				continue
			}
			if x, err := strconv.ParseInt(idStr, 10, 64); err == nil && x >= 0 {
				messageIDs = append(messageIDs, x)
			} else {
				http.Error(w, fmt.Sprintf("Invalid MessageID: %s", idStr), http.StatusBadRequest)
				return
			}
		}
	}
	if v := q.Get("DAC"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x >= 0 {
			dac = x
		} else {
			http.Error(w, "Invalid DAC", http.StatusBadRequest)
			return
		}
	}
	if v := q.Get("FI"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x >= 0 {
			fi = x
		} else {
			http.Error(w, "Invalid FI", http.StatusBadRequest)
			return
		}
	}

	// Parse optional ReceiverID
	if v := q.Get("ReceiverID"); v != "" {
		if x, err := strconv.ParseInt(v, 10, 64); err == nil && x >= 0 {
			receiverID = x
		} else {
			http.Error(w, "Invalid ReceiverID", http.StatusBadRequest)
			return
		}
	}

	// 3) limit (for non-aggregate)
	limit := 1
	if v := q.Get("limit"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			if x > 10000 {
				limit = 10000
			} else {
				limit = x
			}
		} else {
			http.Error(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
	}

	// 4) Parse explicit start/end (RFC3339Nano)
	now := time.Now().UTC()
	var startTime, endTime time.Time
	if s := q.Get("start"); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			startTime = t.UTC()
		} else {
			http.Error(w, "Invalid start timestamp", http.StatusBadRequest)
			return
		}
	}
	if e := q.Get("end"); e != "" {
		if t, err := time.Parse(time.RFC3339Nano, e); err == nil {
			endTime = t.UTC()
		} else {
			http.Error(w, "Invalid end timestamp", http.StatusBadRequest)
			return
		}
	}
	if endTime.IsZero() {
		endTime = now
	}

	// 5) Auto‐granularity (or explicit range param)
	rangeParam := strings.ToLower(q.Get("range"))
	var truncUnit string
	if rangeParam == "" && !startTime.IsZero() && !endTime.IsZero() {
		// auto‐pick by window length
		diff := endTime.Sub(startTime)
		switch {
		case diff <= 24*time.Hour:
			truncUnit = "minute"
		case diff <= 7*24*time.Hour:
			truncUnit = "hour"
		case diff <= 31*24*time.Hour:
			truncUnit = "day"
		default:
			truncUnit = "month"
		}
	} else {
		switch rangeParam {
		case "day":
			truncUnit = "minute"
		case "week":
			truncUnit = "hour"
		case "month":
			truncUnit = "day"
		case "year":
			truncUnit = "month"
		default:
			truncUnit = "minute"
		}
	}

	// 6) If no explicit start/end, apply preset
	if q.Get("start") == "" && q.Get("end") == "" {
		switch rangeParam {
		case "day":
			startTime = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		case "week":
			weekday := int(now.Weekday())
			startTime = time.Date(now.Year(), now.Month(), now.Day()-weekday, 0, 0, 0, 0, time.UTC)
		case "month":
			startTime = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		case "year":
			startTime = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		}
	}

	// 7) Parse aggregateFields
	aggParam := q.Get("aggregateFields")
	var aggs []struct{ expr, alias string }
	if aggParam != "" {
		for _, path := range strings.Split(aggParam, ",") {
			parts := strings.Split(path, ".")
			alias := strings.ToLower(strings.Join(parts, "_") + "_avg")
			jsonPath := "{" + strings.Join(parts, ",") + "}"
			expr := fmt.Sprintf("AVG((packet #>> '%s')::float) AS %s", jsonPath, alias)
			aggs = append(aggs, struct{ expr, alias string }{expr, alias})
		}
	}

	// 8) Aggregation branch with a 2000‐row cap
	if len(aggs) > 0 {
		bucketCol := fmt.Sprintf("date_trunc('%s', timestamp) AS bucket", truncUnit)
		var exprs []string
		for _, a := range aggs {
			exprs = append(exprs, a.expr)
		}

		whereClause := fmt.Sprintf("user_id = %d AND timestamp >= '%s' AND timestamp <= '%s'",
			userID, startTime.Format(time.RFC3339Nano), endTime.Format(time.RFC3339Nano))

		// Add ReceiverID filter if provided
		if receiverID > 0 {
			whereClause += fmt.Sprintf(" AND receiver_id = %d", receiverID)
		}

		// Only check for receiver_id_duplicated IS NULL if no specific receiver is specified
		var duplicateCheck string
		if receiverID <= 0 {
			duplicateCheck = "AND (receiver_id_duplicated IS NULL)"
		} else {
			duplicateCheck = ""
		}

		query := fmt.Sprintf(`
		          SELECT
		            %s,
		            %s
		          FROM messages
		          WHERE %s
		            %s
		          GROUP BY bucket
		          ORDER BY bucket ASC
		          LIMIT 2000;
		      `, bucketCol, strings.Join(exprs, ", "), whereClause, duplicateCheck)
		// log.Printf("SQL Query: %s", query)

		rows, err := QueryDatabaseForUser(userIDStr, query)
		if err != nil {
			http.Error(w, fmt.Sprintf("DB error: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		nulls := make([]sql.NullFloat64, len(aggs))
		scanTargets := []interface{}{new(time.Time)}
		for i := range nulls {
			scanTargets = append(scanTargets, &nulls[i])
		}

		var out []map[string]interface{}
		for rows.Next() {
			var bucket time.Time
			scanTargets[0] = &bucket
			if err := rows.Scan(scanTargets...); err != nil {
				http.Error(w, fmt.Sprintf("Scan error: %v", err), http.StatusInternalServerError)
				return
			}
			rec := map[string]interface{}{
				"timestamp": bucket.UTC().Format(time.RFC3339Nano),
			}
			for i, a := range aggs {
				if nulls[i].Valid {
					rec[a.alias] = nulls[i].Float64
				} else {
					rec[a.alias] = nil
				}
			}
			out = append(out, rec)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
		return
	}

	// 9) Fallback: original latest/limit logic + time filters
	var where []string
	where = append(where, fmt.Sprintf("user_id = %d", userID))
	if len(messageIDs) > 0 {
		if len(messageIDs) == 1 {
			// Single message ID
			where = append(where, fmt.Sprintf("message_id = %d", messageIDs[0]))
		} else {
			// Multiple message IDs - use IN clause
			var idStrings []string
			for _, id := range messageIDs {
				idStrings = append(idStrings, strconv.FormatInt(id, 10))
			}
			where = append(where, fmt.Sprintf("message_id IN (%s)", strings.Join(idStrings, ",")))
		}
	}
	if q.Get("DAC") != "" {
		where = append(where,
			fmt.Sprintf("(packet->'ApplicationID'->>'DesignatedAreaCode')::int = %d", dac))
	}
	if q.Get("FI") != "" {
		where = append(where,
			fmt.Sprintf("(packet->'ApplicationID'->>'FunctionIdentifier')::int = %d", fi))
	}
	if receiverID > 0 {
		where = append(where, fmt.Sprintf("receiver_id = %d", receiverID))
	}
	if !startTime.IsZero() {
		where = append(where,
			fmt.Sprintf("timestamp >= '%s'", startTime.Format(time.RFC3339Nano)))
	}
	if !endTime.IsZero() {
		where = append(where,
			fmt.Sprintf("timestamp <= '%s'", endTime.Format(time.RFC3339Nano)))
	}
	whereClause := strings.Join(where, " AND ")

	var query string
	if len(messageIDs) == 0 && q.Get("DAC") == "" && q.Get("FI") == "" {
		// Only check for receiver_id_duplicated IS NULL if no specific receiver is specified
		var duplicateCheck string
		if receiverID <= 0 {
			duplicateCheck = "AND (receiver_id_duplicated IS NULL)"
		} else {
			duplicateCheck = ""
		}

		query = fmt.Sprintf(`
		          SELECT DISTINCT ON (
		            message_id,
		            (packet->'ApplicationID'->>'DesignatedAreaCode')::int,
		            (packet->'ApplicationID'->>'FunctionIdentifier')::int
		          )
		            message_id,
		            packet,
		            raw_sentence,
		            timestamp,
		            receiver_id
		          FROM messages
		          WHERE %s
		            %s
		          ORDER BY
		            message_id,
		            (packet->'ApplicationID'->>'DesignatedAreaCode')::int,
		            (packet->'ApplicationID'->>'FunctionIdentifier')::int,
		            timestamp DESC;
		      `, whereClause, duplicateCheck)
	} else {
		// Only check for receiver_id_duplicated IS NULL if no specific receiver is specified
		var duplicateCheck string
		if receiverID <= 0 {
			duplicateCheck = "AND (receiver_id_duplicated IS NULL)"
		} else {
			duplicateCheck = ""
		}

		query = fmt.Sprintf(`
		          SELECT
		            message_id,
		            packet,
		            raw_sentence,
		            timestamp,
		            receiver_id
		          FROM messages
		          WHERE %s
		            %s
		          ORDER BY timestamp DESC
		          LIMIT %d;
		      `, whereClause, duplicateCheck, limit)
	}
	// log.Printf("SQL Query: %s", query)

	rows, err := QueryDatabaseForUser(userIDStr, query)
	if err != nil {
		http.Error(w, fmt.Sprintf("DB error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type entry struct {
		MessageID   int             `json:"MessageID"`
		Timestamp   string          `json:"Timestamp"`
		Packet      json.RawMessage `json:"Packet"`
		RawSentence *string         `json:"RawSentence"`
		ReceiverID  *int            `json:"ReceiverID,omitempty"`
	}
	var results []entry

	for rows.Next() {
		var (
			e          entry
			ts         time.Time
			rawSent    sql.NullString
			receiverID sql.NullInt64
		)
		if err := rows.Scan(&e.MessageID, &e.Packet, &rawSent, &ts, &receiverID); err != nil {
			http.Error(w, fmt.Sprintf("Scan error: %v", err), http.StatusInternalServerError)
			return
		}
		if rawSent.Valid {
			e.RawSentence = &rawSent.String
		}
		e.Timestamp = ts.UTC().Format(time.RFC3339Nano)

		// Add ReceiverID if available
		if receiverID.Valid {
			val := int(receiverID.Int64)
			e.ReceiverID = &val
		}

		results = append(results, e)
	}

	if len(results) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("[]"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func summaryHandler(w http.ResponseWriter, r *http.Request, settings *Settings) {
	// Declare 'err' once at the beginning of the function
	var err error

	// Extract latitude, longitude, and radius from query parameters
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	radiusStr := r.URL.Query().Get("radius")

	// Default to 500 if 'maxResults' is not specified
	limitStr := r.URL.Query().Get("maxResults")
	limit := 500
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			http.Error(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
		if parsedLimit > 500 {
			limit = 500
		} else {
			limit = parsedLimit
		}
	}

	// Default to 24 hours if 'maxAge' is not specified
	maxAgeStr := r.URL.Query().Get("maxAge")
	maxAge := 24
	if maxAgeStr != "" {
		parsedMaxAge, err := strconv.Atoi(maxAgeStr)
		if err != nil || parsedMaxAge <= 0 {
			http.Error(w, "Invalid maxage value", http.StatusBadRequest)
			return
		}
		if parsedMaxAge > 720 {
			maxAge = 720
		} else {
			maxAge = parsedMaxAge
		}
	}

	// Extract 'minSpeed' query parameter (optional)
	minSpeedStr := r.URL.Query().Get("minSpeed")
	var minSpeed float64
	if minSpeedStr != "" {
		minSpeed, err = strconv.ParseFloat(minSpeedStr, 64)
		if err != nil || minSpeed < 0 {
			http.Error(w, "Invalid minSpeed value", http.StatusBadRequest)
			return
		}
	}

	// Extract 'UserID' query parameter for filtering (optional)
	useridStr := r.URL.Query().Get("UserID")
	var userid int64
	if useridStr != "" {
		userid, err = strconv.ParseInt(useridStr, 10, 64)
		if err != nil || userid <= 0 {
			http.Error(w, "Invalid UserID value", http.StatusBadRequest)
			return
		}
	}

	// Extract 'receiver' query parameter for filtering (optional)
	receiverStr := r.URL.Query().Get("receiver")
	var receiverID int64
	if receiverStr != "" {
		receiverID, err = strconv.ParseInt(receiverStr, 10, 64)
		if err != nil || receiverID <= 0 {
			http.Error(w, "Invalid receiver value", http.StatusBadRequest)
			return
		}
		log.Printf("HTTP summary request with receiver filter: %d", receiverID)
	}

	// Extract 'types' query parameter for filtering (optional)
	types := r.URL.Query().Get("types")

	// Extract 'typeGroups' query parameter for filtering (optional)
	typeGroups := r.URL.Query().Get("typeGroups")

	// Extract 'classes' query parameter for filtering (optional)
	classes := r.URL.Query().Get("classes")

	// Parse latitude, longitude, and radius if they are provided
	var lat, lon, radius float64
	if latStr != "" && lonStr != "" && radiusStr != "" {
		lat, err = strconv.ParseFloat(latStr, 64)
		if err != nil {
			http.Error(w, "Invalid latitude", http.StatusBadRequest)
			return
		}

		lon, err = strconv.ParseFloat(lonStr, 64)
		if err != nil {
			http.Error(w, "Invalid longitude", http.StatusBadRequest)
			return
		}

		radius, err = strconv.ParseFloat(radiusStr, 64)
		if err != nil || radius <= 0 {
			http.Error(w, "Invalid radius", http.StatusBadRequest)
			return
		}
	}

	p := FilterParams{
		Latitude: lat, Longitude: lon,
		Radius:     radius,
		MaxResults: limit,
		MaxAge:     maxAge,
		MinSpeed:   minSpeed,
		UserID:     userid,
		Types:      types,
		TypeGroups: typeGroups,
		Classes:    classes,
		ReceiverID: receiverID,
	}
	blob, err := getSummaryJSON(p, conf.CacheTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error generating summary: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(blob)
}

// summaryHistoryHandler handles requests to the /summaryhistory endpoint
// It returns vessels seen within a specified time period
func summaryHistoryHandler(w http.ResponseWriter, r *http.Request) {
	// Declare 'err' once at the beginning of the function
	var err error

	// Extract latitude, longitude, and radius from query parameters
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	radiusStr := r.URL.Query().Get("radius")

	// Default to 500 if 'maxResults' is not specified
	limitStr := r.URL.Query().Get("maxResults")
	limit := 500
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			http.Error(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
		if parsedLimit > 500 {
			limit = 500
		} else {
			limit = parsedLimit
		}
	}

	// Extract 'minSpeed' query parameter (optional)
	minSpeedStr := r.URL.Query().Get("minSpeed")
	var minSpeed float64
	if minSpeedStr != "" {
		minSpeed, err = strconv.ParseFloat(minSpeedStr, 64)
		if err != nil || minSpeed < 0 {
			http.Error(w, "Invalid minSpeed value", http.StatusBadRequest)
			return
		}
	}

	// Extract 'UserID' query parameter for filtering (optional)
	useridStr := r.URL.Query().Get("UserID")
	var userid int64
	if useridStr != "" {
		userid, err = strconv.ParseInt(useridStr, 10, 64)
		if err != nil || userid <= 0 {
			http.Error(w, "Invalid UserID value", http.StatusBadRequest)
			return
		}
	}

	// Extract 'receiver' query parameter for filtering (optional)
	receiverStr := r.URL.Query().Get("receiver")
	var receiverID int64
	if receiverStr != "" {
		receiverID, err = strconv.ParseInt(receiverStr, 10, 64)
		if err != nil || receiverID <= 0 {
			http.Error(w, "Invalid receiver value", http.StatusBadRequest)
			return
		}
	}

	// Extract 'types' query parameter for filtering (optional)
	types := r.URL.Query().Get("types")

	// Extract 'typeGroups' query parameter for filtering (optional)
	typeGroups := r.URL.Query().Get("typeGroups")

	// Extract 'classes' query parameter for filtering (optional)
	classes := r.URL.Query().Get("classes")

	// Parse latitude, longitude, and radius if they are provided
	var lat, lon, radius float64
	if latStr != "" && lonStr != "" && radiusStr != "" {
		lat, err = strconv.ParseFloat(latStr, 64)
		if err != nil {
			http.Error(w, "Invalid latitude", http.StatusBadRequest)
			return
		}

		lon, err = strconv.ParseFloat(lonStr, 64)
		if err != nil {
			http.Error(w, "Invalid longitude", http.StatusBadRequest)
			return
		}

		radius, err = strconv.ParseFloat(radiusStr, 64)
		if err != nil || radius <= 0 {
			http.Error(w, "Invalid radius", http.StatusBadRequest)
			return
		}

		// Limit radius to 400 km
		if radius > 400000 { // 400 km in meters
			http.Error(w, "Radius too large, try zooming in", http.StatusBadRequest)
			return
		}
	}

	// Extract 'from' and 'to' time parameters
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	if fromStr == "" || toStr == "" {
		http.Error(w, "Both 'from' and 'to' parameters are required", http.StatusBadRequest)
		return
	}

	// Parse the time parameters
	fromTime, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		http.Error(w, "Invalid 'from' time format. Use RFC3339 format (e.g., 2025-05-21T08:00:00Z)", http.StatusBadRequest)
		return
	}

	toTime, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		http.Error(w, "Invalid 'to' time format. Use RFC3339 format (e.g., 2025-05-21T09:00:00Z)", http.StatusBadRequest)
		return
	}

	// Ensure 'from' is before 'to'
	if fromTime.After(toTime) {
		http.Error(w, "'from' time must be before 'to' time", http.StatusBadRequest)
		return
	}

	// Limit time period to 24 hours
	maxDuration := 24 * time.Hour
	if toTime.Sub(fromTime) > maxDuration {
		http.Error(w, "Time period cannot exceed 24 hours", http.StatusBadRequest)
		return
	}

	// Get summary history results
	results, err := getSummaryHistoryResults(lat, lon, radius, limit, minSpeed, userid, types, typeGroups, classes, receiverID, fromTime, toTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error generating summary history: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// metSummaryHandler handles requests to the /met-summary endpoint
// It fetches meteorological data from all collectors' met_state tables
func metSummaryHandler(w http.ResponseWriter, r *http.Request) {
	// Parse maxage parameter (in hours)
	maxAgeStr := r.URL.Query().Get("maxage")
	maxAge := 24 // Default to 24 hours

	if maxAgeStr != "" {
		parsedMaxAge, err := strconv.Atoi(maxAgeStr)
		if err != nil || parsedMaxAge <= 0 {
			http.Error(w, "Invalid maxage value", http.StatusBadRequest)
			return
		}

		// Maximum allowed is 7 days (168 hours)
		if parsedMaxAge > 168 {
			maxAge = 168
		} else {
			maxAge = parsedMaxAge
		}
	}

	// Get meteorological data from all collectors
	results, err := getMetSummaryResults(maxAge)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error generating met summary: %v", err), http.StatusInternalServerError)
		return
	}

	// Return results as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(results); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
		return
	}
}

// getMetSummaryResults fetches meteorological data from all collectors' met_state tables
func getMetSummaryResults(maxAge int) (map[string]interface{}, error) {
	// Build query to select data from met_state table
	query := `
        SELECT user_id, receiver_id, last_updated, met_data
        FROM met_state
    `

	// Add time filter if maxAge is specified
	if maxAge > 0 {
		cutoff := time.Now().Add(-time.Duration(maxAge) * time.Hour).UTC().Format(time.RFC3339Nano)
		query += fmt.Sprintf(" WHERE last_updated >= '%s'", cutoff)
	}

	// Query all collectors
	results, err := QueryDatabasesForAllShards(query)
	if err != nil {
		return nil, fmt.Errorf("Error querying database: %v", err)
	}

	// Process results
	metSummary := make(map[string]interface{})

	for _, shardData := range results {
		for _, row := range shardData {
			// Extract user_id
			userID, ok := row["user_id"]
			if !ok {
				continue
			}

			var userIDStr string
			switch v := userID.(type) {
			case int:
				userIDStr = fmt.Sprintf("%d", v)
			case int64:
				userIDStr = fmt.Sprintf("%d", v)
			default:
				continue
			}

			// Create entry for this user_id
			entry := make(map[string]interface{})

			// Add receiver_id if available
			if receiverID, ok := row["receiver_id"]; ok {
				entry["receiver_id"] = receiverID
			}

			// Add last_updated timestamp if available
			if lastUpdated, ok := row["last_updated"].(time.Time); ok {
				entry["last_updated"] = lastUpdated.UTC().Format(time.RFC3339)
			}

			// Extract and add met_data
			if metDataRaw, ok := row["met_data"].([]byte); ok {
				var metData map[string]interface{}
				if err := json.Unmarshal(metDataRaw, &metData); err == nil {
					entry["met_data"] = metData
				}
			}

			// Add entry to summary
			metSummary[userIDStr] = entry
		}
	}

	return metSummary, nil
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	var requestBody struct {
		Query      string `json:"query"`
		MaxAge     *int   `json:"maxAge"`     // Using pointer to detect if field is missing
		MaxResults int    `json:"maxResults"` // Optional parameter for limiting results
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing JSON: %v", err), http.StatusBadRequest)
		return
	}
	if len(requestBody.Query) < 3 {
		http.Error(w, "Query must be at least 3 characters long", http.StatusBadRequest)
		return
	}

	// Check if MaxAge is provided
	if requestBody.MaxAge == nil {
		http.Error(w, "MaxAge is a required field", http.StatusBadRequest)
		return
	}

	maxAge := *requestBody.MaxAge

	// No upper limit on MaxAge, just a sanity check for extremely large values
	const maxPossibleAge = 87600 // 10 years in hours
	if maxAge > maxPossibleAge {
		maxAge = maxPossibleAge
	}

	// Set default limit if not provided or if it's invalid
	maxResults := requestBody.MaxResults
	if maxResults <= 0 {
		maxResults = 100 // Default limit
	} else if maxResults > 1000 {
		maxResults = 1000 // Maximum allowed limit
	}

	// compute the cutoff
	currentTime := time.Now()
	var cutoff string
	if maxAge == 0 {
		// If MaxAge is 0, no time limit
		cutoff = "1970-01-01T00:00:00Z" // Beginning of Unix epoch
	} else {
		cutoff = currentTime.Add(-time.Duration(maxAge) * time.Hour).UTC().Format(time.RFC3339)
	}

	// build the query—note we now SELECT state.name and add an OR on state.name ILIKE
	query := fmt.Sprintf(`
        SELECT 
            packet,
            timestamp,
            ais_class,
            count,
            name
        FROM state
        WHERE
          (
            (packet->>'UserID')::text LIKE '%%%[1]s%%'
            OR (packet->>'ImoNumber')::text LIKE '%%%[1]s%%'
            OR (packet->>'Name')::text ILIKE '%%%[1]s%%'
            OR (packet->>'CallSign')::text ILIKE '%%%[1]s%%'
            OR name ILIKE '%%%[1]s%%'
          )
          AND timestamp >= '%[2]s'
        ORDER BY timestamp ASC
        LIMIT %[3]d
    `, requestBody.Query, cutoff, maxResults)

	// then everything else stays the same...
	results, err := QueryDatabasesForAllShards(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
		return
	}

	response := make([]map[string]interface{}, 0)
	for _, shardData := range results {
		for _, row := range shardData {
			packetData, ok := row["packet"].([]byte)
			if !ok {
				continue
			}
			var packetMap map[string]interface{}
			if err := json.Unmarshal(packetData, &packetMap); err != nil {
				continue
			}

			name := getFieldString(packetMap, "Name")
			if ext := getFieldString(packetMap, "NameExtension"); ext != "" {
				name += ext
			}

			lat := getFieldFloat(packetMap, "Latitude")
			lon := getFieldFloat(packetMap, "Longitude")
			if isNull(lat) || isNull(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
				continue
			}

			summary := map[string]interface{}{
				"CallSign":    getFieldString(packetMap, "CallSign"),
				"ImoNumber":   getFieldFloat(packetMap, "ImoNumber"),
				"Name":        name,
				"NumMessages": getFieldInt(row, "count"),
				"UserID":      getFieldFloat(packetMap, "UserID"),
				"Type":        getFieldFloat(packetMap, "Type"),
			}
			if ts, ok := row["timestamp"].(time.Time); ok {
				summary["LastUpdated"] = ts.UTC().Format(time.RFC3339Nano)
			}
			// **Override** with the table’s `name` column, if non-null:
			if tblName, exists := row["name"].(string); exists && tblName != "" {
				summary["Name"] = tblName
			}

			response = append(response, summary)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// historyHandler serves vessel history as CSV, safely handling missing fields.
func historyHandler(w http.ResponseWriter, r *http.Request) {
	// 1) Extract user_id from URL path: expect "/history/{userID}"
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 {
		http.Error(w, "Invalid URL format; expected /history/{userID}", http.StatusBadRequest)
		return
	}
	userID := parts[2]

	// 2) Extract and validate maxAge query parameter
	maxAgeStr := r.URL.Query().Get("maxAge")
	if maxAgeStr == "" {
		http.Error(w, "Missing maxAge query parameter", http.StatusBadRequest)
		return
	}
	maxAge, err := strconv.Atoi(maxAgeStr)
	if err != nil || maxAge <= 0 {
		http.Error(w, "Invalid maxAge value", http.StatusBadRequest)
		return
	}
	if maxAge > 168 {
		http.Error(w, "maxAge cannot exceed 168 hours (1 week)", http.StatusBadRequest)
		return
	}

	// 3) Fetch history results
	results, err := getHistoryResults(userID, maxAge)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error fetching history: %v", err), http.StatusInternalServerError)
		return
	}

	// 4) Prepare CSV response headers
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment; filename=history.csv")

	writer := csv.NewWriter(w)
	defer writer.Flush()

	// 5) Write rows: timestamp, latitude, longitude, sog, cog, trueHeading
	for _, row := range results {
		record := make([]string, 6)

		// timestamp
		if ts, ok := row["timestamp"].(time.Time); ok {
			record[0] = ts.Format(time.RFC3339)
		}

		// latitude & longitude – filter out invalid coordinates
		latVal, latOK := row["latitude"].(float64)
		lonVal, lonOK := row["longitude"].(float64)
		if !latOK || !lonOK || latVal < -90 || latVal > 90 || lonVal < -180 || lonVal > 180 {
			continue
		}
		record[1] = fmt.Sprintf("%f", latVal)
		record[2] = fmt.Sprintf("%f", lonVal)

		// sog
		if sog, ok := row["sog"].(float64); ok {
			record[3] = fmt.Sprintf("%.2f", sog)
		}

		// cog
		if cog, ok := row["cog"].(float64); ok {
			record[4] = fmt.Sprintf("%.2f", cog)
		}

		// trueHeading
		if th, ok := row["trueHeading"].(float64); ok {
			record[5] = fmt.Sprintf("%.2f", th)
		}

		if err := writer.Write(record); err != nil {
			log.Printf("error writing CSV record for user %s: %v", userID, err)
		}
	}
}

// Helper function to safely get an integer from the result map
func getFieldInt(row map[string]interface{}, field string) int {
	if value, ok := row[field].(int); ok {
		return value
	}
	if value, ok := row[field].(int64); ok {
		return int(value)
	}
	return 0
}

// Helper function to safely get a string value from packetData (returns empty string if not found)
func getFieldString(packetData map[string]interface{}, field string) string {
	if value, ok := packetData[field].(string); ok && value != "" {
		return value
	}
	return "" // Return empty string if field is nil or not found
}

// Helper function to safely get a float64 value from packetData (returns 0 if not found)
func getFieldFloat(packetData map[string]interface{}, field string) float64 {
	// Check if the value is already a float64
	if value, ok := packetData[field].(float64); ok {
		return value
	}
	// Check if the value is an integer type and convert it to float64
	if value, ok := packetData[field].(int); ok {
		return float64(value)
	}
	if value, ok := packetData[field].(int64); ok {
		return float64(value)
	}
	// Check if the value is a string that can be parsed into a float
	if value, ok := packetData[field].(string); ok && value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return 0 // Return 0 if value is not found or cannot be converted to float
}

func getNullableFloat(packetData map[string]interface{}, field string) interface{} {
	raw, exists := packetData[field]
	if !exists {
		// never saw the key at all → emit JSON null
		return nil
	}
	if s, ok := raw.(string); ok && s == "" {
		return nil
	}
	return getFieldFloat(packetData, field)
}

// Helper function to safely parse any JSON field into a map (returns nil if parsing fails)
func getFieldJSON(packetData map[string]interface{}, field string) map[string]interface{} {
	if value, ok := packetData[field].(map[string]interface{}); ok {
		return value // If the value is already a map, return it directly
	}
	// If the value is a string (and not empty), try unmarshalling it
	if value, ok := packetData[field].(string); ok && value != "" {
		var jsonData map[string]interface{}
		if err := json.Unmarshal([]byte(value), &jsonData); err == nil {
			return jsonData
		}
	}
	return nil // Return nil if the field is missing or cannot be parsed
}

func formatTimestamp(t time.Time) string {
	// Return the time in UTC with nanosecond precision in the ISO 8601 format
	return t.UTC().Format(time.RFC3339Nano) // Format: "2025-04-21T13:49:56.259736Z"
}

func handleSummaryRequest(client *serverSocket.Socket, data map[string]interface{}) {

	// Extract parameters from the incoming WebSocket message
	lat, _ := data["latitude"].(float64)
	lon, _ := data["longitude"].(float64)
	radius, _ := data["radius"].(float64)
	limit, _ := data["limit"].(int)
	maxAge, _ := data["maxAge"].(int)
	minSpeed, _ := data["minSpeed"].(float64)
	userid, _ := data["UserID"].(int64)

	// Extract receiver parameter
	var receiverID int64
	if receiverVal, ok := data["receiver"].(float64); ok {
		receiverID = int64(receiverVal)
		log.Printf("WebSocket summary request with receiver filter: %d", receiverID)
	}

	// Extract types parameter (comma-delimited string)
	var types string
	if typesVal, ok := data["types"].(string); ok {
		types = typesVal
	}

	// Extract typeGroups parameter (comma-delimited string)
	var typeGroups string
	if typeGroupsVal, ok := data["typeGroups"].(string); ok {
		typeGroups = typeGroupsVal
	}

	// Extract classes parameter (comma-delimited string)
	var classes string
	if classesVal, ok := data["classes"].(string); ok {
		classes = classesVal
	}

	// Log the parameters for debugging
	log.Printf("Client %s requested summary with params: latitude=%.6f, longitude=%.6f, radius=%.2f, maxResults=%d, maxAge=%d, minSpeed=%.2f, UserID=%d, receiver=%d, types=%s, typeGroups=%s, classes=%s",
		client.Id(), lat, lon, radius, limit, maxAge, minSpeed, userid, receiverID, types, typeGroups, classes)

	// Now call the function that generates the summary
	summarizedResults, err := getSummaryResults(lat, lon, radius, limit, maxAge, minSpeed, userid, types, typeGroups, classes, receiverID)
	if err != nil {
		log.Printf("Error fetching summary: %v", err)
		return
	}

	// Send the summary data back to the client
	if err := client.Emit("summaryData", summarizedResults); err != nil {
		log.Printf("Error sending summary data to client %s: %v", client.Id(), err)
	}
}

func userStateHandler(w http.ResponseWriter, r *http.Request) {
	// 1) Parse & validate userID from query string
	idStr := r.URL.Query().Get("UserID")
	if idStr == "" {
		http.Error(w, "Missing userID query parameter", http.StatusBadRequest)
		return
	}
	userID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || userID <= 0 {
		http.Error(w, "Invalid userID", http.StatusBadRequest)
		return
	}

	// 2) Build the SQL with %d so we get "... WHERE s.user_id = 123;"
	query := fmt.Sprintf(`
    WITH msg_types AS (
      SELECT
        user_id,
        ARRAY_AGG(DISTINCT message_id) AS message_types
      FROM messages
      WHERE user_id = %d
        AND (receiver_id_duplicated IS NULL)
      GROUP BY user_id
    ),
    receiver_ids AS (
      SELECT
        user_id,
        ARRAY_AGG(DISTINCT receiver_id) AS receiver_ids
      FROM messages
      WHERE user_id = %d
        AND receiver_id IS NOT NULL
        AND receiver_id != 0
        AND (receiver_id_duplicated IS NULL)
      GROUP BY user_id
    )
    SELECT
      s.packet,
      s.timestamp,
      s.ais_class,
      s.count,
      s.name,
      s.image_url,
      s.receiver_id,
      mt.message_types,
      r.receiver_ids
    FROM state AS s
    LEFT JOIN msg_types AS mt
      ON s.user_id = mt.user_id
    LEFT JOIN receiver_ids AS r
      ON s.user_id = r.user_id
    WHERE s.user_id = %d;
    `, userID, userID, userID)

	// 3) Run the query (still routed to the correct shard by passing the ID string)
	rows, err := QueryDatabaseForUser(strconv.FormatInt(userID, 10), query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// 4) Merge results into one packet‐map
	merged := make(map[string]interface{})
	for rows.Next() {
		var (
			packetJSON   []byte
			ts           string
			aisClass     string
			count        int
			dbName       sql.NullString
			dbImageURL   sql.NullString
			receiverID   sql.NullInt64
			messageTypes pq.StringArray
			receiverIDs  pq.Int64Array
		)
		if err := rows.Scan(&packetJSON, &ts, &aisClass, &count, &dbName, &dbImageURL, &receiverID, &messageTypes, &receiverIDs); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning result: %v", err), http.StatusInternalServerError)
			return
		}

		// Unmarshal the raw JSON packet
		packetData := make(map[string]interface{})
		if err := json.Unmarshal(packetJSON, &packetData); err != nil {
			http.Error(w, fmt.Sprintf("Error unmarshalling packet data: %v", err), http.StatusInternalServerError)
			return
		}

		// Assemble/override fields
		if ext, ok := packetData["NameExtension"].(string); ok && ext != "" {
			base := ""
			if b, ok2 := packetData["Name"].(string); ok2 {
				base = b
			}
			packetData["Name"] = base + ext
		}
		delete(packetData, "NameExtension")

		// Drop sentinel TrueHeading
		if getFieldFloat(packetData, "TrueHeading") == NoTrueHeading {
			delete(packetData, "TrueHeading")
		}

		packetData["AISClass"] = aisClass
		packetData["LastUpdated"] = ts
		packetData["NumMessages"] = count
		packetData["MessageTypes"] = messageTypes

		if dbName.Valid && dbName.String != "" {
			packetData["Name"] = dbName.String
		}
		if dbImageURL.Valid {
			packetData["ImageURL"] = dbImageURL.String
		}

		if name, ok := packetData["Name"].(string); !ok || name == "" {
			packetData["Name"] = fmt.Sprintf("%s (%d)", aisClass, userID)
		}

		merged = packetData

		// Add ReceiverID if available in the database
		if receiverID.Valid {
			merged["ReceiverID"] = receiverID.Int64
		} else {
			merged["ReceiverID"] = nil
		}

		// Add array of unique receiver IDs
		if len(receiverIDs) > 0 {
			merged["ReceiverIDs"] = receiverIDs
		} else {
			merged["ReceiverIDs"] = []int64{}
		}
	}

	// 5) Return as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(merged); err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
	}
}

// Utility function to handle debug logging
func logWithDebug(debug bool, format string, args ...interface{}) {
	if debug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

func myIPHandler(w http.ResponseWriter, r *http.Request) {
	ip := r.Header.Get("X-Forwarded-For")
	if ip != "" {
		// If there are multiple IPs, the client IP will be the first one
		// Extract the first IP from the list
		ip = strings.Split(ip, ",")[0]
	} else {
		// Fallback to r.RemoteAddr if X-Forwarded-For is not set
		ip = r.RemoteAddr
		if idx := strings.Index(ip, ":"); idx != -1 {
			ip = ip[:idx]
		}
	}

	response := map[string]string{
		"ip": ip,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Unable to encode response", http.StatusInternalServerError)
		log.Println("Error encoding response:", err)
	}
}

// Load settings from the settings.json file
func loadSettings(path string) (*Settings, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read settings file: %v", err)
	}

	var settings Settings
	err = json.Unmarshal(data, &settings)
	if err != nil {
		return nil, fmt.Errorf("failed to parse settings: %v", err)
	}

	return &settings, nil
}

// Fetch client list from ingester
func getClients(ingesterHost string, ingesterPort int, debug bool) ([]Client, error) {
	url := fmt.Sprintf("http://%s:%d/clients", ingesterHost, ingesterPort)
	logWithDebug(debug, "Fetching clients from URL: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch clients: %v", err)
	}
	defer resp.Body.Close()

	// Define a new struct to hold the response
	type ClientResponse struct {
		Clients          []Client `json:"clients"`
		ConfiguredShards int      `json:"configured_shards"` // Store the configured_shards
	}

	var clientResponse ClientResponse
	err = json.NewDecoder(resp.Body).Decode(&clientResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client list: %v", err)
	}

	// Store the configured_shards value in the global streamShards variable
	streamShards = clientResponse.ConfiguredShards

	// Debug log for each client and its shards
	if debug {
		log.Println("[DEBUG] Clients found:")
		for _, client := range clientResponse.Clients {
			log.Printf("[DEBUG] Client %s handles shards %v", client.Description, client.Shards)
		}
	}

	logWithDebug(debug, "Successfully fetched %d clients and total configured_shards: %d", len(clientResponse.Clients), streamShards)
	return clientResponse.Clients, nil
}

// Handle changes in clients' configuration and database connections
func handleClientChanges(ingesterHost string, ingesterPort int, debug bool) {
	logWithDebug(debug, "[DEBUG] handleClientChanges start")

	// 1) Fetch the latest client list
	clients, err := getClients(ingesterHost, ingesterPort, debug)
	if err != nil {
		log.Printf("Error fetching clients: %v", err)
		return
	}

	// Build a lookup of currently active descriptions
	existing := make(map[string]Client, len(clients))
	for _, c := range clients {
		existing[c.Description] = c
	}

	// 2) Remove any clients that have disappeared
	clientConnectionsMu.Lock()
	for desc, conn := range clientConnections {
		if _, stillHere := existing[desc]; !stillHere {
			// Remove from map before closing DB to avoid races
			delete(clientConnections, desc)
			log.Printf("Client %s has been removed, disconnecting.", desc)

			// Close the DB in background
			go func(c *ClientConnection) {
				c.Db.Close()
			}(conn)
		}
	}
	clientConnectionsMu.Unlock()

	// 3) Add any new clients (or re-add re-spawned ones)
	for _, client := range clients {
		// If it’s already in the map, skip
		clientConnectionsMu.RLock()
		_, exists := clientConnections[client.Description]
		clientConnectionsMu.RUnlock()
		if exists {
			continue
		}

		logWithDebug(debug, "New client %s detected, connecting to its database.", client.Description)

		// Fetch its DB settings and connect
		settings, err := getClientDatabaseSettings(client.Ip, client.Port, debug)
		if err != nil {
			log.Printf("Error fetching settings for client %s: %v", client.Description, err)
			continue
		}
		db, err := connectToDatabase(settings)
		if err != nil {
			log.Printf("Error connecting to database for client %s: %v", client.Description, err)
			continue
		}
		wsPort, err := fetchCollectorWSPort(client.Ip, client.Port)
		if err != nil {
			log.Printf("cannot fetch socketio_listen for %s: %v", client.Ip, err)
			db.Close()
			continue
		}

		// Store it
		clientConnectionsMu.Lock()
		clientConnections[client.Description] = &ClientConnection{
			Db:     db,
			DbHost: settings.DbHost,
			DbPort: settings.DbPort,
			DbUser: settings.DbUser,
			DbPass: settings.DbPass,
			DbName: settings.DbName,
			Shards: client.Shards,
			WSHost: client.Ip,
			WSPort: wsPort,
		}
		clientConnectionsMu.Unlock()

		log.Printf(
			"Successfully connected to database for client %s: %s@%s:%d/%s",
			client.Description,
			settings.DbUser, settings.DbHost, settings.DbPort, settings.DbName,
		)
	}
}

// fetch WS port from collector’s own /settings
func fetchCollectorWSPort(host string, httpPort int) (int, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/settings", host, httpPort))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var cfg struct {
		SocketIOListen int `json:"socketio_listen"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return 0, err
	}
	return cfg.SocketIOListen, nil
}

// Fetch client database settings (not global Settings, specific to clients)
func getClientDatabaseSettings(ip string, port int, debug bool) (*ClientDatabaseSettings, error) {
	url := fmt.Sprintf("http://%s:%d/settings", ip, port)
	logWithDebug(debug, "Fetching database settings from URL: %s", url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch database settings from client: %v", err)
	}
	defer resp.Body.Close()

	var settings ClientDatabaseSettings
	err = json.NewDecoder(resp.Body).Decode(&settings)
	if err != nil {
		return nil, fmt.Errorf("failed to decode database settings: %v", err)
	}

	// Log the fetched database settings if debug is enabled
	logWithDebug(debug, "Fetched database settings for client: %s:%d/%s",
		settings.DbHost, settings.DbPort, settings.DbName)

	return &settings, nil
}

// Connect to the PostgreSQL database
func connectToDatabase(settings *ClientDatabaseSettings) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		settings.DbHost, settings.DbPort, settings.DbUser, settings.DbPass, settings.DbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Ensure the database connection is successful
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	return db, nil
}

// Start the HTTP server with both Socket.IO and static file serving
func startHTTPServer(port int, mux *http.ServeMux) {
	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

// Set up the HTTP server with routes
// mqttConfigHandler serves the MQTT configuration as JSON
func mqttConfigHandler(w http.ResponseWriter, r *http.Request) {
	// Create a response with the MQTT configuration
	response := map[string]interface{}{
		"mqtt_host":  conf.PublicMQTTWebsocketsHost,
		"mqtt_port":  conf.PublicMQTTPort,
		"mqtt_topic": conf.PublicMQTTTopic,
	}

	// Set content type and write response
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode MQTT configuration", http.StatusInternalServerError)
		log.Printf("Error encoding MQTT configuration: %v", err)
		return
	}
}

func setupServer(settings *Settings) {
	mux := http.NewServeMux()

	// Reverse-proxy for /receivers and /metrics/
	receiversURL, err := url.Parse(conf.ReceiversBaseURL)
	if err != nil {
		log.Fatalf("invalid receivers_base_url: %v", err)
	}
	metricsURL, err := url.Parse(conf.MetricsBaseURL)
	if err != nil {
		log.Fatalf("invalid metrics_base_url: %v", err)
	}
	statisticsURL, err := url.Parse(conf.StatisticsBaseURL)
	if err != nil {
		log.Fatalf("invalid statistics_base_url: %v", err)
	}
	receiversProxy := httputil.NewSingleHostReverseProxy(receiversURL)
	metricsProxy := httputil.NewSingleHostReverseProxy(metricsURL)
	statisticsProxy := httputil.NewSingleHostReverseProxy(statisticsURL)
	mux.Handle("/receivers", receiversProxy)      // receivers JSON endpoint
	mux.Handle("/addreceiver", receiversProxy)    // addreceiver JSON endpoint
	mux.Handle("/editreceiver", receiversProxy)   // editreceiver JSON endpoint
	mux.Handle("/deletereceiver", receiversProxy) // deletereceiver JSON endpoint
	mux.Handle("/password-reset", receiversProxy) // password-reset JSON endpoint
	mux.Handle("/getudpport", receiversProxy)     // getudpport JSON endpoint
	mux.Handle("/receiverip", receiversProxy)     // receiverip JSON endpoint
	mux.Handle("/metrics/", metricsProxy)         // metrics JSON endpoints
	mux.Handle("/statistics/", statisticsProxy)   // statistics JSON endpoints

	// HTTP API endpoints

	mux.HandleFunc("/myip", myIPHandler)
	mux.HandleFunc("/mqtt", mqttConfigHandler)

	mux.HandleFunc("/summary", func(w http.ResponseWriter, r *http.Request) {
		summaryHandler(w, r, conf)
	})

	mux.HandleFunc("/summaryhistory", summaryHistoryHandler)

	mux.HandleFunc("/met-summary", metSummaryHandler)
	mux.HandleFunc("/state", userStateHandler)
	mux.HandleFunc("/history/", historyHandler)
	mux.HandleFunc("/search", searchHandler)
	mux.HandleFunc("/latestmessages", latestMessagesHandler)

	// Static files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.FileServer(http.Dir("web")).ServeHTTP(w, r)
	})

	// Socket.IO setup
	engineServer := types.CreateServer(nil)
	ioServer = serverSocket.NewServer(engineServer, nil)
	mux.Handle("/socket.io/", engineServer)

	// On new WebSocket connection
	ioServer.On("connection", func(args ...any) {
		client := args[0].(*serverSocket.Socket)
		log.Printf("WebSocket client connected: %s", client.Id())

		// Add the client to the 'metrics' room
		client.Join("metrics")

		// Track connected client
		connectedClientsMu.Lock()
		connectedClients[client.Id()] = client
		connectedClientsMu.Unlock()

		// Init this client's subscription set
		clientSubscriptionsMu.Lock()
		clientSubscriptions[client.Id()] = make(map[string]struct{})
		clientSubscriptionsMu.Unlock()

		// —— ais_sub/:userID ——
		client.On("ais_sub/:userID", func(raw ...any) {
			// 1) Extract the requested vessel ID
			userID := raw[0].(string)
			log.Printf("Client %s subscribes to %s", client.Id(), userID)

			// Record the subscription
			clientSubscriptionsMu.Lock()
			clientSubscriptions[client.Id()][userID] = struct{}{}
			clientSubscriptionsMu.Unlock()

			// 2) Find the collector for this shard
			shard := shardForUser(userID)
			var cc *ClientConnection
			clientConnectionsMu.RLock()
			for _, c := range clientConnections {
				for _, s := range c.Shards {
					if s == shard {
						cc = c
						break
					}
				}
				if cc != nil {
					break
				}
			}
			clientConnectionsMu.RUnlock()
			if cc == nil {
				log.Printf("No collector for shard %d", shard)
				return
			}

			// 3) Dial (or reuse) the collector WebSocket
			ws, err := cc.getWSClient()
			if err != nil {
				log.Printf("Error dialing collector WS: %v", err)
				return
			}
			ws.Emit("ais_sub/:userID", userID)

			// 4) Register ais_data handler exactly once
			wsHandlersMu.RLock()
			already := wsHandlersRegistered[ws]
			wsHandlersMu.RUnlock()
			if !already {
				wsHandlersMu.Lock()
				if !wsHandlersRegistered[ws] {
					wsHandlersRegistered[ws] = true
					ws.On("ais_data", func(msg ...any) {

						if len(msg) == 0 {
							return
						}

						// First argument should be our wrapped payload: map[string]interface{}
						wrapper, ok := msg[0].(map[string]interface{})
						if !ok {
							return
						}

						// Drill into the "data" sub-object
						dataObj, ok := wrapper["data"].(map[string]interface{})
						if !ok {
							return
						}

						// Now extract UserID from dataObj
						var msgUserID string
						switch v := dataObj["UserID"].(type) {
						case float64:
							msgUserID = strconv.FormatInt(int64(v), 10)
						case string:
							msgUserID = v
						default:
							return
						}

						// Forward to subscribed browser clients
						wrapped := wrapper // you can reuse the whole wrapper
						connectedClientsMu.RLock()
						clientSubscriptionsMu.RLock()
						for sid, sock := range connectedClients {
							if subs, exists := clientSubscriptions[sid]; exists {
								if _, subscribed := subs[msgUserID]; subscribed {
									sock.Emit("ais_data", wrapped)
									//sock.Emit(fmt.Sprintf("ais_data/%s", msgUserID), wrapped)
								}
							}
						}
						clientSubscriptionsMu.RUnlock()
						connectedClientsMu.RUnlock()
					})

				}
				wsHandlersMu.Unlock()
			}
		})

		// —— ais_unsub/:userID ——
		client.On("ais_unsub/:userID", func(raw ...any) {
			userID := raw[0].(string)
			log.Printf("Client %s unsubscribes from %s", client.Id(), userID)

			// 1) Remove the subscription for *this* browser client
			clientSubscriptionsMu.Lock()
			delete(clientSubscriptions[client.Id()], userID)
			clientSubscriptionsMu.Unlock()

			// 2) If anyone else is still interested, don’t send upstream
			if anySubscriberExists(userID) {
				log.Printf("Still have subscribers for %s; skipping upstream unsubscribe", userID)
				return
			}

			// 3) Nobody else wants it—forward the unsubscribe to the collector
			shard := shardForUser(userID)
			var cc *ClientConnection
			clientConnectionsMu.RLock()
			for _, c := range clientConnections {
				for _, s := range c.Shards {
					if s == shard {
						cc = c
						break
					}
				}
				if cc != nil {
					break
				}
			}
			clientConnectionsMu.RUnlock()

			if cc != nil {
				if ws, err := cc.getWSClient(); err == nil {
					log.Printf("Forwarding ais_unsub for %s upstream", userID)
					ws.Emit("ais_unsub/:userID", userID)
				} else {
					log.Printf("Error getting WS client to forward unsubscribe: %v", err)
				}
			} else {
				log.Printf("No collector found for shard %d (user %s)", shard, userID)
			}
		})

		// —— requestSummary ——
		client.On("requestSummary", func(args ...any) {
			if len(args) < 1 {
				return
			}

			now := time.Now()
			lastReqMu.Lock()
			if t, seen := lastReqTime[client.Id()]; seen && now.Sub(t) < minInterval {
				// too soon — just ignore
				lastReqMu.Unlock()
				return
			}
			lastReqTime[client.Id()] = now
			lastReqMu.Unlock()

			// 1) Decode payload into a map[string]interface{}
			var data map[string]interface{}
			switch raw := args[0].(type) {
			case string:
				if err := json.Unmarshal([]byte(raw), &data); err != nil {
					log.Printf("[requestSummary] invalid JSON payload: %v", err)
					return
				}
			case map[string]interface{}:
				data = raw
			default:
				log.Printf("[requestSummary] unsupported payload type %T", raw)
				return
			}

			// 2) Safely extract each field, with sane defaults
			lat, _ := data["latitude"].(float64)
			lon, _ := data["longitude"].(float64)
			radius, _ := data["radius"].(float64)

			maxResults := 0
			if v, ok := data["maxResults"].(float64); ok {
				maxResults = int(v)
			}

			maxAge := 0
			if v, ok := data["maxAge"].(float64); ok {
				maxAge = int(v)
			}

			minSpeed := 0.0
			if v, ok := data["minSpeed"].(float64); ok {
				minSpeed = v
			}

			updatePeriod := 5 // your default
			if v, ok := data["updatePeriod"].(float64); ok && int(v) > 0 {
				updatePeriod = int(v)
			}

			userID := int64(0)
			if v, ok := data["UserID"].(float64); ok {
				userID = int64(v)
			}

			// Extract types parameter (comma-delimited string)
			var types string
			if typesVal, ok := data["types"].(string); ok {
				types = typesVal
			}

			// Extract typeGroups parameter (comma-delimited string)
			var typeGroups string
			if typeGroupsVal, ok := data["typeGroups"].(string); ok {
				typeGroups = typeGroupsVal
			}

			// Extract classes parameter (comma-delimited string)
			var classes string
			if classesVal, ok := data["classes"].(string); ok {
				classes = classesVal
			}

			// Log the parameters for debugging
			// log.Printf("[requestSummary] Client %s requested with params: lat=%.6f, lon=%.6f, radius=%.2f, maxResults=%d, maxAge=%d, minSpeed=%.2f, UserID=%d, types=%s, typeGroups=%s, classes=%s",
			//     client.Id(), lat, lon, radius, maxResults, maxAge, minSpeed, userID, types, typeGroups, classes)

			// Extract receiver parameter
			var receiverID int64
			if receiverVal, ok := data["receiver"].(float64); ok {
				receiverID = int64(receiverVal)
			}

			// 3) Build FilterParams (LastUpdated will gate the next tick)
			p := FilterParams{
				Latitude:     lat,
				Longitude:    lon,
				Radius:       radius,
				MaxResults:   maxResults,
				MaxAge:       maxAge,
				MinSpeed:     minSpeed,
				UpdatePeriod: updatePeriod,
				UserID:       userID,
				LastUpdated:  time.Now(),
				Types:        types,
				TypeGroups:   typeGroups,
				Classes:      classes,
				ReceiverID:   receiverID,
			}

			// 4) Stash for the ticker
			clientSummaryMu.Lock()
			clientSummaryFilters[client.Id()] = p
			clientSummaryMu.Unlock()

			// 5) Serve immediately via cache/miss, but using raw JSON
			blob, err := getSummaryJSON(p, conf.CacheTime)
			if err != nil {
				log.Printf("[requestSummary] cache error: %v", err)
				return
			}

			// 6) Send the JSON string back
			if err := client.Emit("summaryData", string(blob)); err != nil {
				log.Printf("[requestSummary] emit error: %v", err)
			}
		})

		client.On("metrics/bysource", func(args ...interface{}) {
			// Ensure that the data argument is a map
			if len(args) < 1 {
				log.Printf("No data received in 'metrics/bysource' event")
				return
			}

			data, ok := args[0].(map[string]interface{})
			if !ok {
				log.Printf("Invalid data type: expected map[string]interface{}, got %T", args[0])
				return
			}

			// Call the new handler function
			handleMetricsBysource(client, data)
		})

		client.On("disconnect", func(...any) {
			// 1) Grab the list of this client’s active subscriptions
			clientSubscriptionsMu.Lock()
			subs := clientSubscriptions[client.Id()]
			delete(clientSubscriptions, client.Id())
			clientSubscriptionsMu.Unlock()

			// 2) For each userID this client had, check if anyone else still wants it
			for userID := range subs {
				if !anySubscriberExists(userID) {
					// find the collector for that shard
					shard := shardForUser(userID)
					var cc *ClientConnection
					clientConnectionsMu.RLock()
					for _, c := range clientConnections {
						for _, s := range c.Shards {
							if s == shard {
								cc = c
								break
							}
						}
						if cc != nil {
							break
						}
					}
					clientConnectionsMu.RUnlock()

					// forward unsubscribe upstream
					if cc != nil {
						if ws, err := cc.getWSClient(); err == nil {
							log.Printf("Disconnect: forwarding ais_unsub for %s upstream", userID)
							ws.Emit("ais_unsub/:userID", userID)
						} else {
							log.Printf("Disconnect: error getting WS client for %s: %v", userID, err)
						}
					}
				}
			}

			// 3) Clean up summary and connectedClients as before
			clientSummaryMu.Lock()
			delete(clientSummaryFilters, client.Id())
			clientSummaryMu.Unlock()

			connectedClientsMu.Lock()
			delete(connectedClients, client.Id())
			connectedClientsMu.Unlock()

			log.Printf("WebSocket client disconnected: %s", client.Id())
		})
	})

	// Start HTTP + WS server
	go startHTTPServer(conf.ListenPort, mux)
}

func main() {
	// Load configuration settings
	settingsFile := "settings.json"
	var err error
	conf, err = loadSettings(settingsFile)
	if err != nil {
		log.Fatalf("Error loading settings: %v", err)
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort),
	})
	if err := redisClient.Ping(redisCtx).Err(); err != nil {
		log.Printf("⚠️ Redis ping failed: %v. Continuing without cache.", err)
	}

	// Initialize map to track client database connections
	clientConnections = make(map[string]*ClientConnection)

	// Set up polling interval
	pollInterval := time.Duration(conf.PollInterval) * time.Second
	if pollInterval > 0 {
		go func() {
			ticker := time.NewTicker(pollInterval)
			defer ticker.Stop()

			for {
				<-ticker.C
				func() {
					defer func() {
						if r := recover(); r != nil {
							buf := make([]byte, 1<<10)
							n := runtime.Stack(buf, false)
							log.Printf("[ERROR] panic in poller: %v\n%s", r, buf[:n])
						}
					}()
					logWithDebug(conf.Debug, "Polling clients at %v", time.Now())
					handleClientChanges(conf.IngestHost, conf.IngestPort, conf.Debug)
				}()
			}
		}()
	}

	// Initial fetch and setup
	handleClientChanges(conf.IngestHost, conf.IngestPort, conf.Debug)

	// Set up the server with HTTP and Socket.IO routes
	setupServer(conf)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			clientSummaryMu.Lock()
			for sockID, params := range clientSummaryFilters {
				// Only fire when this client’s update period has elapsed
				if time.Since(params.LastUpdated) < time.Duration(params.UpdatePeriod)*time.Second {
					continue
				}

				// Fetch via Redis cache (or compute & cache on miss)
				blob, err := getSummaryJSON(params, conf.CacheTime)
				if err != nil {
					log.Printf("⚠️ ticker cache error for client %s: %v", sockID, err)
					// still update LastUpdated so we don’t spin on errors
					params.LastUpdated = time.Now()
					clientSummaryFilters[sockID] = params
					continue
				}

				// Emit to that client socket
				connectedClientsMu.RLock()
				sock, ok := connectedClients[sockID]
				connectedClientsMu.RUnlock()
				if ok {
					if err := sock.Emit("summaryData", string(blob)); err != nil {
						log.Printf("⚠️ ticker emit error to client %s: %v", sockID, err)
					}
				}
				// Update LastUpdated so next fire happens after UpdatePeriod
				params.LastUpdated = time.Now()
				clientSummaryFilters[sockID] = params
			}
			clientSummaryMu.Unlock()
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second) // Poll every second
		defer ticker.Stop()

		// Create a custom HTTP client with a 2-second timeout
		client := &http.Client{
			Timeout: 2 * time.Second, // Set a 2-second timeout
		}

		for range ticker.C {
			// Fetch /metrics/ingester data from the configured Metrics URL
			resp, err := client.Get(fmt.Sprintf("%s/metrics/ingester", conf.MetricsBaseURL))
			if err != nil {
				log.Printf("Error fetching /metrics/ingester: %v", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Received non-OK response from /metrics/ingester: %v", resp.StatusCode)
				continue
			}

			// Read and parse the response body
			body, err := io.ReadAll(resp.Body) // Updated to use io.ReadAll instead of ioutil.ReadAll
			if err != nil {
				log.Printf("Error reading response body: %v", err)
				continue
			}

			// Parse the JSON response
			var metrics map[string]interface{}
			if err := json.Unmarshal(body, &metrics); err != nil {
				log.Printf("Error unmarshalling JSON: %v", err)
				continue
			}

			// Extract the relevant fields
			selectedMetrics := map[string]interface{}{
				"window_messages":                    metrics["window_messages"],
				"window_messages_forwarded":          metrics["window_messages_forwarded"],
				"bytes_received_window":              metrics["bytes_received_window"],
				"window_downsampled":                 metrics["window_downsampled"],
				"window_deduplicated":                metrics["window_deduplicated"],
				"window_ratio_forwarded_to_received": metrics["window_ratio_forwarded_to_received"],
				"shards_missing":                     metrics["shards_missing"],
				"metric_window_size_sec":             metrics["metric_window_size_sec"],
				"uptime_seconds":                     metrics["uptime_seconds"],
			}

			// Emit the selected fields to all clients in the 'metrics' room
			connectedClientsMu.RLock()
			for _, client := range connectedClients {
				// Emit the selected data to the 'metrics' room
				if err := client.Emit("metricsData", selectedMetrics); err != nil {
					log.Printf("Error emitting data to client %s: %v", client.Id(), err)
				}
			}
			connectedClientsMu.RUnlock()
		}
	}()

	// Block forever (or handle gracefully shutting down the server if necessary)
	select {}
}
