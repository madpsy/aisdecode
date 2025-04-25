package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"net/http/httputil"
	"os"
	"hash/fnv"
	"strconv"
	"time"
	"strings"
	"sync"
	"runtime"

	"github.com/lib/pq"
	"github.com/zishang520/engine.io/v2/types"
    	clientSocket "github.com/zishang520/socket.io-client-go/socket"
	serverSocket "github.com/zishang520/socket.io/v2/socket"
    	"github.com/zishang520/engine.io-client-go/transports"
)

type FilterParams struct {
    Latitude    float64
    Longitude   float64
    Radius      float64
    MaxResults  int
    MaxAge      int
    MinSpeed    float64
    UpdatePeriod int
    UserID	int64
    LastUpdated time.Time
}

type Settings struct {
	IngestHost  string `json:"ingester_host"`
	IngestPort  int    `json:"ingester_port"`
	ListenPort  int    `json:"listen_port"`
	Debug       bool   `json:"debug"`
	PollInterval int   `json:"poll_interval"`
	ReceiversBaseURL string `json:"receivers_base_url"`
}

// This struct will contain the actual client database connection settings
type ClientDatabaseSettings struct {
	DbHost     string `json:"db_host"`
	DbPort     int    `json:"db_port"`
	DbUser     string `json:"db_user"`
	DbPass     string `json:"db_pass"`
	DbName     string `json:"db_name"`
}

type Client struct {
	Description string   `json:"description"`
	Ip          string   `json:"ip"`
	Port        int      `json:"port"`
	Shards      []int    `json:"shards"`
}

var clientConnections map[string]*ClientConnection
var clientConnectionsMu sync.RWMutex
var streamShards int // Global variable to store the total number of shards
var clientSubscriptionsMu sync.RWMutex
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

// AIS spec: TrueHeading == 511 means “not available”
const NoTrueHeading = 511.0

func shardForUser(userID string) int {
    h := fnv.New32a()
    h.Write([]byte(userID))
    return int(h.Sum32()) % streamShards
}

func QueryDatabaseForUser(userID string, query string) (*sql.Rows, error) {
	shardID := shardForUser(userID)
	var clientDescription string
	var clientConnection *ClientConnection // Use ClientConnection here

	// Iterate over clientConnections to find the ClientConnection for the shard
	clientConnectionsMu.RLock()
	for _, conn := range clientConnections {
		// Search through shards to find the one that handles the user
		for _, shard := range conn.Shards {
			if shard == shardID {
				clientDescription = conn.DbHost
				clientConnection = conn
				break
			}
		}
		if clientConnection != nil {
			break
		}
	}
	
	// If no matching ClientConnection was found, return an error
	if clientConnection == nil {
		return nil, fmt.Errorf("no client found handling shard %d", shardID)
	}

	// Perform the query on the database
	db := clientConnection.Db
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query on database for client %s: %v", clientDescription, err)
	}

	return rows, nil
}

func formatMsg(parts ...any) string {
    b, _ := json.Marshal(parts)
    return string(b)
}

// QueryDatabasesForAllShards queries all shard databases with the same query.
func QueryDatabasesForAllShards(query string) (map[string][]map[string]interface{}, error) {
	results := make(map[string][]map[string]interface{})

	for _, conn := range clientConnections {

		// Perform the query on the current shard's database
		db := conn.Db
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query on database for client %s: %v", conn.DbHost, err)
		}
		defer rows.Close()

		// Log the number of rows returned by the query
		var rowCount int
		columns, err := rows.Columns() // Get the column names dynamically
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for client %s: %v", conn.DbHost, err)
		}

		for rows.Next() {
			// Create a slice to hold column values dynamically
			values := make([]interface{}, len(columns))
			for i := range values {
				values[i] = new(interface{})
			}
			if err := rows.Scan(values...); err != nil {
				log.Printf("Error scanning result for shard %s: %v", conn.DbHost, err)
				continue
			}
			rowData := make(map[string]interface{})
			for i, column := range columns {
				// Store the column value in the rowData map
				rowData[column] = *(values[i].(*interface{}))
			}
			results[conn.DbHost] = append(results[conn.DbHost], rowData)
			rowCount++
		}

		// Log the total number of rows returned for the current shard
		// log.Printf("Shard %s returned %d rows", conn.DbHost, rowCount)
	}
	// log.Printf("Results collected: %+v", results)
	return results, nil
}

func getSummaryResults(lat, lon, radius float64, limit int, maxAge int, minSpeed float64, userid int64) (map[string]interface{}, error) {
   query := `
       SELECT user_id
            , packet
            , timestamp
            , ais_class
            , count
            , name
       FROM state
   `
    
    whereAdded := false

    // If UserID is provided, filter by user_id (UserID)
    if userid > 0 {
        query += fmt.Sprintf(" WHERE user_id = %d", userid)
        whereAdded = true
    }

    // If lat, lon, and radius are specified, filter by distance
    if lat != 0 && lon != 0 && radius != 0 {
        if whereAdded {
            query += fmt.Sprintf(`
                AND ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    ST_SetSRID(ST_Point(%f, %f), 4326)
                ) <= %f
            `, lon, lat, radius)
        } else {
            query += fmt.Sprintf(`
                WHERE ST_DistanceSphere(
                    ST_SetSRID(ST_Point(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
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
            query += fmt.Sprintf(" AND timestamp >= '%s'", cutoff)
        } else {
            query += fmt.Sprintf(" WHERE timestamp >= '%s'", cutoff)
            whereAdded = true
        }
    }

    if minSpeed > 0 {
        if whereAdded {
            query += fmt.Sprintf(
                " AND (packet->>'Sog')::float >= %f",
                minSpeed,
            )
        } else {
            query += fmt.Sprintf(
                " WHERE (packet->>'Sog')::float >= %f",
                minSpeed,
            )
            whereAdded = true
        }
    }

    // Finalizing the query
    query += " ORDER BY timestamp ASC"

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
            summary["MaximumStaticDraught"] = getFieldFloat(packetMap, "MaximumStaticDraught")
            summary["NavigationalStatus"] = getFieldFloat(packetMap, "NavigationalStatus")
            summary["Sog"] = getFieldFloat(packetMap, "Sog")
            summary["Type"] = getFieldFloat(packetMap, "Type")

    	    name := getFieldString(packetMap, "Name")
    	    ext  := getFieldString(packetMap, "NameExtension")
    	    if ext != "" {
    	        name = name + ext
    	    }
    	    summary["Name"] = name

	    // pull out the raw floats
	    lat         := getFieldFloat(packetMap, "Latitude")
	    lon         := getFieldFloat(packetMap, "Longitude")
	    trueheading := getFieldFloat(packetMap, "TrueHeading")

	    // garbage sentinel, skip this row entirely
	    if isNull(lat) || isNull(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		continue
	    }

	    // only then insert them into summary
	    summary["Latitude"]  = lat
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
           summarizedResults[userIDStr] = summary
        }
    }

    return summarizedResults, nil
}

func getHistoryResults(userID string, hours int) ([]map[string]interface{}, error) {
    // Calculate the timestamp for the time period
    currentTime := time.Now()
    pastTime := currentTime.Add(-time.Duration(hours) * time.Hour)

    // SQL query to get the messages for the user in the specified time range with distance filtering
    query := fmt.Sprintf(`
WITH filtered_messages AS (
    SELECT 
        timestamp,
        (packet->>'Longitude')::float AS longitude,
        (packet->>'Latitude')::float  AS latitude,
        (packet->>'Sog')::float       AS sog,
        (packet->>'Cog')::float       AS cog,
        (packet->>'TrueHeading')::float AS trueHeading,
        ROW_NUMBER() OVER (ORDER BY timestamp) AS row_num
    FROM messages
    WHERE user_id = '%s'
      AND timestamp >= '%s'
      AND message_id IN (1,2,3,18,19)
      AND packet->>'Longitude'  IS NOT NULL
      AND packet->>'Latitude'   IS NOT NULL
      -- optionally: AND packet->>'Longitude' <> '' AND packet->>'Latitude' <> ''
),
latest_message AS (
    SELECT 
        timestamp,
        (packet->>'Longitude')::float AS longitude,
        (packet->>'Latitude')::float  AS latitude,
        (packet->>'Sog')::float       AS sog,
        (packet->>'Cog')::float       AS cog,
        (packet->>'TrueHeading')::float AS trueHeading
    FROM messages
    WHERE user_id = '%s'
      AND packet->>'Longitude' IS NOT NULL
      AND packet->>'Latitude'  IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 1
)
SELECT 
    m1.timestamp,
    m1.latitude,
    m1.longitude,
    m1.sog,
    m1.cog,
    m1.trueHeading
FROM filtered_messages m1
LEFT JOIN filtered_messages m2
    ON m1.row_num = m2.row_num + 1
WHERE 
    m2.row_num IS NULL
    OR ST_DistanceSphere(
        ST_SetSRID(ST_Point(m1.longitude, m1.latitude), 4326), 
        ST_SetSRID(ST_Point(m2.longitude, m2.latitude), 4326)
    ) >= 30

UNION ALL

SELECT 
    l.timestamp,
    l.latitude,
    l.longitude,
    l.sog,
    l.cog,
    l.trueHeading
FROM latest_message l

ORDER BY timestamp
LIMIT 2000;
    `, userID, pastTime.UTC().Format(time.RFC3339), userID)

    // Query the database for the results
    rows, err := QueryDatabaseForUser(userID, query)
    if err != nil {
        return nil, fmt.Errorf("error querying database for user %s: %v", userID, err)
    }
    defer rows.Close()

    // Process the query results
    var results []map[string]interface{}
    for rows.Next() {
        var timestamp time.Time
        var latitude, longitude, sog, cog, trueHeading float64

        // Scan the row for the columns
        err := rows.Scan(&timestamp, &latitude, &longitude, &sog, &cog, &trueHeading)
        if err != nil {
            return nil, fmt.Errorf("error scanning row: %v", err)
        }

        // Add the valid result to the list
        results = append(results, map[string]interface{}{
            "timestamp":   timestamp,
            "latitude":    latitude,
            "longitude":   longitude,
            "sog":         sog,
            "cog":         cog,
            "trueHeading": trueHeading,
        })
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
    if cc.wsClient != nil {
        return cc.wsClient, nil
    }

    // use the clientSocket alias, not “socket”
    opts := clientSocket.DefaultOptions()
    opts.SetTransports(types.NewSet(transports.Polling, transports.WebSocket))

    manager := clientSocket.NewManager(
        fmt.Sprintf("http://%s:%d", cc.WSHost, cc.WSPort),
        opts,
    )

    // grab the “/” (or custom) namespace
    cli := manager.Socket("/", opts)

    // attach basic error handlers if you like:
    manager.On("error", func(errs ...any) {
        log.Printf("collector WS manager error: %v", errs)
    })

    cc.wsClient = cli
    return cli, nil
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
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

    // Now call the function that generates the summary, passing the UserID filter if provided
    summarizedResults, err := getSummaryResults(lat, lon, radius, limit, maxAge, minSpeed, userid)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
        return
    }

    // Send the summarized results as JSON
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(summarizedResults); err != nil {
        http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
    }
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
    var requestBody struct {
        Query  string `json:"query"`
        MaxAge int    `json:"maxAge"`
    }

    if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
        http.Error(w, fmt.Sprintf("Error parsing JSON: %v", err), http.StatusBadRequest)
        return
    }
    if len(requestBody.Query) < 3 {
        http.Error(w, "Query must be at least 3 characters long", http.StatusBadRequest)
        return
    }
    if requestBody.MaxAge > 168 {
        requestBody.MaxAge = 168
    }

    // compute the cutoff
    currentTime := time.Now()
    cutoff := currentTime.Add(-time.Duration(requestBody.MaxAge) * time.Hour).UTC().Format(time.RFC3339)

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
        LIMIT 100
    `, requestBody.Query, cutoff)

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
                "CallSign":   getFieldString(packetMap, "CallSign"),
                "ImoNumber":  getFieldFloat(packetMap, "ImoNumber"),
                "Name":       name,
                "NumMessages": getFieldInt(row, "count"),
                "UserID":     getFieldFloat(packetMap, "UserID"),
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

func historyHandler(w http.ResponseWriter, r *http.Request) {
    // Extract user_id from URL path
    parts := strings.Split(r.URL.Path, "/")
    if len(parts) != 3 {
        http.Error(w, "Invalid URL format", http.StatusBadRequest)
        return
    }

    userID := parts[2]

    // Extract maxAge from query parameters
    maxAgeStr := r.URL.Query().Get("maxAge")
    if maxAgeStr == "" {
        http.Error(w, "Missing maxAge query parameter", http.StatusBadRequest)
        return
    }

    // Convert maxAge from string to integer
    maxAge, err := strconv.Atoi(maxAgeStr)
    if err != nil || maxAge <= 0 {
        http.Error(w, "Invalid maxAge value", http.StatusBadRequest)
        return
    }

    // Limit maxAge to 1 week (168 hours)
    if maxAge > 168 {
        http.Error(w, "maxAge cannot exceed 168 hours (1 week)", http.StatusBadRequest)
        return
    }

    // Get the history results based on maxAge
    results, err := getHistoryResults(userID, maxAge)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error fetching history: %v", err), http.StatusInternalServerError)
        return
    }

    for _, row := range results {
        if th, ok := row["trueHeading"].(float64); ok && th == NoTrueHeading {
            delete(row, "trueHeading")
        }
    }

    // Check if the format query parameter is set to "csv"
    format := r.URL.Query().Get("format")
    if format == "csv" {
        // Convert the results to CSV format without column headings
        var csvData string
        for _, row := range results {
            timestamp := row["timestamp"].(time.Time).Format(time.RFC3339)
            latitude := fmt.Sprintf("%f", row["latitude"].(float64))
            longitude := fmt.Sprintf("%f", row["longitude"].(float64))
            sog := fmt.Sprintf("%.2f", row["sog"].(float64)) // Rounded to 2 decimal places
            cog := fmt.Sprintf("%.2f", row["cog"].(float64)) // Rounded to 2 decimal places

            trueHeadingStr := ""
            if th, ok := row["trueHeading"].(float64); ok {
                trueHeadingStr = fmt.Sprintf("%.2f", th)
            }

            csvData += fmt.Sprintf("%s,%s,%s,%s,%s,%s\n", timestamp, latitude, longitude, sog, cog, trueHeadingStr)
        }

        // Set the Content-Type header to "text/csv"
        w.Header().Set("Content-Type", "text/csv")
        w.Header().Set("Content-Disposition", "attachment; filename=history.csv")

        // Write the CSV data to the response (no column headings)
        w.Write([]byte(csvData))
    } else {
        // Convert the results to JSON format
        w.Header().Set("Content-Type", "application/json")
        jsonData, err := json.Marshal(results)
        if err != nil {
            http.Error(w, fmt.Sprintf("Error marshaling results to JSON: %v", err), http.StatusInternalServerError)
            return
        }

        // Write the JSON data to the response
        w.Write(jsonData)
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
    return ""  // Return empty string if field is nil or not found
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

// Helper function to safely parse any JSON field into a map (returns nil if parsing fails)
func getFieldJSON(packetData map[string]interface{}, field string) map[string]interface{} {
    if value, ok := packetData[field].(map[string]interface{}); ok {
        return value  // If the value is already a map, return it directly
    }
    // If the value is a string (and not empty), try unmarshalling it
    if value, ok := packetData[field].(string); ok && value != "" {
        var jsonData map[string]interface{}
        if err := json.Unmarshal([]byte(value), &jsonData); err == nil {
            return jsonData
        }
    }
    return nil  // Return nil if the field is missing or cannot be parsed
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

    // Log the parameters for debugging
    log.Printf("Client %s requested summary with params: latitude=%.6f, longitude=%.6f, radius=%.2f, maxResults=%d, maxAge=%d, minSpeed=%.2f, UserID=%d",
        client.Id(), lat, lon, radius, limit, maxAge, minSpeed, userid)

    // Now call the function that generates the summary
    summarizedResults, err := getSummaryResults(lat, lon, radius, limit, maxAge, minSpeed, userid)
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
    // Extract UserID from URL
    userID := r.URL.Path[len("/state/"):]

   // Modify the SQL query to include the 'count' column and fetch unique message_ids using ARRAY_AGG
   query := fmt.Sprintf(`
       SELECT 
           state.packet, 
           state.timestamp, 
           state.ais_class, 
           state.count, 
           state.name,
           state.image_url,
           ARRAY_AGG(DISTINCT messages.message_id) AS message_types
       FROM state
       LEFT JOIN messages ON state.user_id = messages.user_id
       WHERE state.user_id = '%s'
       GROUP BY 
           state.packet,
           state.timestamp,
           state.ais_class,
           state.count,
           state.name,
           state.image_url;
   `, userID)

    // Query the database for the specific user state and message_ids
    rows, err := QueryDatabaseForUser(userID, query)
    if err != nil {
        http.Error(w, fmt.Sprintf("Error querying database: %v", err), http.StatusInternalServerError)
        return
    }
    defer rows.Close()

    // Create a map to hold the merged results
    mergedResults := make(map[string]interface{})

    // Read the result and add packet data to mergedResults map
    var packetData map[string]interface{}
    for rows.Next() {
	var packet, timestamp, aisClass string
	var count int
	var dbName sql.NullString
	var dbImageURL sql.NullString
	var messageTypes pq.StringArray

        // Scan the result into the respective variables
        if err := rows.Scan(&packet, &timestamp, &aisClass, &count, &dbName, &dbImageURL, &messageTypes); err != nil {
            http.Error(w, fmt.Sprintf("Error scanning result: %v", err), http.StatusInternalServerError)
            return
        }

        // Unmarshal the packet into a map
        if err := json.Unmarshal([]byte(packet), &packetData); err != nil {
            http.Error(w, fmt.Sprintf("Error unmarshalling packet data: %v", err), http.StatusInternalServerError)
            return
        }

        if ext, ok := packetData["NameExtension"].(string); ok && ext != "" {
           base := ""
           if b, ok2 := packetData["Name"].(string); ok2 {
               base = b
           }
           packetData["Name"] = base + ext
       }
       delete(packetData, "NameExtension")

    	if _, exists := packetData["TrueHeading"]; exists {
             if getFieldFloat(packetData, "TrueHeading") == NoTrueHeading {
        	delete(packetData, "TrueHeading")
	     }
	}

        // Add AISClass, LastUpdated, NumMessages, and MessageTypes to the packet data
        packetData["AISClass"]    = aisClass
        packetData["LastUpdated"] = timestamp
        packetData["NumMessages"] = count
        packetData["MessageTypes"] = messageTypes

        // override Name from DB if present
        if dbName.Valid && dbName.String != "" {
            packetData["Name"] = dbName.String
        }
        // add ImageURL from DB if present
        if dbImageURL.Valid {
            packetData["ImageURL"] = dbImageURL.String
        }
        // default Name to AISClass + " Class" if empty
        if nm, ok := packetData["Name"].(string); !ok || nm == "" {
            if classVal, ok := packetData["AISClass"].(string); ok {
                packetData["Name"] = fmt.Sprintf("%s (%s)", classVal, userID)
            }
        }

        // Set the packet data directly as the response
        mergedResults = packetData
    }

    // Send the response as JSON
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(mergedResults); err != nil {
        http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
    }
}



// Utility function to handle debug logging
func logWithDebug(debug bool, format string, args ...interface{}) {
	if debug {
		log.Printf("[DEBUG] "+format, args...)
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
		Clients        []Client `json:"clients"`
		ConfiguredShards int     `json:"configured_shards"` // Store the configured_shards
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
	// Fetch client list from ingester
	clients, err := getClients(ingesterHost, ingesterPort, debug)
	if err != nil {
		log.Printf("Error fetching clients: %v", err)
		return
	}
	logWithDebug(debug, "Ingester clients: %+v", clients)
	// Map to track existing clients
	existingClients := make(map[string]Client)

	// Process each client
	for _, client := range clients {
		existingClients[client.Description] = client

		// Check if the client already has an open database connection
		if _, exists := clientConnections[client.Description]; !exists {
			// New client or client has lost its connection, so we connect
			logWithDebug(debug, "New client %s detected, connecting to its database.", client.Description)
			clientSettings, err := getClientDatabaseSettings(client.Ip, client.Port, debug)
			if err != nil {
				log.Printf("Error fetching settings for client %s: %v", client.Description, err)
				continue
			}
			// Connect to the database for the new client
			db, err := connectToDatabase(clientSettings)
			if err != nil {
				log.Printf("Error connecting to database for client %s: %v", client.Description, err)
				continue
			}
			clientConnectionsMu.Lock()
			wsPort, err := fetchCollectorWSPort(client.Ip, client.Port)
			if err != nil {
	                    log.Printf("cannot fetch socketio_listen for %s: %v", client.Ip, err)
           	     	    continue

		        }
                        clientConnections[client.Description] = &ClientConnection{
				Db:         db,
				DbHost:     clientSettings.DbHost,
				DbPort:     clientSettings.DbPort,
				DbUser:     clientSettings.DbUser,
				DbPass:     clientSettings.DbPass,
				DbName:     clientSettings.DbName,
				Shards:     client.Shards,
			        WSHost:     client.Ip,
			        WSPort:     wsPort,
			}
			clientConnectionsMu.Unlock()
			log.Printf("Successfully connected to database for client %s: %s@%s:%d/%s",
				client.Description, clientSettings.DbUser, clientSettings.DbHost, clientSettings.DbPort, clientSettings.DbName)
		}
	}

	// Handle clients that have been removed (no longer in the list)
	for description, conn := range clientConnections {
		if _, exists := existingClients[description]; !exists {
			// Client has been removed, disconnect from its database
			log.Printf("Client %s has been removed, disconnecting.", description)
			conn.Db.Close()
			clientConnectionsMu.Lock()
	                delete(clientConnections, description)
            	        clientConnectionsMu.Unlock()
		}
	}
}

// fetch WS port from collector’s own /settings
func fetchCollectorWSPort(host string, httpPort int) (int, error) {
    resp, err := http.Get(fmt.Sprintf("http://%s:%d/settings", host, httpPort))
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()

    var cfg struct { SocketIOListen int `json:"socketio_listen"` }
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
func setupServer(settings *Settings) {
    mux := http.NewServeMux()

    // Reverse-proxy for /receivers
    receiversTarget, err := url.Parse(settings.ReceiversBaseURL)
    if err != nil {
        log.Fatalf("invalid receivers_base_url: %v", err)
    }
    mux.Handle("/receivers", httputil.NewSingleHostReverseProxy(receiversTarget))

    // HTTP API endpoints
    mux.HandleFunc("/summary", summaryHandler)
    mux.HandleFunc("/state/", userStateHandler)
    mux.HandleFunc("/history/", historyHandler)
    mux.HandleFunc("/search", searchHandler)

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

        // Track connected client
        connectedClientsMu.Lock()
        connectedClients[client.Id()] = client
        connectedClientsMu.Unlock()

        // Init this client's subscription set
        clientSubscriptions[client.Id()] = make(map[string]struct{})

        // —— ais_sub/:userID ——
        client.On("ais_sub/:userID", func(raw ...any) {
    // 1) Extract the requested vessel ID from the subscription call
    userID := raw[0].(string)
    log.Printf("Client %s subscribes to %s", client.Id(), userID)
    clientSubscriptions[client.Id()][userID] = struct{}{}

    // 2) Find the collector connection responsible for this shard
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

    // 3) Dial (or reuse) the collector WebSocket and forward our subscription
    ws, err := cc.getWSClient()
    if err != nil {
        log.Printf("Error dialing collector WS: %v", err)
        return
    }
    ws.Emit("ais_sub/:userID", userID)

    // 4) Register a single ais_data handler on this collector socket
    if !wsHandlersRegistered[ws] {
        wsHandlersRegistered[ws] = true
        ws.On("ais_data", func(msg ...any) {
            // a) extract the MMSI from the incoming payload
            var msgUserID string
            if len(msg) > 0 {
                if m, ok := msg[0].(map[string]interface{}); ok {
                    if f, ok2 := m["UserID"].(float64); ok2 {
                        msgUserID = strconv.FormatInt(int64(f), 10)
                    } else if s, ok2 := m["UserID"].(string); ok2 {
                        msgUserID = s
                    }
                }
                if msgUserID == "" {
                    if s, ok := msg[0].(string); ok {
                        var m2 map[string]interface{}
                        if err := json.Unmarshal([]byte(s), &m2); err == nil {
                            if f, ok2 := m2["UserID"].(float64); ok2 {
                                msgUserID = strconv.FormatInt(int64(f), 10)
                            } else if ss, ok2 := m2["UserID"].(string); ok2 {
                                msgUserID = ss
                            }
                        }
                    }
                }
            }
            if msgUserID == "" {
                return // cannot route without a UserID
            }

            // b) wrap the raw AIS payload in a top‐level "data" field
            var rawPayload any
            if len(msg) == 1 {
                rawPayload = msg[0]
            } else {
                rawPayload = msg
            }
            wrapped := map[string]any{"data": rawPayload}

            // c) forward to each browser client subscribed to this MMSI
            connectedClientsMu.RLock()
            for sid, sock := range connectedClients {
                if subs, exists := clientSubscriptions[sid]; exists {
                    if _, subscribed := subs[msgUserID]; subscribed {
                        sock.Emit("ais_data",           wrapped)
                        sock.Emit(fmt.Sprintf("ais_data/%s", msgUserID), wrapped)
                    }
                }
            }
            connectedClientsMu.RUnlock()
        })
    }
})


        // —— ais_unsub/:userID ——
        client.On("ais_unsub/:userID", func(raw ...any) {
            userID := raw[0].(string)
            log.Printf("Client %s unsubscribes from %s", client.Id(), userID)
            delete(clientSubscriptions[client.Id()], userID)

            // forward unsubscribe
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
                    ws.Emit("ais_unsub/:userID", userID)
                }
            }
        })

        // —— requestSummary ——
        client.On("requestSummary", func(args ...any) {
            if len(args) < 1 {
                log.Printf("No data received with 'requestSummary'")
                return
            }
            dataStr, ok := args[0].(string)
            if !ok {
                log.Printf("Invalid data format for 'requestSummary'")
                return
            }
            var data map[string]interface{}
            if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
                log.Printf("Error unmarshalling data: %v", err)
                return
            }

            lat, _ := data["latitude"].(float64)
            lon, _ := data["longitude"].(float64)
            radius, _ := data["radius"].(float64)
            maxResults, _ := data["maxResults"].(float64)
            maxAge, _ := data["maxAge"].(float64)
            minSpeed, _ := data["minSpeed"].(float64)
            updatePeriod, _ := data["updatePeriod"].(float64)
            userIDf, _ := data["UserID"].(float64)
            if updatePeriod == 0 {
                updatePeriod = 5
            }

            clientSummaryMu.Lock()
            clientSummaryFilters[client.Id()] = FilterParams{
                Latitude:     lat,
                Longitude:    lon,
                Radius:       radius,
                MaxResults:   int(maxResults),
                MaxAge:       int(maxAge),
                MinSpeed:     minSpeed,
                UpdatePeriod: int(updatePeriod),
                UserID:       int64(userIDf),
                LastUpdated:  time.Now(),
            }
            clientSummaryMu.Unlock()

            summarized, err := getSummaryResults(
                lat, lon, radius,
                int(maxResults), int(maxAge), minSpeed, int64(userIDf),
            )
            if err != nil {
                log.Printf("Error fetching summary: %v", err)
                return
            }
            bs, _ := json.Marshal(summarized)
            client.Emit("summaryData", string(bs))
        })

        // —— disconnect cleanup ——
        client.On("disconnect", func(...any) {
            delete(clientSubscriptions, client.Id())

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
    go startHTTPServer(settings.ListenPort, mux)
}

func main() {
	// Load configuration settings
	settingsFile := "settings.json"
	settings, err := loadSettings(settingsFile)
	if err != nil {
		log.Fatalf("Error loading settings: %v", err)
	}

	// Initialize map to track client database connections
	clientConnections = make(map[string]*ClientConnection)

	// Set up polling interval
	pollInterval := time.Duration(settings.PollInterval) * time.Second
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
	            logWithDebug(settings.Debug, "Polling clients at %v", time.Now())
	            handleClientChanges(settings.IngestHost, settings.IngestPort, settings.Debug)
	        }()
	    }
	  }()
	}

	// Initial fetch and setup
	handleClientChanges(settings.IngestHost, settings.IngestPort, settings.Debug)

	// Set up the server with HTTP and Socket.IO routes
	setupServer(settings)

	go func() {
	    ticker := time.NewTicker(1 * time.Second) // Check every second
	    defer ticker.Stop()

	    for {
	        <-ticker.C

        	// Iterate over each client and check if it's time to send an update
        	clientSummaryMu.RLock()
	        snapshot := make(map[serverSocket.SocketId]FilterParams, len(clientSummaryFilters))
	        for id, params := range clientSummaryFilters {
	            snapshot[id] = params
	        }
	        clientSummaryMu.RUnlock()
	
	        for clientID, settings := range snapshot {

	            // Check if the update period has elapsed
	            if time.Since(settings.LastUpdated) >= time.Duration(settings.UpdatePeriod)*time.Second {
	                // Retrieve the client from the connectedClients map using the clientID
            		connectedClientsMu.RLock()
		        client, exists := connectedClients[clientID]
		        connectedClientsMu.RUnlock()
	                if exists {
	                    // Use the stored parameters for that client
	                    summarizedResults, err := getSummaryResults(
	                        settings.Latitude, settings.Longitude, settings.Radius,
	                        settings.MaxResults, settings.MaxAge, settings.MinSpeed, settings.UserID,
	                    )
	                    if err != nil {
	                        log.Printf("Error fetching summary for client %s: %v", clientID, err)
	                        continue
	                    }

	                    summaryJSON, err := json.Marshal(summarizedResults)
	                    if err != nil {
	                        log.Printf("Error marshaling summary data for client %s: %v", clientID, err)
	                        continue
        	            }

        	            // Send the summary data to the client
        	            if err := client.Emit("summaryData", string(summaryJSON)); err != nil {
	                        log.Printf("Error sending summary data to client %s: %v", clientID, err)
        	            }

        	            // Update the last sent time only after successfully sending the update
        	            clientSummaryMu.Lock()
			    clientSummaryFilters[clientID] = FilterParams{
        	                Latitude:    settings.Latitude,
        	                Longitude:   settings.Longitude,
        	                Radius:      settings.Radius,
        	                MaxResults:  settings.MaxResults,
        	                MaxAge:      settings.MaxAge,
        	                MinSpeed:    settings.MinSpeed,
        	                UpdatePeriod: settings.UpdatePeriod,
				UserID:	     settings.UserID,
        	                LastUpdated: time.Now(),  // Update only after sending
        	            }
			    clientSummaryMu.Unlock()
        	        } else {
        	            log.Printf("Client %s not found", clientID)
        	        }
        	    }
        	}
	    }
	}()

	// Block forever (or handle gracefully shutting down the server if necessary)
	select {}
}
