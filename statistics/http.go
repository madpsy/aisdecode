package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sort"
    "strconv"
    "time"
)

// vessel is the JSON shape returned by /statistics/stats/top-sog.
type vessel struct {
    UserID    int       `json:"user_id"`
    Name      string    `json:"name,omitempty"`
    ImageURL  string    `json:"image_url,omitempty"`
    AISClass  string    `json:"ais_class,omitempty"` // new
    Type      string    `json:"type,omitempty"`      // new
    MaxSog    float64   `json:"max_sog,omitempty"`
    Timestamp time.Time `json:"timestamp,omitempty"`
    Lat       float64   `json:"lat,omitempty"`
    Lon       float64   `json:"lon,omitempty"`
    Count     int       `json:"count,omitempty"`
    Distance  int       `json:"distance,omitempty"`  // for top-distance
}

// distanceVessel is the JSON shape returned by /statistics/stats/top-distance.
type distanceVessel struct {
    UserID    int       `json:"user_id"`
    Name      string    `json:"name,omitempty"`
    ImageURL  string    `json:"image_url,omitempty"`
    AISClass  string    `json:"ais_class,omitempty"`
    Type      string    `json:"type,omitempty"`
    Distance  int       `json:"distance"`
    Timestamp time.Time `json:"timestamp,omitempty"`
    Lat       float64   `json:"lat,omitempty"`
    Lon       float64   `json:"lon,omitempty"`
    ReceiverID int      `json:"receiver_id,omitempty"`
    ReceiverName string `json:"receiver_name,omitempty"`
}

// typeCount is the JSON shape returned by /statistics/stats/top-types.
type typeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// classCount is the JSON shape returned by /statistics/stats/top-classes.
type classCount struct {
	Class string `json:"class"`
	Count int    `json:"count"`
}

// userCount is the JSON shape returned by /statistics/stats/user-counts.
type userCount struct {
	UserID int    `json:"user_id"`
	Name   string `json:"name,omitempty"`
	Count  int    `json:"count"`
}

// posVessel is the JSON shape returned by /statistics/stats/top-positions.
type posVessel struct {
    UserID   int    `json:"user_id"`
    Name     string `json:"name"`
    ImageURL string `json:"image_url"`
    AISClass string `json:"ais_class,omitempty"` // new
    Type     string `json:"type,omitempty"`      // new
    Count    int    `json:"count"`
}

// enrichVessels looks up and injects Name/ImageURL/AISClass/Type for each vessel in-place.
func enrichVessels(vs []vessel) []vessel {
    ids := make([]int, len(vs))
    for i, v := range vs {
        ids[i] = v.UserID
    }
    meta, err := fetchVesselMetadata(ids)
    if err != nil {
        log.Printf("enrichVessels: metadata fetch error: %v", err)
        return vs
    }
    for i := range vs {
        if m, ok := meta[vs[i].UserID]; ok {
            vs[i].Name     = m.Name
            vs[i].ImageURL = m.ImageURL
            vs[i].AISClass = m.AISClass
            vs[i].Type     = m.Type
        }
    }
    return vs
}

// enrichPosVessels looks up and injects Name/ImageURL/AISClass/Type for each posVessel in-place.
func enrichPosVessels(ps []posVessel) []posVessel {
    ids := make([]int, len(ps))
    for i, v := range ps {
        ids[i] = v.UserID
    }
    meta, err := fetchVesselMetadata(ids)
    if err != nil {
        log.Printf("enrichPosVessels: metadata fetch error: %v", err)
        return ps
    }
    for i := range ps {
        if m, ok := meta[ps[i].UserID]; ok {
            ps[i].Name     = m.Name
            ps[i].ImageURL = m.ImageURL
            ps[i].AISClass = m.AISClass
            ps[i].Type     = m.Type
        }
    }
    return ps
}

func StartServer(port int) {
    mux := http.NewServeMux()
    registerHandlers(mux)
    addr := fmt.Sprintf(":%d", port)
    log.Printf("Listening on %s...", addr)
    log.Fatal(http.ListenAndServe(addr, mux))
}

func registerHandlers(mux *http.ServeMux) {
	fs := http.FileServer(http.Dir("web"))
	mux.Handle("/statistics/", http.StripPrefix("/statistics/", fs))

	mux.HandleFunc("/statistics/stats/top-sog", topSogHandler)
	mux.HandleFunc("/statistics/stats/top-types", topTypesHandler)
	mux.HandleFunc("/statistics/stats/top-classes", topClassesHandler)
	mux.HandleFunc("/statistics/stats/top-positions", topPositionsHandler)
	mux.HandleFunc("/statistics/stats/top-distance", topDistanceHandler)
	mux.HandleFunc("/statistics/stats/user-counts", userCountsHandler)
	mux.HandleFunc("/statistics/stats/coverage-map", coverageMapHandler)
	mux.HandleFunc("/statistics/stats/time-series", timeSeriesHandler)
}

func topSogHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    receiverID := parseReceiverIDParam(r)
    
    // Include receiver_id in cache key if specified
    var cacheKey string
    if receiverID > 0 {
        cacheKey = fmt.Sprintf("top-sog:%dd:r%d", days, receiverID)
    } else {
        cacheKey = fmt.Sprintf("top-sog:%dd", days)
    }

    var vs []vessel
    if ok, _ := cacheGet(cacheKey, &vs); ok {
        vs = enrichVessels(vs)
        respondJSON(w, vs)
        return
    }

    // Build query with optional receiver_id filter
    var qry string
    if receiverID > 0 {
        qry = fmt.Sprintf(`
            SELECT DISTINCT ON (m.user_id)
                   m.user_id,
                   (m.packet->>'Sog')::float       AS max_sog,
                   m.timestamp,
                   (m.packet->>'Latitude')::float  AS lat,
                   (m.packet->>'Longitude')::float AS lon
              FROM messages m
             WHERE m.message_id IN (1,2,3,18,19)
               AND (m.packet->>'Sog')::float <> 102.3
               AND m.timestamp >= now() - INTERVAL '%d days'
               AND m.receiver_id = %d
             ORDER BY m.user_id, max_sog DESC
        `, days, receiverID)
    } else {
        qry = fmt.Sprintf(`
            SELECT DISTINCT ON (m.user_id)
                   m.user_id,
                   (m.packet->>'Sog')::float       AS max_sog,
                   m.timestamp,
                   (m.packet->>'Latitude')::float  AS lat,
                   (m.packet->>'Longitude')::float AS lon
              FROM messages m
             WHERE m.message_id IN (1,2,3,18,19)
               AND (m.packet->>'Sog')::float <> 102.3
               AND m.timestamp >= now() - INTERVAL '%d days'
             ORDER BY m.user_id, max_sog DESC
        `, days)
    }

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }

    maxMap := make(map[int]struct {
        sog float64
        ts  time.Time
        lat float64
        lon float64
    })
    for _, recs := range shardResults {
        for _, rec := range recs {
            uid, _ := parseInt(rec["user_id"])
            sog, _ := parseFloat(rec["max_sog"])
            ts, _ := parseTime(rec["timestamp"])
            lat, _ := parseFloat(rec["lat"])
            lon, _ := parseFloat(rec["lon"])
            prev := maxMap[uid]
            if sog > prev.sog {
                maxMap[uid] = struct {
                    sog float64
                    ts  time.Time
                    lat float64
                    lon float64
                }{sog, ts, lat, lon}
            }
        }
    }

    vs = make([]vessel, 0, len(maxMap))
    for uid, d := range maxMap {
        vs = append(vs, vessel{
            UserID:    uid,
            MaxSog:    d.sog,
            Timestamp: d.ts,
            Lat:       d.lat,
            Lon:       d.lon,
        })
    }

    sort.Slice(vs, func(i, j int) bool {
        return vs[i].MaxSog > vs[j].MaxSog
    })
    if len(vs) > 10 {
        vs = vs[:10]
    }

    vs = enrichVessels(vs)
    cacheSet(cacheKey, vs)
    respondJSON(w, vs)
}

func topTypesHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    receiverID := parseReceiverIDParam(r)
    
    // Include receiver_id in cache key if specified
    var cacheKey string
    if receiverID > 0 {
        cacheKey = fmt.Sprintf("top-types:%dd:r%d", days, receiverID)
    } else {
        cacheKey = fmt.Sprintf("top-types:%dd", days)
    }

    var counts []typeCount
    if ok, _ := cacheGet(cacheKey, &counts); ok {
        respondJSON(w, counts)
        return
    }

    // Build query with optional receiver_id filter
    var qry string
    if receiverID > 0 {
        qry = fmt.Sprintf(`
            SELECT (packet->>'Type') AS vessel_type,
                   COUNT(*)               AS cnt
              FROM state
             WHERE timestamp >= now() - INTERVAL '%d days'
               AND (packet->>'Type') IS NOT NULL
               AND TRIM((packet->>'Type')) <> ''
               AND receiver_id = %d
             GROUP BY vessel_type
        `, days, receiverID)
    } else {
        qry = fmt.Sprintf(`
            SELECT (packet->>'Type') AS vessel_type,
                   COUNT(*)               AS cnt
              FROM state
             WHERE timestamp >= now() - INTERVAL '%d days'
               AND (packet->>'Type') IS NOT NULL
               AND TRIM((packet->>'Type')) <> ''
             GROUP BY vessel_type
        `, days)
    }

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }

    agg := make(map[string]int)
    for _, recs := range shardResults {
        for _, rec := range recs {
            typ, _ := parseString(rec["vessel_type"])
            if typ == "" {
                continue
            }
            cnt, _ := parseInt(rec["cnt"])
            agg[typ] += cnt
        }
    }

    counts = make([]typeCount, 0, len(agg))
    for typ, cnt := range agg {
        counts = append(counts, typeCount{Type: typ, Count: cnt})
    }

    sort.Slice(counts, func(i, j int) bool {
        return counts[i].Count > counts[j].Count
    })
    if len(counts) > 10 {
        counts = counts[:10]
    }

    cacheSet(cacheKey, counts)
    respondJSON(w, counts)
}

func topClassesHandler(w http.ResponseWriter, r *http.Request) {
        days := parseDaysParam(r)
        receiverID := parseReceiverIDParam(r)
        
        // Include receiver_id in cache key if specified
        var cacheKey string
        if receiverID > 0 {
            cacheKey = fmt.Sprintf("top-classes:%dd:r%d", days, receiverID)
        } else {
            cacheKey = fmt.Sprintf("top-classes:%dd", days)
        }
    
        var counts []classCount
        if ok, _ := cacheGet(cacheKey, &counts); ok {
            respondJSON(w, counts)
            return
        }
    
        // Build query with optional receiver_id filter
        var qry string
        if receiverID > 0 {
            qry = fmt.Sprintf(`
                SELECT ais_class AS vessel_class,
                       COUNT(*)  AS cnt
                  FROM state
                 WHERE timestamp >= now() - INTERVAL '%d days'
                   AND ais_class IS NOT NULL
                   AND TRIM(ais_class) <> ''
                   AND receiver_id = %d
                 GROUP BY vessel_class
            `, days, receiverID)
        } else {
            qry = fmt.Sprintf(`
                SELECT ais_class AS vessel_class,
                       COUNT(*)  AS cnt
                  FROM state
                 WHERE timestamp >= now() - INTERVAL '%d days'
                   AND ais_class IS NOT NULL
                   AND TRIM(ais_class) <> ''
                 GROUP BY vessel_class
            `, days)
        }
    
        shardResults, err := QueryDatabasesForAllShards(qry)
        if err != nil {
            respondError(w, err)
            return
        }
    
        agg := make(map[string]int)
        for _, recs := range shardResults {
            for _, rec := range recs {
                class, _ := parseString(rec["vessel_class"])
                if class == "" {
                    continue
                }
                cnt, _ := parseInt(rec["cnt"])
                agg[class] += cnt
            }
        }
    
        counts = make([]classCount, 0, len(agg))
        for class, cnt := range agg {
            counts = append(counts, classCount{Class: class, Count: cnt})
        }
    
        sort.Slice(counts, func(i, j int) bool {
            return counts[i].Count > counts[j].Count
        })
        if len(counts) > 10 {
            counts = counts[:10]
        }
    
        cacheSet(cacheKey, counts)
        respondJSON(w, counts)
    }

func topPositionsHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    receiverID := parseReceiverIDParam(r)
    
    // Include receiver_id in cache key if specified
    var cacheKey string
    if receiverID > 0 {
        cacheKey = fmt.Sprintf("top-positions:%dd:r%d", days, receiverID)
    } else {
        cacheKey = fmt.Sprintf("top-positions:%dd", days)
    }

    var ps []posVessel
    if ok, _ := cacheGet(cacheKey, &ps); ok {
        respondJSON(w, ps)
        return
    }

    // Build query with optional receiver_id filter
    var qry string
    if receiverID > 0 {
        qry = fmt.Sprintf(`
            SELECT user_id,
                   COUNT(*) AS cnt
              FROM messages
             WHERE message_id IN (1,2,3,18,19)
               AND timestamp >= now() - INTERVAL '%d days'
               AND receiver_id = %d
             GROUP BY user_id
        `, days, receiverID)
    } else {
        qry = fmt.Sprintf(`
            SELECT user_id,
                   COUNT(*) AS cnt
              FROM messages
             WHERE message_id IN (1,2,3,18,19)
               AND timestamp >= now() - INTERVAL '%d days'
             GROUP BY user_id
        `, days)
    }

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }

    countMap := make(map[int]int)
    for _, recs := range shardResults {
        for _, rec := range recs {
            uid, _ := parseInt(rec["user_id"])
            cnt, _ := parseInt(rec["cnt"])
            countMap[uid] += cnt
        }
    }

    raw := make([]struct{ uid, cnt int }, 0, len(countMap))
    for uid, cnt := range countMap {
        raw = append(raw, struct{ uid, cnt int }{uid, cnt})
    }
    sort.Slice(raw, func(i, j int) bool {
        return raw[i].cnt > raw[j].cnt
    })
    if len(raw) > 10 {
        raw = raw[:10]
    }

    ps = make([]posVessel, len(raw))
    for i, r := range raw {
        ps[i] = posVessel{UserID: r.uid, Count: r.cnt}
    }

    ps = enrichPosVessels(ps)
    cacheSet(cacheKey, ps)
    respondJSON(w, ps)
}

// parseDaysParam reads 'days' query param, defaults to 1, max 30.
func parseDaysParam(r *http.Request) int {
    days := 1
    if d := r.URL.Query().Get("days"); d != "" {
        if n, err := strconv.Atoi(d); err == nil && n > 0 {
            if n > 30 {
                // Cap at 30 days to prevent excessive queries
                days = 30
            } else {
                days = n
            }
        }
    }
    return days
}

// parseReceiverIDParam reads 'receiver_id' query param, returns -1 if not specified.
func parseReceiverIDParam(r *http.Request) int {
    if rid := r.URL.Query().Get("receiver_id"); rid != "" {
        if n, err := strconv.Atoi(rid); err == nil && n > 0 {
            return n
        }
    }
    return -1 // -1 indicates no receiver_id filter
}

// respondJSON serializes v as JSON to the response.
func respondJSON(w http.ResponseWriter, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(v)
}

// respondError sends a 500 status with the error message.
func respondError(w http.ResponseWriter, err error) {
    http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
}

func topDistanceHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    receiverID := parseReceiverIDParam(r)
    
    // Include receiver_id in cache key if specified
    var cacheKey string
    if receiverID > 0 {
        cacheKey = fmt.Sprintf("top-distance:%dd:r%d", days, receiverID)
    } else {
        cacheKey = fmt.Sprintf("top-distance:%dd", days)
    }

    var vs []distanceVessel
    if ok, _ := cacheGet(cacheKey, &vs); ok {
        // Enrich with vessel metadata
        ids := make([]int, len(vs))
        for i, v := range vs {
            ids[i] = v.UserID
        }
        meta, err := fetchVesselMetadata(ids)
        if err != nil {
            log.Printf("topDistanceHandler: metadata fetch error: %v", err)
        } else {
            for i := range vs {
                if m, ok := meta[vs[i].UserID]; ok {
                    vs[i].Name = m.Name
                    vs[i].ImageURL = m.ImageURL
                    vs[i].AISClass = m.AISClass
                    vs[i].Type = m.Type
                }
            }
        }
        respondJSON(w, vs)
        return
    }

    // Build query with optional receiver_id filter and exclude SAR vessels
    var qry string
    if receiverID > 0 {
        qry = fmt.Sprintf(`
            SELECT DISTINCT ON (m.user_id, m.receiver_id)
                   m.user_id,
                   m.distance,
                   m.timestamp,
                   (m.packet->>'Latitude')::float  AS lat,
                   (m.packet->>'Longitude')::float AS lon,
                   m.receiver_id,
                   s.ais_class
               FROM messages m
               LEFT JOIN state s ON m.user_id = s.user_id
              WHERE m.distance IS NOT NULL
                AND m.distance <= 500000  -- Filter out spurious values over 500km
                AND m.timestamp >= now() - INTERVAL '%d days'
                AND m.receiver_id = %d
                AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
              ORDER BY m.user_id, m.receiver_id, m.distance DESC
        `, days, receiverID)
    } else {
        qry = fmt.Sprintf(`
            SELECT DISTINCT ON (m.user_id, m.receiver_id)
                   m.user_id,
                   m.distance,
                   m.timestamp,
                   (m.packet->>'Latitude')::float  AS lat,
                   (m.packet->>'Longitude')::float AS lon,
                   m.receiver_id,
                   s.ais_class
               FROM messages m
               LEFT JOIN state s ON m.user_id = s.user_id
              WHERE m.distance IS NOT NULL
                AND m.distance <= 500000  -- Filter out spurious values over 1000km
                AND m.timestamp >= now() - INTERVAL '%d days'
                AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
              ORDER BY m.user_id, m.receiver_id, m.distance DESC
        `, days)
    }

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }

    // Collect all results from all shards
    allResults := make([]struct {
        userID     int
        distance   int
        timestamp  time.Time
        lat, lon   float64
        receiverID int
        aisClass   string
    }, 0)
    
    for _, recs := range shardResults {
        for _, rec := range recs {
            uid, _ := parseInt(rec["user_id"])
            dist, _ := parseInt(rec["distance"])
            ts, _ := parseTime(rec["timestamp"])
            lat, _ := parseFloat(rec["lat"])
            lon, _ := parseFloat(rec["lon"])
            rid, _ := parseInt(rec["receiver_id"])
            aisClass, _ := parseString(rec["ais_class"])
            
            allResults = append(allResults, struct {
                userID     int
                distance   int
                timestamp  time.Time
                lat, lon   float64
                receiverID int
                aisClass   string
            }{uid, dist, ts, lat, lon, rid, aisClass})
        }
    }
    
    // Sort by distance (descending)
    sort.Slice(allResults, func(i, j int) bool {
        return allResults[i].distance > allResults[j].distance
    })
    
    // Take top 10
    if len(allResults) > 10 {
        allResults = allResults[:10]
    }

    // Convert to distanceVessel objects
    vs = make([]distanceVessel, len(allResults))
    for i, r := range allResults {
        vs[i] = distanceVessel{
            UserID:     r.userID,
            Distance:   r.distance,
            Timestamp:  r.timestamp,
            Lat:        r.lat,
            Lon:        r.lon,
            ReceiverID: r.receiverID,
            AISClass:   r.aisClass, // Set AIS class directly from query result
        }
    }
    
    // Enrich with vessel metadata (name, image URL, and type only - we already have AIS class)
    ids := make([]int, len(vs))
    for i, v := range vs {
        ids[i] = v.UserID
    }
    meta, err := fetchVesselMetadata(ids)
    if err != nil {
        log.Printf("topDistanceHandler: metadata fetch error: %v", err)
    } else {
        for i := range vs {
            if m, ok := meta[vs[i].UserID]; ok {
                vs[i].Name = m.Name
                vs[i].ImageURL = m.ImageURL
                // We already have AISClass from the query
                vs[i].Type = m.Type
            }
        }
    }

    cacheSet(cacheKey, vs)
    respondJSON(w, vs)
}

// userCountsHandler returns statistics about unique userIDs and their message counts.
func userCountsHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	
	// Include receiver_id in cache key if specified
	var cacheKey string
	if receiverID > 0 {
		cacheKey = fmt.Sprintf("user-counts:%dd:r%d", days, receiverID)
	} else {
		cacheKey = fmt.Sprintf("user-counts:%dd", days)
	}

	var users []userCount
	if ok, _ := cacheGet(cacheKey, &users); ok {
		// Enrich with vessel metadata
		ids := make([]int, len(users))
		for i, u := range users {
			ids[i] = u.UserID
		}
		meta, err := fetchVesselMetadata(ids)
		if err != nil {
			log.Printf("userCountsHandler: metadata fetch error: %v", err)
		} else {
			for i := range users {
				if m, ok := meta[users[i].UserID]; ok {
					users[i].Name = m.Name
				}
			}
		}
		respondJSON(w, users)
		return
	}

	// Build query with optional receiver_id filter
	var qry string
	if receiverID > 0 {
		qry = fmt.Sprintf(`
			SELECT user_id,
				   COUNT(*) AS cnt
			  FROM messages
			 WHERE timestamp >= now() - INTERVAL '%d days'
			   AND receiver_id = %d
			 GROUP BY user_id
			 ORDER BY cnt DESC
			 LIMIT 100
		`, days, receiverID)
	} else {
		qry = fmt.Sprintf(`
			SELECT user_id,
				   COUNT(*) AS cnt
			  FROM messages
			 WHERE timestamp >= now() - INTERVAL '%d days'
			 GROUP BY user_id
			 ORDER BY cnt DESC
			 LIMIT 100
		`, days)
	}

	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		respondError(w, err)
		return
	}

	countMap := make(map[int]int)
	for _, recs := range shardResults {
		for _, rec := range recs {
			uid, _ := parseInt(rec["user_id"])
			cnt, _ := parseInt(rec["cnt"])
			countMap[uid] += cnt
		}
	}

	users = make([]userCount, 0, len(countMap))
	for uid, cnt := range countMap {
		users = append(users, userCount{
			UserID: uid,
			Count:  cnt,
		})
	}

	// Sort by count in descending order
	sort.Slice(users, func(i, j int) bool {
		return users[i].Count > users[j].Count
	})

	// Limit to 100 results
	if len(users) > 100 {
		users = users[:100]
	}

	// Enrich with vessel metadata
	ids := make([]int, len(users))
	for i, u := range users {
		ids[i] = u.UserID
	}
	meta, err := fetchVesselMetadata(ids)
	if err != nil {
		log.Printf("userCountsHandler: metadata fetch error: %v", err)
	} else {
		for i := range users {
			if m, ok := meta[users[i].UserID]; ok {
				users[i].Name = m.Name
			}
		}
	}

	cacheSet(cacheKey, users)
	respondJSON(w, users)
}

// coverageMapHandler generates a grid-based coverage map for a specific receiver or all receivers
// Optional parameters:
// - receiver_id (default: all receivers)
// - days (default: 7)
func coverageMapHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    receiverId := parseReceiverIDParam(r)
    
    // Define cache key
    var cacheKey string
    if receiverId < 0 {
        cacheKey = fmt.Sprintf("coverage-map:%dd:all", days)
    } else {
        cacheKey = fmt.Sprintf("coverage-map:%dd:r%d", days, receiverId)
    }
    
    // Define the response structure
    type GridCell struct {
        Lat   float64 `json:"lat"`
        Lon   float64 `json:"lon"`
        Count int     `json:"count"`
    }
    
    var coverageData []GridCell
    
    // Try to get from cache
    if ok, _ := cacheGet(cacheKey, &coverageData); ok {
        respondJSON(w, coverageData)
        return
    }
    
    // Grid size in degrees (approximately 1km at the equator)
    const gridSize = 0.01
    
    // Build query with PostGIS functions
    var qry string
    if receiverId < 0 {
        // Query for all receivers
        qry = fmt.Sprintf(`
            SELECT
                ST_Y(ST_Centroid(ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                ))) AS lat,
                ST_X(ST_Centroid(ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                ))) AS lon,
                COUNT(*) AS count
            FROM messages
            WHERE message_id IN (1,2,3,18,19)
                AND timestamp >= now() - INTERVAL '%d days'
                AND (packet->>'Latitude')::float IS NOT NULL
                AND (packet->>'Longitude')::float IS NOT NULL
                AND (packet->>'Latitude')::float BETWEEN -90 AND 90
                AND (packet->>'Longitude')::float BETWEEN -180 AND 180
                AND distance IS NOT NULL AND distance <= 500000 -- Only include points with valid distances <= 500km
            GROUP BY
                ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                )
        `, gridSize, gridSize, days, gridSize)
    } else {
        // Query for a specific receiver
        qry = fmt.Sprintf(`
            SELECT
                ST_Y(ST_Centroid(ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                ))) AS lat,
                ST_X(ST_Centroid(ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                ))) AS lon,
                COUNT(*) AS count
            FROM messages
            WHERE message_id IN (1,2,3,18,19)
                AND receiver_id = %d
                AND timestamp >= now() - INTERVAL '%d days'
                AND (packet->>'Latitude')::float IS NOT NULL
                AND (packet->>'Longitude')::float IS NOT NULL
                AND (packet->>'Latitude')::float BETWEEN -90 AND 90
                AND (packet->>'Longitude')::float BETWEEN -180 AND 180
                AND distance IS NOT NULL AND distance <= 500000 -- Only include points with valid distances <= 500km
            GROUP BY
                ST_SnapToGrid(
                    ST_SetSRID(ST_MakePoint(
                        (packet->>'Longitude')::float,
                        (packet->>'Latitude')::float
                    ), 4326),
                    %f
                )
        `, gridSize, gridSize, receiverId, days, gridSize)
    }
    
    // Query all shards
    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }
    
    // Process results from all shards
    cellMap := make(map[string]GridCell)
    
    for _, recs := range shardResults {
        for _, rec := range recs {
            lat, _ := parseFloat(rec["lat"])
            lon, _ := parseFloat(rec["lon"])
            count, _ := parseInt(rec["count"])
            
            // Create a key for the grid cell to aggregate across shards
            key := fmt.Sprintf("%.6f:%.6f", lat, lon)
            
            if cell, exists := cellMap[key]; exists {
                cell.Count += count
                cellMap[key] = cell
            } else {
                cellMap[key] = GridCell{
                    Lat:   lat,
                    Lon:   lon,
                    Count: count,
                }
            }
        }
    }
    
    // Convert map to slice
    coverageData = make([]GridCell, 0, len(cellMap))
    for _, cell := range cellMap {
        coverageData = append(coverageData, cell)
    }
    
    // Cache the result
    cacheSet(cacheKey, coverageData)
    respondJSON(w, coverageData)
}

// TimeSeriesType represents the type of time series data
type TimeSeriesType string

const (
	TimeSeriesMessages TimeSeriesType = "messages"
	TimeSeriesVessels  TimeSeriesType = "vessels"
)

// TimeSeriesPeriod represents the time period for aggregation
type TimeSeriesPeriod string

const (
	TimeSeriesDaily   TimeSeriesPeriod = "daily"
	TimeSeriesMonthly TimeSeriesPeriod = "monthly"
	TimeSeriesYearly  TimeSeriesPeriod = "yearly"
)

// TimeSeriesDataPoint represents a single data point in the time series
type TimeSeriesDataPoint struct {
	Timestamp time.Time         `json:"timestamp"`
	Values    map[string]int    `json:"values"` // Key is message_id or ais_class
}

// TimeSeriesResponse is the JSON response for the time-series endpoint
type TimeSeriesResponse struct {
	Type     TimeSeriesType     `json:"type"`
	Period   TimeSeriesPeriod   `json:"period"`
	DataPoints []TimeSeriesDataPoint `json:"data_points"`
}

// parseTimeSeriesTypeParam reads 'type' query param, defaults to "messages"
func parseTimeSeriesTypeParam(r *http.Request) TimeSeriesType {
	typeParam := r.URL.Query().Get("type")
	if typeParam == "vessels" {
		return TimeSeriesVessels
	}
	return TimeSeriesMessages
}

// parseTimeSeriesPeriodParam reads 'period' query param, defaults to "daily"
func parseTimeSeriesPeriodParam(r *http.Request) TimeSeriesPeriod {
	periodParam := r.URL.Query().Get("period")
	switch periodParam {
	case "monthly":
		return TimeSeriesMonthly
	case "yearly":
		return TimeSeriesYearly
	default:
		return TimeSeriesDaily
	}
}

// timeSeriesHandler provides time series data for messages or vessels
// Query parameters:
// - type: "messages" or "vessels" (default: "messages")
// - period: "daily", "monthly", "yearly" (default: "daily")
// - days: number of days to look back (default: 1, max: 30 for daily, 365 for monthly, 1825 for yearly)
// - receiver_id: optional filter for a specific receiver
func timeSeriesHandler(w http.ResponseWriter, r *http.Request) {
	seriesType := parseTimeSeriesTypeParam(r)
	period := parseTimeSeriesPeriodParam(r)
	receiverID := parseReceiverIDParam(r)
	
	// Adjust max days based on period
	maxDays := 30
	if period == TimeSeriesMonthly {
		maxDays = 365
	} else if period == TimeSeriesYearly {
		maxDays = 1825 // ~5 years
	}
	
	// Parse days with custom max
	days := 1
	if d := r.URL.Query().Get("days"); d != "" {
		if n, err := strconv.Atoi(d); err == nil && n > 0 {
			if n > maxDays {
				days = maxDays
			} else {
				days = n
			}
		}
	}
	
	// Build cache key
	var cacheKey string
	if receiverID > 0 {
		cacheKey = fmt.Sprintf("time-series:%s:%s:%dd:r%d", seriesType, period, days, receiverID)
	} else {
		cacheKey = fmt.Sprintf("time-series:%s:%s:%dd", seriesType, period, days)
	}
	
	var response TimeSeriesResponse
	if ok, _ := cacheGet(cacheKey, &response); ok {
		respondJSON(w, response)
		return
	}
	
	// Initialize response
	response = TimeSeriesResponse{
		Type:   seriesType,
		Period: period,
	}
	
	// Build and execute query based on type and period
	var err error
	if seriesType == TimeSeriesMessages {
		response.DataPoints, err = fetchMessageTimeSeries(period, days, receiverID)
	} else {
		response.DataPoints, err = fetchVesselTimeSeries(period, days, receiverID)
	}
	
	if err != nil {
		respondError(w, err)
		return
	}
	
	cacheSet(cacheKey, response)
	respondJSON(w, response)
}

// fetchMessageTimeSeries retrieves message counts grouped by time period and message_id
func fetchMessageTimeSeries(period TimeSeriesPeriod, days int, receiverID int) ([]TimeSeriesDataPoint, error) {
	// Define time format and interval based on period
	var timeInterval string
	
	switch period {
	case TimeSeriesDaily:
		timeInterval = "1 hour"
	case TimeSeriesMonthly:
		timeInterval = "1 day"
	case TimeSeriesYearly:
		timeInterval = "1 month"
	}
	
	// Build query with optional receiver_id filter
	var qry string
	if receiverID > 0 {
		qry = fmt.Sprintf(`
			WITH time_series AS (
				SELECT
					generate_series(
						DATE_TRUNC('%s', NOW() - INTERVAL '%d days'),
						DATE_TRUNC('%s', NOW()),
						INTERVAL '%s'
					) AS ts
			)
			SELECT
				ts.ts AS timestamp,
				COALESCE(m.message_id, 0) AS message_id,
				COALESCE(COUNT(m.id), 0) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
				AND m.receiver_id = %d
			GROUP BY
				ts.ts, m.message_id
			ORDER BY
				ts.ts, m.message_id
		`,
		getIntervalName(period), days, getIntervalName(period), timeInterval,
		getIntervalName(period), receiverID)
	} else {
		qry = fmt.Sprintf(`
			WITH time_series AS (
				SELECT
					generate_series(
						DATE_TRUNC('%s', NOW() - INTERVAL '%d days'),
						DATE_TRUNC('%s', NOW()),
						INTERVAL '%s'
					) AS ts
			)
			SELECT
				ts.ts AS timestamp,
				COALESCE(m.message_id, 0) AS message_id,
				COALESCE(COUNT(m.id), 0) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
			GROUP BY
				ts.ts, m.message_id
			ORDER BY
				ts.ts, m.message_id
		`,
		getIntervalName(period), days, getIntervalName(period), timeInterval,
		getIntervalName(period))
	}
	
	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		return nil, err
	}
	
	// Process results
	timeMap := make(map[time.Time]map[string]int)
	
	for _, recs := range shardResults {
		for _, rec := range recs {
			ts, err := parseTime(rec["timestamp"])
			if err != nil {
				continue
			}
			
			msgID, err := parseInt(rec["message_id"])
			if err != nil {
				continue
			}
			
			count, err := parseInt(rec["count"])
			if err != nil {
				continue
			}
			
			// Convert message_id to string for the map key
			msgIDStr := fmt.Sprintf("%d", msgID)
			
			// Initialize map for this timestamp if needed
			if _, ok := timeMap[ts]; !ok {
				timeMap[ts] = make(map[string]int)
			}
			
			// Add count to existing value (aggregating across shards)
			timeMap[ts][msgIDStr] += count
		}
	}
	
	// Convert map to sorted slice
	var timestamps []time.Time
	for ts := range timeMap {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})
	
	// Build response
	dataPoints := make([]TimeSeriesDataPoint, len(timestamps))
	for i, ts := range timestamps {
		dataPoints[i] = TimeSeriesDataPoint{
			Timestamp: ts,
			Values:    timeMap[ts],
		}
	}
	
	return dataPoints, nil
}

// fetchVesselTimeSeries retrieves unique vessel counts grouped by time period and ais_class
func fetchVesselTimeSeries(period TimeSeriesPeriod, days int, receiverID int) ([]TimeSeriesDataPoint, error) {
	// Define time format and interval based on period
	var timeInterval string
	
	switch period {
	case TimeSeriesDaily:
		timeInterval = "1 hour"
	case TimeSeriesMonthly:
		timeInterval = "1 day"
	case TimeSeriesYearly:
		timeInterval = "1 month"
	}
	
	// Build query with optional receiver_id filter
	var qry string
	if receiverID > 0 {
		qry = fmt.Sprintf(`
			WITH time_series AS (
				SELECT
					generate_series(
						DATE_TRUNC('%s', NOW() - INTERVAL '%d days'),
						DATE_TRUNC('%s', NOW()),
						INTERVAL '%s'
					) AS ts
			)
			SELECT
				ts.ts AS timestamp,
				COALESCE(s.ais_class, 'UNKNOWN') AS ais_class,
				COUNT(DISTINCT m.user_id) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
				AND m.receiver_id = %d
			LEFT JOIN
				state s ON m.user_id = s.user_id
			GROUP BY
				ts.ts, s.ais_class
			ORDER BY
				ts.ts, s.ais_class
		`,
		getIntervalName(period), days, getIntervalName(period), timeInterval,
		getIntervalName(period), receiverID)
	} else {
		qry = fmt.Sprintf(`
			WITH time_series AS (
				SELECT
					generate_series(
						DATE_TRUNC('%s', NOW() - INTERVAL '%d days'),
						DATE_TRUNC('%s', NOW()),
						INTERVAL '%s'
					) AS ts
			)
			SELECT
				ts.ts AS timestamp,
				COALESCE(s.ais_class, 'UNKNOWN') AS ais_class,
				COUNT(DISTINCT m.user_id) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
			LEFT JOIN
				state s ON m.user_id = s.user_id
			GROUP BY
				ts.ts, s.ais_class
			ORDER BY
				ts.ts, s.ais_class
		`,
		getIntervalName(period), days, getIntervalName(period), timeInterval,
		getIntervalName(period))
	}
	
	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		return nil, err
	}
	
	// Process results
	timeMap := make(map[time.Time]map[string]int)
	
	for _, recs := range shardResults {
		for _, rec := range recs {
			ts, err := parseTime(rec["timestamp"])
			if err != nil {
				continue
			}
			
			aisClass, err := parseString(rec["ais_class"])
			if err != nil || aisClass == "" {
				aisClass = "UNKNOWN"
			}
			
			count, err := parseInt(rec["count"])
			if err != nil {
				continue
			}
			
			// Initialize map for this timestamp if needed
			if _, ok := timeMap[ts]; !ok {
				timeMap[ts] = make(map[string]int)
			}
			
			// Add count to existing value (aggregating across shards)
			timeMap[ts][aisClass] += count
		}
	}
	
	// Convert map to sorted slice
	var timestamps []time.Time
	for ts := range timeMap {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})
	
	// Build response
	dataPoints := make([]TimeSeriesDataPoint, len(timestamps))
	for i, ts := range timestamps {
		dataPoints[i] = TimeSeriesDataPoint{
			Timestamp: ts,
			Values:    timeMap[ts],
		}
	}
	
	return dataPoints, nil
}

// getIntervalName returns the PostgreSQL interval name for a given period
func getIntervalName(period TimeSeriesPeriod) string {
	switch period {
	case TimeSeriesDaily:
		return "hour"
	case TimeSeriesMonthly:
		return "day"
	case TimeSeriesYearly:
		return "month"
	default:
		return "hour"
	}
}
