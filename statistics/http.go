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
    mux.HandleFunc("/statistics/stats/top-positions", topPositionsHandler)
    mux.HandleFunc("/statistics/stats/top-distance", topDistanceHandler)
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

// parseDaysParam reads 'days' query param, defaults to 1.
func parseDaysParam(r *http.Request) int {
    days := 1
    if d := r.URL.Query().Get("days"); d != "" {
        if n, err := strconv.Atoi(d); err == nil && n > 0 {
            days = n
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
                AND m.timestamp >= now() - INTERVAL '%d days'
                AND m.receiver_id = %d
                AND (s.ais_class IS NULL OR s.ais_class != 'SAR')
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
                AND m.timestamp >= now() - INTERVAL '%d days'
                AND (s.ais_class IS NULL OR s.ais_class != 'SAR')
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
    }, 0)

    for _, recs := range shardResults {
        for _, rec := range recs {
            uid, _ := parseInt(rec["user_id"])
            dist, _ := parseInt(rec["distance"])
            ts, _ := parseTime(rec["timestamp"])
            lat, _ := parseFloat(rec["lat"])
            lon, _ := parseFloat(rec["lon"])
            recID, _ := parseInt(rec["receiver_id"])
            
            // Extract AIS class directly from the query result
            var aisClass string
            if v, ok := rec["ais_class"].(string); ok {
                aisClass = v
            } else if b, ok := rec["ais_class"].([]byte); ok {
                aisClass = string(b)
            }
            
            allResults = append(allResults, struct {
                userID     int
                distance   int
                timestamp  time.Time
                lat, lon   float64
                receiverID int
                aisClass   string
            }{
                userID:     uid,
                distance:   dist,
                timestamp:  ts,
                lat:        lat,
                lon:        lon,
                receiverID: recID,
                aisClass:   aisClass,
            })
        }
    }

    // Create a map to ensure unique user_id + receiver_id combinations
    uniquePairs := make(map[string]struct {
        userID     int
        distance   int
        timestamp  time.Time
        lat, lon   float64
        receiverID int
        aisClass   string
    })
    
    for _, r := range allResults {
        // Create a unique key for each user_id + receiver_id pair
        key := fmt.Sprintf("%d-%d", r.userID, r.receiverID)
        
        // If this pair isn't in the map yet, or if this distance is greater than the stored one
        if existing, exists := uniquePairs[key]; !exists || r.distance > existing.distance {
            uniquePairs[key] = r
        }
    }
    
    // Convert map back to slice
    allResults = make([]struct {
        userID     int
        distance   int
        timestamp  time.Time
        lat, lon   float64
        receiverID int
        aisClass   string
    }, 0, len(uniquePairs))
    
    for _, r := range uniquePairs {
        allResults = append(allResults, r)
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
