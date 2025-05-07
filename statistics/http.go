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

// vessel is the JSON shape returned by /statistics/stats/top-sog
type vessel struct {
    UserID    int       `json:"user_id"`
    MaxSog    float64   `json:"max_sog"`
    Timestamp time.Time `json:"timestamp"`
    Lat       float64   `json:"lat"`
    Lon       float64   `json:"lon"`
}

// StartServer sets up HTTP handlers and begins listening.
func StartServer(port int) {
    mux := http.NewServeMux()
    registerHandlers(mux)

    addr := fmt.Sprintf(":%d", port)
    log.Printf("Listening on %s...", addr)
    if err := http.ListenAndServe(addr, mux); err != nil {
        log.Fatalf("HTTP server error: %v", err)
    }
}

// registerHandlers attaches your routes to the mux.
func registerHandlers(mux *http.ServeMux) {
    // Serve static assets under /statistics/
    fs := http.FileServer(http.Dir("web"))
    mux.Handle("/statistics/", http.StripPrefix("/statistics/", fs))

    // Stats endpoint
    mux.HandleFunc("/statistics/stats/top-sog", topSogHandler)
}

// topSogHandler returns the top 10 vessels by max SOG (excluding 102.3) in the last x days,
// including the timestamp and position where that max was observed.
// It caches results in Redis under key "top-sog:<days>d" with TTL == conf.CacheTime seconds.
// Query-param: ?days=N  (default 1)
func topSogHandler(w http.ResponseWriter, r *http.Request) {
    // 1) parse days
    days := 1
    if d := r.URL.Query().Get("days"); d != "" {
        n, err := strconv.Atoi(d)
        if err != nil || n <= 0 {
            http.Error(w, "invalid days parameter", http.StatusBadRequest)
            return
        }
        days = n
    }

    cacheKey := fmt.Sprintf("top-sog:%dd", days)

    // 2) Try Redis cache
    var vessels []vessel
    if ok, err := cacheGet(cacheKey, &vessels); err != nil {
        log.Printf("cache get error: %v", err)
    } else if ok {
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(vessels)
        return
    }

    // 3) Not cached: run per-shard DISTINCT ON query to get each user's top record
    qry := fmt.Sprintf(`
        SELECT DISTINCT ON (user_id)
               user_id,
               (packet->>'Sog')::float AS max_sog,
               timestamp,
               (packet->>'Latitude')::float AS lat,
               (packet->>'Longitude')::float AS lon
          FROM messages
         WHERE message_id IN (1,2,3,18,19)
           AND (packet->>'Sog')::float <> 102.3
           AND timestamp >= now() - INTERVAL '%d days'
         ORDER BY user_id, max_sog DESC
    `, days)

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
        return
    }

    // 4) Merge across shards to get global max per user
    type recType struct {
        Sog  float64
        Ts   time.Time
        Lat  float64
        Lon  float64
    }
    maxMap := make(map[int]recType)
    for _, recs := range shardResults {
        for _, rec := range recs {
            // extract user_id
            var uid int
            switch v := rec["user_id"].(type) {
            case int:
                uid = v
            case int64:
                uid = int(v)
            case float64:
                uid = int(v)
            default:
                continue
            }
            // extract max_sog
            var sog float64
            switch v := rec["max_sog"].(type) {
            case float64:
                sog = v
            case []byte:
                sog, _ = strconv.ParseFloat(string(v), 64)
            }
            // extract timestamp
            var ts time.Time
            switch v := rec["timestamp"].(type) {
            case time.Time:
                ts = v
            case []byte:
                ts, _ = time.Parse(time.RFC3339, string(v))
            case string:
                ts, _ = time.Parse(time.RFC3339, v)
            }
            // extract lat
            var lat float64
            switch v := rec["lat"].(type) {
            case float64:
                lat = v
            case []byte:
                lat, _ = strconv.ParseFloat(string(v), 64)
            }
            // extract lon
            var lon float64
            switch v := rec["lon"].(type) {
            case float64:
                lon = v
            case []byte:
                lon, _ = strconv.ParseFloat(string(v), 64)
            }

            if prev, ok := maxMap[uid]; !ok || sog > prev.Sog {
                maxMap[uid] = recType{Sog: sog, Ts: ts, Lat: lat, Lon: lon}
            }
        }
    }

    // 5) Sort into slice and take top 10
    vessels = make([]vessel, 0, len(maxMap))
    for uid, data := range maxMap {
        vessels = append(vessels, vessel{
            UserID:    uid,
            MaxSog:    data.Sog,
            Timestamp: data.Ts,
            Lat:       data.Lat,
            Lon:       data.Lon,
        })
    }
    sort.Slice(vessels, func(i, j int) bool {
        return vessels[i].MaxSog > vessels[j].MaxSog
    })
    if len(vessels) > 10 {
        vessels = vessels[:10]
    }

    // 6) Store in cache
    if err := cacheSet(cacheKey, vessels); err != nil {
        log.Printf("cache set error: %v", err)
    }

    // 7) Return JSON
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(vessels); err != nil {
        http.Error(w, fmt.Sprintf("encoding error: %v", err), http.StatusInternalServerError)
    }
}
