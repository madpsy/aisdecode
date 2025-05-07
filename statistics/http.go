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
    Name      string    `json:"name"`
    ImageURL  string    `json:"image_url"`
    MaxSog    float64   `json:"max_sog"`
    Timestamp time.Time `json:"timestamp"`
    Lat       float64   `json:"lat"`
    Lon       float64   `json:"lon"`
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
}

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
    var vessels []vessel

    // 2) try cache
    if ok, err := cacheGet(cacheKey, &vessels); err != nil {
        log.Printf("cache get error: %v", err)
    } else if ok {
        // 2a) even on a hit, back‐fill any missing metadata
        ids := make([]int, 0, len(vessels))
        for _, v := range vessels {
            if v.Name == "" || v.ImageURL == "" {
                ids = append(ids, v.UserID)
            }
        }
        if len(ids) > 0 {
            metaMap, err := fetchVesselMetadata(ids)
            if err != nil {
                log.Printf("metadata fetch error: %v", err)
            } else {
                for i, v := range vessels {
                    if md, found := metaMap[v.UserID]; found {
                        vessels[i].Name = md.Name
                        vessels[i].ImageURL = md.ImageURL
                    }
                }
                // update cache in background
                go func(key string, data []vessel) {
                    if err := cacheSet(key, data); err != nil {
                        log.Printf("cache update error: %v", err)
                    }
                }(cacheKey, vessels)
            }
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(vessels)
        return
    }

    // 3) not cached: query messages
    qry := fmt.Sprintf(`
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

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
        return
    }

    // 4) merge per‐shard into global map
    type recType struct {
        Sog float64
        Ts  time.Time
        Lat float64
        Lon float64
    }
    maxMap := make(map[int]recType, 16)
    for _, recs := range shardResults {
        for _, rec := range recs {
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
            var sog float64
            switch v := rec["max_sog"].(type) {
            case float64:
                sog = v
            case []byte:
                sog, _ = strconv.ParseFloat(string(v), 64)
            }
            var ts time.Time
            switch v := rec["timestamp"].(type) {
            case time.Time:
                ts = v
            case string:
                ts, _ = time.Parse(time.RFC3339, v)
            case []byte:
                ts, _ = time.Parse(time.RFC3339, string(v))
            }
            var lat float64
            switch v := rec["lat"].(type) {
            case float64:
                lat = v
            case []byte:
                lat, _ = strconv.ParseFloat(string(v), 64)
            }
            var lon float64
            switch v := rec["lon"].(type) {
            case float64:
                lon = v
            case []byte:
                lon, _ = strconv.ParseFloat(string(v), 64)
            }

            if prev, found := maxMap[uid]; !found || sog > prev.Sog {
                maxMap[uid] = recType{Sog: sog, Ts: ts, Lat: lat, Lon: lon}
            }
        }
    }

    // 5) build vessels slice & ids
    vessels = make([]vessel, 0, len(maxMap))
    ids := make([]int, 0, len(maxMap))
    for uid, d := range maxMap {
        vessels = append(vessels, vessel{
            UserID:    uid,
            MaxSog:    d.Sog,
            Timestamp: d.Ts,
            Lat:       d.Lat,
            Lon:       d.Lon,
        })
        ids = append(ids, uid)
    }

    // 6) sort & take top 10
    sort.Slice(vessels, func(i, j int) bool {
        return vessels[i].MaxSog > vessels[j].MaxSog
    })
    if len(vessels) > 10 {
        vessels = vessels[:10]
        ids = ids[:10]
    }

    // 7) fetch metadata for those top 10
    metaMap, err := fetchVesselMetadata(ids)
    if err != nil {
        log.Printf("metadata fetch error: %v", err)
    } else {
        for i, v := range vessels {
            if md, found := metaMap[v.UserID]; found {
                vessels[i].Name = md.Name
                vessels[i].ImageURL = md.ImageURL
            }
        }
    }

    // 8) cache the full records
    if err := cacheSet(cacheKey, vessels); err != nil {
        log.Printf("cache set error: %v", err)
    }

    // 9) return
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(vessels)
}
