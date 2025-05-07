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
        // back-fill any missing metadata (should be rare)
        ids := []int{}
        for _, v := range vessels {
            if v.Name == "" || v.ImageURL == "" {
                ids = append(ids, v.UserID)
            }
        }
        if len(ids) > 0 {
            meta, err := fetchVesselMetadata(ids)
            if err != nil {
                log.Printf("metadata fetch error: %v", err)
            } else {
                for i, v := range vessels {
                    if md, found := meta[v.UserID]; found {
                        vessels[i].Name = md.Name
                        vessels[i].ImageURL = md.ImageURL
                    }
                }
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

    // 3) not cached: per-shard query for max-SOG records
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

    // 4) merge into map[user_id]record
    type rec struct {
        Sog       float64
        Ts, Lat, Lon interface{}
    }
    maxMap := make(map[int]rec, len(shardResults))
    for _, recs := range shardResults {
        for _, r := range recs {
            var uid int
            switch v := r["user_id"].(type) {
            case int:
                uid = v
            case int64:
                uid = int(v)
            case float64:
                uid = int(v)
            default:
                continue
            }
            sog, _ := parseFloat(r["max_sog"])
            ts, _ := parseTime(r["timestamp"])
            lat, _ := parseFloat(r["lat"])
            lon, _ := parseFloat(r["lon"])

            prev, found := maxMap[uid]
            if !found || sog > prev.Sog {
                maxMap[uid] = rec{Sog: sog, Ts: ts, Lat: lat, Lon: lon}
            }
        }
    }

    // 5) build vessels slice
    vessels = make([]vessel, 0, len(maxMap))
    for uid, d := range maxMap {
        vessels = append(vessels, vessel{
            UserID:    uid,
            MaxSog:    d.Sog,
            Timestamp: d.Ts.(time.Time),
            Lat:       d.Lat.(float64),
            Lon:       d.Lon.(float64),
        })
    }

    // 6) sort & truncate to top 10
    sort.Slice(vessels, func(i, j int) bool {
        return vessels[i].MaxSog > vessels[j].MaxSog
    })
    if len(vessels) > 10 {
        vessels = vessels[:10]
    }

    // 7) fetch metadata for exactly these top 10
    ids := make([]int, len(vessels))
    for i, v := range vessels {
        ids[i] = v.UserID
    }
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

    // 8) cache the complete results
    if err := cacheSet(cacheKey, vessels); err != nil {
        log.Printf("cache set error: %v", err)
    }

    // 9) respond
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(vessels)
}

// helpers to coerce interface{} into float64 or time.Time
func parseFloat(i interface{}) (float64, error) {
    switch v := i.(type) {
    case float64:
        return v, nil
    case []byte:
        return strconv.ParseFloat(string(v), 64)
    case string:
        return strconv.ParseFloat(v, 64)
    default:
        return 0, fmt.Errorf("cannot parse float: %v", i)
    }
}
func parseTime(i interface{}) (time.Time, error) {
    switch v := i.(type) {
    case time.Time:
        return v, nil
    case []byte:
        return time.Parse(time.RFC3339, string(v))
    case string:
        return time.Parse(time.RFC3339, v)
    default:
        return time.Time{}, fmt.Errorf("cannot parse time: %v", i)
    }
}
