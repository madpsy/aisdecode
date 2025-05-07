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

// typeCount is the JSON shape returned by /statistics/stats/top-types
type typeCount struct {
    Type  string `json:"type"`
    Count int    `json:"count"`
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
}

func topSogHandler(w http.ResponseWriter, r *http.Request) {
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
    if ok, err := cacheGet(cacheKey, &vessels); err != nil {
        log.Printf("cache get error: %v", err)
    } else if ok {
        // back-fill missing metadata
        ids := []int{}
        for _, v := range vessels {
            if v.Name == "" || v.ImageURL == "" {
                ids = append(ids, v.UserID)
            }
        }
        if len(ids) > 0 {
            if meta, err := fetchVesselMetadata(ids); err == nil {
                for i, v := range vessels {
                    if md, found := meta[v.UserID]; found {
                        vessels[i].Name = md.Name
                        vessels[i].ImageURL = md.ImageURL
                    }
                }
                go cacheSet(cacheKey, vessels)
            }
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(vessels)
        return
    }

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

    type rec struct {
        Sog float64
        Ts  time.Time
        Lat float64
        Lon float64
    }
    maxMap := make(map[int]rec, 16)
    for _, recs := range shardResults {
        for _, r := range recs {
            uid, _ := parseInt(r["user_id"])
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

    sort.Slice(vessels, func(i, j int) bool {
        return vessels[i].MaxSog > vessels[j].MaxSog
    })
    if len(vessels) > 10 {
        vessels = vessels[:10]
        ids = ids[:10]
    }

    if metaMap, err := fetchVesselMetadata(ids); err == nil {
        for i, v := range vessels {
            if md, found := metaMap[v.UserID]; found {
                vessels[i].Name = md.Name
                vessels[i].ImageURL = md.ImageURL
            }
        }
    }

    if err := cacheSet(cacheKey, vessels); err != nil {
        log.Printf("cache set error: %v", err)
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(vessels)
}

func topTypesHandler(w http.ResponseWriter, r *http.Request) {
    days := 1
    if d := r.URL.Query().Get("days"); d != "" {
        n, err := strconv.Atoi(d)
        if err != nil || n <= 0 {
            http.Error(w, "invalid days parameter", http.StatusBadRequest)
            return
        }
        days = n
    }

    cacheKey := fmt.Sprintf("top-types:%dd", days)
    var counts []typeCount
    if ok, err := cacheGet(cacheKey, &counts); err != nil {
        log.Printf("cache get error: %v", err)
    } else if ok {
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(counts)
        return
    }

    qry := fmt.Sprintf(`
        SELECT (packet->>'Type') AS vessel_type,
               COUNT(*)               AS cnt
          FROM state
         WHERE timestamp >= now() - INTERVAL '%d days'
           AND (packet->>'Type') IS NOT NULL
           AND TRIM((packet->>'Type')) <> ''
         GROUP BY vessel_type
    `, days)

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
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

    if err := cacheSet(cacheKey, counts); err != nil {
        log.Printf("cache set error: %v", err)
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(counts)
}