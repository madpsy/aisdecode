package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sort"
    "strconv"
)

// vessel is the JSON shape returned by /statistics/stats/top-sog.
type vessel struct {
    UserID    int     `json:"user_id"`
    Name      string  `json:"name,omitempty"`
    ImageURL  string  `json:"image_url,omitempty"`
    MaxSog    float64 `json:"max_sog,omitempty"`
    Count     int     `json:"count,omitempty"`
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
    Count    int    `json:"count"`
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
}

func topSogHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    cacheKey := fmt.Sprintf("top-sog:%dd", days)

    var vessels []vessel
    if ok, _ := cacheGet(cacheKey, &vessels); ok {
        // back-fill missing metadata
        ids := []int{}
        for _, v := range vessels {
            if v.Name == "" || v.ImageURL == "" {
                ids = append(ids, v.UserID)
            }
        }
        if len(ids) > 0 {
            if meta, err := fetchVesselMetadata(ids); err == nil {
                for i, vv := range vessels {
                    if md, found := meta[vv.UserID]; found {
                        vessels[i].Name = md.Name
                        vessels[i].ImageURL = md.ImageURL
                    }
                }
                go cacheSet(cacheKey, vessels)
            }
        }
        respondJSON(w, vessels)
        return
    }

    qry := fmt.Sprintf(`
        SELECT DISTINCT ON (m.user_id)
               m.user_id,
               (m.packet->>'Sog')::float AS max_sog
          FROM messages m
         WHERE m.message_id IN (1,2,3,18,19)
           AND (m.packet->>'Sog')::float <> 102.3
           AND m.timestamp >= now() - INTERVAL '%d days'
         ORDER BY m.user_id, max_sog DESC
    `, days)

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        respondError(w, err)
        return
    }

    maxMap := make(map[int]float64)
    for _, recs := range shardResults {
        for _, rec := range recs {
            uid, _ := parseInt(rec["user_id"])
            sog, _ := parseFloat(rec["max_sog"])
            if prev, found := maxMap[uid]; !found || sog > prev {
                maxMap[uid] = sog
            }
        }
    }

    vessels = make([]vessel, 0, len(maxMap))
    ids := make([]int, 0, len(maxMap))
    for uid, sog := range maxMap {
        vessels = append(vessels, vessel{
            UserID: uid,
            MaxSog: sog,
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
        for i, vv := range vessels {
            if md, found := metaMap[vv.UserID]; found {
                vessels[i].Name = md.Name
                vessels[i].ImageURL = md.ImageURL
            }
        }
    }

    cacheSet(cacheKey, vessels)
    respondJSON(w, vessels)
}

func topTypesHandler(w http.ResponseWriter, r *http.Request) {
    days := parseDaysParam(r)
    cacheKey := fmt.Sprintf("top-types:%dd", days)

    var counts []typeCount
    if ok, _ := cacheGet(cacheKey, &counts); ok {
        respondJSON(w, counts)
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
    cacheKey := fmt.Sprintf("top-positions:%dd", days)

    var results []posVessel
    if ok, _ := cacheGet(cacheKey, &results); ok {
        // back-fill missing metadata on cache hit
        ids := []int{}
        for _, v := range results {
            if v.Name == "" || v.ImageURL == "" {
                ids = append(ids, v.UserID)
            }
        }
        if len(ids) > 0 {
            if meta, err := fetchVesselMetadata(ids); err == nil {
                for i, vv := range results {
                    if md, found := meta[vv.UserID]; found {
                        results[i].Name = md.Name
                        results[i].ImageURL = md.ImageURL
                    }
                }
                go cacheSet(cacheKey, results)
            }
        }
        respondJSON(w, results)
        return
    }

    qry := fmt.Sprintf(`
        SELECT user_id,
               COUNT(*) AS cnt
          FROM messages
         WHERE message_id IN (1,2,3,18,19)
           AND timestamp >= now() - INTERVAL '%d days'
         GROUP BY user_id
    `, days)

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

    results = make([]posVessel, 0, len(countMap))
    ids := make([]int, 0, len(countMap))
    for uid, cnt := range countMap {
        results = append(results, posVessel{
            UserID: uid,
            Count:  cnt,
        })
        ids = append(ids, uid)
    }

    sort.Slice(results, func(i, j int) bool {
        return results[i].Count > results[j].Count
    })
    if len(results) > 10 {
        results = results[:10]
        ids = ids[:10]
    }

    // fetch metadata before responding & caching
    if metaMap, err := fetchVesselMetadata(ids); err == nil {
        for i, vv := range results {
            if md, found := metaMap[vv.UserID]; found {
                results[i].Name = md.Name
                results[i].ImageURL = md.ImageURL
            }
        }
    }

    cacheSet(cacheKey, results)
    respondJSON(w, results)
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

// respondJSON serializes v as JSON to the response.
func respondJSON(w http.ResponseWriter, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(v)
}

// respondError sends a 500 status with the error message.
func respondError(w http.ResponseWriter, err error) {
    http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
}
