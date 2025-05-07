package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "sort"
    "strconv"
)

// vessel is the JSON shape returned by /statistics/stats/top-sog
type vessel struct {
    UserID int     `json:"user_id"`
    MaxSog float64 `json:"max_sog"`
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
    // 1) Static assets from ./web at /statistics/...
    fs := http.FileServer(http.Dir("web"))
    mux.Handle("/statistics/", http.StripPrefix("/statistics/", fs))

    // 2) API endpoint at /statistics/stats/top-sog
    mux.HandleFunc("/statistics/stats/top-sog", topSogHandler)
}

// topSogHandler returns the top 10 vessels by max SOG in the last x days.
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

    // 2) fan-out per-shard query
    qry := fmt.Sprintf(`
        SELECT user_id,
               MAX((packet->>'Sog')::float) AS max_sog
          FROM messages
         WHERE message_id IN (1,2,3,18,19)
           AND timestamp >= now() - INTERVAL '%d days'
         GROUP BY user_id
    `, days)

    shardResults, err := QueryDatabasesForAllShards(qry)
    if err != nil {
        http.Error(w, fmt.Sprintf("query error: %v", err), http.StatusInternalServerError)
        return
    }

    // 3) merge per-shard into global map[user_id]max_sog
    maxMap := make(map[int]float64)
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
            if prev, ok := maxMap[uid]; !ok || sog > prev {
                maxMap[uid] = sog
            }
        }
    }

    // 4) sort into slice and take top 10
    vessels := make([]vessel, 0, len(maxMap))
    for uid, sog := range maxMap {
        vessels = append(vessels, vessel{UserID: uid, MaxSog: sog})
    }
    sort.Slice(vessels, func(i, j int) bool {
        return vessels[i].MaxSog > vessels[j].MaxSog
    })
    if len(vessels) > 10 {
        vessels = vessels[:10]
    }

    // 5) return JSON
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(vessels); err != nil {
        http.Error(w, fmt.Sprintf("encoding error: %v", err), http.StatusInternalServerError)
    }
}
