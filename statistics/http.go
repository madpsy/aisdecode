package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
	Distance  int       `json:"distance,omitempty"` // for top-distance
}

// distanceVessel is the JSON shape returned by /statistics/stats/top-distance.
type distanceVessel struct {
	UserID       int       `json:"user_id"`
	Name         string    `json:"name,omitempty"`
	ImageURL     string    `json:"image_url,omitempty"`
	AISClass     string    `json:"ais_class,omitempty"`
	Type         string    `json:"type,omitempty"`
	Distance     int       `json:"distance"`
	Timestamp    time.Time `json:"timestamp,omitempty"`
	Lat          float64   `json:"lat,omitempty"`
	Lon          float64   `json:"lon,omitempty"`
	ReceiverID   int       `json:"receiver_id,omitempty"`
	ReceiverName string    `json:"receiver_name,omitempty"`
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

// GridCell is the JSON shape returned by /statistics/stats/coverage-map and /statistics/stats/duplicates-heatmap.
type GridCell struct {
	Lat           float64  `json:"lat"`
	Lon           float64  `json:"lon"`
	Count         int      `json:"count"`
	ReceiverIDs   []int    `json:"receiver_ids,omitempty"`   // For duplicates-heatmap
	ReceiverNames []string `json:"receiver_names,omitempty"` // For duplicates-heatmap
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
			vs[i].Name = m.Name
			vs[i].ImageURL = m.ImageURL
			vs[i].AISClass = m.AISClass
			vs[i].Type = m.Type
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
			ps[i].Name = m.Name
			ps[i].ImageURL = m.ImageURL
			ps[i].AISClass = m.AISClass
			ps[i].Type = m.Type
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
	mux.HandleFunc("/statistics/stats/top-duplicates", topDuplicatesHandler)
	mux.HandleFunc("/statistics/stats/duplicates-heatmap", duplicatesHeatmapHandler)

	// VHF lift detection endpoints
	mux.HandleFunc("/statistics/stats/vhf-range-anomalies", vhfRangeAnomaliesHandler)
	mux.HandleFunc("/statistics/stats/vhf-direction-anomalies", vhfDirectionAnomaliesHandler)
}

func topSogHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-sog:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-sog:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-sog:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-sog:%dd", days)
		}
	}

	var vs []vessel
	if ok, _ := cacheGet(cacheKey, &vs); ok {
		vs = enrichVessels(vs)
		respondJSON(w, vs)
		return
	}

	// Build query with optional receiver_id filter and time range
	var qry string
	if receiverID > 0 {
		if timeRange.UseRange {
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
                   AND m.timestamp >= '%s'
                   AND m.timestamp <= '%s'
                   AND m.receiver_id = %d
                 ORDER BY m.user_id, max_sog DESC
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
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
                   AND m.receiver_id = %d
                 ORDER BY m.user_id, max_sog DESC
            `, days, receiverID)
		}
	} else {
		if timeRange.UseRange {
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
                   AND m.timestamp >= '%s'
                   AND m.timestamp <= '%s'
                 ORDER BY m.user_id, max_sog DESC
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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
			ts, err := parseTime(rec["timestamp"])
			if err != nil {
				ts = time.Now()
			}
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
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-types:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-types:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-types:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-types:%dd", days)
		}
	}

	var counts []typeCount
	if ok, _ := cacheGet(cacheKey, &counts); ok {
		respondJSON(w, counts)
		return
	}

	// First, get a list of user_ids from the messages table if a receiver_id is specified
	var userIDs []int
	if receiverID > 0 {
		var userIDQuery string
		if timeRange.UseRange {
			userIDQuery = fmt.Sprintf(`
                SELECT DISTINCT user_id
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= '%s'
                   AND timestamp <= '%s'
                   AND receiver_id = %d
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
		} else {
			userIDQuery = fmt.Sprintf(`
                SELECT DISTINCT user_id
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= now() - INTERVAL '%d days'
                   AND receiver_id = %d
            `, days, receiverID)
		}

		userIDResults, err := QueryDatabasesForAllShards(userIDQuery)
		if err != nil {
			respondError(w, err)
			return
		}

		// Extract user_ids from results
		userIDMap := make(map[int]bool)
		for _, recs := range userIDResults {
			for _, rec := range recs {
				uid, _ := parseInt(rec["user_id"])
				if uid > 0 {
					userIDMap[uid] = true
				}
			}
		}

		// Convert map to slice
		for uid := range userIDMap {
			userIDs = append(userIDs, uid)
		}

		// If no vessels found for this receiver, return empty result
		if len(userIDs) == 0 {
			respondJSON(w, []typeCount{})
			return
		}
	}

	// Build query with optional user_id filter from messages and time range
	var qry string
	if receiverID > 0 {
		// Build IN clause for user_ids
		var userIDsStr strings.Builder
		for i, uid := range userIDs {
			if i > 0 {
				userIDsStr.WriteString(",")
			}
			userIDsStr.WriteString(fmt.Sprintf("%d", uid))
		}

		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                SELECT (packet->>'Type') AS vessel_type,
                       COUNT(*)               AS cnt
                  FROM state
                 WHERE timestamp >= '%s'
                   AND timestamp <= '%s'
                   AND (packet->>'Type') IS NOT NULL
                   AND TRIM((packet->>'Type')) <> ''
                   AND user_id IN (%s)
                 GROUP BY vessel_type
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), userIDsStr.String())
		} else {
			qry = fmt.Sprintf(`
                SELECT (packet->>'Type') AS vessel_type,
                       COUNT(*)               AS cnt
                  FROM state
                 WHERE timestamp >= now() - INTERVAL '%d days'
                   AND (packet->>'Type') IS NOT NULL
                   AND TRIM((packet->>'Type')) <> ''
                   AND user_id IN (%s)
                 GROUP BY vessel_type
            `, days, userIDsStr.String())
		}
	} else {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                SELECT (packet->>'Type') AS vessel_type,
                       COUNT(*)               AS cnt
                  FROM state
                 WHERE timestamp >= '%s'
                   AND timestamp <= '%s'
                   AND (packet->>'Type') IS NOT NULL
                   AND TRIM((packet->>'Type')) <> ''
                 GROUP BY vessel_type
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-classes:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-classes:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-classes:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-classes:%dd", days)
		}
	}

	var counts []classCount
	if ok, _ := cacheGet(cacheKey, &counts); ok {
		respondJSON(w, counts)
		return
	}

	// First, get a list of user_ids from the messages table if a receiver_id is specified
	var userIDs []int
	if receiverID > 0 {
		var userIDQuery string
		if timeRange.UseRange {
			userIDQuery = fmt.Sprintf(`
                SELECT DISTINCT user_id
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= '%s'
                   AND timestamp <= '%s'
                   AND receiver_id = %d
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
		} else {
			userIDQuery = fmt.Sprintf(`
                SELECT DISTINCT user_id
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= now() - INTERVAL '%d days'
                   AND receiver_id = %d
            `, days, receiverID)
		}

		userIDResults, err := QueryDatabasesForAllShards(userIDQuery)
		if err != nil {
			respondError(w, err)
			return
		}

		// Extract user_ids from results
		userIDMap := make(map[int]bool)
		for _, recs := range userIDResults {
			for _, rec := range recs {
				uid, _ := parseInt(rec["user_id"])
				if uid > 0 {
					userIDMap[uid] = true
				}
			}
		}

		// Convert map to slice
		for uid := range userIDMap {
			userIDs = append(userIDs, uid)
		}

		// If no vessels found for this receiver, return empty result
		if len(userIDs) == 0 {
			respondJSON(w, []classCount{})
			return
		}
	}

	// Build query with optional user_id filter from messages and time range
	var qry string
	if receiverID > 0 {
		// Build IN clause for user_ids
		var userIDsStr strings.Builder
		for i, uid := range userIDs {
			if i > 0 {
				userIDsStr.WriteString(",")
			}
			userIDsStr.WriteString(fmt.Sprintf("%d", uid))
		}

		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                    SELECT ais_class AS vessel_class,
                           COUNT(*)  AS cnt
                      FROM state
                     WHERE timestamp >= '%s'
                       AND timestamp <= '%s'
                       AND ais_class IS NOT NULL
                       AND TRIM(ais_class) <> ''
                       AND user_id IN (%s)
                     GROUP BY vessel_class
                `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), userIDsStr.String())
		} else {
			qry = fmt.Sprintf(`
                    SELECT ais_class AS vessel_class,
                           COUNT(*)  AS cnt
                      FROM state
                     WHERE timestamp >= now() - INTERVAL '%d days'
                       AND ais_class IS NOT NULL
                       AND TRIM(ais_class) <> ''
                       AND user_id IN (%s)
                     GROUP BY vessel_class
                `, days, userIDsStr.String())
		}
	} else {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                    SELECT ais_class AS vessel_class,
                           COUNT(*)  AS cnt
                      FROM state
                     WHERE timestamp >= '%s'
                       AND timestamp <= '%s'
                       AND ais_class IS NOT NULL
                       AND TRIM(ais_class) <> ''
                     GROUP BY vessel_class
                `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-positions:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-positions:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-positions:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-positions:%dd", days)
		}
	}

	var ps []posVessel
	if ok, _ := cacheGet(cacheKey, &ps); ok {
		respondJSON(w, ps)
		return
	}

	// Build query with optional receiver_id filter and time range
	var qry string
	if receiverID > 0 {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                SELECT user_id,
                       COUNT(*) AS cnt
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= '%s'
                   AND timestamp <= '%s'
                   AND receiver_id = %d
                 GROUP BY user_id
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
		} else {
			qry = fmt.Sprintf(`
                SELECT user_id,
                       COUNT(*) AS cnt
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= now() - INTERVAL '%d days'
                   AND receiver_id = %d
                 GROUP BY user_id
            `, days, receiverID)
		}
	} else {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
                SELECT user_id,
                       COUNT(*) AS cnt
                  FROM messages
                 WHERE message_id IN (1,2,3,18,19)
                   AND timestamp >= '%s'
                   AND timestamp <= '%s'
                 GROUP BY user_id
            `, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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

// TimeRange represents a time range for queries
type TimeRange struct {
	From     time.Time
	To       time.Time
	UseRange bool // Whether to use the time range or fall back to days
}

// TimeRangeError represents an error with time range parameters
type TimeRangeError struct {
	Message string
}

func (e TimeRangeError) Error() string {
	return e.Message
}

// parseTimeRangeParams reads 'from' and 'to' query params, validates them, and returns a TimeRange.
// If both params are valid, UseRange will be true.
// If there's an error with the parameters, it returns a TimeRangeError.
func parseTimeRangeParams(r *http.Request) (TimeRange, error) {
	var result TimeRange

	// Check if both parameters are provided
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	// If neither parameter is provided, just return with UseRange=false
	if fromStr == "" && toStr == "" {
		return result, nil
	}

	// If only one parameter is provided, return an error
	if fromStr == "" && toStr != "" {
		return result, TimeRangeError{Message: "Missing 'from' parameter when 'to' is specified"}
	}
	if fromStr != "" && toStr == "" {
		return result, TimeRangeError{Message: "Missing 'to' parameter when 'from' is specified"}
	}

	// Parse 'from' parameter
	fromTime, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		return result, TimeRangeError{Message: "Invalid 'from' date format. Expected RFC3339 format (e.g., 2023-01-01T00:00:00Z)"}
	}

	// Parse 'to' parameter
	toTime, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		return result, TimeRangeError{Message: "Invalid 'to' date format. Expected RFC3339 format (e.g., 2023-01-01T00:00:00Z)"}
	}

	// Validate the range
	if fromTime.After(toTime) {
		return result, TimeRangeError{Message: "'from' date must be before 'to' date"}
	}

	// Check if range exceeds 30 days
	maxDuration := 30 * 24 * time.Hour
	if toTime.Sub(fromTime) > maxDuration {
		return result, TimeRangeError{Message: "Time range cannot exceed 30 days"}
	}

	result.From = fromTime
	result.To = toTime
	result.UseRange = true

	return result, nil
}

// respondJSON serializes v as JSON to the response.
func respondJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	// Debug log the response for duplicates-heatmap endpoint
	if heatmapData, ok := v.([]GridCell); ok {
		log.Printf("Responding with %d grid cells", len(heatmapData))
		if len(heatmapData) > 0 {
			log.Printf("First grid cell: %+v", heatmapData[0])
			if len(heatmapData[0].ReceiverIDs) > 0 {
				log.Printf("First grid cell has receiver IDs: %v", heatmapData[0].ReceiverIDs)
			}
			if len(heatmapData[0].ReceiverNames) > 0 {
				log.Printf("First grid cell has receiver names: %v", heatmapData[0].ReceiverNames)
			}
		}
	}

	json.NewEncoder(w).Encode(v)
}

// respondError sends a 500 status with the error message.
func respondError(w http.ResponseWriter, err error) {
	http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
}

func topDistanceHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-distance:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-distance:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-distance:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-distance:%dd", days)
		}
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
		if timeRange.UseRange {
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
                    AND m.distance <= %d  -- Filter out spurious values over configured distance
                    AND m.timestamp >= '%s'
                    AND m.timestamp <= '%s'
                    AND m.receiver_id = %d
                    AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
                  ORDER BY m.user_id, m.receiver_id, m.distance DESC
            `, conf.MaxDistanceMeters, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
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
                    AND m.distance <= %d  -- Filter out spurious values over configured distance
                    AND m.timestamp >= now() - INTERVAL '%d days'
                    AND m.receiver_id = %d
                    AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
                  ORDER BY m.user_id, m.receiver_id, m.distance DESC
            `, conf.MaxDistanceMeters, days, receiverID)
		}
	} else {
		if timeRange.UseRange {
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
                    AND m.distance <= %d  -- Filter out spurious values over configured distance
                    AND m.timestamp >= '%s'
                    AND m.timestamp <= '%s'
                    AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
                  ORDER BY m.user_id, m.receiver_id, m.distance DESC
            `, conf.MaxDistanceMeters, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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
                    AND m.distance <= %d  -- Filter out spurious values over configured distance
                    AND m.timestamp >= now() - INTERVAL '%d days'
                    AND (s.ais_class IS NULL OR (s.ais_class != 'SAR' AND s.ais_class != 'BASE' AND s.ais_class != 'AtoN'))
                  ORDER BY m.user_id, m.receiver_id, m.distance DESC
            `, conf.MaxDistanceMeters, days)
		}
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
			ts, err := parseTime(rec["timestamp"])
			if err != nil {
				ts = time.Now()
			}
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
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("user-counts:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("user-counts:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("user-counts:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("user-counts:%dd", days)
		}
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

	// Build query with optional receiver_id filter and time range
	var qry string
	if receiverID > 0 {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
				SELECT user_id,
					   COUNT(*) AS cnt
				  FROM messages
				 WHERE timestamp >= '%s'
				   AND timestamp <= '%s'
				   AND receiver_id = %d
				 GROUP BY user_id
				 ORDER BY cnt DESC
				 LIMIT 100
			`, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), receiverID)
		} else {
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
		}
	} else {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
				SELECT user_id,
					   COUNT(*) AS cnt
				  FROM messages
				 WHERE timestamp >= '%s'
				   AND timestamp <= '%s'
				 GROUP BY user_id
				 ORDER BY cnt DESC
				 LIMIT 100
			`, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
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
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Define cache key with time range if provided
	var cacheKey string
	if timeRange.UseRange {
		if receiverId < 0 {
			cacheKey = fmt.Sprintf("coverage-map:%s-%s:all",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		} else {
			cacheKey = fmt.Sprintf("coverage-map:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverId)
		}
	} else {
		if receiverId < 0 {
			cacheKey = fmt.Sprintf("coverage-map:%dd:all", days)
		} else {
			cacheKey = fmt.Sprintf("coverage-map:%dd:r%d", days, receiverId)
		}
	}

	// Define the response structure

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
		if timeRange.UseRange {
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
                    AND timestamp >= '%s'
                    AND timestamp <= '%s'
                    AND (packet->>'Latitude')::float IS NOT NULL
                    AND (packet->>'Longitude')::float IS NOT NULL
                    AND (packet->>'Latitude')::float BETWEEN -90 AND 90
                    AND (packet->>'Longitude')::float BETWEEN -180 AND 180
                    AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
                GROUP BY
                    ST_SnapToGrid(
                        ST_SetSRID(ST_MakePoint(
                            (packet->>'Longitude')::float,
                            (packet->>'Latitude')::float
                        ), 4326),
                        %f
                    )
            `, gridSize, gridSize, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), conf.MaxDistanceMeters, gridSize)
		} else {
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
                    AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
                GROUP BY
                    ST_SnapToGrid(
                        ST_SetSRID(ST_MakePoint(
                            (packet->>'Longitude')::float,
                            (packet->>'Latitude')::float
                        ), 4326),
                        %f
                    )
            `, gridSize, gridSize, days, conf.MaxDistanceMeters, gridSize)
		}
	} else {
		// Query for a specific receiver
		if timeRange.UseRange {
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
                    AND timestamp >= '%s'
                    AND timestamp <= '%s'
                    AND (packet->>'Latitude')::float IS NOT NULL
                    AND (packet->>'Longitude')::float IS NOT NULL
                    AND (packet->>'Latitude')::float BETWEEN -90 AND 90
                    AND (packet->>'Longitude')::float BETWEEN -180 AND 180
                    AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
                GROUP BY
                    ST_SnapToGrid(
                        ST_SetSRID(ST_MakePoint(
                            (packet->>'Longitude')::float,
                            (packet->>'Latitude')::float
                        ), 4326),
                        %f
                    )
            `, gridSize, gridSize, receiverId, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), conf.MaxDistanceMeters, gridSize)
		} else {
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
                    AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
                GROUP BY
                    ST_SnapToGrid(
                        ST_SetSRID(ST_MakePoint(
                            (packet->>'Longitude')::float,
                            (packet->>'Latitude')::float
                        ), 4326),
                        %f
                    )
            `, gridSize, gridSize, receiverId, days, conf.MaxDistanceMeters, gridSize)
		}
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
	TimeSeriesMessages   TimeSeriesType = "messages"
	TimeSeriesVessels    TimeSeriesType = "vessels"
	TimeSeriesDuplicates TimeSeriesType = "duplicates"
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
	Timestamp time.Time      `json:"timestamp"`
	Values    map[string]int `json:"values"` // Key is message_id or ais_class
}

// TimeSeriesResponse is the JSON response for the time-series endpoint
type TimeSeriesResponse struct {
	Type       TimeSeriesType        `json:"type"`
	Period     TimeSeriesPeriod      `json:"period"`
	DataPoints []TimeSeriesDataPoint `json:"data_points"`
}

// parseTimeSeriesTypeParam reads 'type' query param, defaults to "messages"
func parseTimeSeriesTypeParam(r *http.Request) TimeSeriesType {
	typeParam := r.URL.Query().Get("type")
	switch typeParam {
	case "vessels":
		return TimeSeriesVessels
	case "duplicates":
		return TimeSeriesDuplicates
	default:
		return TimeSeriesMessages
	}
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
	switch seriesType {
	case TimeSeriesMessages:
		response.DataPoints, err = fetchMessageTimeSeries(period, days, receiverID)
	case TimeSeriesVessels:
		response.DataPoints, err = fetchVesselTimeSeries(period, days, receiverID)
	case TimeSeriesDuplicates:
		response.DataPoints, err = fetchDuplicateTimeSeries(period, days, receiverID)
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

// duplicateVessel is the JSON shape returned by /statistics/stats/top-duplicates.
type duplicateVessel struct {
	UserID         int       `json:"user_id"`
	Name           string    `json:"name,omitempty"`
	ImageURL       string    `json:"image_url,omitempty"`
	AISClass       string    `json:"ais_class,omitempty"`
	Type           string    `json:"type,omitempty"`
	DuplicateCount int       `json:"duplicate_count"`
	ReceiverID     int       `json:"receiver_id"`
	ReceiverName   string    `json:"receiver_name,omitempty"`
	LastSeen       time.Time `json:"last_seen,omitempty"`
}

// topDuplicatesHandler provides the top 10 vessels with the most duplicate messages
func topDuplicatesHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-duplicates:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-duplicates:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("top-duplicates:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("top-duplicates:%dd", days)
		}
	}

	var vessels []duplicateVessel
	if ok, _ := cacheGet(cacheKey, &vessels); ok {
		// Enrich with vessel metadata
		vessels = enrichDuplicateVessels(vessels)

		// Enrich with receiver names
		vessels = enrichDuplicateVesselReceivers(vessels)

		respondJSON(w, vessels)
		return
	}

	// Build query with optional receiver_id filter and time range
	var qry string
	if receiverID > 0 {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
				SELECT
					m.user_id,
					m.receiver_id_duplicated AS receiver_id,
					COUNT(*) AS duplicate_count,
					MAX(m.timestamp) AS last_seen
				FROM
					messages m
				WHERE
					m.receiver_id_duplicated IS NOT NULL
					AND m.receiver_id = %d
					AND m.timestamp >= '%s'
					AND m.timestamp <= '%s'
				GROUP BY
					m.user_id, m.receiver_id_duplicated
				ORDER BY
					duplicate_count DESC
				LIMIT 10
			`, receiverID, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
		} else {
			qry = fmt.Sprintf(`
				SELECT
					m.user_id,
					m.receiver_id_duplicated AS receiver_id,
					COUNT(*) AS duplicate_count,
					MAX(m.timestamp) AS last_seen
				FROM
					messages m
				WHERE
					m.receiver_id_duplicated IS NOT NULL
					AND m.receiver_id = %d
					AND m.timestamp >= now() - INTERVAL '%d days'
				GROUP BY
					m.user_id, m.receiver_id_duplicated
				ORDER BY
					duplicate_count DESC
				LIMIT 10
			`, receiverID, days)
		}
	} else {
		if timeRange.UseRange {
			qry = fmt.Sprintf(`
				SELECT
					m.user_id,
					m.receiver_id_duplicated AS receiver_id,
					COUNT(*) AS duplicate_count,
					MAX(m.timestamp) AS last_seen
				FROM
					messages m
				WHERE
					m.receiver_id_duplicated IS NOT NULL
					AND m.timestamp >= '%s'
					AND m.timestamp <= '%s'
				GROUP BY
					m.user_id, m.receiver_id_duplicated
				ORDER BY
					duplicate_count DESC
				LIMIT 10
			`, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339))
		} else {
			qry = fmt.Sprintf(`
				SELECT
					m.user_id,
					m.receiver_id_duplicated AS receiver_id,
					COUNT(*) AS duplicate_count,
					MAX(m.timestamp) AS last_seen
				FROM
					messages m
				WHERE
					m.receiver_id_duplicated IS NOT NULL
					AND m.timestamp >= now() - INTERVAL '%d days'
				GROUP BY
					m.user_id, m.receiver_id_duplicated
				ORDER BY
					duplicate_count DESC
				LIMIT 10
			`, days)
		}
	}

	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		respondError(w, err)
		return
	}

	// Aggregate results from all shards
	type dupKey struct {
		userID     int
		receiverID int
	}

	dupMap := make(map[dupKey]struct {
		count    int
		lastSeen time.Time
	})

	for _, recs := range shardResults {
		for _, rec := range recs {
			userID, _ := parseInt(rec["user_id"])
			receiverID, _ := parseInt(rec["receiver_id"])
			count, _ := parseInt(rec["duplicate_count"])
			lastSeen, err := parseTime(rec["last_seen"])
			if err != nil {
				// Log the error to help diagnose the issue
				log.Printf("Warning: Could not parse last_seen timestamp for user_id=%d, receiver_id=%d: %v, raw value: %v",
					userID, receiverID, err, rec["last_seen"])

				// If we can't parse the timestamp, use the current time instead
				// This ensures we don't return the zero value
				lastSeen = time.Now()
			}

			key := dupKey{userID, receiverID}
			existing, exists := dupMap[key]
			if !exists || lastSeen.After(existing.lastSeen) {
				dupMap[key] = struct {
					count    int
					lastSeen time.Time
				}{count, lastSeen}
			} else if exists {
				// Keep the latest timestamp but add the counts
				dupMap[key] = struct {
					count    int
					lastSeen time.Time
				}{existing.count + count, existing.lastSeen}
			}
		}
	}

	// Convert to slice for sorting
	vessels = make([]duplicateVessel, 0, len(dupMap))
	for key, data := range dupMap {
		vessels = append(vessels, duplicateVessel{
			UserID:         key.userID,
			ReceiverID:     key.receiverID,
			DuplicateCount: data.count,
			LastSeen:       data.lastSeen,
		})
	}

	// Sort by duplicate count descending
	sort.Slice(vessels, func(i, j int) bool {
		return vessels[i].DuplicateCount > vessels[j].DuplicateCount
	})

	// Limit to top 10
	if len(vessels) > 10 {
		vessels = vessels[:10]
	}

	// Enrich with vessel metadata
	vessels = enrichDuplicateVessels(vessels)

	// Enrich with receiver names
	vessels = enrichDuplicateVesselReceivers(vessels)

	cacheSet(cacheKey, vessels)
	respondJSON(w, vessels)
}

// enrichDuplicateVessels looks up and injects Name/ImageURL/AISClass/Type for each vessel in-place.
func enrichDuplicateVessels(vs []duplicateVessel) []duplicateVessel {
	ids := make([]int, len(vs))
	for i, v := range vs {
		ids[i] = v.UserID
	}
	meta, err := fetchVesselMetadata(ids)
	if err != nil {
		log.Printf("enrichDuplicateVessels: metadata fetch error: %v", err)
		return vs
	}
	for i := range vs {
		if m, ok := meta[vs[i].UserID]; ok {
			vs[i].Name = m.Name
			vs[i].ImageURL = m.ImageURL
			vs[i].AISClass = m.AISClass
			vs[i].Type = m.Type
		}
	}
	return vs
}

// enrichDuplicateVesselReceivers looks up and injects ReceiverName for each vessel in-place.
func enrichDuplicateVesselReceivers(vs []duplicateVessel) []duplicateVessel {
	// Get unique receiver IDs
	receiverIDs := make(map[int]bool)
	for _, v := range vs {
		receiverIDs[v.ReceiverID] = true
	}

	// Convert to slice
	ids := make([]int, 0, len(receiverIDs))
	for id := range receiverIDs {
		ids = append(ids, id)
	}

	// Fetch receiver names
	receivers, err := fetchReceivers(ids)
	if err != nil {
		log.Printf("enrichDuplicateVesselReceivers: receiver fetch error: %v", err)
		return vs
	}

	// Update vessel records
	for i := range vs {
		if r, ok := receivers[vs[i].ReceiverID]; ok {
			vs[i].ReceiverName = r
		} else {
			vs[i].ReceiverName = fmt.Sprintf("Receiver %d", vs[i].ReceiverID)
		}
	}

	return vs
}

// fetchReceivers fetches receiver names by ID
func fetchReceivers(ids []int) (map[int]string, error) {
	if len(ids) == 0 {
		return make(map[int]string), nil
	}

	// Debug log the IDs being fetched
	log.Printf("Fetching receivers for IDs: %v", ids)

	// Convert IDs to string for query
	idStrs := make([]string, len(ids))
	for i, id := range ids {
		idStrs[i] = strconv.Itoa(id)
	}

	// Query receivers API
	resp, err := http.Get("/receivers")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("receivers API returned status %d", resp.StatusCode)
	}

	var receiverList []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&receiverList); err != nil {
		return nil, err
	}

	// Create map of ID to name
	result := make(map[int]string)
	for _, r := range receiverList {
		result[r.ID] = r.Name
	}

	return result, nil
}

// fetchDuplicateTimeSeries retrieves duplicate message counts grouped by time period and original receiver
func fetchDuplicateTimeSeries(period TimeSeriesPeriod, days int, receiverID int) ([]TimeSeriesDataPoint, error) {
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
				COALESCE(m.receiver_id_duplicated, 0) AS original_receiver_id,
				COALESCE(COUNT(m.id), 0) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
				AND m.receiver_id = %d
				AND m.receiver_id_duplicated IS NOT NULL
			GROUP BY
				ts.ts, m.receiver_id_duplicated
			ORDER BY
				ts.ts, m.receiver_id_duplicated
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
				COALESCE(m.receiver_id_duplicated, 0) AS original_receiver_id,
				COALESCE(COUNT(m.id), 0) AS count
			FROM
				time_series ts
			LEFT JOIN
				messages m ON DATE_TRUNC('%s', m.timestamp) = ts.ts
				AND m.receiver_id_duplicated IS NOT NULL
			GROUP BY
				ts.ts, m.receiver_id_duplicated
			ORDER BY
				ts.ts, m.receiver_id_duplicated
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

			originalReceiverID, err := parseInt(rec["original_receiver_id"])
			if err != nil {
				continue
			}

			count, err := parseInt(rec["count"])
			if err != nil {
				continue
			}

			// Convert receiver_id to string for the map key
			receiverIDStr := fmt.Sprintf("%d", originalReceiverID)

			// Initialize map for this timestamp if needed
			if _, ok := timeMap[ts]; !ok {
				timeMap[ts] = make(map[string]int)
			}

			// Add count to existing value (aggregating across shards)
			timeMap[ts][receiverIDStr] += count
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

// VHF Lift Detection Types and Handlers

// RangeAnomaly represents a period of unusual reception range
type RangeAnomaly struct {
	ReceiverID        int       `json:"receiver_id"`
	ReceiverName      string    `json:"receiver_name,omitempty"`
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
	NormalRangeKm     float64   `json:"normal_range_km"`
	AnomalyRangeKm    float64   `json:"anomaly_range_km"`
	PercentIncrease   float64   `json:"percent_increase"`
	MaxUserID         int       `json:"max_user_id"`
	MaxVesselName     string    `json:"max_vessel_name,omitempty"`
	MaxLat            float64   `json:"max_lat"`
	MaxLon            float64   `json:"max_lon"`
	MaxDistanceMeters float64   `json:"max_distance_meters"`
}

// DirectionAnomaly represents an unusual directional pattern in reception
type DirectionAnomaly struct {
	ReceiverID        int       `json:"receiver_id"`
	ReceiverName      string    `json:"receiver_name,omitempty"`
	Direction         string    `json:"direction"`
	StartTime         time.Time `json:"start_time"`
	EndTime           time.Time `json:"end_time"`
	NormalCount       int       `json:"normal_count"`
	AnomalyCount      int       `json:"anomaly_count"`
	PercentIncrease   float64   `json:"percent_increase"`
	MaxDistanceMeters float64   `json:"max_distance_meters"`
}

// vhfRangeAnomaliesHandler detects unusual increases in reception range
func vhfRangeAnomaliesHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("vhf-range-anomalies:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("vhf-range-anomalies:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("vhf-range-anomalies:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("vhf-range-anomalies:%dd", days)
		}
	}

	var anomalies []RangeAnomaly
	if ok, _ := cacheGet(cacheKey, &anomalies); ok {
		// Enrich with receiver names if needed
		if len(anomalies) > 0 {
			receiverIDs := make([]int, 0, len(anomalies))
			for _, a := range anomalies {
				if a.ReceiverID > 0 {
					receiverIDs = append(receiverIDs, a.ReceiverID)
				}
			}

			if len(receiverIDs) > 0 {
				receivers, err := fetchReceivers(receiverIDs)
				if err == nil {
					for i := range anomalies {
						if name, ok := receivers[anomalies[i].ReceiverID]; ok {
							anomalies[i].ReceiverName = name
						}
					}
				}
			}
		}

		respondJSON(w, anomalies)
		return
	}

	// Build query to get maximum distance per hour
	var qry string
	if timeRange.UseRange {
		qry = buildRangeAnomalyQuery(timeRange.From, timeRange.To, receiverID)
	} else {
		// Calculate from and to dates based on days parameter
		to := time.Now()
		from := to.AddDate(0, 0, -days)
		qry = buildRangeAnomalyQuery(from, to, receiverID)
	}

	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		respondError(w, err)
		return
	}

	// Process results to find anomalies
	anomalies = detectRangeAnomalies(shardResults)

	// Ensure we always return an array, even if empty
	if anomalies == nil {
		anomalies = []RangeAnomaly{}
	}

	// Enrich with vessel names
	for i := range anomalies {
		if anomalies[i].MaxUserID > 0 {
			meta, err := getVesselMetadata(anomalies[i].MaxUserID)
			if err == nil && meta.Name != "" {
				anomalies[i].MaxVesselName = meta.Name
			}
		}
	}

	// Cache results
	cacheSet(cacheKey, anomalies)
	respondJSON(w, anomalies)
}

// buildRangeAnomalyQuery creates SQL to get hourly max distances
func buildRangeAnomalyQuery(from, to time.Time, receiverID int) string {
	var qry string

	if receiverID > 0 {
		qry = fmt.Sprintf(`
			WITH receiver_locations AS (
				SELECT id, latitude, longitude
				FROM receivers
				WHERE id = %d
			),
			hourly_data AS (
				SELECT
					date_trunc('hour', m.timestamp) AS hour_start,
					m.receiver_id,
					m.user_id,
					(m.packet->>'Latitude')::float AS lat,
					(m.packet->>'Longitude')::float AS lon
				FROM messages m
				WHERE m.message_id IN (1,2,3,18,19)
				AND m.timestamp >= '%s'
				AND m.timestamp <= '%s'
				AND m.receiver_id = %d
				AND (m.packet->>'Latitude') IS NOT NULL
				AND (m.packet->>'Longitude') IS NOT NULL
				AND (m.packet->>'Latitude')::float <> 91.0
				AND (m.packet->>'Longitude')::float <> 181.0
				AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
			)
			SELECT
				h.hour_start,
				h.receiver_id,
				h.user_id,
				h.lat,
				h.lon,
				ST_DistanceSphere(
					ST_MakePoint(h.lon, h.lat),
					ST_MakePoint(r.longitude, r.latitude)
				) AS distance_meters
			FROM hourly_data h
			JOIN receiver_locations r ON h.receiver_id = r.id
			ORDER BY hour_start, distance_meters DESC
		`, receiverID, from.Format(time.RFC3339), to.Format(time.RFC3339), receiverID, conf.MaxDistanceMeters)
	} else {
		qry = fmt.Sprintf(`
			WITH receiver_locations AS (
				SELECT id, latitude, longitude
				FROM receivers
			),
			hourly_data AS (
				SELECT
					date_trunc('hour', m.timestamp) AS hour_start,
					m.receiver_id,
					m.user_id,
					(m.packet->>'Latitude')::float AS lat,
					(m.packet->>'Longitude')::float AS lon
				FROM messages m
				WHERE m.message_id IN (1,2,3,18,19)
				AND m.timestamp >= '%s'
				AND m.timestamp <= '%s'
				AND (m.packet->>'Latitude') IS NOT NULL
				AND (m.packet->>'Longitude') IS NOT NULL
				AND (m.packet->>'Latitude')::float <> 91.0
				AND (m.packet->>'Longitude')::float <> 181.0
				AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
			)
			SELECT
				h.hour_start,
				h.receiver_id,
				h.user_id,
				h.lat,
				h.lon,
				ST_DistanceSphere(
					ST_MakePoint(h.lon, h.lat),
					ST_MakePoint(r.longitude, r.latitude)
				) AS distance_meters
			FROM hourly_data h
			JOIN receiver_locations r ON h.receiver_id = r.id
			ORDER BY hour_start, distance_meters DESC
		`, from.Format(time.RFC3339), to.Format(time.RFC3339), conf.MaxDistanceMeters)
	}

	return qry
}

// detectRangeAnomalies processes query results to find unusual range increases
func detectRangeAnomalies(shardResults map[string][]map[string]interface{}) []RangeAnomaly {
	// Group max distances by receiver and hour
	type hourlyMax struct {
		hour        time.Time
		maxDistance float64
		userID      int
		lat         float64
		lon         float64
	}

	receiverHourlyMax := make(map[int][]hourlyMax)

	for _, recs := range shardResults {
		for _, rec := range recs {
			hour, err := parseTime(rec["hour_start"])
			if err != nil {
				hour = time.Now()
			}
			receiverID, _ := parseInt(rec["receiver_id"])
			userID, _ := parseInt(rec["user_id"])
			lat, _ := parseFloat(rec["lat"])
			lon, _ := parseFloat(rec["lon"])
			distance, _ := parseFloat(rec["distance_meters"])

			// Skip if we already have a record for this hour and receiver with a greater distance
			found := false
			for i, existing := range receiverHourlyMax[receiverID] {
				if existing.hour.Equal(hour) {
					found = true
					if distance > existing.maxDistance {
						receiverHourlyMax[receiverID][i] = hourlyMax{
							hour:        hour,
							maxDistance: distance,
							userID:      userID,
							lat:         lat,
							lon:         lon,
						}
					}
					break
				}
			}

			if !found {
				receiverHourlyMax[receiverID] = append(receiverHourlyMax[receiverID], hourlyMax{
					hour:        hour,
					maxDistance: distance,
					userID:      userID,
					lat:         lat,
					lon:         lon,
				})
			}
		}
	}

	// Calculate baseline and detect anomalies
	var anomalies []RangeAnomaly

	for receiverID, hourlyData := range receiverHourlyMax {
		// Sort by hour
		sort.Slice(hourlyData, func(i, j int) bool {
			return hourlyData[i].hour.Before(hourlyData[j].hour)
		})

		// Need at least 2 hours of data to establish a baseline
		if len(hourlyData) < 2 {
			continue
		}

		// Calculate baseline (median of first 24 hours or all available if less)
		baselineCount := min(24, len(hourlyData))
		baselineDistances := make([]float64, baselineCount)
		for i := 0; i < baselineCount; i++ {
			baselineDistances[i] = hourlyData[i].maxDistance
		}
		sort.Float64s(baselineDistances)
		baselineDistance := baselineDistances[baselineCount/2] // median

		// Look for anomalies (20%+ increase over baseline)
		anomalyThreshold := baselineDistance * 1.2

		var currentAnomaly *RangeAnomaly

		for i, data := range hourlyData {
			if data.maxDistance > anomalyThreshold {
				// Start or continue anomaly
				if currentAnomaly == nil {
					currentAnomaly = &RangeAnomaly{
						ReceiverID:        receiverID,
						StartTime:         data.hour,
						EndTime:           data.hour.Add(time.Hour),
						NormalRangeKm:     baselineDistance / 1000.0, // convert to km
						AnomalyRangeKm:    data.maxDistance / 1000.0, // convert to km
						PercentIncrease:   (data.maxDistance - baselineDistance) / baselineDistance * 100.0,
						MaxUserID:         data.userID,
						MaxLat:            data.lat,
						MaxLon:            data.lon,
						MaxDistanceMeters: data.maxDistance,
					}
				} else {
					// Update end time and max values if needed
					currentAnomaly.EndTime = data.hour.Add(time.Hour)
					if data.maxDistance > currentAnomaly.MaxDistanceMeters {
						currentAnomaly.MaxDistanceMeters = data.maxDistance
						currentAnomaly.AnomalyRangeKm = data.maxDistance / 1000.0
						currentAnomaly.PercentIncrease = (data.maxDistance - baselineDistance) / baselineDistance * 100.0
						currentAnomaly.MaxUserID = data.userID
						currentAnomaly.MaxLat = data.lat
						currentAnomaly.MaxLon = data.lon
					}
				}

				// If this is the last data point or next point is not anomalous, save the anomaly
				if i == len(hourlyData)-1 || hourlyData[i+1].maxDistance <= anomalyThreshold {
					if currentAnomaly != nil {
						// Only consider anomalies that span at least 2 hours to filter out random errors
						startHour := currentAnomaly.StartTime.Hour()
						endHour := currentAnomaly.EndTime.Hour()
						if startHour != endHour || currentAnomaly.StartTime.Day() != currentAnomaly.EndTime.Day() {
							anomalies = append(anomalies, *currentAnomaly)
						}
						currentAnomaly = nil
					}
				}
			}
		}
	}

	// Sort anomalies by percent increase (descending)
	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].PercentIncrease > anomalies[j].PercentIncrease
	})

	return anomalies
}

// vhfDirectionAnomaliesHandler detects unusual directional patterns in reception
func vhfDirectionAnomaliesHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverID := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Include receiver_id and time range in cache key if specified
	var cacheKey string
	if timeRange.UseRange {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("vhf-direction-anomalies:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverID)
		} else {
			cacheKey = fmt.Sprintf("vhf-direction-anomalies:%s-%s",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		}
	} else {
		if receiverID > 0 {
			cacheKey = fmt.Sprintf("vhf-direction-anomalies:%dd:r%d", days, receiverID)
		} else {
			cacheKey = fmt.Sprintf("vhf-direction-anomalies:%dd", days)
		}
	}

	var anomalies []DirectionAnomaly
	if ok, _ := cacheGet(cacheKey, &anomalies); ok {
		// Enrich with receiver names if needed
		if len(anomalies) > 0 {
			receiverIDs := make([]int, 0, len(anomalies))
			for _, a := range anomalies {
				if a.ReceiverID > 0 {
					receiverIDs = append(receiverIDs, a.ReceiverID)
				}
			}

			if len(receiverIDs) > 0 {
				receivers, err := fetchReceivers(receiverIDs)
				if err == nil {
					for i := range anomalies {
						if name, ok := receivers[anomalies[i].ReceiverID]; ok {
							anomalies[i].ReceiverName = name
						}
					}
				}
			}
		}

		respondJSON(w, anomalies)
		return
	}

	// Build query to get directional data
	var qry string
	if timeRange.UseRange {
		qry = buildDirectionAnomalyQuery(timeRange.From, timeRange.To, receiverID)
	} else {
		// Calculate from and to dates based on days parameter
		to := time.Now()
		from := to.AddDate(0, 0, -days)
		qry = buildDirectionAnomalyQuery(from, to, receiverID)
	}

	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		respondError(w, err)
		return
	}

	// Process results to find directional anomalies
	anomalies = detectDirectionAnomalies(shardResults)

	// Ensure we always return an array, even if empty
	if anomalies == nil {
		anomalies = []DirectionAnomaly{}
	}

	// Cache results
	cacheSet(cacheKey, anomalies)
	respondJSON(w, anomalies)
}

// buildDirectionAnomalyQuery creates SQL to get hourly directional data
func buildDirectionAnomalyQuery(from, to time.Time, receiverID int) string {
	var qry string

	if receiverID > 0 {
		qry = fmt.Sprintf(`
			WITH receiver_locations AS (
				SELECT id, latitude, longitude
				FROM receivers
				WHERE id = %d
			),
			message_data AS (
				SELECT
					date_trunc('hour', m.timestamp) AS hour_start,
					m.receiver_id,
					(m.packet->>'Latitude')::float AS lat,
					(m.packet->>'Longitude')::float AS lon
				FROM messages m
				WHERE m.message_id IN (1,2,3,18,19)
				AND m.timestamp >= '%s'
				AND m.timestamp <= '%s'
				AND m.receiver_id = %d
				AND (m.packet->>'Latitude') IS NOT NULL
				AND (m.packet->>'Longitude') IS NOT NULL
				AND (m.packet->>'Latitude')::float <> 91.0
				AND (m.packet->>'Longitude')::float <> 181.0
				AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
			),
			message_with_distance AS (
				SELECT
					m.hour_start,
					m.receiver_id,
					m.lat,
					m.lon,
					ST_DistanceSphere(
						ST_MakePoint(m.lon, m.lat),
						ST_MakePoint(r.longitude, r.latitude)
					) AS distance_meters,
					degrees(ST_Azimuth(
						ST_MakePoint(r.longitude, r.latitude),
						ST_MakePoint(m.lon, m.lat)
					)) AS bearing
				FROM message_data m
				JOIN receiver_locations r ON m.receiver_id = r.id
			)
			SELECT
				hour_start,
				receiver_id,
				CASE
					WHEN bearing BETWEEN 0 AND 22.5 OR bearing BETWEEN 337.5 AND 360 THEN 'N'
					WHEN bearing BETWEEN 22.5 AND 67.5 THEN 'NE'
					WHEN bearing BETWEEN 67.5 AND 112.5 THEN 'E'
					WHEN bearing BETWEEN 112.5 AND 157.5 THEN 'SE'
					WHEN bearing BETWEEN 157.5 AND 202.5 THEN 'S'
					WHEN bearing BETWEEN 202.5 AND 247.5 THEN 'SW'
					WHEN bearing BETWEEN 247.5 AND 292.5 THEN 'W'
					WHEN bearing BETWEEN 292.5 AND 337.5 THEN 'NW'
				END AS direction,
				COUNT(*) AS message_count,
				MAX(distance_meters) AS max_distance
			FROM message_with_distance
			GROUP BY hour_start, receiver_id, direction
			ORDER BY hour_start, direction
		`, receiverID, from.Format(time.RFC3339), to.Format(time.RFC3339), receiverID, conf.MaxDistanceMeters)
	} else {
		qry = fmt.Sprintf(`
			WITH receiver_locations AS (
				SELECT id, latitude, longitude
				FROM receivers
			),
			message_data AS (
				SELECT
					date_trunc('hour', m.timestamp) AS hour_start,
					m.receiver_id,
					(m.packet->>'Latitude')::float AS lat,
					(m.packet->>'Longitude')::float AS lon
				FROM messages m
				WHERE m.message_id IN (1,2,3,18,19)
				AND m.timestamp >= '%s'
				AND m.timestamp <= '%s'
				AND (m.packet->>'Latitude') IS NOT NULL
				AND (m.packet->>'Longitude') IS NOT NULL
				AND (m.packet->>'Latitude')::float <> 91.0
				AND (m.packet->>'Longitude')::float <> 181.0
				AND distance IS NOT NULL AND distance <= %d -- Only include points with valid distances <= configured distance
			),
			message_with_distance AS (
				SELECT
					m.hour_start,
					m.receiver_id,
					m.lat,
					m.lon,
					ST_DistanceSphere(
						ST_MakePoint(m.lon, m.lat),
						ST_MakePoint(r.longitude, r.latitude)
					) AS distance_meters,
					degrees(ST_Azimuth(
						ST_MakePoint(r.longitude, r.latitude),
						ST_MakePoint(m.lon, m.lat)
					)) AS bearing
				FROM message_data m
				JOIN receiver_locations r ON m.receiver_id = r.id
			)
			SELECT
				hour_start,
				receiver_id,
				CASE
					WHEN bearing BETWEEN 0 AND 22.5 OR bearing BETWEEN 337.5 AND 360 THEN 'N'
					WHEN bearing BETWEEN 22.5 AND 67.5 THEN 'NE'
					WHEN bearing BETWEEN 67.5 AND 112.5 THEN 'E'
					WHEN bearing BETWEEN 112.5 AND 157.5 THEN 'SE'
					WHEN bearing BETWEEN 157.5 AND 202.5 THEN 'S'
					WHEN bearing BETWEEN 202.5 AND 247.5 THEN 'SW'
					WHEN bearing BETWEEN 247.5 AND 292.5 THEN 'W'
					WHEN bearing BETWEEN 292.5 AND 337.5 THEN 'NW'
				END AS direction,
				COUNT(*) AS message_count,
				MAX(distance_meters) AS max_distance
			FROM message_with_distance
			GROUP BY hour_start, receiver_id, direction
			ORDER BY hour_start, receiver_id, direction
		`, from.Format(time.RFC3339), to.Format(time.RFC3339), conf.MaxDistanceMeters)
	}

	return qry
}

// detectDirectionAnomalies processes query results to find unusual directional patterns
// duplicatesHeatmapHandler provides a grid-based heatmap of duplicate messages
func duplicatesHeatmapHandler(w http.ResponseWriter, r *http.Request) {
	days := parseDaysParam(r)
	receiverId := parseReceiverIDParam(r)
	timeRange, err := parseTimeRangeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Define cache key with time range if provided
	var cacheKey string
	if timeRange.UseRange {
		if receiverId < 0 {
			cacheKey = fmt.Sprintf("duplicates-heatmap:%s-%s:all",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"))
		} else {
			cacheKey = fmt.Sprintf("duplicates-heatmap:%s-%s:r%d",
				timeRange.From.Format("2006-01-02T15:04:05Z"),
				timeRange.To.Format("2006-01-02T15:04:05Z"),
				receiverId)
		}
	} else {
		if receiverId < 0 {
			cacheKey = fmt.Sprintf("duplicates-heatmap:%dd:all", days)
		} else {
			cacheKey = fmt.Sprintf("duplicates-heatmap:%dd:r%d", days, receiverId)
		}
	}

	// Define the response structure - reusing GridCell from coverageMapHandler
	var heatmapData []GridCell

	// Try to get from cache
	if ok, _ := cacheGet(cacheKey, &heatmapData); ok {
		respondJSON(w, heatmapData)
		return
	}

	// Grid size in degrees (approximately 1km at the equator)
	const gridSize = 0.01

	// Build query with PostGIS functions
	var qry string
	if receiverId < 0 {
		// Query for all receivers
		if timeRange.UseRange {
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
	                   COUNT(*) AS count,
	                   array_agg(DISTINCT receiver_id_duplicated) AS receiver_ids
	               FROM messages
	               WHERE receiver_id_duplicated IS NOT NULL
	                   AND timestamp >= '%s'
	                   AND timestamp <= '%s'
	                   AND (packet->>'Latitude')::float IS NOT NULL
	                   AND (packet->>'Longitude')::float IS NOT NULL
	                   AND (packet->>'Latitude')::float BETWEEN -90 AND 90
	                   AND (packet->>'Longitude')::float BETWEEN -180 AND 180
	           `, gridSize, gridSize, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), gridSize)
		} else {
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
	                   COUNT(*) AS count,
	                   array_agg(DISTINCT receiver_id_duplicated) AS receiver_ids
	               FROM messages
	               WHERE receiver_id_duplicated IS NOT NULL
	                   AND timestamp >= now() - INTERVAL '%d days'
	                   AND (packet->>'Latitude')::float IS NOT NULL
	                   AND (packet->>'Longitude')::float IS NOT NULL
	                   AND (packet->>'Latitude')::float BETWEEN -90 AND 90
	                   AND (packet->>'Longitude')::float BETWEEN -180 AND 180
	           `, gridSize, gridSize, days, gridSize)
		}
	} else {
		// Query for a specific receiver
		if timeRange.UseRange {
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
	                   COUNT(*) AS count,
	                   array_agg(DISTINCT receiver_id_duplicated) AS receiver_ids
	               FROM messages
	               WHERE receiver_id_duplicated IS NOT NULL
	                   AND receiver_id = %d
	                   AND timestamp >= '%s'
	                   AND timestamp <= '%s'
	                   AND (packet->>'Latitude')::float IS NOT NULL
	                   AND (packet->>'Longitude')::float IS NOT NULL
	                   AND (packet->>'Latitude')::float BETWEEN -90 AND 90
	                   AND (packet->>'Longitude')::float BETWEEN -180 AND 180
	               GROUP BY
	                   ST_SnapToGrid(
	                       ST_SetSRID(ST_MakePoint(
	                           (packet->>'Longitude')::float,
	                           (packet->>'Latitude')::float
	                       ), 4326),
	                       %f
	                   )
	           `, gridSize, gridSize, receiverId, timeRange.From.Format(time.RFC3339), timeRange.To.Format(time.RFC3339), gridSize)
		} else {
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
	                   COUNT(*) AS count,
	                   array_agg(DISTINCT receiver_id_duplicated) AS receiver_ids
	               FROM messages
	               WHERE receiver_id_duplicated IS NOT NULL
	                   AND receiver_id = %d
	                   AND timestamp >= now() - INTERVAL '%d days'
	                   AND (packet->>'Latitude')::float IS NOT NULL
	                   AND (packet->>'Longitude')::float IS NOT NULL
	                   AND (packet->>'Latitude')::float BETWEEN -90 AND 90
	                   AND (packet->>'Longitude')::float BETWEEN -180 AND 180
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
	}

	// Debug log the SQL query
	log.Printf("SQL query: %s", qry)

	// Query all shards
	shardResults, err := QueryDatabasesForAllShards(qry)
	if err != nil {
		respondError(w, err)
		return
	}

	// Debug log the raw query results
	log.Printf("Raw query results: %+v", shardResults)

	// Debug log the first few records to see if receiver_id is present
	for shardName, recs := range shardResults {
		if len(recs) > 0 {
			log.Printf("First record from shard %s: %+v", shardName, recs[0])
			if receiverID, ok := recs[0]["receiver_id"]; ok {
				log.Printf("receiver_id found: %v (type: %T)", receiverID, receiverID)
			} else {
				log.Printf("receiver_id not found in record")
				log.Printf("Available keys: %v", getMapKeys(recs[0]))
			}
			break
		}
	}

	// Process results from all shards
	cellMap := make(map[string]GridCell)
	allReceiverIDs := make(map[int]bool)

	for _, recs := range shardResults {
		for _, rec := range recs {
			lat, _ := parseFloat(rec["lat"])
			lon, _ := parseFloat(rec["lon"])
			count, _ := parseInt(rec["count"])

			// Get receiver_ids array
			var receiverIDs []int
			if receiverIDsStr, ok := rec["receiver_ids"]; ok && receiverIDsStr != nil {
				// Debug log the raw receiver_ids value
				log.Printf("Raw receiver_ids: %v (type: %T)", receiverIDsStr, receiverIDsStr)

				// Parse the PostgreSQL array format: {1,2,3}
				if pgArray, ok := receiverIDsStr.(string); ok && strings.HasPrefix(pgArray, "{") && strings.HasSuffix(pgArray, "}") {
					// Remove the braces and split by comma
					pgArray = pgArray[1 : len(pgArray)-1]
					if pgArray != "" {
						idStrs := strings.Split(pgArray, ",")
						for _, idStr := range idStrs {
							id, err := strconv.Atoi(idStr)
							if err == nil && id > 0 {
								receiverIDs = append(receiverIDs, id)
								allReceiverIDs[id] = true
								log.Printf("Added receiver ID: %d to allReceiverIDs", id)
							}
						}
					}
				}

				log.Printf("Parsed receiver_ids: %v", receiverIDs)
			} else {
				log.Printf("No receiver_ids found in record: %v", rec)
				log.Printf("Available keys in record: %v", getMapKeys(rec))
			}

			// Create a key for the grid cell to aggregate across shards
			key := fmt.Sprintf("%.6f:%.6f", lat, lon)
			log.Printf("Processing grid cell with key: %s", key)

			if cell, exists := cellMap[key]; exists {
				log.Printf("Cell already exists with %d receiver IDs", len(cell.ReceiverIDs))
				cell.Count += count

				// Add these receiver IDs if they're not already in the list
				for _, newID := range receiverIDs {
					if newID > 0 {
						found := false
						for _, existingID := range cell.ReceiverIDs {
							if existingID == newID {
								found = true
								break
							}
						}
						if !found {
							log.Printf("Adding receiver ID %d to existing cell", newID)
							cell.ReceiverIDs = append(cell.ReceiverIDs, newID)
						} else {
							log.Printf("Receiver ID %d already in cell", newID)
						}
					}
				}

				cellMap[key] = cell
				log.Printf("Updated cell now has %d receiver IDs", len(cell.ReceiverIDs))
			} else {
				log.Printf("Creating new cell with %d receiver IDs", len(receiverIDs))
				cellMap[key] = GridCell{
					Lat:         lat,
					Lon:         lon,
					Count:       count,
					ReceiverIDs: receiverIDs,
				}
				log.Printf("New cell created with receiver IDs: %v", receiverIDs)
			}
		}
	}

	// Fetch receiver names for all receiver IDs
	receiverIDsList := make([]int, 0, len(allReceiverIDs))
	for id := range allReceiverIDs {
		receiverIDsList = append(receiverIDsList, id)
	}
	// Fetch receiver names for all receiver IDs
	receiverNames, err := fetchReceivers(receiverIDsList)
	if err != nil {
		log.Printf("duplicatesHeatmapHandler: Error fetching receiver names: %v", err)
	}

	// Convert map to slice and add receiver names
	heatmapData = make([]GridCell, 0, len(cellMap))
	for key, cell := range cellMap {
		// Debug log the cell's receiver IDs
		log.Printf("Cell %s has receiver IDs: %v", key, cell.ReceiverIDs)

		// Add receiver names if available
		if len(cell.ReceiverIDs) > 0 && len(receiverNames) > 0 {
			names := make([]string, 0, len(cell.ReceiverIDs))
			for _, id := range cell.ReceiverIDs {
				if name, ok := receiverNames[id]; ok {
					names = append(names, name)
					log.Printf("Added receiver name %s for ID %d", name, id)
				} else {
					names = append(names, fmt.Sprintf("Receiver %d", id))
					log.Printf("No name found for receiver ID %d", id)
				}
			}
			cell.ReceiverNames = names
			// Update the cell in the map with the new ReceiverNames
			cellMap[key] = cell

			// Debug log the cell's receiver names
			log.Printf("Cell %s now has receiver names: %v", key, cell.ReceiverNames)
		} else {
			log.Printf("Cell %s has no receiver IDs or no receiver names available", key)
		}
		heatmapData = append(heatmapData, cell)
	}

	// Cache the result
	cacheSet(cacheKey, heatmapData)
	respondJSON(w, heatmapData)
}

// Helper function to get the keys of a map
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func detectDirectionAnomalies(shardResults map[string][]map[string]interface{}) []DirectionAnomaly {
	// Group by receiver, hour, and direction
	type directionData struct {
		count       int
		maxDistance float64
	}

	type hourData map[string]directionData   // direction -> data
	type receiverData map[time.Time]hourData // hour -> direction data

	receiverDirections := make(map[int]receiverData)

	for _, recs := range shardResults {
		for _, rec := range recs {
			hour, err := parseTime(rec["hour_start"])
			if err != nil {
				hour = time.Now()
			}
			receiverID, _ := parseInt(rec["receiver_id"])
			direction, _ := parseString(rec["direction"])
			count, _ := parseInt(rec["message_count"])
			maxDistance, _ := parseFloat(rec["max_distance"])

			// Initialize maps if needed
			if _, exists := receiverDirections[receiverID]; !exists {
				receiverDirections[receiverID] = make(receiverData)
			}

			if _, exists := receiverDirections[receiverID][hour]; !exists {
				receiverDirections[receiverID][hour] = make(hourData)
			}

			// Store the data
			receiverDirections[receiverID][hour][direction] = directionData{
				count:       count,
				maxDistance: maxDistance,
			}
		}
	}

	// Calculate baselines and detect anomalies
	var anomalies []DirectionAnomaly

	for receiverID, data := range receiverDirections {
		// Convert to time-sorted slice for easier processing
		type timeDirectionData struct {
			hour      time.Time
			direction string
			count     int
			maxDist   float64
		}

		var timeSeriesData []timeDirectionData

		for hour, dirData := range data {
			for dir, counts := range dirData {
				timeSeriesData = append(timeSeriesData, timeDirectionData{
					hour:      hour,
					direction: dir,
					count:     counts.count,
					maxDist:   counts.maxDistance,
				})
			}
		}

		// Sort by hour
		sort.Slice(timeSeriesData, func(i, j int) bool {
			return timeSeriesData[i].hour.Before(timeSeriesData[j].hour)
		})

		// Group by direction to calculate baselines
		directionBaselines := make(map[string]int)
		directionCounts := make(map[string]int)

		for _, d := range timeSeriesData {
			directionBaselines[d.direction] += d.count
			directionCounts[d.direction]++
		}

		// Calculate average counts per direction
		for dir := range directionBaselines {
			// Need at least 2 data points for a direction to establish a baseline
			if directionCounts[dir] >= 2 {
				directionBaselines[dir] = directionBaselines[dir] / directionCounts[dir]
			} else {
				// Remove directions with insufficient data
				delete(directionBaselines, dir)
			}
		}

		// Look for anomalies (50%+ increase in a specific direction)
		var currentAnomalies = make(map[string]*DirectionAnomaly)

		for _, d := range timeSeriesData {
			baseline := directionBaselines[d.direction]
			if baseline == 0 {
				continue // Skip directions with no baseline
			}

			// Check if this is an anomaly (50%+ increase)
			anomalyThreshold := float64(baseline) * 1.5
			if float64(d.count) > anomalyThreshold {
				// Start or continue anomaly
				if _, exists := currentAnomalies[d.direction]; !exists {
					currentAnomalies[d.direction] = &DirectionAnomaly{
						ReceiverID:        receiverID,
						Direction:         d.direction,
						StartTime:         d.hour,
						EndTime:           d.hour.Add(time.Hour),
						NormalCount:       baseline,
						AnomalyCount:      d.count,
						PercentIncrease:   (float64(d.count) - float64(baseline)) / float64(baseline) * 100.0,
						MaxDistanceMeters: d.maxDist,
					}
				} else {
					// Update end time and max values if needed
					currentAnomalies[d.direction].EndTime = d.hour.Add(time.Hour)
					if d.count > currentAnomalies[d.direction].AnomalyCount {
						currentAnomalies[d.direction].AnomalyCount = d.count
						currentAnomalies[d.direction].PercentIncrease =
							(float64(d.count) - float64(baseline)) / float64(baseline) * 100.0
					}
					if d.maxDist > currentAnomalies[d.direction].MaxDistanceMeters {
						currentAnomalies[d.direction].MaxDistanceMeters = d.maxDist
					}
				}
			} else {
				// If this direction had an anomaly and it's now over, save it
				if anom, exists := currentAnomalies[d.direction]; exists {
					// Only consider anomalies that span at least 2 hours to filter out random errors
					startHour := anom.StartTime.Hour()
					endHour := anom.EndTime.Hour()
					if startHour != endHour || anom.StartTime.Day() != anom.EndTime.Day() {
						anomalies = append(anomalies, *anom)
					}
					delete(currentAnomalies, d.direction)
				}
			}
		}

		// Add any remaining anomalies
		for _, anom := range currentAnomalies {
			// Only consider anomalies that span at least 2 hours to filter out random errors
			startHour := anom.StartTime.Hour()
			endHour := anom.EndTime.Hour()
			if startHour != endHour || anom.StartTime.Day() != anom.EndTime.Day() {
				anomalies = append(anomalies, *anom)
			}
		}
	}

	// Sort anomalies by percent increase (descending)
	sort.Slice(anomalies, func(i, j int) bool {
		return anomalies[i].PercentIncrease > anomalies[j].PercentIncrease
	})

	return anomalies
}
