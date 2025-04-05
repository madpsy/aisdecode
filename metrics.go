// metrics.go
package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------
// Global aggregator variables for different roll-up intervals.
var (
	minuteAgg MetricsAggregator
	hourAgg   MetricsAggregator
	dayAgg    MetricsAggregator
	weekAgg   MetricsAggregator
	monthAgg  MetricsAggregator
)

// -----------------------------------------------------------------------------
// Aggregation types and helper functions.

// AggregatedMetric holds the minimum, maximum, and average for a given metric.
type AggregatedMetric struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	Ave float64 `json:"average"`
}

// NumericAggregator maintains running statistics for a numeric metric.
type NumericAggregator struct {
	min   float64
	max   float64
	sum   float64
	count int
}

// newNumericAggregator creates a new NumericAggregator with proper initial values.
func newNumericAggregator() NumericAggregator {
	return NumericAggregator{
		min:   math.Inf(1),
		max:   math.Inf(-1),
		sum:   0,
		count: 0,
	}
}

func (na *NumericAggregator) update(value float64) {
	if value < na.min {
		na.min = value
	}
	if value > na.max {
		na.max = value
	}
	na.sum += value
	na.count++
}

func (na *NumericAggregator) average() float64 {
	if na.count == 0 {
		return 0
	}
	return na.sum / float64(na.count)
}

func (na *NumericAggregator) reset() {
	na.min = math.Inf(1)
	na.max = math.Inf(-1)
	na.sum = 0
	na.count = 0
}

func defaultMetricsAggregate() MetricsAggregate {
	return MetricsAggregate{
		Timestamp: time.Now().UTC(),
		SerialMessagesPerSec: AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		UDPMessagesPerSec:    AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		SerialMessagesPerMin: AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		UDPMessagesPerMin:    AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		TotalDeduplications:  AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		ActiveWebSockets:     AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		NumVesselsClassA:     AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		NumVesselsClassB:     AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		NumVesselsAtoN:       AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		NumVesselsBaseStation: AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		NumVesselsSAR:         AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		TotalKnownVessels:     AggregatedMetric{Min: 0, Max: 0, Ave: 0},
		TotalMessages:         0,
	}
}

// MetricsAggregator collects running stats for each metric.
// Note: TotalMessages is cumulative so we only record its latest value.
type MetricsAggregator struct {
	SerialMessagesPerSec  NumericAggregator
	UDPMessagesPerSec     NumericAggregator
	SerialMessagesPerMin  NumericAggregator
	UDPMessagesPerMin     NumericAggregator
	TotalDeduplications   NumericAggregator
	ActiveWebSockets      NumericAggregator
	NumVesselsClassA      NumericAggregator
	NumVesselsClassB      NumericAggregator
	NumVesselsAtoN        NumericAggregator
	NumVesselsBaseStation NumericAggregator
	NumVesselsSAR         NumericAggregator
	TotalKnownVessels     NumericAggregator
	TotalMessages         int
}

// update updates the aggregator with a new live Metrics sample.
// (Metrics is assumed to be your existing type for live metrics.)
func (ma *MetricsAggregator) update(m Metrics) {
	ma.SerialMessagesPerSec.update(m.SerialMessagesPerSec)
	ma.UDPMessagesPerSec.update(m.UDPMessagesPerSec)
	ma.SerialMessagesPerMin.update(m.SerialMessagesPerMin)
	ma.UDPMessagesPerMin.update(m.UDPMessagesPerMin)
	ma.TotalDeduplications.update(float64(m.TotalDeduplications))
	ma.ActiveWebSockets.update(float64(m.ActiveWebSockets))
	ma.NumVesselsClassA.update(float64(m.NumVesselsClassA))
	ma.NumVesselsClassB.update(float64(m.NumVesselsClassB))
	ma.NumVesselsAtoN.update(float64(m.NumVesselsAtoN))
	ma.NumVesselsBaseStation.update(float64(m.NumVesselsBaseStation))
	ma.NumVesselsSAR.update(float64(m.NumVesselsSAR))
	ma.TotalKnownVessels.update(float64(m.TotalKnownVessels))
	ma.TotalMessages = m.TotalMessages // keep the latest cumulative value
}

// MetricsAggregate represents a finalized snapshot of aggregated metrics.
type MetricsAggregate struct {
	Timestamp             time.Time        `json:"timestamp"`
	SerialMessagesPerSec  AggregatedMetric `json:"serial_messages_per_sec"`
	UDPMessagesPerSec     AggregatedMetric `json:"udp_messages_per_sec"`
	SerialMessagesPerMin  AggregatedMetric `json:"serial_messages_per_min"`
	UDPMessagesPerMin     AggregatedMetric `json:"udp_messages_per_min"`
	TotalDeduplications   AggregatedMetric `json:"total_deduplications"`
	ActiveWebSockets      AggregatedMetric `json:"active_websockets"`
	NumVesselsClassA      AggregatedMetric `json:"num_vessels_class_a"`
	NumVesselsClassB      AggregatedMetric `json:"num_vessels_class_b"`
	NumVesselsAtoN        AggregatedMetric `json:"num_vessels_aton"`
	NumVesselsBaseStation AggregatedMetric `json:"num_vessels_base_station"`
	NumVesselsSAR         AggregatedMetric `json:"num_vessels_sar"`
	TotalKnownVessels     AggregatedMetric `json:"total_known_vessels"`
	TotalMessages         int              `json:"total_messages"`
}

// finalize produces a snapshot from the aggregator.
func (ma *MetricsAggregator) finalize() MetricsAggregate {
	return MetricsAggregate{
		Timestamp: time.Now().UTC(),
		SerialMessagesPerSec: AggregatedMetric{
			Min: ma.SerialMessagesPerSec.min,
			Max: ma.SerialMessagesPerSec.max,
			Ave: ma.SerialMessagesPerSec.average(),
		},
		UDPMessagesPerSec: AggregatedMetric{
			Min: ma.UDPMessagesPerSec.min,
			Max: ma.UDPMessagesPerSec.max,
			Ave: ma.UDPMessagesPerSec.average(),
		},
		SerialMessagesPerMin: AggregatedMetric{
			Min: ma.SerialMessagesPerMin.min,
			Max: ma.SerialMessagesPerMin.max,
			Ave: ma.SerialMessagesPerMin.average(),
		},
		UDPMessagesPerMin: AggregatedMetric{
			Min: ma.UDPMessagesPerMin.min,
			Max: ma.UDPMessagesPerMin.max,
			Ave: ma.UDPMessagesPerMin.average(),
		},
		TotalDeduplications: AggregatedMetric{
			Min: ma.TotalDeduplications.min,
			Max: ma.TotalDeduplications.max,
			Ave: ma.TotalDeduplications.average(),
		},
		ActiveWebSockets: AggregatedMetric{
			Min: ma.ActiveWebSockets.min,
			Max: ma.ActiveWebSockets.max,
			Ave: ma.ActiveWebSockets.average(),
		},
		NumVesselsClassA: AggregatedMetric{
			Min: ma.NumVesselsClassA.min,
			Max: ma.NumVesselsClassA.max,
			Ave: ma.NumVesselsClassA.average(),
		},
		NumVesselsClassB: AggregatedMetric{
			Min: ma.NumVesselsClassB.min,
			Max: ma.NumVesselsClassB.max,
			Ave: ma.NumVesselsClassB.average(),
		},
		NumVesselsAtoN: AggregatedMetric{
			Min: ma.NumVesselsAtoN.min,
			Max: ma.NumVesselsAtoN.max,
			Ave: ma.NumVesselsAtoN.average(),
		},
		NumVesselsBaseStation: AggregatedMetric{
			Min: ma.NumVesselsBaseStation.min,
			Max: ma.NumVesselsBaseStation.max,
			Ave: ma.NumVesselsBaseStation.average(),
		},
		NumVesselsSAR: AggregatedMetric{
			Min: ma.NumVesselsSAR.min,
			Max: ma.NumVesselsSAR.max,
			Ave: ma.NumVesselsSAR.average(),
		},
		TotalKnownVessels: AggregatedMetric{
			Min: ma.TotalKnownVessels.min,
			Max: ma.TotalKnownVessels.max,
			Ave: ma.TotalKnownVessels.average(),
		},
		TotalMessages: ma.TotalMessages,
	}
}

// reset clears the aggregator (except for the cumulative TotalMessages).
func (ma *MetricsAggregator) reset() {
	ma.SerialMessagesPerSec.reset()
	ma.UDPMessagesPerSec.reset()
	ma.SerialMessagesPerMin.reset()
	ma.UDPMessagesPerMin.reset()
	ma.TotalDeduplications.reset()
	ma.ActiveWebSockets.reset()
	ma.NumVesselsClassA.reset()
	ma.NumVesselsClassB.reset()
	ma.NumVesselsAtoN.reset()
	ma.NumVesselsBaseStation.reset()
	ma.NumVesselsSAR.reset()
	ma.TotalKnownVessels.reset()
	// TotalMessages is cumulative and is not reset.
}

// -----------------------------------------------------------------------------
// MetricsHistory holds snapshots for different aggregation intervals.
type MetricsHistory struct {
	MinuteAggregates []MetricsAggregate `json:"minute_aggregates"` // 1-minute snapshots taken hourly
	HourAggregates   []MetricsAggregate `json:"hour_aggregates"`   // 1-hour snapshots taken daily
	DayAggregates    []MetricsAggregate `json:"day_aggregates"`    // 1-day snapshots taken weekly
	WeekAggregates   []MetricsAggregate `json:"week_aggregates"`   // 1-week snapshots taken monthly
	MonthAggregates  []MetricsAggregate `json:"month_aggregates"`  // 1-month snapshots taken yearly
}

var (
	metricsHistory MetricsHistory
	mhMutex        sync.Mutex
)

// writeMetricsHistory writes the snapshots to a JSON file in the specified state directory.
func writeMetricsHistory(stateDir string) error {
	mhMutex.Lock()
	defer mhMutex.Unlock()
	filePath := filepath.Join(stateDir, "metrics.json")
	data, err := json.MarshalIndent(metricsHistory, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

// -----------------------------------------------------------------------------
// getCurrentMetrics collects the current live metrics.
// This function uses variables such as serialCounter, totalMessages, dedupeMessages,
// vesselDataMutex, vesselData, calculateVesselCounts, activeRooms, and clients,
// which are defined in your main program.
func getCurrentMetrics() Metrics {
	vesselDataMutex.Lock()
	totalKnown := len(vesselData)
	vesselDataMutex.Unlock()

	counts := calculateVesselCounts()

	roomsCopy := make(map[string]int)
	for room, count := range activeRooms {
		roomsCopy[room] = count
	}

	return Metrics{
		SerialMessagesPerSec:  float64(serialCounter.Count(1 * time.Second)),
		SerialMessagesPerMin:  float64(serialCounter.Count(1 * time.Minute)),
		UDPMessagesPerSec:     float64(udpCounter.Count(1 * time.Second)),
		UDPMessagesPerMin:     float64(udpCounter.Count(1 * time.Minute)),
		TotalMessages:         totalMessages,
		TotalDeduplications:   dedupeMessages,
		ActiveWebSockets:      len(clients),
		ActiveWebSocketRooms:  roomsCopy,
		NumVesselsClassA:      counts["Class A"],
		NumVesselsClassB:      counts["Class B"],
		NumVesselsAtoN:        counts["AtoN"],
		NumVesselsBaseStation: counts["Base Station"],
		NumVesselsSAR:         counts["SAR"],
		TotalKnownVessels:     totalKnown,
	}
}

// -----------------------------------------------------------------------------
// StartMetrics launches goroutines that update the aggregators and periodically
// write out snapshots to a file. Call StartMetrics(stateDir, noState)
// from your main() function.
func StartMetrics(stateDir string, noState bool) {
    // Ensure the state directory exists.
    if err := os.MkdirAll(stateDir, 0755); err != nil {
        log.Printf("Error creating state directory: %v", err)
    }

    filePath := filepath.Join(stateDir, "metrics.json")

    if !noState {
        // Try to read the existing state file.
        if data, err := os.ReadFile(filePath); err == nil {
            // If reading is successful, unmarshal into metricsHistory.
            mhMutex.Lock()
            err = json.Unmarshal(data, &metricsHistory)
            mhMutex.Unlock()
            if err != nil {
                log.Printf("Error unmarshaling metrics history: %v", err)
            } else {
                log.Printf("Loaded metrics history from %s", filePath)
            }
        } else if os.IsNotExist(err) {
            // File doesn't exist, write default state.
            defaultAgg := defaultMetricsAggregate()
            mhMutex.Lock()
            metricsHistory = MetricsHistory{
                MinuteAggregates: []MetricsAggregate{defaultAgg},
                HourAggregates:   []MetricsAggregate{defaultAgg},
                DayAggregates:    []MetricsAggregate{defaultAgg},
                WeekAggregates:   []MetricsAggregate{defaultAgg},
                MonthAggregates:  []MetricsAggregate{defaultAgg},
            }
            mhMutex.Unlock()
            if err := writeMetricsHistory(stateDir); err != nil {
                log.Printf("Error writing default metrics history: %v", err)
            } else {
                log.Printf("Wrote default metrics history to %s", filePath)
            }
        } else {
            // Some other error occurred reading the file.
            log.Printf("Error reading metrics history: %v", err)
        }
    } else {
        // noState is true, initialize default state without reading file.
        defaultAgg := defaultMetricsAggregate()
        mhMutex.Lock()
        metricsHistory = MetricsHistory{
            MinuteAggregates: []MetricsAggregate{defaultAgg},
            HourAggregates:   []MetricsAggregate{defaultAgg},
            DayAggregates:    []MetricsAggregate{defaultAgg},
            WeekAggregates:   []MetricsAggregate{defaultAgg},
            MonthAggregates:  []MetricsAggregate{defaultAgg},
        }
        mhMutex.Unlock()
		if err := writeMetricsHistory(stateDir); err != nil {
			log.Printf("Error writing default metrics history: %v", err)
		} else {
			log.Printf("Wrote default metrics history to %s", filePath)
		}
	}

	// Update aggregators every second.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			m := getCurrentMetrics()
			minuteAgg.update(m)
			hourAgg.update(m)
			dayAgg.update(m)
			weekAgg.update(m)
			monthAgg.update(m)
		}
	}()

	// 1-minute snapshot every hour.
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := minuteAgg.finalize()
			mhMutex.Lock()
			metricsHistory.MinuteAggregates = append(metricsHistory.MinuteAggregates, snapshot)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			minuteAgg.reset()
		}
	}()

	// 1-hour snapshot every day.
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := hourAgg.finalize()
			mhMutex.Lock()
			metricsHistory.HourAggregates = append(metricsHistory.HourAggregates, snapshot)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			hourAgg.reset()
		}
	}()

	// 1-day snapshot every week (e.g., Sunday at midnight).
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for now := range ticker.C {
			if now.Weekday() == time.Sunday && now.Hour() == 0 {
				snapshot := dayAgg.finalize()
				mhMutex.Lock()
				metricsHistory.DayAggregates = append(metricsHistory.DayAggregates, snapshot)
				mhMutex.Unlock()
				if !noState {
					if err := writeMetricsHistory(stateDir); err != nil {
						log.Printf("Error writing metrics history: %v", err)
					}
				}
				dayAgg.reset()
			}
		}
	}()

	// 1-week snapshot every month (1st day of the month at midnight).
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for now := range ticker.C {
			if now.Day() == 1 && now.Hour() == 0 {
				snapshot := weekAgg.finalize()
				mhMutex.Lock()
				metricsHistory.WeekAggregates = append(metricsHistory.WeekAggregates, snapshot)
				mhMutex.Unlock()
				if !noState {
					if err := writeMetricsHistory(stateDir); err != nil {
						log.Printf("Error writing metrics history: %v", err)
					}
				}
				weekAgg.reset()
			}
		}
	}()

	// 1-month snapshot every year (January 1st at midnight).
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for now := range ticker.C {
			if now.Month() == time.January && now.Day() == 1 && now.Hour() == 0 {
				snapshot := monthAgg.finalize()
				mhMutex.Lock()
				metricsHistory.MonthAggregates = append(metricsHistory.MonthAggregates, snapshot)
				mhMutex.Unlock()
				if !noState {
					if err := writeMetricsHistory(stateDir); err != nil {
						log.Printf("Error writing metrics history: %v", err)
					}
				}
				monthAgg.reset()
			}
		}
	}()
}