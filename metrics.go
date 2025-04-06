// metrics.go
package main

import (
	"encoding/json"
	"log"
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
)

// Assume startTime is defined in your main package (e.g., in aisdecode.go):
// var startTime = time.Now()

// -----------------------------------------------------------------------------
// Aggregation types and helper functions.

// AggregatedMetric now only needs the average value.
type AggregatedMetric struct {
	Ave float64 `json:"average"`
}

// NumericAggregator maintains running statistics for a numeric metric.
type NumericAggregator struct {
	sum   float64
	count int
}

// newNumericAggregator creates a new NumericAggregator.
func newNumericAggregator() NumericAggregator {
	return NumericAggregator{
		sum:   0,
		count: 0,
	}
}

func (na *NumericAggregator) update(value float64) {
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
	na.sum = 0
	na.count = 0
}

// defaultMetricsAggregate creates a default snapshot with zero averages.
func defaultMetricsAggregate() MetricsAggregate {
	return MetricsAggregate{
		Timestamp:             time.Now().UTC(),
		SerialMessagesPerSec:  AggregatedMetric{Ave: 0},
		UDPMessagesPerSec:     AggregatedMetric{Ave: 0},
		SerialMessagesPerMin:  AggregatedMetric{Ave: 0},
		UDPMessagesPerMin:     AggregatedMetric{Ave: 0},
		TotalDeduplications:   AggregatedMetric{Ave: 0},
		ActiveWebSockets:      AggregatedMetric{Ave: 0},
		NumVesselsClassA:      AggregatedMetric{Ave: 0},
		NumVesselsClassB:      AggregatedMetric{Ave: 0},
		NumVesselsAtoN:        AggregatedMetric{Ave: 0},
		NumVesselsBaseStation: AggregatedMetric{Ave: 0},
		NumVesselsSAR:         AggregatedMetric{Ave: 0},
		TotalKnownVessels:     AggregatedMetric{Ave: 0},
		TotalMessages:         0,
		// For default, you can set uptime to 0.
		UptimeSeconds: 0,
	}
}

// MetricsAggregator collects running stats for each metric.
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

// update the aggregator with a new live Metrics sample.
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
	ma.TotalMessages = m.TotalMessages // cumulative
}

// MetricsAggregate represents a finalized snapshot of average metrics.
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
	// NEW: Uptime field added here.
	UptimeSeconds         int              `json:"uptime_seconds"`
}

// finalize produces a snapshot from the aggregator (only averages).
func (ma *MetricsAggregator) finalize() MetricsAggregate {
	return MetricsAggregate{
		Timestamp: time.Now().UTC(),
		SerialMessagesPerSec: AggregatedMetric{
			Ave: ma.SerialMessagesPerSec.average(),
		},
		UDPMessagesPerSec: AggregatedMetric{
			Ave: ma.UDPMessagesPerSec.average(),
		},
		SerialMessagesPerMin: AggregatedMetric{
			Ave: ma.SerialMessagesPerMin.average(),
		},
		UDPMessagesPerMin: AggregatedMetric{
			Ave: ma.UDPMessagesPerMin.average(),
		},
		TotalDeduplications: AggregatedMetric{
			Ave: ma.TotalDeduplications.average(),
		},
		ActiveWebSockets: AggregatedMetric{
			Ave: ma.ActiveWebSockets.average(),
		},
		NumVesselsClassA: AggregatedMetric{
			Ave: ma.NumVesselsClassA.average(),
		},
		NumVesselsClassB: AggregatedMetric{
			Ave: ma.NumVesselsClassB.average(),
		},
		NumVesselsAtoN: AggregatedMetric{
			Ave: ma.NumVesselsAtoN.average(),
		},
		NumVesselsBaseStation: AggregatedMetric{
			Ave: ma.NumVesselsBaseStation.average(),
		},
		NumVesselsSAR: AggregatedMetric{
			Ave: ma.NumVesselsSAR.average(),
		},
		TotalKnownVessels: AggregatedMetric{
			Ave: ma.TotalKnownVessels.average(),
		},
		TotalMessages:  ma.TotalMessages,
		// NEW: Compute uptime using startTime.
		UptimeSeconds: int(time.Since(startTime).Seconds()),
	}
}

// reset clears the aggregator (except for TotalMessages).
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
}

// -----------------------------------------------------------------------------
// MetricsHistory holds snapshots for different aggregation intervals.
type MetricsHistory struct {
	MinuteAverages []MetricsAggregate `json:"minute_averages"` // 60 values: 1 per minute for last 60 minutes
	HourAverages   []MetricsAggregate `json:"hour_averages"`   // 24 values: 1 per hour for last 24 hours
	DayAverages    []MetricsAggregate `json:"day_averages"`    // 7 values: 1 per day for last 7 days
	WeekAverages   []MetricsAggregate `json:"week_averages"`   // 52 values: 1 per week for last 52 weeks
}

var (
	metricsHistory MetricsHistory
	mhMutex        sync.Mutex
)

// helper function to append a snapshot while enforcing a maximum length.
func appendSnapshot(slice []MetricsAggregate, snap MetricsAggregate, maxLen int) []MetricsAggregate {
	slice = append(slice, snap)
	if len(slice) > maxLen {
		// Remove the oldest snapshot.
		slice = slice[1:]
	}
	return slice
}

// writeMetricsHistory writes the snapshots to a JSON file.
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
// It uses the existing global variables and functions from aisdecode.go.
func getCurrentMetrics() Metrics {
	// Use calculateVesselCounts() from aisdecode.go to count vessels.
	counts := calculateVesselCounts()
	// Total known vessels is simply the number of entries in vesselData.
	totalKnown := len(vesselData)
	uptimeSeconds := int(time.Since(startTime).Seconds())

	return Metrics{
		SerialMessagesPerSec:  float64(serialCounter.Count(1 * time.Second)),
		SerialMessagesPerMin:  float64(serialCounter.Count(1 * time.Minute)),
		UDPMessagesPerSec:     float64(udpCounter.Count(1 * time.Second)),
		UDPMessagesPerMin:     float64(udpCounter.Count(1 * time.Minute)),
		TotalMessages:         totalMessages,
		TotalDeduplications:   dedupeMessages,
		ActiveWebSockets:      len(clients),
		NumVesselsClassA:      counts["Class A"],
		NumVesselsClassB:      counts["Class B"],
		NumVesselsAtoN:        counts["AtoN"],
		NumVesselsBaseStation: counts["Base Station"],
		NumVesselsSAR:         counts["SAR"],
		TotalKnownVessels:     totalKnown,
		UptimeSeconds:         uptimeSeconds,
	}
}

// -----------------------------------------------------------------------------
// StartMetrics launches goroutines that update the aggregators and write snapshots.
func StartMetrics(stateDir string, noState bool) {
	// Ensure state directory exists.
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		log.Printf("Error creating state directory: %v", err)
	}

	filePath := filepath.Join(stateDir, "metrics.json")
	if !noState {
		if data, err := os.ReadFile(filePath); err == nil {
			mhMutex.Lock()
			err = json.Unmarshal(data, &metricsHistory)
			mhMutex.Unlock()
			if err != nil {
				log.Printf("Error unmarshaling metrics history: %v", err)
			} else {
				log.Printf("Loaded metrics history from %s", filePath)
			}
		} else if os.IsNotExist(err) {
			// Initialize default history.
			defaultAgg := defaultMetricsAggregate()
			mhMutex.Lock()
			metricsHistory = MetricsHistory{
				MinuteAverages: []MetricsAggregate{defaultAgg},
				HourAverages:   []MetricsAggregate{defaultAgg},
				DayAverages:    []MetricsAggregate{defaultAgg},
				WeekAverages:   []MetricsAggregate{defaultAgg},
			}
			mhMutex.Unlock()
			if err := writeMetricsHistory(stateDir); err != nil {
				log.Printf("Error writing default metrics history: %v", err)
			} else {
				log.Printf("Wrote default metrics history to %s", filePath)
			}
		} else {
			log.Printf("Error reading metrics history: %v", err)
		}
	} else {
		// noState: initialize default state.
		defaultAgg := defaultMetricsAggregate()
		mhMutex.Lock()
		metricsHistory = MetricsHistory{
			MinuteAverages: []MetricsAggregate{defaultAgg},
			HourAverages:   []MetricsAggregate{defaultAgg},
			DayAverages:    []MetricsAggregate{defaultAgg},
			WeekAverages:   []MetricsAggregate{defaultAgg},
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
		}
	}()

	// 1-minute snapshot: every minute, finalize minuteAgg.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := minuteAgg.finalize()
			mhMutex.Lock()
			metricsHistory.MinuteAverages = appendSnapshot(metricsHistory.MinuteAverages, snapshot, 60)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			minuteAgg.reset()
		}
	}()

	// 1-hour snapshot: every hour, finalize hourAgg.
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := hourAgg.finalize()
			mhMutex.Lock()
			metricsHistory.HourAverages = appendSnapshot(metricsHistory.HourAverages, snapshot, 24)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			hourAgg.reset()
		}
	}()

	// 1-day snapshot: every 24 hours, finalize dayAgg.
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := dayAgg.finalize()
			mhMutex.Lock()
			metricsHistory.DayAverages = appendSnapshot(metricsHistory.DayAverages, snapshot, 7)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			dayAgg.reset()
		}
	}()

	// 1-week snapshot: every week, finalize weekAgg.
	go func() {
		ticker := time.NewTicker(7 * 24 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := weekAgg.finalize()
			mhMutex.Lock()
			metricsHistory.WeekAverages = appendSnapshot(metricsHistory.WeekAverages, snapshot, 52)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
			}
			weekAgg.reset()
		}
	}()
}
