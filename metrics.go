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
	minuteAgg 		MetricsAggregator
	hourAgg   		MetricsAggregator
	dayAgg    		MetricsAggregator
	weekAgg   		MetricsAggregator
	lastAggregatedMetrics   MetricsAggregate
)

// -----------------------------------------------------------------------------
// Aggregation types and helper functions.

// MaxAggregator tracks the highest value seen.
type MaxAggregator struct {
    Max float64 `json:"max"`
}

// AggregatedMetric now only needs the average value.
type AggregatedMetric struct {
	Ave float64 `json:"average"`
}

// NumericAggregator maintains running statistics for a numeric metric.
// Fields are exported to allow persistence.
type NumericAggregator struct {
	Sum   float64 `json:"sum"`
	Count int     `json:"count"`
}

// newNumericAggregator creates a new NumericAggregator.
func newNumericAggregator() NumericAggregator {
	return NumericAggregator{Sum: 0, Count: 0}
}

// newMaxAggregator creates a new MaxAggregator.
func newMaxAggregator() MaxAggregator {
    return MaxAggregator{Max: 0} // start at 0 (or a very low value if negative values are possible)
}

func (na *NumericAggregator) update(value float64) {
	na.Sum += value
	na.Count++
}

func (na *NumericAggregator) average() float64 {
	if na.Count == 0 {
		return 0
	}
	return na.Sum / float64(na.Count)
}

func (na *NumericAggregator) reset() {
	na.Sum = 0
	na.Count = 0
}

func (ma *MaxAggregator) update(value float64) {
    if value > ma.Max {
        ma.Max = value
    }
}


// reset clears the max value.
func (ma *MaxAggregator) reset() {
    ma.Max = 0
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
		UptimeSeconds:         0,
	        MaxDistanceMeters:     AggregatedMetric{Ave: 0},
	        AverageDistanceMeters: AggregatedMetric{Ave: 0},
	}
}

// MetricsAggregator collects running stats for each metric and now includes
// a StartTime to track the beginning of the current aggregation period.
type MetricsAggregator struct {
	StartTime              time.Time         `json:"start_time"`
	SerialMessagesPerSec   NumericAggregator `json:"serial_messages_per_sec"`
	UDPMessagesPerSec      NumericAggregator `json:"udp_messages_per_sec"`
	SerialMessagesPerMin   NumericAggregator `json:"serial_messages_per_min"`
	UDPMessagesPerMin      NumericAggregator `json:"udp_messages_per_min"`
	TotalDeduplications    NumericAggregator `json:"total_deduplications"`
	ActiveWebSockets       NumericAggregator `json:"active_websockets"`
	NumVesselsClassA       NumericAggregator `json:"num_vessels_class_a"`
	NumVesselsClassB       NumericAggregator `json:"num_vessels_class_b"`
	NumVesselsAtoN         NumericAggregator `json:"num_vessels_aton"`
	NumVesselsBaseStation  NumericAggregator `json:"num_vessels_base_station"`
	NumVesselsSAR          NumericAggregator `json:"num_vessels_sar"`
	TotalKnownVessels      NumericAggregator `json:"total_known_vessels"`
	TotalMessages          int               `json:"total_messages"`
        MaxDistanceMeters      MaxAggregator     `json:"max_distance_meters"`
        AvgDistanceMeters      NumericAggregator `json:"avg_distance_meters"`
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
        ma.MaxDistanceMeters.update(m.MaxDistanceMeters)
        ma.AvgDistanceMeters.update(m.AverageDistanceMeters)
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
	UptimeSeconds         int              `json:"uptime_seconds"`
	MaxDistanceMeters     AggregatedMetric `json:"max_distance_meters"`
        AverageDistanceMeters AggregatedMetric `json:"average_distance_meters"`
}

// finalize produces a snapshot from the aggregator using its own StartTime,
// rounding each average to ensure whole-number output.
func (ma *MetricsAggregator) finalize() MetricsAggregate {
	return MetricsAggregate{
		Timestamp: time.Now().UTC(),
		SerialMessagesPerSec: AggregatedMetric{
			Ave: math.Round(ma.SerialMessagesPerSec.average()),
		},
		UDPMessagesPerSec: AggregatedMetric{
			Ave: math.Round(ma.UDPMessagesPerSec.average()),
		},
		SerialMessagesPerMin: AggregatedMetric{
			Ave: math.Round(ma.SerialMessagesPerMin.average()),
		},
		UDPMessagesPerMin: AggregatedMetric{
			Ave: math.Round(ma.UDPMessagesPerMin.average()),
		},
		TotalDeduplications: AggregatedMetric{
			Ave: math.Round(ma.TotalDeduplications.average()),
		},
		ActiveWebSockets: AggregatedMetric{
			Ave: math.Round(ma.ActiveWebSockets.average()),
		},
		NumVesselsClassA: AggregatedMetric{
			Ave: math.Round(ma.NumVesselsClassA.average()),
		},
		NumVesselsClassB: AggregatedMetric{
			Ave: math.Round(ma.NumVesselsClassB.average()),
		},
		NumVesselsAtoN: AggregatedMetric{
			Ave: math.Round(ma.NumVesselsAtoN.average()),
		},
		NumVesselsBaseStation: AggregatedMetric{
			Ave: math.Round(ma.NumVesselsBaseStation.average()),
		},
		NumVesselsSAR: AggregatedMetric{
			Ave: math.Round(ma.NumVesselsSAR.average()),
		},
		TotalKnownVessels: AggregatedMetric{
			Ave: math.Round(ma.TotalKnownVessels.average()),
		},
		TotalMessages:  ma.TotalMessages,
		UptimeSeconds: int(time.Since(ma.StartTime).Seconds()),
		MaxDistanceMeters: AggregatedMetric{
	            Ave: math.Round(ma.MaxDistanceMeters.Max),
	        },
	        AverageDistanceMeters: AggregatedMetric{
	            Ave: math.Round(ma.AvgDistanceMeters.average()),
	        },
	}
}

// reset clears the aggregator (except for TotalMessages) and resets its StartTime.
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
	ma.StartTime = time.Now().UTC()
        ma.MaxDistanceMeters.reset()
        ma.AvgDistanceMeters.reset()
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

// appendSnapshot appends a snapshot while enforcing a maximum length.
func appendSnapshot(slice []MetricsAggregate, snap MetricsAggregate, maxLen int) []MetricsAggregate {
	slice = append(slice, snap)
	if len(slice) > maxLen {
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
// AggregatorsState holds the state of all aggregators for persistence.
type AggregatorsState struct {
	MinuteAgg   MetricsAggregator `json:"minute_agg"`
	HourAgg     MetricsAggregator `json:"hour_agg"`
	DayAgg      MetricsAggregator `json:"day_agg"`
	WeekAgg     MetricsAggregator `json:"week_agg"`
	LastUpdated time.Time         `json:"last_updated"`
}

func writeAggregatorsState(stateDir string) error {
	state := AggregatorsState{
		MinuteAgg:   minuteAgg,
		HourAgg:     hourAgg,
		DayAgg:      dayAgg,
		WeekAgg:     weekAgg,
		LastUpdated: time.Now(),
	}
	filePath := filepath.Join(stateDir, "metrics-aggregators.json")
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0644)
}

func loadAggregatorsState(stateDir string) error {
	filePath := filepath.Join(stateDir, "metrics-aggregators.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	var state AggregatorsState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	// If the state is too old, discard it.
	if time.Since(state.LastUpdated) > 30*time.Minute {
		log.Printf("Aggregator state is older than threshold; starting fresh")
		return nil
	}
	minuteAgg = state.MinuteAgg
	hourAgg = state.HourAgg
	dayAgg = state.DayAgg
	weekAgg = state.WeekAgg
	return nil
}

// -----------------------------------------------------------------------------
// getCurrentMetrics collects the current live metrics.
// It uses calculateVesselCounts() from aisdecode.go and other global variables
// (vesselData, serialCounter, udpCounter, totalMessages, dedupeMessages, clients, etc.)
func getCurrentMetrics() Metrics {
    counts := calculateVesselCounts()
    totalKnown := len(vesselData)
    uptimeSeconds := int(time.Since(startTime).Seconds())

    // Compute the rolling 1-minute metrics for distance.
    var rollingSum float64
    var rollingCount int
    var rollingMax float64

    // Lock and process the rollingDistances slice.
    distancesMutex.Lock()
    cutoff := time.Now().Add(-1 * time.Minute)
    // Build a new window containing only recent measurements.
    var newWindow []DataPoint
    for _, dp := range rollingDistances {
        if dp.Timestamp.After(cutoff) {
            newWindow = append(newWindow, dp)
            rollingSum += dp.Distance
            rollingCount++
            if dp.Distance > rollingMax {
                rollingMax = dp.Distance
            }
        }
    }
    // Replace the old slice with the filtered (new) window.
    rollingDistances = newWindow
    distancesMutex.Unlock()

    // Compute average. (Round the result if desired.)
    avgDistance := 0.0
    if rollingCount > 0 {
        avgDistance = math.Round(rollingSum / float64(rollingCount))
    }
    maxDistRounded := math.Round(rollingMax)

    return Metrics{
        SerialMessagesPerSec:    float64(serialCounter.Count(1 * time.Second)),
        SerialMessagesPerMin:    float64(serialCounter.Count(1 * time.Minute)),
        UDPMessagesPerSec:       float64(udpCounter.Count(1 * time.Second)),
        UDPMessagesPerMin:       float64(udpCounter.Count(1 * time.Minute)),
        TotalMessages:           totalMessages,
        TotalDeduplications:     dedupeMessages,
        ActiveWebSockets:        len(clients),
        NumVesselsClassA:        counts["Class A"],
        NumVesselsClassB:        counts["Class B"],
        NumVesselsAtoN:          counts["AtoN"],
        NumVesselsBaseStation:   counts["Base Station"],
        NumVesselsSAR:           counts["SAR"],
        TotalKnownVessels:       totalKnown,
        UptimeSeconds:           uptimeSeconds,
        MaxDistanceMeters:       maxDistRounded,
        AverageDistanceMeters:   avgDistance,
    }
}

// -----------------------------------------------------------------------------
// checkAndFinalizeAggregators checks if hour, day, and week aggregators have
// exceeded their respective periods. If so, it finalizes, appends the snapshot,
// persists state if needed, and resets the aggregator.
func checkAndFinalizeAggregators(stateDir string, noState bool) {
	now := time.Now().UTC()

	// Check hourly aggregator (1 hour)
	if now.Sub(hourAgg.StartTime) >= time.Hour {
		snapshot := hourAgg.finalize()
		mhMutex.Lock()
		metricsHistory.HourAverages = appendSnapshot(metricsHistory.HourAverages, snapshot, 24)
		mhMutex.Unlock()
		if !noState {
			if err := writeMetricsHistory(stateDir); err != nil {
				log.Printf("Error writing metrics history: %v", err)
			}
			if err := writeAggregatorsState(stateDir); err != nil {
				log.Printf("Error writing aggregator state: %v", err)
			}
		}
		hourAgg.reset()
	}

	// Check daily aggregator (24 hours)
	if now.Sub(dayAgg.StartTime) >= 24*time.Hour {
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

	// Check weekly aggregator (7 days)
	if now.Sub(weekAgg.StartTime) >= 7*24*time.Hour {
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
}

// -----------------------------------------------------------------------------
// StartMetrics launches goroutines that update the aggregators, finalize snapshots,
// and write state. It also initializes aggregator start times and loads persisted state.
func StartMetrics(stateDir string, noState bool) {
	// Ensure state directory exists.
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		log.Printf("Error creating state directory: %v", err)
	}

	historyPath := filepath.Join(stateDir, "metrics.json")
	if !noState {
		if data, err := os.ReadFile(historyPath); err == nil {
			mhMutex.Lock()
			err = json.Unmarshal(data, &metricsHistory)
			mhMutex.Unlock()
			if err != nil {
				log.Printf("Error unmarshaling metrics history: %v", err)
			} else {
				log.Printf("Loaded metrics history from %s", historyPath)
			}
		} else if os.IsNotExist(err) {
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
				log.Printf("Wrote default metrics history to %s", historyPath)
			}
		} else {
			log.Printf("Error reading metrics history: %v", err)
		}
		// Load persisted aggregator state.
		if err := loadAggregatorsState(stateDir); err != nil {
			log.Printf("No valid existing aggregator state found, starting fresh")
		} else {
			log.Printf("Loaded aggregator state from %s", filepath.Join(stateDir, "metrics-aggregators.json"))
		}
	} else {
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
			log.Printf("Wrote default metrics history to %s", historyPath)
		}
	}

	// Initialize aggregator start times if not already set.
	now := time.Now().UTC()
	if minuteAgg.StartTime.IsZero() {
		minuteAgg.StartTime = now
	}
	if hourAgg.StartTime.IsZero() {
		hourAgg.StartTime = now
	}
	if dayAgg.StartTime.IsZero() {
		dayAgg.StartTime = now
	}
	if weekAgg.StartTime.IsZero() {
		weekAgg.StartTime = now
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

	// Finalize minute aggregator every minute.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			snapshot := minuteAgg.finalize()
			lastAggregatedMetrics = snapshot
			mhMutex.Lock()
			metricsHistory.MinuteAverages = appendSnapshot(metricsHistory.MinuteAverages, snapshot, 60)
			mhMutex.Unlock()
			if !noState {
				if err := writeMetricsHistory(stateDir); err != nil {
					log.Printf("Error writing metrics history: %v", err)
				}
				if err := writeAggregatorsState(stateDir); err != nil {
					log.Printf("Error writing aggregator state: %v", err)
				}
			}
			minuteAgg.reset()
		}
	}()

	// Check and finalize longer aggregators (hourly, daily, weekly) every minute.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			checkAndFinalizeAggregators(stateDir, noState)
		}
	}()
}
