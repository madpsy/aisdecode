package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// StatisticsSettings contains the settings for the statistics report feature
type StatisticsSettings struct {
	BaseURL    string `json:"statistics_base_url"`
	Enabled    bool   `json:"statistics_report_enabled"`
	ReportTime string `json:"statistics_report_time"` // Format: "day,hour:minute" e.g. "saturday,01:00"
}

// ReceiverStats represents statistics for a single receiver
type ReceiverStats struct {
	TopSOG       []vessel      `json:"top_sog"`
	TopTypes     []typeCount   `json:"top_types"`
	TopClasses   []classCount  `json:"top_classes"`
	TopPositions []posVessel   `json:"top_positions"`
	TopDistance  []distVessel  `json:"top_distance"`
	Coverage     []coveragePoint `json:"coverage"`
}

// vessel represents a vessel with speed data
type vessel struct {
	UserID    int       `json:"user_id"`
	Name      string    `json:"name,omitempty"`
	ImageURL  string    `json:"image_url,omitempty"`
	AISClass  string    `json:"ais_class,omitempty"`
	Type      string    `json:"type,omitempty"`
	MaxSog    float64   `json:"max_sog,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Lat       float64   `json:"lat,omitempty"`
	Lon       float64   `json:"lon,omitempty"`
	Count     int       `json:"count,omitempty"`
}

// distVessel represents a vessel with distance data
type distVessel struct {
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

// typeCount represents a vessel type count
type typeCount struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

// classCount represents an AIS class count
type classCount struct {
	Class string `json:"class"`
	Count int    `json:"count"`
}

// posVessel represents a vessel with position count
type posVessel struct {
	UserID   int    `json:"user_id"`
	Name     string `json:"name"`
	ImageURL string `json:"image_url"`
	AISClass string `json:"ais_class,omitempty"`
	Type     string `json:"type,omitempty"`
	Count    int    `json:"count"`
}

// coveragePoint represents a point on the coverage map
type coveragePoint struct {
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Count int     `json:"count"`
}

// VesselTypeMap maps vessel type IDs to descriptions
type VesselTypeMap map[string]string

// parseReportTime parses the report time string into a day of week and hour/minute
func parseReportTime(timeStr string) (time.Weekday, int, int, error) {
	parts := strings.Split(timeStr, ",")
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid report time format: %s", timeStr)
	}

	// Parse day of week
	var day time.Weekday
	switch strings.ToLower(parts[0]) {
	case "sunday":
		day = time.Sunday
	case "monday":
		day = time.Monday
	case "tuesday":
		day = time.Tuesday
	case "wednesday":
		day = time.Wednesday
	case "thursday":
		day = time.Thursday
	case "friday":
		day = time.Friday
	case "saturday":
		day = time.Saturday
	default:
		return 0, 0, 0, fmt.Errorf("invalid day of week: %s", parts[0])
	}

	// Parse hour and minute
	timeParts := strings.Split(parts[1], ":")
	if len(timeParts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid time format: %s", parts[1])
	}

	hour, err := strconv.Atoi(timeParts[0])
	if err != nil || hour < 0 || hour > 23 {
		return 0, 0, 0, fmt.Errorf("invalid hour: %s", timeParts[0])
	}

	minute, err := strconv.Atoi(timeParts[1])
	if err != nil || minute < 0 || minute > 59 {
		return 0, 0, 0, fmt.Errorf("invalid minute: %s", timeParts[1])
	}

	return day, hour, minute, nil
}

// isTimeToSendReport checks if it's time to send the weekly report based on the configured schedule
func isTimeToSendReport(lastReportTime time.Time, reportTimeStr string) (bool, error) {
	day, hour, minute, err := parseReportTime(reportTimeStr)
	if err != nil {
		return false, err
	}

	now := time.Now()
	
	// Check if it's the right day of the week
	if now.Weekday() != day {
		return false, nil
	}

	// Check if it's within 5 minutes of the scheduled time
	if now.Hour() != hour || (now.Minute() < minute || now.Minute() >= minute+5) {
		return false, nil
	}

	// Make sure we haven't sent a report in the last 23 hours
	if now.Sub(lastReportTime) < 23*time.Hour {
		return false, nil
	}

	return true, nil
}

// fetchVesselTypeMap fetches the vessel type mapping from the statistics API
func fetchVesselTypeMap(baseURL string) (VesselTypeMap, error) {
	vesselTypeMap := make(VesselTypeMap)
	
	// Fetch vessel type mappings
	vesselTypesURL := fmt.Sprintf("%s/vessel_types.json", baseURL)
	if err := fetchJSON(vesselTypesURL, &vesselTypeMap); err != nil {
		return nil, fmt.Errorf("error fetching vessel type mappings: %w", err)
	}
	
	return vesselTypeMap, nil
}

// fetchReceiverStatistics fetches statistics for a specific receiver
func fetchReceiverStatistics(baseURL string, receiverID int, days int) (*ReceiverStats, error) {
	stats := &ReceiverStats{}

	// We no longer fetch top SOG as per requirements

	// Fetch top types
	topTypesURL := fmt.Sprintf("%s/statistics/stats/top-types?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(topTypesURL, &stats.TopTypes); err != nil {
		return nil, fmt.Errorf("error fetching top types: %w", err)
	}

	// Fetch top classes
	topClassesURL := fmt.Sprintf("%s/statistics/stats/top-classes?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(topClassesURL, &stats.TopClasses); err != nil {
		return nil, fmt.Errorf("error fetching top classes: %w", err)
	}

	// Fetch top positions
	topPositionsURL := fmt.Sprintf("%s/statistics/stats/top-positions?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(topPositionsURL, &stats.TopPositions); err != nil {
		return nil, fmt.Errorf("error fetching top positions: %w", err)
	}

	// Fetch top distance
	topDistanceURL := fmt.Sprintf("%s/statistics/stats/top-distance?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(topDistanceURL, &stats.TopDistance); err != nil {
		return nil, fmt.Errorf("error fetching top distance: %w", err)
	}

	// Fetch coverage map
	coverageURL := fmt.Sprintf("%s/statistics/stats/coverage-map?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(coverageURL, &stats.Coverage); err != nil {
		return nil, fmt.Errorf("error fetching coverage map: %w", err)
	}

	return stats, nil
}

// fetchJSON fetches JSON data from a URL and unmarshals it into the provided target
func fetchJSON(url string, target interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API returned status: %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

// generateStatisticsReport generates a weekly statistics report for a receiver
func generateStatisticsReport(rec Receiver, stats *ReceiverStats, days int, vesselTypeMap VesselTypeMap) string {
	// Record start time for report generation duration
	startTime := time.Now()
	
	var report strings.Builder

	// Header
	report.WriteString(fmt.Sprintf("Weekly Statistics Report for %s (ID: %d)\n", rec.Name, rec.ID))
	report.WriteString(fmt.Sprintf("Report Period: Last %d days\n", days))
	report.WriteString(fmt.Sprintf("Generated on: %s\n\n", startTime.Format("January 2, 2006 at 15:04:05 (UTC)")))

	// Coverage Statistics (now first)
	report.WriteString("=== Coverage Statistics ===\n")
	if len(stats.Coverage) > 0 {
		totalPoints := len(stats.Coverage)
		totalReports := 0
		for _, p := range stats.Coverage {
			totalReports += p.Count
		}
		report.WriteString(fmt.Sprintf("Total position reports: %d\n", totalReports))
		
		// Calculate approximate coverage area (very rough estimate)
		// Each grid cell is approximately 1km x 1km at the equator
		report.WriteString(fmt.Sprintf("Approximate coverage area: %d sq km\n", totalPoints))
	} else {
		report.WriteString("No coverage data available\n")
	}
	report.WriteString("\n")

	// Top Vessels by Distance (now second)
	report.WriteString("=== Top Vessels by Distance ===\n")
	if len(stats.TopDistance) > 0 {
		for i, v := range stats.TopDistance {
			report.WriteString(fmt.Sprintf("%d. %s: %d km on %s\n",
				i+1,
				v.Name,
				v.Distance/1000, // Convert meters to kilometers
				v.Timestamp.Format("Jan 2, 15:04")))
		}
	} else {
		report.WriteString("No distance data available\n")
	}
	report.WriteString("\n")

	// Top Vessels by Position Reports (now third)
	report.WriteString("=== Top Vessels by Position Reports ===\n")
	if len(stats.TopPositions) > 0 {
		for i, v := range stats.TopPositions {
			report.WriteString(fmt.Sprintf("%d. %s: %d reports\n",
				i+1,
				v.Name,
				v.Count))
		}
	} else {
		report.WriteString("No position report data available\n")
	}
	report.WriteString("\n")

	// Top AIS Classes (now fourth)
	report.WriteString("=== Top AIS Classes ===\n")
	if len(stats.TopClasses) > 0 {
		for i, c := range stats.TopClasses {
			report.WriteString(fmt.Sprintf("%d. Class %s: %d vessels\n", i+1, c.Class, c.Count))
		}
	} else {
		report.WriteString("No AIS class data available\n")
	}
	report.WriteString("\n")

	// Top Vessel Types (now fifth)
	report.WriteString("=== Top Vessel Types ===\n")
	if len(stats.TopTypes) > 0 {
		for i, t := range stats.TopTypes {
			// Try to translate the vessel type ID to a description
			typeDescription := t.Type
			if description, ok := vesselTypeMap[t.Type]; ok && description != "" {
				typeDescription = description
			}
			report.WriteString(fmt.Sprintf("%d. %s: %d vessels\n", i+1, typeDescription, t.Count))
		}
	} else {
		report.WriteString("No vessel type data available\n")
	}
	report.WriteString("\n")

	// Calculate report generation duration
	duration := time.Since(startTime)
	
	// Footer with links
	receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, rec.ID)
	report.WriteString(fmt.Sprintf("Report generated in %.2f seconds\n\n", duration.Seconds()))
	report.WriteString(fmt.Sprintf("View detailed statistics: %s\n\n", receiverURL))
	report.WriteString(fmt.Sprintf("Thank you for contributing to our AIS network!\n\n"))
	report.WriteString(fmt.Sprintf("AIS Decoder Team\nhttps://%s/\n", settings.SiteDomain))
	
	// Add unsubscribe info
	report.WriteString(fmt.Sprintf("\nTo unsubscribe from these reports, visit your receiver page and disable notifications."))

	return report.String()
}

// sendWeeklyStatisticsReports sends weekly statistics reports to all eligible receivers
func sendWeeklyStatisticsReports() {
	// Check if statistics reporting is enabled
	if !settings.StatisticsReportEnabled || settings.StatisticsBaseURL == "" {
		log.Println("Statistics reporting is disabled or base URL not set")
		return
	}

	log.Println("Starting weekly statistics report generation")

	// Fetch vessel type mappings
	vesselTypeMap, err := fetchVesselTypeMap(settings.StatisticsBaseURL)
	if err != nil {
		log.Printf("Error fetching vessel type mappings: %v", err)
		// Continue with empty map, we'll use raw type IDs
		vesselTypeMap = make(VesselTypeMap)
	}

	// Fetch all receivers
	receivers, err := fetchReceivers()
	if err != nil {
		log.Printf("Error fetching receivers for statistics reports: %v", err)
		return
	}

	// Get current time
	now := time.Now()
	
	// Number of days to include in the report
	days := 7

	// Track how many reports were sent
	reportsSent := 0

	for _, receiver := range receivers {
		// Skip receivers with notifications disabled or no email
		if !receiver.Notifications || receiver.Email == "" {
			continue
		}

		// Skip receivers with no LastSeen value
		if receiver.LastSeen == "" {
			continue
		}

		// Parse LastSeen time
		lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
		if err != nil {
			log.Printf("Error parsing LastSeen time for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Skip receivers not seen in the last 7 days
		if now.Sub(lastSeen) > 7*24*time.Hour {
			continue
		}

		// Fetch statistics for this receiver
		stats, err := fetchReceiverStatistics(settings.StatisticsBaseURL, receiver.ID, days)
		if err != nil {
			log.Printf("Error fetching statistics for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Generate the report
		reportBody := generateStatisticsReport(receiver, stats, days, vesselTypeMap)

		// Create a copy of the receiver for sendEmail
		receiverCopy := receiver

		// Send the email
		emailSentTo, err := sendEmail("statistics_report", receiverCopy, reportBody)
		if err != nil {
			log.Printf("Error sending statistics report for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Log the alert
		if err := logAlert("statistics_report", receiver.ID, emailSentTo, 
			fmt.Sprintf("Weekly statistics report sent for receiver %s (ID: %d)", receiver.Name, receiver.ID)); err != nil {
			log.Printf("Error logging statistics report alert for receiver %d: %v", receiver.ID, err)
		}

		reportsSent++
		log.Printf("Sent statistics report for receiver %d (%s)", receiver.ID, receiver.Name)
	}

	log.Printf("Weekly statistics report generation complete. Sent %d reports.", reportsSent)
}

// startStatisticsReportScheduler starts the scheduler for weekly statistics reports
func startStatisticsReportScheduler(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Initialize the last run time
	lastReportTime := time.Now().Add(-24 * time.Hour)

	for {
		select {
		case <-ticker.C:
			// Check if it's time to send reports
			if settings.StatisticsReportEnabled && settings.StatisticsBaseURL != "" && settings.StatisticsReportTime != "" {
				shouldSend, err := isTimeToSendReport(lastReportTime, settings.StatisticsReportTime)
				if err != nil {
					log.Printf("Error checking report time: %v", err)
				} else if shouldSend {
					log.Println("It's time to send weekly statistics reports")
					sendWeeklyStatisticsReports()
					lastReportTime = time.Now()
				}
			}
		case <-ctx.Done():
			log.Println("Statistics report scheduler stopped")
			return
		}
	}
}