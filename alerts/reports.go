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

// duplicateVessel represents a vessel with duplicate message data
type duplicateVessel struct {
	UserID         int    `json:"user_id"`
	Name           string `json:"name,omitempty"`
	ImageURL       string `json:"image_url,omitempty"`
	AISClass       string `json:"ais_class,omitempty"`
	Type           string `json:"type,omitempty"`
	DuplicateCount int    `json:"duplicate_count"`
	ReceiverID     int    `json:"receiver_id"`
	ReceiverName   string `json:"receiver_name,omitempty"`
}

// ReceiverStats represents statistics for a single receiver
type ReceiverStats struct {
	TopSOG        []vessel          `json:"top_sog"`
	TopTypes      []typeCount       `json:"top_types"`
	TopClasses    []classCount      `json:"top_classes"`
	TopPositions  []posVessel       `json:"top_positions"`
	TopDistance   []distVessel      `json:"top_distance"`
	TopDuplicates []duplicateVessel `json:"top_duplicates"`
	Coverage      []coveragePoint   `json:"coverage"`
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
	vesselTypesURL := fmt.Sprintf("%s/statistics/vessel_types.json", baseURL)
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

	// Fetch top duplicates
	topDuplicatesURL := fmt.Sprintf("%s/statistics/stats/top-duplicates?receiver_id=%d&days=%d", baseURL, receiverID, days)
	if err := fetchJSON(topDuplicatesURL, &stats.TopDuplicates); err != nil {
		log.Printf("Warning: error fetching top duplicates: %v", err)
		// Continue without duplicates data
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
func generateStatisticsReport(rec Receiver, stats *ReceiverStats, days int, vesselTypeMap VesselTypeMap, startTime time.Time) string {
	var report strings.Builder

	// Header
	report.WriteString(fmt.Sprintf("Weekly Statistics Report for %s (ID: %d)\n", rec.Name, rec.ID))
	report.WriteString(fmt.Sprintf("Report Period: Last %d days\n", days))
	report.WriteString(fmt.Sprintf("Generated on: %s\n\n", time.Now().Format("January 2, 2006 at 15:04:05 (UTC)")))

	// Weekly Uptime (new section)
	report.WriteString("=== Weekly Uptime ===\n")

	// Access the UptimePctWeek field directly from the Receiver struct
	if rec.UptimePctWeek > 0 {
		report.WriteString(fmt.Sprintf("Weekly Uptime: %.2f%%\n", rec.UptimePctWeek))
	} else {
		report.WriteString("Weekly Uptime: Not available\n")
	}
	report.WriteString("\n")

	// Coverage Statistics
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

	// Top Duplicated Vessels
	report.WriteString("=== Top Duplicated Vessels ===\n")
	if len(stats.TopDuplicates) > 0 {
		for i, d := range stats.TopDuplicates {
			// Try to translate the vessel type ID to a description
			typeDescription := d.Type
			if description, ok := vesselTypeMap[d.Type]; ok && description != "" {
				typeDescription = description
			}

			report.WriteString(fmt.Sprintf("%d. %s (%s): %d duplicate messages (original receiver: %s)\n",
				i+1,
				d.Name,
				typeDescription,
				d.DuplicateCount,
				d.ReceiverName))
		}
	} else {
		report.WriteString("No duplicate data available\n")
	}
	report.WriteString("\n")

	// Calculate report generation duration (including API fetch time)
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

// FailedReport represents a report that failed to send
type FailedReport struct {
	ReceiverID   int
	ReceiverName string
	Email        string
	Error        string
}

// sendWeeklyStatisticsReports sends weekly statistics reports to all eligible receivers
func sendWeeklyStatisticsReports() {
	// Check if statistics reporting is enabled
	if !settings.StatisticsReportEnabled || settings.StatisticsBaseURL == "" {
		log.Println("Statistics reporting is disabled or base URL not set")
		return
	}

	log.Println("Starting weekly statistics report generation")

	// Record overall start time for total processing duration
	overallStartTime := time.Now()

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

	// Track statistics
	reportsSent := 0
	reportsSuccessful := 0
	failedReports := []FailedReport{}

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

		// Record start time for this specific report's generation duration
		reportStartTime := time.Now()

		// Fetch statistics for this receiver
		stats, err := fetchReceiverStatistics(settings.StatisticsBaseURL, receiver.ID, days)
		if err != nil {
			log.Printf("Error fetching statistics for receiver %d: %v", receiver.ID, err)
			failedReports = append(failedReports, FailedReport{
				ReceiverID:   receiver.ID,
				ReceiverName: receiver.Name,
				Email:        receiver.Email,
				Error:        fmt.Sprintf("Error fetching statistics: %v", err),
			})
			continue
		}

		// Generate the report
		reportBody := generateStatisticsReport(receiver, stats, days, vesselTypeMap, reportStartTime)

		// Create a copy of the receiver for sendEmail
		receiverCopy := receiver

		// Send the email
		emailSentTo, err := sendEmail("statistics_report", receiverCopy, reportBody)
		if err != nil {
			log.Printf("Error sending statistics report for receiver %d: %v", receiver.ID, err)
			failedReports = append(failedReports, FailedReport{
				ReceiverID:   receiver.ID,
				ReceiverName: receiver.Name,
				Email:        receiver.Email,
				Error:        fmt.Sprintf("Error sending email: %v", err),
			})
			continue
		}

		// Log the alert - ensure we log each email separately if multiple recipients
		if strings.Contains(emailSentTo, ",") {
			// Split the comma-separated list of emails
			emailList := strings.Split(emailSentTo, ",")
			for _, email := range emailList {
				email = strings.TrimSpace(email)
				if email == "" {
					continue
				}

				if err := logAlert("statistics_report", receiver.ID, email,
					fmt.Sprintf("Weekly statistics report sent for receiver %s (ID: %d)", receiver.Name, receiver.ID)); err != nil {
					log.Printf("Error logging statistics report alert for receiver %d to %s: %v", receiver.ID, email, err)
				}
			}
		} else {
			// Single email recipient
			if err := logAlert("statistics_report", receiver.ID, emailSentTo,
				fmt.Sprintf("Weekly statistics report sent for receiver %s (ID: %d)", receiver.Name, receiver.ID)); err != nil {
				log.Printf("Error logging statistics report alert for receiver %d: %v", receiver.ID, err)
			}
		}

		reportsSent++
		reportsSuccessful++
		log.Printf("Sent statistics report for receiver %d (%s)", receiver.ID, receiver.Name)
	}

	// Calculate total processing time
	totalProcessingTime := time.Since(overallStartTime)

	// Send summary email to admins
	sendReportSummaryToAdmins(reportsSent, reportsSuccessful, failedReports, totalProcessingTime)

	log.Printf("Weekly statistics report generation complete. Sent %d reports (%d successful, %d failed) in %.2f seconds.",
		reportsSent, reportsSuccessful, len(failedReports), totalProcessingTime.Seconds())
}

// sendReportSummaryToAdmins sends a summary of the report processing to site admins
func sendReportSummaryToAdmins(totalReports, successfulReports int, failedReports []FailedReport, processingTime time.Duration) {
	if settings.ToAddresses == "" {
		log.Println("No admin email addresses configured, skipping report summary")
		return
	}

	subject := fmt.Sprintf("Weekly Statistics Report Summary - %s", time.Now().Format("Jan 2, 2006"))

	var body strings.Builder
	body.WriteString(fmt.Sprintf("Weekly Statistics Report Processing Summary\n\n"))
	body.WriteString(fmt.Sprintf("Total reports processed: %d\n", totalReports))
	body.WriteString(fmt.Sprintf("Successful reports: %d\n", successfulReports))
	body.WriteString(fmt.Sprintf("Failed reports: %d\n", len(failedReports)))
	body.WriteString(fmt.Sprintf("Total processing time: %.2f seconds\n\n", processingTime.Seconds()))

	if len(failedReports) > 0 {
		body.WriteString("Failed Reports Details:\n")
		for i, report := range failedReports {
			body.WriteString(fmt.Sprintf("%d. Receiver ID: %d, Name: %s, Email: %s\n   Error: %s\n\n",
				i+1, report.ReceiverID, report.ReceiverName, report.Email, report.Error))
		}
	}

	body.WriteString(fmt.Sprintf("\nGenerated on: %s\n", time.Now().Format("January 2, 2006 at 15:04:05 (UTC)")))
	body.WriteString(fmt.Sprintf("\nAIS Decoder Team\nhttps://%s/\n", settings.SiteDomain))

	// Split the comma-separated list of admin emails
	adminEmails := strings.Split(settings.ToAddresses, ",")

	for _, email := range adminEmails {
		email = strings.TrimSpace(email)
		if email == "" {
			continue
		}

		err := sendSingleEmail(subject, body.String(), email)
		if err != nil {
			log.Printf("Error sending report summary to admin (%s): %v", email, err)
		} else {
			log.Printf("Sent report summary to admin: %s", email)

			// Log the alert for each admin email
			if err := logAlert("report_summary", -1, email,
				fmt.Sprintf("Weekly statistics report summary sent with %d total reports (%d successful, %d failed)",
					totalReports, successfulReports, len(failedReports))); err != nil {
				log.Printf("Error logging report summary alert for %s: %v", email, err)
			}
		}
	}
}

// sendWeeklyOfflineReceiversReport sends a weekly report of all receivers that have been offline for over a week
func sendWeeklyOfflineReceiversReport() {
	log.Println("Starting weekly offline receivers report generation")

	// Record start time for processing duration
	startTime := time.Now()

	// Fetch all receivers
	receivers, err := fetchReceivers()
	if err != nil {
		log.Printf("Error fetching receivers for offline report: %v", err)
		return
	}

	// Get current time
	now := time.Now()

	// Filter receivers that have been offline for over a week (7 days)
	var offlineReceivers []Receiver
	for _, receiver := range receivers {
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

		// Check if receiver has been offline for over a week
		if now.Sub(lastSeen) > 7*24*time.Hour {
			offlineReceivers = append(offlineReceivers, receiver)
		}
	}

	// If no offline receivers, log and return
	if len(offlineReceivers) == 0 {
		log.Println("No receivers offline for over a week, skipping report")
		return
	}

	// Generate the report
	var report strings.Builder

	// Header
	report.WriteString(fmt.Sprintf("Weekly Offline Receivers Report\n"))
	report.WriteString(fmt.Sprintf("Generated on: %s\n\n", time.Now().Format("January 2, 2006 at 15:04:05 (UTC)")))
	report.WriteString(fmt.Sprintf("The following %d receivers have been offline for over a week:\n\n", len(offlineReceivers)))

	// List all offline receivers
	for i, receiver := range offlineReceivers {
		// Parse the LastSeen time to format it in a human-readable way
		lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
		lastSeenStr := receiver.LastSeen
		if err == nil {
			lastSeenStr = lastSeen.Format("January 2, 2006 at 15:04:05 (UTC)")
		}

		// Calculate how long the receiver has been offline
		offlineDuration := now.Sub(lastSeen)
		offlineDays := int(offlineDuration.Hours() / 24)

		// Prepare UDP port display
		udpPortDisplay := "Not set"
		if receiver.UDPPort != nil {
			udpPortDisplay = strconv.Itoa(*receiver.UDPPort)
		}

		// Create a link to the receiver
		receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, receiver.ID)

		report.WriteString(fmt.Sprintf("%d. Receiver: %s (ID: %d)\n", i+1, receiver.Name, receiver.ID))
		report.WriteString(fmt.Sprintf("   Description: %s\n", receiver.Description))
		report.WriteString(fmt.Sprintf("   Email: %s\n", receiver.Email))
		report.WriteString(fmt.Sprintf("   UDP Port: %s\n", udpPortDisplay))
		report.WriteString(fmt.Sprintf("   Last seen: %s (%d days ago)\n", lastSeenStr, offlineDays))
		report.WriteString(fmt.Sprintf("   Receiver URL: %s\n\n", receiverURL))
	}

	// Footer
	report.WriteString(fmt.Sprintf("\nTotal offline receivers: %d\n", len(offlineReceivers)))
	report.WriteString(fmt.Sprintf("Report generated in %.2f seconds\n\n", time.Since(startTime).Seconds()))
	report.WriteString(fmt.Sprintf("AIS Decoder Team\nhttps://%s/\n", settings.SiteDomain))

	// Send the report to site admins
	if settings.ToAddresses == "" {
		log.Println("No admin email addresses configured, skipping offline receivers report")
		return
	}

	subject := fmt.Sprintf("Weekly Offline Receivers Report - %s", time.Now().Format("Jan 2, 2006"))

	// Split the comma-separated list of admin emails
	adminEmails := strings.Split(settings.ToAddresses, ",")

	for _, email := range adminEmails {
		email = strings.TrimSpace(email)
		if email == "" {
			continue
		}

		err := sendSingleEmail(subject, report.String(), email)
		if err != nil {
			log.Printf("Error sending offline receivers report to admin (%s): %v", email, err)
		} else {
			log.Printf("Sent offline receivers report to admin: %s", email)

			// Log the alert for each admin email
			if err := logAlert("offline_receivers_report", -1, email,
				fmt.Sprintf("Weekly offline receivers report sent with %d receivers", len(offlineReceivers))); err != nil {
				log.Printf("Error logging offline receivers report alert for %s: %v", email, err)
			}
		}
	}

	// Alert logging is now done for each individual email above

	log.Printf("Weekly offline receivers report generation complete. Reported %d offline receivers.", len(offlineReceivers))
}

// startStatisticsReportScheduler starts the scheduler for weekly statistics reports and offline receivers reports
func startStatisticsReportScheduler(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Initialize the last run times
	lastStatisticsReportTime := time.Now().Add(-24 * time.Hour)
	lastOfflineReportTime := time.Now().Add(-24 * time.Hour)

	// Configure offline receivers report time (Saturday at 09:00)
	offlineReportTime := "saturday,09:00"

	for {
		select {
		case <-ticker.C:
			// Check if it's time to send statistics reports
			if settings.StatisticsReportEnabled && settings.StatisticsBaseURL != "" && settings.StatisticsReportTime != "" {
				shouldSend, err := isTimeToSendReport(lastStatisticsReportTime, settings.StatisticsReportTime)
				if err != nil {
					log.Printf("Error checking statistics report time: %v", err)
				} else if shouldSend {
					log.Println("It's time to send weekly statistics reports")
					sendWeeklyStatisticsReports()
					lastStatisticsReportTime = time.Now()
				}
			}

			// Check if it's time to send offline receivers report
			shouldSend, err := isTimeToSendReport(lastOfflineReportTime, offlineReportTime)
			if err != nil {
				log.Printf("Error checking offline report time: %v", err)
			} else if shouldSend {
				log.Println("It's time to send weekly offline receivers report")
				sendWeeklyOfflineReceiversReport()
				lastOfflineReportTime = time.Now()
			}
		case <-ctx.Done():
			log.Println("Report scheduler stopped")
			return
		}
	}
}
