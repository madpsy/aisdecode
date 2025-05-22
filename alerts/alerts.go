package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Settings holds configuration for the alerts service.
type Settings struct {
	ListenPort            int    `json:"listen_port"`
	SMTPHost              string `json:"smtp_host"`
	SMTPPort              int    `json:"smtp_port"`
	SMTPUser              string `json:"smtp_user"`
	SMTPPass              string `json:"smtp_pass"`
	SMTPUseTLS            bool   `json:"smtp_use_tls"`
	SMTPTLSSkipVerify     bool   `json:"smtp_tls_skip_verify"`
	FromAddress           string `json:"from_address"`
	FromName              string `json:"from_name"`
	ToAddresses           string `json:"to_addresses"`
	SiteDomain            string `json:"site_domain"` // Domain for password reset links
	ReceiversBaseURL      string `json:"receivers_base_url"`
	DBHost                string `json:"db_host"`
	DBPort                int    `json:"db_port"`
	DBUser                string `json:"db_user"`
	DBPass                string `json:"db_pass"`
	DBName                string `json:"db_name"`
	ReceiverOfflineHours  int    `json:"receiver_offline_hours"` // Hours before a receiver is considered offline
	StatisticsBaseURL     string `json:"statistics_base_url"`    // Base URL for statistics API
	StatisticsReportEnabled bool  `json:"statistics_report_enabled"` // Whether to send statistics reports
	StatisticsReportTime  string `json:"statistics_report_time"` // When to send statistics reports (day,hour:minute)
}

// Receiver represents the payload sent by the receivers service.
type Receiver struct {
	ID               int                    `json:"id"`
	LastUpdated      string                 `json:"lastupdated"`
	LastSeen         string                 `json:"lastseen"`
	Description      string                 `json:"description"`
	Latitude         float64                `json:"latitude"`
	Longitude        float64                `json:"longitude"`
	Name             string                 `json:"name"`
	Email            string                 `json:"email"`
	URL              *string                `json:"url,omitempty"`
	UDPPort          *int                   `json:"udp_port,omitempty"`
	Notifications    bool                   `json:"notifications"`
	RequestIPAddress string                 `json:"request_ip_address,omitempty"` // IP address of who added the receiver
	CustomFields     map[string]interface{} `json:"custom_fields,omitempty"` // For additional fields like reset tokens
}

// Alert represents an alert record in the database
type Alert struct {
	ID        int       `json:"id"`
	Type      string    `json:"type"`
	ReceiverID int      `json:"receiver_id"`
	SentAt    time.Time `json:"sent_at"`
	Message   string    `json:"message"`
}

// ReceiverNotification represents a notification record for receivers
type ReceiverNotification struct {
	ID         int       `json:"id"`
	ReceiverID int       `json:"receiver_id"`
	LastSentAt time.Time `json:"last_sent_at"`
	Count      int       `json:"count"`
}

var (
	settings Settings
	db       *sql.DB
)

func loadSettings() {
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}
	
	// Set default value for ReceiverOfflineHours if not specified
	if settings.ReceiverOfflineHours <= 0 {
		settings.ReceiverOfflineHours = 24 // Default to 24 hours if not specified
		log.Println("ReceiverOfflineHours not specified in settings.json, using default value of 24 hours")
	}
	
	// Log statistics report settings
	if settings.StatisticsReportEnabled {
		if settings.StatisticsBaseURL == "" {
			log.Println("Warning: Statistics reporting is enabled but statistics_base_url is not set")
		} else if settings.StatisticsReportTime == "" {
			log.Println("Warning: Statistics reporting is enabled but statistics_report_time is not set")
		} else {
			log.Printf("Statistics reporting enabled, scheduled for %s", settings.StatisticsReportTime)
		}
	}
}

func initDB() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		settings.DBHost, settings.DBPort, settings.DBUser, settings.DBPass, settings.DBName)
	
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS alerts (
			id SERIAL PRIMARY KEY,
			type VARCHAR(50) NOT NULL,
			receiver_id INTEGER NOT NULL, -- Can be -1 for system alerts without a specific receiver
			email VARCHAR(255),           -- Email address the alert was sent to
			sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
			message TEXT NOT NULL
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create alerts table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS receiver_notifications (
			id SERIAL PRIMARY KEY,
			receiver_id INTEGER NOT NULL UNIQUE,
			last_sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
			count INTEGER NOT NULL DEFAULT 1
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create receiver_notifications table: %w", err)
	}

	log.Println("Database initialized successfully")
	return nil
}

// sendSingleEmail sends an email to a single recipient
func sendSingleEmail(subject, body, toAddress string) error {
	// Generate a Message-ID
	hostname := settings.SiteDomain
	if hostname == "" {
		hostname = settings.SMTPHost
	}
	messageID := fmt.Sprintf("<%d.%d@%s>", time.Now().Unix(), time.Now().UnixNano()%1000000, hostname)
	
	// Format current time as per RFC 5322
	currentTime := time.Now().Format("Mon, 02 Jan 2006 15:04:05 -0700")
	
	msg := []byte(
		"From: " + settings.FromName + " (" + settings.SiteDomain + ") <" + settings.FromAddress + ">\r\n" +
		"Reply-To: " + settings.FromName + " (" + settings.SiteDomain + ") <" + settings.FromAddress + ">\r\n" +
		"To: " + toAddress + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Date: " + currentTime + "\r\n" +
		"Message-Id: " + messageID + "\r\n" +
		"\r\n" + body + "\r\n")
	
	addr := fmt.Sprintf("%s:%d", settings.SMTPHost, settings.SMTPPort)
	auth := smtp.PlainAuth("", settings.SMTPUser, settings.SMTPPass, settings.SMTPHost)

	if settings.SMTPUseTLS {
		// Use STARTTLS to upgrade the connection
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("SMTP dial error: %w", err)
		}
		c, err := smtp.NewClient(conn, settings.SMTPHost)
		if err != nil {
			return fmt.Errorf("SMTP client error: %w", err)
		}
		defer c.Close()

		tlsConfig := &tls.Config{ServerName: settings.SMTPHost, InsecureSkipVerify: settings.SMTPTLSSkipVerify}
		if err := c.StartTLS(tlsConfig); err != nil {
			return fmt.Errorf("SMTP STARTTLS error: %w", err)
		}

		if err := c.Auth(auth); err != nil {
			return fmt.Errorf("SMTP auth error: %w", err)
		}
		if err := c.Mail(settings.FromAddress); err != nil {
			return fmt.Errorf("SMTP mail error: %w", err)
		}
		
		if err := c.Rcpt(toAddress); err != nil {
			return fmt.Errorf("SMTP rcpt error for %s: %w", toAddress, err)
		}
		
		w, err := c.Data()
		if err != nil {
			return fmt.Errorf("SMTP data error: %w", err)
		}
		_, err = w.Write(msg)
		if err != nil {
			return fmt.Errorf("SMTP write error: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("SMTP close error: %w", err)
		}
		return c.Quit()
	}

	return smtp.SendMail(addr, auth, settings.FromAddress, []string{toAddress}, msg)
}

func sendEmail(alertType string, rec Receiver, customBody string) (string, error) {
	// Translate alert_type into a human-readable subject
	var subject string
	var body string
	var toAddresses string
	
	// Default to the configured to_addresses
	toAddresses = settings.ToAddresses
	
	switch alertType {
	case "blocked_ip_attempt", "blocked_ip_failed_attempts":
		// This alert is sent when a blocked IP tries to sign up
		subject = "Blocked IP Signup Attempt"
		
		// Only send to admin addresses
		toAddresses = settings.ToAddresses
		
		// Extract information from custom fields
		var reason string
		var unblockAt string
		var attempts int
		var attemptedEmail string
		var attemptedName string
		var attemptedDescription string
		var attemptedLatitude float64
		var attemptedLongitude float64
		
		if rec.CustomFields != nil {
			if r, ok := rec.CustomFields["reason"].(string); ok {
				reason = r
			}
			if u, ok := rec.CustomFields["unblock_at"].(string); ok {
				unblockAt = u
			}
			if a, ok := rec.CustomFields["attempts"].(float64); ok {
				attempts = int(a)
			}
			if e, ok := rec.CustomFields["attempted_email"].(string); ok {
				attemptedEmail = e
			}
			if n, ok := rec.CustomFields["attempted_name"].(string); ok {
				attemptedName = n
			}
			if d, ok := rec.CustomFields["attempted_description"].(string); ok {
				attemptedDescription = d
			}
			if lat, ok := rec.CustomFields["attempted_latitude"].(float64); ok {
				attemptedLatitude = lat
			}
			if lon, ok := rec.CustomFields["attempted_longitude"].(float64); ok {
				attemptedLongitude = lon
			}
		}
		
		// Format the unblock time in a human-readable way
		unblockTime, err := time.Parse(time.RFC3339, unblockAt)
		unblockTimeStr := unblockAt
		if err == nil {
			unblockTimeStr = unblockTime.Format("January 2, 2006 at 15:04:05 (UTC)")
		}
		
		// Create a location link for Google Maps if we have coordinates
		locationLink := ""
		if attemptedLatitude != 0 || attemptedLongitude != 0 {
			locationLink = fmt.Sprintf("https://maps.google.com/?q=%f,%f", attemptedLatitude, attemptedLongitude)
		}
		
		// Create the email body
		body = fmt.Sprintf(
			"Alert: Blocked IP Address Attempted to Sign Up\n\n"+
			"An IP address that is currently blocked attempted to create a new receiver.\n\n"+
			"Block Details:\n"+
			"- IP Address: %s\n"+
			"- Reason for Block: %s\n"+
			"- Block Expires: %s\n"+
			"- Attempt Count: %d\n\n"+
			"Attempted Signup Details:\n"+
			"- Email: %s\n"+
			"- Name: %s\n"+
			"- Description: %s\n"+
			"- Coordinates: %f, %f\n",
			rec.RequestIPAddress, reason, unblockTimeStr, attempts,
			attemptedEmail, attemptedName, attemptedDescription, attemptedLatitude, attemptedLongitude,
		)
		
		// Add location link if available
		if locationLink != "" {
			body += fmt.Sprintf("- Location Map: %s\n", locationLink)
		}
		
		body += "\nThis is an automated security alert.\n\n"+
			"AIS Decoder Team\nhttps://" + settings.SiteDomain + "/"
	case "receiver_added":
		// Include the receiver's name in the subject
		subject = fmt.Sprintf("New Receiver Added: %s", rec.Name)
		
		// For receiver added, send to both the site owner addresses and the receiver's email address
		if rec.Email != "" {
			// If we have a receiver email, add it to the site owner addresses
			toAddresses = settings.ToAddresses
			if toAddresses != "" && rec.Email != "" {
				toAddresses += "," + rec.Email
			} else if rec.Email != "" {
				toAddresses = rec.Email
			}
		}
		
		// Prepare display values for URL and UDP port
		var urlDisplay, udpPortDisplay string
		if rec.URL != nil {
			urlDisplay = *rec.URL
		}
		if rec.UDPPort != nil {
			udpPortDisplay = strconv.Itoa(*rec.UDPPort)
		}
		
		// Create a link to the receiver
		receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, rec.ID)
		
		// Create a more user-friendly email with a welcome message
		body = fmt.Sprintf(
			"Hello,\n\n"+
			"Welcome to AIS Decoder! Your new receiver '%s' has been successfully registered.\n\n"+
			"Your UDP Port: %s\n\n"+ // Highlight the UDP port as most important
			"Please ensure your feeder is configured to send to ingest.%s UDP port %s\n\n"+
			"Receiver Details:\n"+
			"- ID: %d\n"+
			"- Name: %s\n"+
			"- Description: %s\n"+
			"- Latitude: %f\n"+
			"- Longitude: %f\n"+
			"- Last Updated: %s\n"+
			"- Website: %s\n\n"+
			"You can view your receiver's details here: %s\n\n"+
			"Thank you for contributing to our AIS network!\n\n"+
			"AIS Decoder Team\nhttps://" + settings.SiteDomain + "/",
			rec.Name, udpPortDisplay, settings.SiteDomain, udpPortDisplay,
			rec.ID, rec.Name, rec.Description, rec.Latitude, rec.Longitude, rec.LastUpdated, urlDisplay,
			receiverURL,
		)
		
		// Add IP address info in the admin-only version if needed
		if settings.ToAddresses != "" {
			adminBody := body + fmt.Sprintf("\n\nAdditional Admin Info:\nAdded from IP: %s", rec.RequestIPAddress)
			
			// If we're sending to both admin and user, send separate emails
			if rec.Email != "" && settings.ToAddresses != "" {
				// First send the user-friendly version to the receiver owner
				err := sendSingleEmail(subject, body, rec.Email)
				if err != nil {
					log.Printf("Error sending user email for receiver %d: %v", rec.ID, err)
				}
				
				// Log the alert for the user email
				if err := logAlert(alertType, rec.ID, rec.Email, fmt.Sprintf("Alert type: %s for receiver %s (ID: %d) - user email", alertType, rec.Name, rec.ID)); err != nil {
					log.Printf("Error logging user alert to database: %v", err)
				}
				
				// Then prepare the admin version with IP info
				body = adminBody
				toAddresses = settings.ToAddresses
			} else {
				// If sending to only one type of recipient, use the admin version
				body = adminBody
			}
		}
	case "receiver_deleted":
		// Include the receiver's name in the subject
		subject = fmt.Sprintf("Receiver Deleted: %s", rec.Name)
		
		// Check if this was an admin action
		isAdminAction := false
		if rec.CustomFields != nil {
			isAdminAction, _ = rec.CustomFields["is_admin_action"].(bool)
		}
		
		// For receiver deleted, only include the user's email if it wasn't an admin action
		if rec.Email != "" && !isAdminAction {
			// If we have a receiver email and it's not an admin action, add it to the site owner addresses
			toAddresses = settings.ToAddresses
			if toAddresses != "" && rec.Email != "" {
				toAddresses += "," + rec.Email
			} else if rec.Email != "" {
				toAddresses = rec.Email
			}
		} else {
			// If it's an admin action or no email, just use the site owner addresses
			toAddresses = settings.ToAddresses
		}
		
		// Prepare display values for URL and UDP port
		var urlDisplay, udpPortDisplay string
		if rec.URL != nil {
			urlDisplay = *rec.URL
		}
		if rec.UDPPort != nil {
			udpPortDisplay = strconv.Itoa(*rec.UDPPort)
		}
		
		// Create a user-friendly email with a "sorry to see you go" message
		body = fmt.Sprintf(
			"Hello,\n\n"+
			"We're sorry to see you go. Your receiver '%s' has been successfully deleted from our system.\n\n"+
			"Receiver Details:\n"+
			"- ID: %d\n"+
			"- Name: %s\n"+
			"- Description: %s\n"+
			"- Latitude: %f\n"+
			"- Longitude: %f\n"+
			"- URL: %s\n"+
			"- UDP Port: %s\n\n"+
			"Thank you for your contribution to our AIS network. We hope to see you again in the future!\n\n"+
			"AIS Decoder Team\nhttps://" + settings.SiteDomain + "/",
			rec.Name, rec.ID, rec.Name, rec.Description, rec.Latitude, rec.Longitude, urlDisplay, udpPortDisplay,
		)
		
		// Add IP address info in the admin-only version if needed
		if settings.ToAddresses != "" {
			ipInfo := ""
			if rec.RequestIPAddress != "" {
				ipInfo = fmt.Sprintf("\n\nAdditional Admin Info:\nDeleted from IP: %s", rec.RequestIPAddress)
			}
			
			adminBody := body + ipInfo
			
			// If we're sending to both admin and user, send separate emails
			if rec.Email != "" && settings.ToAddresses != "" && !isAdminAction {
				// First send the user-friendly version to the receiver owner
				err := sendSingleEmail(subject, body, rec.Email)
				if err != nil {
					log.Printf("Error sending user email for deleted receiver %d: %v", rec.ID, err)
				}
				
				// Log the alert for the user email
				if err := logAlert(alertType, rec.ID, rec.Email, fmt.Sprintf("Alert type: %s for receiver %s (ID: %d) - user email", alertType, rec.Name, rec.ID)); err != nil {
					log.Printf("Error logging user alert to database: %v", err)
				}
				
				// Then prepare the admin version with IP info
				body = adminBody
				toAddresses = settings.ToAddresses
			} else {
				// If sending to only one type of recipient, use the admin version
				body = adminBody
			}
		}
	case "password_reset":
		subject = "Password Reset Request"
		// Extract reset link and email from the custom fields
		resetToken, _ := rec.CustomFields["reset_token"].(string)
		email, _ := rec.CustomFields["email"].(string)
		
		// For password reset, send to the user's email address
		if email != "" {
			toAddresses = email
		}
		
		// Create reset link using the site domain from settings
		resetLink := fmt.Sprintf("https://%s/resetpassword.html?token=%s", settings.SiteDomain, resetToken)
		
		body = fmt.Sprintf(
			"Hello,\n\nA password reset has been requested for your receiver account.\n\n"+
			"Click the link below to reset your password:\n\n%s\n\n"+
			"This link will expire in 24 hours.\n\n"+
			"If you did not request this password reset, please ignore this email.\n\n"+
			"Thank you,\nAIS Decoder Team\nhttps://" + settings.SiteDomain + "/",
			resetLink,
		)
	case "receiver_offline":
		subject = fmt.Sprintf("Your AIS receiver '%s' has not been seen for over %d hours", rec.Name, settings.ReceiverOfflineHours)
		
		// For receiver offline notifications, send to both the receiver's email address
		// and the site owner's addresses
		if rec.Email != "" {
			// If we have a receiver email, add it to the site owner addresses
			toAddresses = settings.ToAddresses
			if toAddresses != "" && rec.Email != "" {
				toAddresses += "," + rec.Email
			} else if rec.Email != "" {
				toAddresses = rec.Email
			}
		}
		
		if customBody != "" {
			body = customBody
		} else {
			// Parse the LastSeen time to format it in a human-readable way
			lastSeen, err := time.Parse(time.RFC3339, rec.LastSeen)
			lastSeenStr := rec.LastSeen
			if err == nil {
				lastSeenStr = lastSeen.Format("January 2, 2006 at 15:04:05 (UTC)")
			}
			
			receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, rec.ID)
			
			// Prepare UDP port display
			udpPortDisplay := "Not set"
			if rec.UDPPort != nil {
				udpPortDisplay = strconv.Itoa(*rec.UDPPort)
			}
			
			body = fmt.Sprintf(
				"Hello,\n\nYour AIS receiver '%s' (ID: %d) has not been seen for over %d hours.\n\n"+
				"Receiver Details:\n"+
				"- Name: %s\n"+
				"- Description: %s\n"+
				"- UDP Port: %s\n"+
				"- Last seen: %s\n\n"+
				"Please check your receiver's connection and ensure it's properly configured to send data to ingest.%s UDP port %s\n\n"+
				"You can view your receiver's details and disable notifications here: %s\n\n"+
				"Thank you for contributing to our AIS network!\n\nAIS Decoder Team\nhttps://" + settings.SiteDomain + "/",
				rec.Name, rec.ID, settings.ReceiverOfflineHours, rec.Name, rec.Description, udpPortDisplay, lastSeenStr, settings.SiteDomain, udpPortDisplay, receiverURL,
			)
		}
	case "receiver_updated":
		// Include the receiver's name in the subject
		subject = fmt.Sprintf("Receiver Updated: %s", rec.Name)
		
		// Check if this was an admin action
		isAdminAction := false
		if rec.CustomFields != nil {
			isAdminAction, _ = rec.CustomFields["is_admin_action"].(bool)
		}
		
		// For receiver updates, only include the user's email if it wasn't an admin action
		if rec.Email != "" && !isAdminAction {
			// If we have a receiver email and it's not an admin action, add it to the site owner addresses
			toAddresses = settings.ToAddresses
			if toAddresses != "" && rec.Email != "" {
				toAddresses += "," + rec.Email
			} else if rec.Email != "" {
				toAddresses = rec.Email
			}
		} else {
			// If it's an admin action or no email, just use the site owner addresses
			toAddresses = settings.ToAddresses
		}
		
		// Extract the changed fields from custom fields
		changedFields, _ := rec.CustomFields["changed_fields"].(map[string]interface{})
		
		// Create a user-friendly email with details about what changed
		var changesText strings.Builder
		
		if changedFields != nil {
			for field, change := range changedFields {
				changeMap, ok := change.(map[string]interface{})
				if !ok {
					continue
				}
				
				// Special handling for password
				if field == "password" {
					changesText.WriteString("- Password: Changed (for security, values not shown)\n")
					continue
				}
				
				// Format the change based on field type
				switch field {
				case "name", "description", "email", "url":
					oldVal := fmt.Sprintf("%v", changeMap["old"])
					newVal := fmt.Sprintf("%v", changeMap["new"])
					changesText.WriteString(fmt.Sprintf("- %s: Changed from '%s' to '%s'\n",
						strings.Title(field), oldVal, newVal))
				case "latitude", "longitude":
					oldVal := fmt.Sprintf("%.6f", changeMap["old"])
					newVal := fmt.Sprintf("%.6f", changeMap["new"])
					changesText.WriteString(fmt.Sprintf("- %s: Changed from %s to %s\n",
						strings.Title(field), oldVal, newVal))
				case "notifications":
					newVal := fmt.Sprintf("%v", changeMap["new"])
					notificationStatus := "Disabled"
					if newVal == "true" {
						notificationStatus = "Enabled"
					}
					changesText.WriteString(fmt.Sprintf("- Notifications: %s\n", notificationStatus))
				default:
					oldVal := fmt.Sprintf("%v", changeMap["old"])
					newVal := fmt.Sprintf("%v", changeMap["new"])
					changesText.WriteString(fmt.Sprintf("- %s: Changed from '%s' to '%s'\n",
						strings.Title(field), oldVal, newVal))
				}
			}
		}
		
		// If no changes were found or formatted
		if changesText.Len() == 0 {
			changesText.WriteString("- Some details were updated\n")
		}
		
		// Create a link to the receiver
		receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, rec.ID)
		
		// Create the email body
		body = fmt.Sprintf(
			"Hello,\n\n"+
			"Your receiver '%s' has been updated with the following changes:\n\n%s\n"+
			"You can view your receiver's details here: %s\n\n"+
			"Thank you for contributing to our AIS network!\n\n"+
			"AIS Decoder Team\nhttps://" + settings.SiteDomain + "/",
			rec.Name, changesText.String(), receiverURL,
		)
		
		// Add IP address info in the admin-only version if needed
		if settings.ToAddresses != "" {
			ipInfo := ""
			if rec.RequestIPAddress != "" {
				ipInfo = fmt.Sprintf("\n\nAdditional Admin Info:\nUpdated from IP: %s", rec.RequestIPAddress)
			}
			
			adminBody := body + ipInfo
			
			// If we're sending to both admin and user, send separate emails
			if rec.Email != "" && settings.ToAddresses != "" {
				// First send the user-friendly version to the receiver owner
				err := sendSingleEmail(subject, body, rec.Email)
				if err != nil {
					log.Printf("Error sending user email for updated receiver %d: %v", rec.ID, err)
				}
				
				// Log the alert for the user email
				if err := logAlert(alertType, rec.ID, rec.Email, fmt.Sprintf("Alert type: %s for receiver %s (ID: %d) - user email", alertType, rec.Name, rec.ID)); err != nil {
					log.Printf("Error logging user alert to database: %v", err)
				}
				
				// Then prepare the admin version with IP info
				body = adminBody
				toAddresses = settings.ToAddresses
			} else {
				// If sending to only one type of recipient, use the admin version
				body = adminBody
			}
		}
	case "statistics_report":
		subject = fmt.Sprintf("Weekly AIS Statistics Report for %s", rec.Name)
		
		// For statistics reports, send only to the receiver's email address
		if rec.Email != "" {
			toAddresses = rec.Email
		} else {
			// If no receiver email, don't send the report
			return "", fmt.Errorf("no email address for receiver %d", rec.ID)
		}
		
		// Use the custom body provided for the report
		if customBody != "" {
			body = customBody
		} else {
			body = fmt.Sprintf(
				"Weekly AIS Statistics Report for %s\n\n"+
				"This is your weekly AIS statistics report. No data is available at this time.\n\n"+
				"Thank you for contributing to our AIS network!\n\n"+
				"AIS Decoder Team\nhttps://%s/",
				rec.Name, settings.SiteDomain,
			)
		}
	default:
		subject = alertType
		body = fmt.Sprintf("Alert type: %s\nReceiver ID: %d\nName: %s\nDescription: %s\n",
			alertType, rec.ID, rec.Name, rec.Description)
	}
	// If there are multiple recipients, send to each one individually
	if strings.Contains(toAddresses, ",") {
		toList := strings.Split(toAddresses, ",")
		var lastErr error
		for _, addr := range toList {
			recipientAddr := strings.TrimSpace(addr)
			if err := sendSingleEmail(subject, body, recipientAddr); err != nil {
				log.Printf("Error sending email to %s: %v", recipientAddr, err)
				lastErr = err
			}
		}
		if lastErr != nil {
			return toAddresses, fmt.Errorf("error sending to one or more recipients: %w", lastErr)
		}
		return toAddresses, nil
	} else {
		// Single recipient
		return toAddresses, sendSingleEmail(subject, body, toAddresses)
	}
}

func alertHandler(w http.ResponseWriter, r *http.Request) {
    log.Printf("alertHandler: incoming %s %s", r.Method, r.URL.Path)
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var alertMsg struct {
		AlertType string                 `json:"alert_type"`
		Receiver  Receiver               `json:"receiver"`
		Custom    map[string]interface{} `json:"custom,omitempty"`
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &alertMsg); err != nil {
	    http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
	    return
	}
	log.Printf("alertHandler: webhook received for alert_type %s Receiver ID %d, Name %s", alertMsg.AlertType, alertMsg.Receiver.ID, alertMsg.Receiver.Name)
	
	// Initialize CustomFields if it's nil
	if alertMsg.Receiver.CustomFields == nil {
		alertMsg.Receiver.CustomFields = make(map[string]interface{})
	}
	
	// Copy any custom fields from the top-level custom field
	if alertMsg.Custom != nil {
		for k, v := range alertMsg.Custom {
			alertMsg.Receiver.CustomFields[k] = v
		}
	}
	
	emailSentTo, err := sendEmail(alertMsg.AlertType, alertMsg.Receiver, "")
	if err != nil {
	    log.Printf("alertHandler: sendEmail error for alert_type %s Receiver ID %d: %v", alertMsg.AlertType, alertMsg.Receiver.ID, err)
	    http.Error(w, "Failed to send alert email: "+err.Error(), http.StatusInternalServerError)
	    return
	}
	
	// Log the alert to the database
	// For password reset, ensure we have a valid receiver ID (might be 0 for new users)
	receiverID := alertMsg.Receiver.ID
	if alertMsg.AlertType == "password_reset" && receiverID == 0 {
		// Use -1 as a placeholder for system alerts without a specific receiver
		receiverID = -1
	}
	
	message := fmt.Sprintf("Alert type: %s for receiver %s (ID: %d)",
		alertMsg.AlertType, alertMsg.Receiver.Name, alertMsg.Receiver.ID)
	if err := logAlert(alertMsg.AlertType, receiverID, emailSentTo, message); err != nil {
		log.Printf("Error logging alert to database: %v", err)
	}
	
	log.Printf("alertHandler: alert email sent for alert_type %s Receiver ID %d", alertMsg.AlertType, alertMsg.Receiver.ID)
	w.WriteHeader(http.StatusOK)
}

// logAlert records an alert in the database
func logAlert(alertType string, receiverID int, email string, message string) error {
	_, err := db.Exec(
		"INSERT INTO alerts (type, receiver_id, email, sent_at, message) VALUES ($1, $2, $3, NOW(), $4)",
		alertType, receiverID, email, message,
	)
	return err
}

// logAlertMultipleEmails records an alert in the database for each email address
func logAlertMultipleEmails(alertType string, receiverID int, emails []string, message string) error {
	for _, email := range emails {
		if err := logAlert(alertType, receiverID, email, message); err != nil {
			return err
		}
	}
	return nil
}

// shouldSendNotification determines if we should send a notification for a receiver
// based on when the last notification was sent
func shouldSendNotification(receiverID int) (bool, error) {
	var lastSent time.Time
	var count int

	err := db.QueryRow(
		"SELECT last_sent_at, count FROM receiver_notifications WHERE receiver_id = $1",
		receiverID,
	).Scan(&lastSent, &count)

	if err == sql.ErrNoRows {
		// No previous notification, should send
		return true, nil
	} else if err != nil {
		return false, err
	}

	// If first notification (count=1), we already sent it
	// For subsequent notifications, send weekly
	if count == 1 {
		// Check if it's been a week since the last notification
		return time.Since(lastSent) >= 7*24*time.Hour, nil
	}

	return false, nil
}

// updateNotificationRecord updates or creates a notification record for a receiver
func updateNotificationRecord(receiverID int) error {
	// Try to update existing record
	result, err := db.Exec(
		"UPDATE receiver_notifications SET last_sent_at = NOW(), count = count + 1 WHERE receiver_id = $1",
		receiverID,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	// If no rows were affected, insert a new record
	if rowsAffected == 0 {
		_, err = db.Exec(
			"INSERT INTO receiver_notifications (receiver_id, last_sent_at, count) VALUES ($1, NOW(), 1)",
			receiverID,
		)
		return err
	}

	return nil
}

// fetchReceivers gets the current list of receivers from the receivers service
func fetchReceivers() ([]Receiver, error) {
	url := fmt.Sprintf("%s/admin/receivers", settings.ReceiversBaseURL)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch receivers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("receivers API returned status: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var receivers []Receiver
	if err := json.Unmarshal(body, &receivers); err != nil {
		return nil, fmt.Errorf("failed to parse receivers: %w", err)
	}

	return receivers, nil
}

// checkOfflineReceivers checks for receivers that haven't been seen for over 24 hours
func checkOfflineReceivers() {
	receivers, err := fetchReceivers()
	if err != nil {
		log.Printf("Error fetching receivers: %v", err)
		return
	}

	now := time.Now()
	for _, receiver := range receivers {
		// Skip receivers with ID 0, notifications disabled, or no email
		if receiver.ID == 0 || !receiver.Notifications || receiver.Email == "" {
			continue
		}

		// Skip receivers with no LastSeen value or parse the LastSeen time
		if receiver.LastSeen == "" {
			continue // Silently skip receivers with no LastSeen value
		}
		
		lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
		if err != nil {
			// Only log parsing errors for non-empty LastSeen values
			log.Printf("Error parsing LastSeen time for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Check if receiver hasn't been seen for over the configured time period
		if now.Sub(lastSeen) > time.Duration(settings.ReceiverOfflineHours)*time.Hour {
			// Check if we should send a notification
			shouldSend, err := shouldSendNotification(receiver.ID)
			if err != nil {
				log.Printf("Error checking notification status for receiver %d: %v", receiver.ID, err)
				continue
			}

			if shouldSend {
				// Create a copy of the receiver for sendEmail
				receiverCopy := receiver
				
				// Set the email address to use for this notification
				if receiver.Email != "" {
					// Use the receiver's email directly in the sendEmail function
					// The function will use this as the recipient
				}
				
				// Parse the LastSeen time to format it in a human-readable way
				lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
				lastSeenStr := receiver.LastSeen
				if err == nil {
					lastSeenStr = lastSeen.Format("January 2, 2006 at 15:04:05 (UTC)")
				}
				
				// Create custom message
				receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, receiver.ID)
				
				// Prepare UDP port display
				udpPortDisplay := "Not set"
				if receiver.UDPPort != nil {
					udpPortDisplay = strconv.Itoa(*receiver.UDPPort)
				}
				
				message := fmt.Sprintf(
					"Your AIS receiver '%s' (ID: %d) has not been seen for over %d hours.\n\n"+
					"Receiver Details:\n"+
					"- Name: %s\n"+
					"- Description: %s\n"+
					"- UDP Port: %s\n"+
					"- Last seen: %s\n\n"+
					"Please check your receiver's connection and ensure it's properly configured to send data to ingest.%s UDP port %s\n\n"+
					"You can view your receiver's details and disable notifications here: %s\n\n"+
					"Thank you for contributing to our AIS network!\n\n"+
					"AIS Decoder Team\nhttps://%s/",
					receiver.Name, receiver.ID, settings.ReceiverOfflineHours, receiver.Name, receiver.Description, udpPortDisplay, lastSeenStr, settings.SiteDomain, udpPortDisplay, receiverURL, settings.SiteDomain,
				)

				// Send the email
				emailSentTo, err := sendEmail("receiver_offline", receiverCopy, message)
				if err != nil {
					log.Printf("Error sending offline notification for receiver %d: %v", receiver.ID, err)
					continue
				}

				// Log the alert
				if err := logAlert("receiver_offline", receiver.ID, emailSentTo, message); err != nil {
					log.Printf("Error logging alert for receiver %d: %v", receiver.ID, err)
				}

				// Update notification record
				if err := updateNotificationRecord(receiver.ID); err != nil {
					log.Printf("Error updating notification record for receiver %d: %v", receiver.ID, err)
				}

				log.Printf("Sent offline notification for receiver %d (%s)", receiver.ID, receiver.Name)
			}
		}
	}
}

// checkWeeklyInactiveReceivers checks for receivers that haven't been seen for over 1 week
// and sends a summary email to site admins only
func checkWeeklyInactiveReceivers() {
	// Only proceed if we have admin email addresses configured
	if settings.ToAddresses == "" {
		log.Println("No admin email addresses configured, skipping weekly inactive receivers check")
		return
	}

	receivers, err := fetchReceivers()
	if err != nil {
		log.Printf("Error fetching receivers for weekly check: %v", err)
		return
	}

	now := time.Now()
	var inactiveReceivers []Receiver
	oneWeek := 7 * 24 * time.Hour

	for _, receiver := range receivers {
		// Skip receivers with no LastSeen value
		if receiver.LastSeen == "" {
			continue
		}
		
		lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
		if err != nil {
			log.Printf("Error parsing LastSeen time for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Check if receiver hasn't been seen for over 1 week
		if now.Sub(lastSeen) > oneWeek {
			inactiveReceivers = append(inactiveReceivers, receiver)
		}
	}

	// If no inactive receivers, no need to send an email
	if len(inactiveReceivers) == 0 {
		log.Println("No receivers inactive for over 1 week, skipping weekly report")
		return
	}

	// Build the email body with a summary of inactive receivers
	var bodyBuilder strings.Builder
	bodyBuilder.WriteString(fmt.Sprintf("Weekly Inactive Receivers Report - %s\n\n", now.Format("2006-01-02")))
	bodyBuilder.WriteString(fmt.Sprintf("The following %d receivers have not been seen for over 1 week:\n\n", len(inactiveReceivers)))

	for i, receiver := range inactiveReceivers {
		lastSeen, _ := time.Parse(time.RFC3339, receiver.LastSeen)
		lastSeenStr := lastSeen.Format("January 2, 2006 at 15:04:05 (UTC)")
		daysInactive := int(now.Sub(lastSeen).Hours() / 24)
		
		bodyBuilder.WriteString(fmt.Sprintf("%d. Receiver ID: %d\n", i+1, receiver.ID))
		bodyBuilder.WriteString(fmt.Sprintf("   Name: %s\n", receiver.Name))
		bodyBuilder.WriteString(fmt.Sprintf("   Description: %s\n", receiver.Description))
		bodyBuilder.WriteString(fmt.Sprintf("   Last seen: %s (%d days ago)\n", lastSeenStr, daysInactive))
		bodyBuilder.WriteString(fmt.Sprintf("   Email: %s\n", receiver.Email))
		
		// Add UDP port if available
		if receiver.UDPPort != nil {
			bodyBuilder.WriteString(fmt.Sprintf("   UDP Port: %d\n", *receiver.UDPPort))
		}
		
		// Add URL if available
		if receiver.URL != nil && *receiver.URL != "" {
			bodyBuilder.WriteString(fmt.Sprintf("   URL: %s\n", *receiver.URL))
		}
		
		// Add link to receiver details
		receiverURL := fmt.Sprintf("https://%s/metrics/receiver.html?receiver=%d", settings.SiteDomain, receiver.ID)
		bodyBuilder.WriteString(fmt.Sprintf("   Details: %s\n\n", receiverURL))
	}

	bodyBuilder.WriteString(fmt.Sprintf("\nThis is an automated weekly report sent every Saturday at midnight.\n"))
	bodyBuilder.WriteString(fmt.Sprintf("\nAIS Decoder Team\nhttps://%s/", settings.SiteDomain))

	// Create a dummy receiver for the sendEmail function
	dummyReceiver := Receiver{
		ID:          -1,
		Name:        "System",
		Description: "Weekly Inactive Receivers Report",
	}

	// Send the email to admins only
	emailSentTo, err := sendEmail("weekly_inactive_receivers", dummyReceiver, bodyBuilder.String())
	if err != nil {
		log.Printf("Error sending weekly inactive receivers report: %v", err)
		return
	}

	// Log the alert
	if err := logAlert("weekly_inactive_receivers", -1, emailSentTo, fmt.Sprintf("Weekly report of %d inactive receivers", len(inactiveReceivers))); err != nil {
		log.Printf("Error logging weekly inactive receivers alert: %v", err)
	}

	log.Printf("Sent weekly inactive receivers report to %s", emailSentTo)
}

// startReceiverMonitoring starts the background monitoring of receivers
func startReceiverMonitoring(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Initialize the last run time for weekly check
	lastWeeklyCheck := time.Now()

	for {
		select {
		case <-ticker.C:
			// Regular offline receiver check
			checkOfflineReceivers()
			
			// Check if it's Saturday at midnight (or close to it)
			now := time.Now()
			isSaturday := now.Weekday() == time.Saturday
			isNearMidnight := now.Hour() == 0 && now.Minute() < 5
			
			// Ensure we only run once per week by checking if it's been at least 23 hours since last check
			timeSinceLastCheck := now.Sub(lastWeeklyCheck)
			
			if isSaturday && isNearMidnight && timeSinceLastCheck >= 23*time.Hour {
				log.Println("Running weekly inactive receivers check")
				checkWeeklyInactiveReceivers()
				lastWeeklyCheck = now
			}
		case <-ctx.Done():
			log.Println("Receiver monitoring stopped")
			return
		}
	}
}

func main() {
	loadSettings()
	
	// Initialize database
	if err := initDB(); err != nil {
		log.Fatalf("Database initialization error: %v", err)
	}
	defer db.Close()
	
	// Create a context for all background goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Start receiver monitoring in a goroutine
	go startReceiverMonitoring(ctx)
	
	// Start statistics report scheduler if enabled
	if settings.StatisticsReportEnabled && settings.StatisticsBaseURL != "" && settings.StatisticsReportTime != "" {
		go startStatisticsReportScheduler(ctx)
	}
	
	// Start HTTP server
	addr := fmt.Sprintf(":%d", settings.ListenPort)
	http.HandleFunc("/", alertHandler)
	log.Printf("Starting alerts service on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}