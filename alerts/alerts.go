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
	ListenPort         int    `json:"listen_port"`
	SMTPHost           string `json:"smtp_host"`
	SMTPPort           int    `json:"smtp_port"`
	SMTPUser           string `json:"smtp_user"`
	SMTPPass           string `json:"smtp_pass"`
	SMTPUseTLS         bool   `json:"smtp_use_tls"`
	SMTPTLSSkipVerify  bool   `json:"smtp_tls_skip_verify"`
	FromAddress        string `json:"from_address"`
	FromName           string `json:"from_name"`
	ToAddresses        string `json:"to_addresses"`
	SiteDomain         string `json:"site_domain"` // Domain for password reset links
	ReceiversBaseURL   string `json:"receivers_base_url"`
	DBHost             string `json:"db_host"`
	DBPort             int    `json:"db_port"`
	DBUser             string `json:"db_user"`
	DBPass             string `json:"db_pass"`
	DBName             string `json:"db_name"`
}

// Receiver represents the payload sent by the receivers service.
type Receiver struct {
	ID           int                    `json:"id"`
	LastUpdated  string                 `json:"lastupdated"`
	LastSeen     string                 `json:"lastseen"`
	Description  string                 `json:"description"`
	Latitude     float64                `json:"latitude"`
	Longitude    float64                `json:"longitude"`
	Name         string                 `json:"name"`
	Email        string                 `json:"email"`
	URL          *string                `json:"url,omitempty"`
	UDPPort      *int                   `json:"udp_port,omitempty"`
	Notifications bool                  `json:"notifications"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"` // For additional fields like reset tokens
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

func sendEmail(alertType string, rec Receiver, customBody string) (string, error) {
	// Translate alert_type into a human-readable subject
	var subject string
	var body string
	var toAddresses string
	
	// Default to the configured to_addresses
	toAddresses = settings.ToAddresses
	
	switch alertType {
	case "receiver_added":
		subject = "New Receiver Added"
		// Prepare display values for URL and UDP port
		var urlDisplay, udpPortDisplay string
		if rec.URL != nil {
			urlDisplay = *rec.URL
		}
		if rec.UDPPort != nil {
			udpPortDisplay = strconv.Itoa(*rec.UDPPort)
		}
		body = fmt.Sprintf(
			"A new receiver was added:\n\nID: %d\nName: %s\nDescription: %s\nLatitude: %f\nLongitude: %f\nLast Updated: %s\nURL: %s\nUDP Port: %s\n",
			rec.ID, rec.Name, rec.Description, rec.Latitude, rec.Longitude, rec.LastUpdated, urlDisplay, udpPortDisplay,
		)
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
			"Thank you,\nAIS Decoder Team",
			resetLink,
		)
	case "receiver_offline":
		subject = fmt.Sprintf("Your AIS receiver '%s' has not been seen for over 24 hours", rec.Name)
		
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
			body = fmt.Sprintf(
				"Hello,\n\nYour AIS receiver '%s' (ID: %d) has not been seen for over 24 hours.\n\n"+
				"Receiver Details:\n"+
				"- Name: %s\n"+
				"- Description: %s\n"+
				"- Last seen: %s\n\n"+
				"Please check your receiver's connection and ensure it's properly configured.\n\n"+
				"You can view your receiver's details and disable notifications here: %s\n\n"+
				"Thank you,\nAIS Decoder Team",
				rec.Name, rec.ID, rec.Name, rec.Description, lastSeenStr, receiverURL,
			)
		}
	default:
		subject = alertType
		body = fmt.Sprintf("Alert type: %s\nReceiver ID: %d\nName: %s\nDescription: %s\n",
			alertType, rec.ID, rec.Name, rec.Description)
	}
	msg := []byte(
		"From: " + settings.FromName + " <" + settings.FromAddress + ">\r\n" +
		"To: " + toAddresses + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" + body + "\r\n")
	addr := fmt.Sprintf("%s:%d", settings.SMTPHost, settings.SMTPPort)
	auth := smtp.PlainAuth("", settings.SMTPUser, settings.SMTPPass, settings.SMTPHost)

	if settings.SMTPUseTLS {
		// Use STARTTLS to upgrade the connection
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return "", fmt.Errorf("SMTP dial error: %w", err)
		}
		c, err := smtp.NewClient(conn, settings.SMTPHost)
		if err != nil {
			return "", fmt.Errorf("SMTP client error: %w", err)
		}
		defer c.Close()

		tlsConfig := &tls.Config{ServerName: settings.SMTPHost, InsecureSkipVerify: settings.SMTPTLSSkipVerify}
		if err := c.StartTLS(tlsConfig); err != nil {
			return "", fmt.Errorf("SMTP STARTTLS error: %w", err)
		}

		if err := c.Auth(auth); err != nil {
			return "", fmt.Errorf("SMTP auth error: %w", err)
		}
		if err := c.Mail(settings.FromAddress); err != nil {
			return "", fmt.Errorf("SMTP mail error: %w", err)
		}
		toList := strings.Split(toAddresses, ",")
		for _, addr := range toList {
			if err := c.Rcpt(strings.TrimSpace(addr)); err != nil {
				return "", fmt.Errorf("SMTP rcpt error for %s: %w", addr, err)
			}
		}
		w, err := c.Data()
		if err != nil {
			return "", fmt.Errorf("SMTP data error: %w", err)
		}
		_, err = w.Write(msg)
		if err != nil {
			return "", fmt.Errorf("SMTP write error: %w", err)
		}
		if err := w.Close(); err != nil {
			return "", fmt.Errorf("SMTP close error: %w", err)
		}
		return toAddresses, c.Quit()
	}

	toList := strings.Split(toAddresses, ",")
	return toAddresses, smtp.SendMail(addr, auth, settings.FromAddress, toList, msg)
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

		// Parse the LastSeen time
		lastSeen, err := time.Parse(time.RFC3339, receiver.LastSeen)
		if err != nil {
			log.Printf("Error parsing LastSeen time for receiver %d: %v", receiver.ID, err)
			continue
		}

		// Check if receiver hasn't been seen for over 24 hours
		if now.Sub(lastSeen) > 24*time.Hour {
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
				receiverURL := fmt.Sprintf("https://%s/receivers.html?receiver=%d", settings.SiteDomain, receiver.ID)
				message := fmt.Sprintf(
					"Your AIS receiver '%s' (ID: %d) has not been seen for over 24 hours.\n\n"+
					"Receiver Details:\n"+
					"- Name: %s\n"+
					"- Description: %s\n"+
					"- Last seen: %s\n\n"+
					"Please check your receiver's connection and ensure it's properly configured.\n\n"+
					"You can view your receiver's details and disable notifications here: %s",
					receiver.Name, receiver.ID, receiver.Name, receiver.Description, lastSeenStr, receiverURL,
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

// startReceiverMonitoring starts the background monitoring of receivers
func startReceiverMonitoring(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			checkOfflineReceivers()
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
	
	// Start receiver monitoring in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startReceiverMonitoring(ctx)
	
	// Start HTTP server
	addr := fmt.Sprintf(":%d", settings.ListenPort)
	http.HandleFunc("/", alertHandler)
	log.Printf("Starting alerts service on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}