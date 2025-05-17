package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/smtp"
	"strconv"
	"strings"
)

// Settings holds configuration for the alerts service.
type Settings struct {
	ListenPort  int    `json:"listen_port"`
	SMTPHost    string `json:"smtp_host"`
	SMTPPort    int    `json:"smtp_port"`
	SMTPUser    string `json:"smtp_user"`
	SMTPPass           string `json:"smtp_pass"`
	SMTPUseTLS         bool   `json:"smtp_use_tls"`
	SMTPTLSSkipVerify  bool   `json:"smtp_tls_skip_verify"`
	FromAddress        string `json:"from_address"`
	FromName           string `json:"from_name"`
	ToAddresses        string `json:"to_addresses"`
	SiteDomain         string `json:"site_domain"`  // Domain for password reset links
}

// Receiver represents the payload sent by the receivers service.
type Receiver struct {
	ID           int                    `json:"id"`
	LastUpdated  string                 `json:"lastupdated"`
	Description  string                 `json:"description"`
	Latitude     float64                `json:"latitude"`
	Longitude    float64                `json:"longitude"`
	Name         string                 `json:"name"`
	URL          *string                `json:"url,omitempty"`
	UDPPort      *int                   `json:"udp_port,omitempty"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"` // For additional fields like reset tokens
}

var settings Settings

func loadSettings() {
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}
}

func sendEmail(alertType string, rec Receiver) error {
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
		toList := strings.Split(toAddresses, ",")
		for _, addr := range toList {
			if err := c.Rcpt(strings.TrimSpace(addr)); err != nil {
				return fmt.Errorf("SMTP rcpt error for %s: %w", addr, err)
			}
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

	toList := strings.Split(toAddresses, ",")
	return smtp.SendMail(addr, auth, settings.FromAddress, toList, msg)
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
	
	if err := sendEmail(alertMsg.AlertType, alertMsg.Receiver); err != nil {
	    log.Printf("alertHandler: sendEmail error for alert_type %s Receiver ID %d: %v", alertMsg.AlertType, alertMsg.Receiver.ID, err)
	    http.Error(w, "Failed to send alert email: "+err.Error(), http.StatusInternalServerError)
	    return
	}
	log.Printf("alertHandler: alert email sent for alert_type %s Receiver ID %d", alertMsg.AlertType, alertMsg.Receiver.ID)
	w.WriteHeader(http.StatusOK)
}

func main() {
	loadSettings()
	addr := fmt.Sprintf(":%d", settings.ListenPort)
	http.HandleFunc("/", alertHandler)
	log.Printf("Starting alerts service on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}