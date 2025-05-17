package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/smtp"
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
	ToAddresses        string `json:"to_addresses"`
}

// Receiver represents the payload sent by the receivers service.
type Receiver struct {
	ID          int      `json:"id"`
	LastUpdated string   `json:"lastupdated"`
	Description string   `json:"description"`
	Latitude    float64  `json:"latitude"`
	Longitude   float64  `json:"longitude"`
	Name        string   `json:"name"`
	URL         *string  `json:"url,omitempty"`
	UDPPort     *int     `json:"udp_port,omitempty"`
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
	switch alertType {
	case "receiver_added":
		subject = "New Receiver Added"
	default:
		subject = alertType
	}
	body := fmt.Sprintf(
		"A new receiver was added:\n\nID: %d\nName: %s\nDescription: %s\nLatitude: %f\nLongitude: %f\nLast Updated: %s\nURL: %v\nUDP Port: %v\n",
		rec.ID, rec.Name, rec.Description, rec.Latitude, rec.Longitude, rec.LastUpdated, rec.URL, rec.UDPPort,
	)
	msg := []byte("To: " + settings.ToAddresses + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" + body + "\r\n")
	addr := fmt.Sprintf("%s:%d", settings.SMTPHost, settings.SMTPPort)
	auth := smtp.PlainAuth("", settings.SMTPUser, settings.SMTPPass, settings.SMTPHost)

	if settings.SMTPUseTLS {
		tlsConfig := &tls.Config{ServerName: settings.SMTPHost, InsecureSkipVerify: settings.SMTPTLSSkipVerify}
		conn, err := tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return fmt.Errorf("TLS dial error: %w", err)
		}
		c, err := smtp.NewClient(conn, settings.SMTPHost)
		if err != nil {
			return fmt.Errorf("SMTP client error: %w", err)
		}
		defer c.Close()

		if err := c.Auth(auth); err != nil {
			return fmt.Errorf("SMTP auth error: %w", err)
		}
		if err := c.Mail(settings.FromAddress); err != nil {
			return fmt.Errorf("SMTP mail error: %w", err)
		}
		toList := strings.Split(settings.ToAddresses, ",")
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

	toList := strings.Split(settings.ToAddresses, ",")
	return smtp.SendMail(addr, auth, settings.FromAddress, toList, msg)
}

func alertHandler(w http.ResponseWriter, r *http.Request) {
    log.Printf("alertHandler: incoming %s %s", r.Method, r.URL.Path)
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var alertMsg struct {
		AlertType string   `json:"alert_type"`
		Receiver  Receiver `json:"receiver"`
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