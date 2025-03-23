package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"

	ais "github.com/BertoldVdb/go-ais"
	"github.com/BertoldVdb/go-ais/aisnmea"
	"github.com/zishang520/engine.io/v2/types"
	"github.com/zishang520/socket.io/v2/socket"
)

// Global client list and mutex.
var (
	clients      []*socket.Socket
	clientsMutex sync.Mutex
)

// Deduplication state: stores messages and their timestamps.
type dedupeState struct {
	message   string
	timestamp time.Time
}

// Check if the message is a duplicate within the deduplication window.
func isDuplicate(message string, dedupeWindow []dedupeState, windowDuration time.Duration) bool {
	// Normalize the message (remove any leading/trailing spaces or newline characters)
	message = strings.TrimSpace(message)

	// Remove messages older than the deduplication window
	now := time.Now()
	dedupeWindow = filterWindow(dedupeWindow, now.Add(-windowDuration))

	// Check if the message is already in the deduplication window
	for _, state := range dedupeWindow {
		// Compare message content (normalized)
		if state.message == message && now.Sub(state.timestamp) < windowDuration {
			return true // Duplicate found
		}
	}
	return false
}

// Filter the deduplication window to only include messages within the time range.
func filterWindow(window []dedupeState, cutoff time.Time) []dedupeState {
	filtered := []dedupeState{}
	for _, state := range window {
		if state.timestamp.After(cutoff) {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func main() {
	// Command-line flags.
	serialPort := flag.String("serial-port", "", "Serial port device (optional)")
	baud := flag.Int("baud", 38400, "Baud rate (default: 38400), ignored if -serial-port is not specified")
	wsPort := flag.Int("ws-port", 8100, "WebSocket port (default: 8100)")
	webRoot := flag.String("web-root", ".", "Web root directory (default: current directory)")
	debug := flag.Bool("debug", false, "Enable debug output")
	showDecodes := flag.Bool("show-decodes", false, "Output the decoded messages")
	aggregator := flag.String("aggregator", "", "Aggregator host/ip:port (optional)")
	udpListenPort := flag.Int("udp-listen-port", 8101, "UDP listen port for incoming NMEA data (default: 8101)")
	dedupeWindowDuration := flag.Int("dedupe-window", 1000, "Deduplication window in milliseconds (default: 1000, set to 0 to disable deduplication)")
	flag.Parse()

	// --- Setup Socket.IO server ---
	// Create an Engine.IO server.
	engineServer := types.CreateServer(nil)
	// Wrap the Engine.IO server with a Socket.IO server.
	sioServer := socket.NewServer(engineServer, nil)

	// Handle new Socket.IO connections.
	sioServer.On("connection", func(args ...any) {
		// The first argument is the connected client.
		client := args[0].(*socket.Socket)
		log.Printf("Socket.IO client connected: %s", client.Id())

		// Add client to global list.
		clientsMutex.Lock()
		clients = append(clients, client)
		clientsMutex.Unlock()

		// Join the "ais_data" room.
		client.Join("ais_data")

		// Handle client disconnect.
		client.On("disconnect", func(args ...any) {
			log.Printf("Socket.IO client disconnected: %s", client.Id())
			clientsMutex.Lock()
			// Remove the client from the global list.
			for i, c := range clients {
				if c == client {
					clients = append(clients[:i], clients[i+1:]...)
					break
				}
			}
			clientsMutex.Unlock()
		})
	})

	// --- Setup HTTP server ---
	// Serve static files from webRoot.
	fs := http.FileServer(http.Dir(*webRoot))
	http.Handle("/", fs)
	// Mount the Socket.IO endpoint.
	http.Handle("/socket.io/", engineServer)

	// Start HTTP server in a separate goroutine.
	go func() {
		addr := fmt.Sprintf(":%d", *wsPort)
		log.Printf("Starting HTTP/Socket.IO server on %s, serving web root: %s", addr, filepath.Clean(*webRoot))
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// --- Setup AIS decoder ---
	var port serial.Port
	if *serialPort != "" {
		// Only open serial port if -serial-port is provided.
		mode := &serial.Mode{BaudRate: *baud}
		var err error
		port, err = serial.Open(*serialPort, mode)
		if err != nil {
			log.Fatalf("failed to open serial port: %v", err)
		}
		defer port.Close()
	}

	// Create an AIS codec.
	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	// Create an NMEA codec using the AIS codec.
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	// If the aggregator option is set, create a UDP connection
	var udpConn *net.UDPConn
	if *aggregator != "" {
		// Validate the host and port from the provided aggregator string
		parts := strings.Split(*aggregator, ":")
		if len(parts) != 2 {
			log.Fatal("Invalid aggregator format. Expected host/ip:port")
		}

		host, port := parts[0], parts[1]

		// Parse the port as an integer
		udpPort, err := strconv.Atoi(port)
		if err != nil {
			log.Fatalf("Invalid port number: %v", err)
		}

		// Resolve the UDP address
		udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, udpPort))
		if err != nil {
			log.Fatalf("Failed to resolve UDP address: %v", err)
		}

		// Create a UDP connection
		udpConn, err = net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			log.Fatalf("Failed to create UDP connection: %v", err)
		}
		defer udpConn.Close()
		log.Printf("[DEBUG] Connected to aggregator at %s", udpAddr.String())
	}

	// Separate deduplication windows: one for WebSocket and one for the aggregator
	var websocketDedupeWindow []dedupeState
	var aggregatorDedupeWindow []dedupeState
	windowDuration := time.Duration(*dedupeWindowDuration) * time.Millisecond

	// --- Start UDP listener for incoming NMEA data ---
	// Listen for incoming UDP messages on the specified port.
	udpAddr := fmt.Sprintf(":%d", *udpListenPort)
	udpListener, err := net.ListenPacket("udp", udpAddr)
	if err != nil {
		log.Fatalf("Error starting UDP listener: %v", err)
	}
	defer udpListener.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, addr, err := udpListener.ReadFrom(buf)
			if err != nil {
				log.Printf("Error reading UDP message: %v", err)
				continue
			}

			// Extract the raw NMEA sentence from the UDP packet
			rawNmea := string(buf[:n])
			currentTime := time.Now().Format(time.RFC3339) // Capture the timestamp
			source := addr.String()                        // Capture the source IP address of the UDP message

			// Debug log: Output the raw NMEA data, source, and timestamp
			if *debug {
				log.Printf("[DEBUG] Received from UDP (%s) at %s: %s", source, currentTime, rawNmea)
			}

			// Check deduplication for incoming data (both UDP and Serial)
			if *dedupeWindowDuration > 0 && isDuplicate(rawNmea, aggregatorDedupeWindow, windowDuration) {
				if *debug {
					log.Printf("[DEBUG] Dropped duplicate message from %s at %s: %s", source, currentTime, rawNmea)
				}
				continue
			}

			// Update the aggregator deduplication window
			aggregatorDedupeWindow = append(aggregatorDedupeWindow, dedupeState{message: rawNmea, timestamp: time.Now()})

			// Decode the sentence
			decoded, err := nmeaCodec.ParseSentence(rawNmea)
			if err != nil {
				log.Printf("Error decoding sentence: %v", err)
				continue
			}
			if decoded == nil || decoded.Packet == nil {
				continue
			}

			// Format the decoded AIS packet as prettified JSON.
			out, err := json.MarshalIndent(decoded.Packet, "", "  ")
			if err != nil {
				log.Printf("Error formatting AIS packet: %v", err)
				continue
			}
			// Determine a friendly type name.
			typeName := fmt.Sprintf("%T", decoded.Packet)
			typeName = strings.TrimPrefix(typeName, "*")
			// Create the final message payload.
			message := fmt.Sprintf("%s: %s", typeName, out)

			// Output the decoded message if -show-decodes is set.
			if *showDecodes {
				log.Println("Decoded AIS Packet:", message)
			}

			// Broadcast the decoded AIS data to all connected Socket.IO clients.
			clientsMutex.Lock()
			for _, client := range clients {
				go func(c *socket.Socket, msg string) {
					if err := c.Emit("ais_data", msg); err != nil {
						log.Printf("Error sending decoded AIS data to client %s: %v", c.Id(), err)
					}
				}(client, message)
			}
			clientsMutex.Unlock()

			// **Prevent duplicates from being sent to the aggregator**
			if udpConn != nil {
				// Check if the message has already been sent to the aggregator
				if !isDuplicate(rawNmea, aggregatorDedupeWindow, windowDuration) {
					log.Printf("[DEBUG] Sending message to aggregator: %s", rawNmea)
					_, err := udpConn.Write([]byte(rawNmea))
					if err != nil {
						log.Printf("Error sending raw NMEA sentence over UDP: %v", err)
					}
				} else {
					if *debug {
						log.Printf("[DEBUG] Dropped duplicate message to aggregator: %s", rawNmea)
					}
				}
			}
		}
	}()

	// --- Read from serial port line-by-line (if -serial-port is specified) ---
	if *serialPort != "" {
		// Only read from serial if the serial port is provided
		scanner := bufio.NewScanner(port)
		for scanner.Scan() {
			line := scanner.Text()
			currentTime := time.Now().Format(time.RFC3339) // Capture the timestamp
			source := "Serial"                             // Source for serial data

			// Debug log: Output the raw NMEA data and timestamp
			if *debug {
				log.Printf("[DEBUG] Received from Serial (%s) at %s: %s", source, currentTime, line)
			}

			// Process only lines that look like NMEA sentences.
			if len(line) == 0 || (line[0] != '!' && line[0] != '$') {
				continue
			}

			// Check deduplication for serial data
			if *dedupeWindowDuration > 0 && isDuplicate(line, websocketDedupeWindow, windowDuration) {
				if *debug {
					log.Printf("[DEBUG] Dropped duplicate serial message (%s) at %s: %s", source, currentTime, line)
				}
				continue
			}

			// Update the websocket deduplication window
			websocketDedupeWindow = append(websocketDedupeWindow, dedupeState{message: line, timestamp: time.Now()})

			// Decode the sentence
			decoded, err := nmeaCodec.ParseSentence(line)
			if err != nil {
				log.Printf("Error decoding sentence: %v", err)
				continue
			}
			if decoded == nil || decoded.Packet == nil {
				continue
			}

			// Format the decoded AIS packet as prettified JSON.
			out, err := json.MarshalIndent(decoded.Packet, "", "  ")
			if err != nil {
				log.Printf("Error formatting AIS packet: %v", err)
				continue
			}
			// Determine a friendly type name.
			typeName := fmt.Sprintf("%T", decoded.Packet)
			typeName = strings.TrimPrefix(typeName, "*")
			// Create the final message payload.
			message := fmt.Sprintf("%s: %s", typeName, out)

			// Output the decoded message if -show-decodes is set.
			if *showDecodes {
				log.Println("Decoded AIS Packet:", message)
			}

			// Broadcast the decoded AIS data to all connected Socket.IO clients.
			clientsMutex.Lock()
			for _, client := range clients {
				go func(c *socket.Socket, msg string) {
					if err := c.Emit("ais_data", msg); err != nil {
						log.Printf("Error sending decoded AIS data to client %s: %v", c.Id(), err)
					}
				}(client, message)
			}
			clientsMutex.Unlock()

			// **Prevent duplicates from being sent to the aggregator**
			if udpConn != nil {
				// Check if the message has already been sent to the aggregator
				if !isDuplicate(line, aggregatorDedupeWindow, windowDuration) {
					log.Printf("[DEBUG] Sending message to aggregator: %s", line)
					_, err := udpConn.Write([]byte(line))
					if err != nil {
						log.Printf("Error sending raw NMEA sentence over UDP: %v", err)
					}
				} else {
					if *debug {
						log.Printf("[DEBUG] Dropped duplicate message to aggregator: %s", line)
					}
				}
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading from serial port: %v", err)
		}
	}

	// Wait forever.
	select {}
}
