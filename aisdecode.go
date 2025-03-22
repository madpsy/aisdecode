package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

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

func main() {
	// Command-line flags.
	serialPort := flag.String("serial-port", "/dev/ttyUSB0", "Serial port device (default: /dev/ttyUSB0)")
	baud := flag.Int("baud", 38400, "Baud rate (default: 38400)")
	wsPort := flag.Int("ws-port", 8100, "WebSocket port (default: 8100)")
	webRoot := flag.String("web-root", ".", "Web root directory (default: current directory)")
	debug := flag.Bool("debug", false, "Enable debug output")
	showDecodes := flag.Bool("show-decodes", false, "Output the decoded messages")
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
	// Open the serial port.
	mode := &serial.Mode{BaudRate: *baud}
	port, err := serial.Open(*serialPort, mode)
	if err != nil {
		log.Fatalf("failed to open serial port: %v", err)
	}
	defer port.Close()

	// Create an AIS codec.
	codec := ais.CodecNew(false, false)
	codec.DropSpace = true
	// Create an NMEA codec using the AIS codec.
	nmeaCodec := aisnmea.NMEACodecNew(codec)

	// Read from the serial port line-by-line.
	scanner := bufio.NewScanner(port)
	for scanner.Scan() {
		line := scanner.Text()
		if *debug {
			log.Printf("Raw line: %s", line)
		}
		// Process only lines that look like NMEA sentences.
		if len(line) == 0 || (line[0] != '!' && line[0] != '$') {
			continue
		}

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

		// Broadcast the AIS data to all connected Socket.IO clients.
		clientsMutex.Lock()
		for _, client := range clients {
			go func(c *socket.Socket, msg string) {
				if err := c.Emit("ais_data", msg); err != nil {
					log.Printf("Error sending AIS data to client %s: %v", c.Id(), err)
				}
			}(client, message)
		}
		clientsMutex.Unlock()
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from serial port: %v", err)
	}

	// Wait forever.
	select {}
}
