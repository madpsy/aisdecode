package main

import (
    "bufio"
    "flag"
    "io"
    "log"
    "net"
    "strings"
    "time"

    "go.bug.st/serial"
)

func main() {
    // Command-line flags
    serialPort := flag.String("serial-port", "/dev/ttyUSB0", "Serial port device")
    baud := flag.Int("baud", 38400, "Baud rate")
    udpAddrs := flag.String("udp", "127.0.0.1:8101", "Comma-separated UDP destinations")
    debug := flag.Bool("debug", false, "Enable debug logging of forwarded data")
    flag.Parse()

    // Open serial port
    mode := &serial.Mode{BaudRate: *baud}
    port, err := serial.Open(*serialPort, mode)
    if err != nil {
        log.Fatalf("Failed to open %s: %v", *serialPort, err)
    }
    defer port.Close()
    log.Printf("Listening on serial %s @ %d baud", *serialPort, *baud)

    // Parse UDP destinations
    dests := splitAndTrim(*udpAddrs, ",")
    conns := make([]*net.UDPConn, 0, len(dests))
    for _, d := range dests {
        udpAddr, err := net.ResolveUDPAddr("udp", d)
        if err != nil {
            log.Fatalf("Invalid UDP address %q: %v", d, err)
        }
        conn, err := net.DialUDP("udp", nil, udpAddr)
        if err != nil {
            log.Fatalf("Failed to dial %s: %v", udpAddr, err)
        }
        conns = append(conns, conn)
        log.Printf("Forwarding to %s", udpAddr)
    }

    reader := bufio.NewReader(port)
    for {
        // ReadBytes includes the '\n' (and preserves '\r' if present)
        frame, err := reader.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                time.Sleep(10 * time.Millisecond)
                continue
            }
            log.Fatalf("Serial read error: %v", err)
        }

        if *debug {
            log.Printf("Forwarding raw frame: %q", frame)
        }

        for _, conn := range conns {
            sendWithRetry(conn, frame)
        }
    }
}

// sendWithRetry writes data, retrying once on error.
func sendWithRetry(conn *net.UDPConn, data []byte) {
    if _, err := conn.Write(data); err != nil {
        log.Printf("Write error to %s: %v — retrying", conn.RemoteAddr(), err)
        time.Sleep(100 * time.Millisecond)
        if _, err2 := conn.Write(data); err2 != nil {
            log.Printf("Retry failed to %s: %v", conn.RemoteAddr(), err2)
        }
    }
}

// splitAndTrim splits s by sep and trims whitespace.
func splitAndTrim(s, sep string) []string {
    parts := strings.Split(s, sep)
    out := make([]string, 0, len(parts))
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p != "" {
            out = append(out, p)
        }
    }
    return out
}
