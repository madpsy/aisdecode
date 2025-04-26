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
    // CLI flags
    serialPort := flag.String("serial-port", "/dev/ttyUSB0", "Serial port device")
    baud := flag.Int("baud", 38400, "Baud rate")
    udpAddrs := flag.String("udp", "127.0.0.1:8101", "Comma-separated UDP destinations")
    debug := flag.Bool("debug", false, "Enable debug logging of forwarded data")
    flag.Parse()

    // Open serial
    mode := &serial.Mode{BaudRate: *baud}
    port, err := serial.Open(*serialPort, mode)
    if err != nil {
        log.Fatalf("Failed to open %s: %v", *serialPort, err)
    }
    defer port.Close()
    log.Printf("Listening on %s @ %d baud", *serialPort, *baud)

    // Setup UDP conns
    dests := splitAndTrim(*udpAddrs, ",")
    conns := make([]*net.UDPConn, len(dests))
    for i, d := range dests {
        addr, err := net.ResolveUDPAddr("udp", d)
        if err != nil {
            log.Fatalf("Invalid UDP addr %q: %v", d, err)
        }
        c, err := net.DialUDP("udp", nil, addr)
        if err != nil {
            log.Fatalf("Dial %s: %v", addr, err)
        }
        conns[i] = c
        log.Printf("Forwarding to %s", addr)
    }

    reader := bufio.NewReader(port)
    for {
        frame, err := reader.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                time.Sleep(10 * time.Millisecond)
                continue
            }
            log.Fatalf("Serial read error: %v", err)
        }
        if *debug {
            log.Printf("Forwarding: %q", frame)
        }
        for _, c := range conns {
            c.Write(frame) // no retry
        }
    }
}

// splitAndTrim splits and trims.
func splitAndTrim(s, sep string) []string {
    parts := strings.Split(s, sep)
    out := parts[:0]
    for _, p := range parts {
        if t := strings.TrimSpace(p); t != "" {
            out = append(out, t)
        }
    }
    return out
}
