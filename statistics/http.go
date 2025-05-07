package main

import (
    "fmt"
    "log"
    "net/http"
)

// StartServer sets up HTTP handlers and begins listening.
func StartServer(port int) {
    mux := http.NewServeMux()
    registerHandlers(mux)

    addr := fmt.Sprintf(":%d", port)
    log.Printf("Listening on %s...", addr)
    if err := http.ListenAndServe(addr, mux); err != nil {
        log.Fatalf("HTTP server error: %v", err)
    }
}

// registerHandlers attaches your routes to the mux.
func registerHandlers(mux *http.ServeMux) {
    // Serve static files from ./web
    mux.Handle("/", http.FileServer(http.Dir("web")))

    // TODO: Add your other routes here, e.g.:
    // mux.HandleFunc("/stats", statsHandler)
}
