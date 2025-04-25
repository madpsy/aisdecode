// mqttproxy.go
package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync"
    "syscall"

    mqtt "github.com/eclipse/paho.mqtt.golang"
    "github.com/zishang520/engine.io/v2/config"
    engine "github.com/zishang520/engine.io/v2/engine"
    "github.com/zishang520/engine.io/v2/types"
    "github.com/zishang520/engine.io/v2/utils"
    socketio "github.com/zishang520/socket.io/v2/socket"
)

type LocalSettings struct {
    IngestHost       string `json:"ingester_host"`
    IngestPort       int    `json:"ingester_port"`
    ListenPort       int    `json:"listen_port"`
    Debug            bool   `json:"debug"`
}

type IngesterSettings struct {
    MQTTServer      string `json:"mqtt_server"`
    MQTTTLS         bool   `json:"mqtt_tls"`
    MQTTAuth        string `json:"mqtt_auth"`
    MQTTTopicPrefix string `json:"mqtt_topic"`
}

var (
    localDebug bool

    mqttClient mqtt.Client
    ioServer   *socketio.Server

    connectedClients   = make(map[socketio.SocketId]*socketio.Socket)
    connectedClientsMu sync.RWMutex

    userSubscribers   = make(map[string]map[socketio.SocketId]struct{})
    userSubscribersMu sync.RWMutex

    clientSubscriptions   = make(map[socketio.SocketId]map[string]struct{})
    clientSubscriptionsMu sync.RWMutex
)

func logDebug(format string, args ...interface{}) {
    if localDebug {
        log.Printf("DEBUG › "+format, args...)
    }
}

func loadLocalSettings(path string) (*LocalSettings, error) {
    b, err := ioutil.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var s LocalSettings
    if err := json.Unmarshal(b, &s); err != nil {
        return nil, err
    }
    return &s, nil
}

func fetchIngesterSettings(host string, port int) (*IngesterSettings, error) {
    url := fmt.Sprintf("http://%s:%d/settings", host, port)
    logDebug("Fetching ingester settings from %s", url)
    resp, err := http.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var s IngesterSettings
    if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
        return nil, err
    }
    logDebug("Got ingester settings: %+v", s)
    return &s, nil
}

func initMQTT(cfg *IngesterSettings) {
    opts := mqtt.NewClientOptions()
    scheme := "tcp://"
    if cfg.MQTTTLS {
        scheme = "ssl://"
    }
    brokerURL := scheme + cfg.MQTTServer
    opts.AddBroker(brokerURL).SetClientID("socketio-mqtt-bridge")

    if cfg.MQTTAuth != "" {
        parts := strings.SplitN(cfg.MQTTAuth, ":", 2)
        if len(parts) == 2 {
            opts.SetUsername(parts[0]).SetPassword(parts[1])
            logDebug("Configured MQTT auth user=%s", parts[0])
        }
    }

    opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
        parts := strings.Split(msg.Topic(), "/")
        if len(parts) < 3 {
            return
        }
        userID := parts[2]

        userSubscribersMu.RLock()
        subs := userSubscribers[userID]
        userSubscribersMu.RUnlock()
        if len(subs) == 0 {
            return
        }

        connectedClientsMu.RLock()
        for sid := range subs {
            if sock, ok := connectedClients[sid]; ok {
                sock.Emit("ais_data", map[string]interface{}{
                    "userID":  userID,
                    "message": string(msg.Payload()),
                })
            }
        }
        connectedClientsMu.RUnlock()
    })

    mqttClient = mqtt.NewClient(opts)
    if tok := mqttClient.Connect(); tok.Wait() && tok.Error() != nil {
        log.Fatalf("MQTT connect error: %v", tok.Error())
    }
    logDebug("Connected to MQTT %s", brokerURL)
}

func setupSocketIO(prefix string) {
    log.Printf("⚓️ setupSocketIO: waiting for connections…")
    ioServer.On("connection", func(args ...any) {
	log.Printf("⚓️ Socket.IO: connection callback fired")
        client := args[0].(*socketio.Socket)
        sid := client.Id()

        connectedClientsMu.Lock()
        connectedClients[sid] = client
        connectedClientsMu.Unlock()

        client.On("ais_data/:userID", func(raw ...any) {
            userID := raw[0].(string)

            clientSubscriptionsMu.Lock()
            if clientSubscriptions[sid] == nil {
                clientSubscriptions[sid] = make(map[string]struct{})
            }
            _, already := clientSubscriptions[sid][userID]
            clientSubscriptions[sid][userID] = struct{}{}
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            subs := userSubscribers[userID]
            if subs == nil {
                subs = make(map[socketio.SocketId]struct{})
                userSubscribers[userID] = subs
            }
            subs[sid] = struct{}{}
            if !already {
                topic := fmt.Sprintf("%s/#/%s/#/message", prefix, userID)
                mqttClient.Subscribe(topic, 0, nil)
            }
            userSubscribersMu.Unlock()
        })

        client.On("ais_unsub/:userID", func(raw ...any) {
            userID := raw[0].(string)

            clientSubscriptionsMu.Lock()
            delete(clientSubscriptions[sid], userID)
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            if subs := userSubscribers[userID]; subs != nil {
                delete(subs, sid)
                if len(subs) == 0 {
                    mqttClient.Unsubscribe(fmt.Sprintf("%s/#/%s/#/message", prefix, userID))
                    delete(userSubscribers, userID)
                }
            }
            userSubscribersMu.Unlock()
        })

        client.On("disconnect", func(_ ...any) {
            connectedClientsMu.Lock()
            delete(connectedClients, sid)
            connectedClientsMu.Unlock()

            clientSubscriptionsMu.Lock()
            topics := clientSubscriptions[sid]
            delete(clientSubscriptions, sid)
            clientSubscriptionsMu.Unlock()

            userSubscribersMu.Lock()
            for userID := range topics {
                if subs := userSubscribers[userID]; subs != nil {
                    delete(subs, sid)
                    if len(subs) == 0 {
                        mqttClient.Unsubscribe(fmt.Sprintf("%s/#/%s/#/message", prefix, userID))
                        delete(userSubscribers, userID)
                    }
                }
            }
            userSubscribersMu.Unlock()
        })
    })
}

func main() {
    local, err := loadLocalSettings("settings.json")
    if err != nil {
        log.Fatalf("Error loading settings.json: %v", err)
    }
    localDebug = local.Debug

    ing, err := fetchIngesterSettings(local.IngestHost, local.IngestPort)
    if err != nil {
        log.Fatalf("Error fetching ingester settings: %v", err)
    }

    initMQTT(ing)

    // Engine.IO + CORS
    serverOpts := &config.ServerOptions{}
    serverOpts.SetAllowEIO3(true)
    serverOpts.SetCors(&types.Cors{Origin: "*", Credentials: true})

    // 1) HTTP server for Engine.IO & Socket.IO
    httpServer := types.NewWebServer(nil)
    engine.Attach(httpServer, serverOpts)

    // CORS mux wrapper
    mux := http.NewServeMux()
    mux.HandleFunc("/socket.io/", func(w http.ResponseWriter, r *http.Request) {
	log.Printf("Incoming Socket.IO request: %s %s", r.Method, r.URL)
        w.Header().Set("Access-Control-Allow-Origin", "*")
        if r.Method == http.MethodOptions {
            w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
            w.WriteHeader(http.StatusNoContent)
            return
        }
        httpServer.ServeHTTP(w, r)
    })

    // 2) Socket.IO on the same HTTP server
    ioServer = socketio.NewServer(httpServer, nil)
    setupSocketIO(ing.MQTTTopicPrefix)

    addr := fmt.Sprintf(":%d", local.ListenPort)
    go func() {
        log.Printf("Listening on %s", addr)
        if err := http.ListenAndServe(addr, mux); err != nil {
            log.Fatalf("Listen error: %v", err)
        }
    }()

    // Graceful shutdown
    sigC := make(chan os.Signal, 1)
    signal.Notify(sigC, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
    <-sigC
    utils.Log().Println("Shutting down…")
}
