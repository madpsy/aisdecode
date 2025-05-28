package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// ─────────────────────────────────────────────────────────────────────────────
// Settings & Data Models
// ─────────────────────────────────────────────────────────────────────────────

type Settings struct {
	ListenPort        int    `json:"listen_port"`
	RedisHost         string `json:"redis_host"`
	RedisPort         int    `json:"redis_port"`
	TileTTL           int    `json:"tile_ttl"`           // seconds
	OpenWeatherMapKey string `json:"openweathermap_key"` // API key for OpenWeatherMap
	BaseDomain        string `json:"base_domain"`        // Base domain for request validation
	Debug             bool   `json:"debug"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Globals
// ─────────────────────────────────────────────────────────────────────────────

var (
	settings    Settings
	redisClient *redis.Client
)

// ─────────────────────────────────────────────────────────────────────────────
// Entry Point
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	// Load settings
	data, err := ioutil.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Error reading settings.json: %v", err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Error parsing settings.json: %v", err)
	}

	// Validate OpenWeatherMap API key
	if settings.OpenWeatherMapKey == "" || settings.OpenWeatherMapKey == "your_api_key_here" {
		log.Fatalf("OpenWeatherMap API key not configured in settings.json")
	}

	// Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", settings.RedisHost, settings.RedisPort),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Error connecting to Redis: %v", err)
	}

	// Create cache directory if it doesn't exist
	os.MkdirAll("cache", 0755)

	// HTTP handlers
	http.HandleFunc("/weather/tile/", handleWeatherTile)
	http.HandleFunc("/weather/summary", handleWeatherSummary)

	// Only serve static files if web directory exists
	if _, err := os.Stat("web"); !os.IsNotExist(err) {
		http.Handle("/weather/", http.StripPrefix("/weather/", http.FileServer(http.Dir("web"))))
	}

	addr := fmt.Sprintf(":%d", settings.ListenPort)
	log.Printf("Weather service listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// ─────────────────────────────────────────────────────────────────────────────
// Domain Validation
// ─────────────────────────────────────────────────────────────────────────────

// validateDomain checks if the request is coming from the allowed domain
func validateDomain(r *http.Request) bool {
	// If no base domain is configured, allow all requests
	if settings.BaseDomain == "" {
		return true
	}

	// Get all relevant headers
	referer := r.Header.Get("Referer")
	origin := r.Header.Get("Origin")
	host := r.Host
	forwardedHost := r.Header.Get("X-Forwarded-Host")
	userAgent := r.Header.Get("User-Agent")

	// Better browser detection - check User-Agent for common browser strings
	// This helps catch browser requests that don't have Referer/Origin headers
	isBrowserUserAgent := strings.Contains(userAgent, "Mozilla/") ||
		strings.Contains(userAgent, "Chrome/") ||
		strings.Contains(userAgent, "Safari/") ||
		strings.Contains(userAgent, "Firefox/") ||
		strings.Contains(userAgent, "Edge/") ||
		strings.Contains(userAgent, "Opera/")

	// Check if this looks like a browser request (has browser User-Agent OR has Referer/Origin)
	isBrowserRequest := isBrowserUserAgent || referer != "" || origin != ""

	if settings.Debug {
		log.Printf("Request from: %s, User-Agent: %s, isBrowser: %v",
			r.RemoteAddr, userAgent, isBrowserRequest)
	}

	// For browser requests, check for valid Referer or Origin
	if isBrowserRequest {
		// If there's a Referer header, it must match our domain
		if referer != "" {
			if settings.Debug {
				log.Printf("Checking Referer header: %s", referer)
			}
			refURL, err := url.Parse(referer)
			if err == nil && strings.HasSuffix(refURL.Host, settings.BaseDomain) {
				if settings.Debug {
					log.Printf("Request allowed: Referer matches base domain")
				}
				return true
			}
		}

		// If there's an Origin header, it must match our domain
		if origin != "" {
			if settings.Debug {
				log.Printf("Checking Origin header: %s", origin)
			}
			originURL, err := url.Parse(origin)
			if err == nil && strings.HasSuffix(originURL.Host, settings.BaseDomain) {
				if settings.Debug {
					log.Printf("Request allowed: Origin matches base domain")
				}
				return true
			}
		}

		// For browser requests without Referer/Origin, check if Host matches our domain
		// This handles browser image/resource requests that don't send Referer
		if strings.HasSuffix(host, settings.BaseDomain) {
			if settings.Debug {
				log.Printf("Browser request allowed: Host matches base domain")
			}
			return true
		}

		// Check X-Forwarded-Host as well for proxied requests
		if forwardedHost != "" && strings.HasSuffix(forwardedHost, settings.BaseDomain) {
			if settings.Debug {
				log.Printf("Browser request allowed: X-Forwarded-Host matches base domain")
			}
			return true
		}

		// If we get here, the browser request is not from our domain
		if settings.Debug {
			log.Printf("Browser request rejected: No headers match base domain")
		}
		return false
	} else {
		// For non-browser requests (like curl), ALWAYS reject
		// This blocks all curl and similar tool requests
		if settings.Debug {
			log.Printf("Non-browser request rejected from: %s", r.RemoteAddr)
		}
		return false
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Weather Tile Handler
// ─────────────────────────────────────────────────────────────────────────────

func handleWeatherTile(w http.ResponseWriter, r *http.Request) {
	// Validate request domain
	if !validateDomain(r) {
		http.Error(w, "Unauthorized domain", http.StatusForbidden)
		if settings.Debug {
			log.Printf("Rejected request from unauthorized domain: %s", r.Host)
		}
		return
	}

	// Extract tile parameters from URL path
	// Expected formats:
	// - /weather/tile/{layer}/{z}/{x}/{y}
	// - /tile/{layer}/{z}/{x}/{y}
	parts := strings.Split(r.URL.Path, "/")

	// Filter out empty parts (from leading slash)
	var filteredParts []string
	for _, part := range parts {
		if part != "" {
			filteredParts = append(filteredParts, part)
		}
	}

	if settings.Debug {
		log.Printf("Path parts: %v", filteredParts)
	}

	// Check if we have enough parts for a valid tile request
	if len(filteredParts) < 4 {
		http.Error(w, "Invalid tile request format", http.StatusBadRequest)
		if settings.Debug {
			log.Printf("Invalid tile request: %s (not enough path parts)", r.URL.Path)
		}
		return
	}

	// Find the position of "tile" in the path
	tileIndex := -1
	for i, part := range filteredParts {
		if part == "tile" {
			tileIndex = i
			break
		}
	}

	if tileIndex == -1 || tileIndex+3 >= len(filteredParts) {
		http.Error(w, "Invalid tile request format", http.StatusBadRequest)
		if settings.Debug {
			log.Printf("Invalid tile request: %s (missing 'tile' part or incomplete path)", r.URL.Path)
		}
		return
	}

	// Extract parameters based on the position of "tile"
	layer := filteredParts[tileIndex+1]
	z := filteredParts[tileIndex+2]
	x := filteredParts[tileIndex+3]
	y := filteredParts[tileIndex+4]

	if settings.Debug {
		log.Printf("Tile request: layer=%s, z=%s, x=%s, y=%s", layer, z, x, y)
	}

	// Validate layer type
	validLayers := map[string]bool{
		"precipitation": true,
		"clouds":        true,
		"pressure":      true,
		"wind":          true,
		"temp":          true,
	}

	if !validLayers[layer] {
		http.Error(w, "Invalid layer type", http.StatusBadRequest)
		return
	}

	// Create cache key
	cacheKey := fmt.Sprintf("weather:tile:%s:%s:%s:%s", layer, z, x, y)

	// Try to get from Redis cache
	cachedTile, err := redisClient.Get(ctx, cacheKey).Bytes()
	if err == nil {
		// Cache hit
		if settings.Debug {
			log.Printf("Cache hit for %s", cacheKey)
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", settings.TileTTL))
		w.Write(cachedTile)
		return
	}

	// Cache miss, fetch from OpenWeatherMap
	if settings.Debug {
		log.Printf("Cache miss for %s, fetching from OpenWeatherMap", cacheKey)
	}

	// Construct OpenWeatherMap URL
	owmURL := fmt.Sprintf(
		"https://tile.openweathermap.org/map/%s/%s/%s/%s.png?appid=%s",
		layer, z, x, y, settings.OpenWeatherMapKey,
	)

	// Fetch tile from OpenWeatherMap
	resp, err := http.Get(owmURL)
	if err != nil {
		log.Printf("Error fetching tile from OpenWeatherMap: %v", err)
		http.Error(w, "Error fetching weather tile", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("OpenWeatherMap returned status %d", resp.StatusCode)
		http.Error(w, fmt.Sprintf("OpenWeatherMap error: %s", resp.Status), resp.StatusCode)
		return
	}

	// Read the tile data
	tileData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading tile data: %v", err)
		http.Error(w, "Error reading weather tile", http.StatusInternalServerError)
		return
	}

	// Store in Redis cache with TTL
	err = redisClient.Set(ctx, cacheKey, tileData, time.Duration(settings.TileTTL)*time.Second).Err()
	if err != nil {
		log.Printf("Error caching tile in Redis: %v", err)
	}

	// Serve the tile
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", settings.TileTTL))
	w.Write(tileData)
}

// ─────────────────────────────────────────────────────────────────────────────
// Weather Summary Handler
// ─────────────────────────────────────────────────────────────────────────────

func handleWeatherSummary(w http.ResponseWriter, r *http.Request) {
	// Validate request domain
	if !validateDomain(r) {
		http.Error(w, "Unauthorized domain", http.StatusForbidden)
		if settings.Debug {
			log.Printf("Rejected request from unauthorized domain: %s", r.Host)
		}
		return
	}

	// Parse query parameters
	query := r.URL.Query()
	latStr := query.Get("lat")
	lonStr := query.Get("lon")

	if latStr == "" || lonStr == "" {
		http.Error(w, "Missing required parameters: lat and lon", http.StatusBadRequest)
		return
	}

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		http.Error(w, "Invalid latitude value", http.StatusBadRequest)
		return
	}

	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		http.Error(w, "Invalid longitude value", http.StatusBadRequest)
		return
	}

	// Create cache key
	cacheKey := fmt.Sprintf("weather:summary:%f:%f", lat, lon)

	// Try to get from Redis cache
	cachedData, err := redisClient.Get(ctx, cacheKey).Bytes()
	if err == nil {
		// Cache hit
		if settings.Debug {
			log.Printf("Cache hit for %s", cacheKey)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", settings.TileTTL))
		w.Write(cachedData)
		return
	}

	// Cache miss, fetch from OpenWeatherMap
	if settings.Debug {
		log.Printf("Cache miss for %s, fetching from OpenWeatherMap", cacheKey)
	}

	// Construct OpenWeatherMap URL for current weather
	owmURL := fmt.Sprintf(
		"https://api.openweathermap.org/data/2.5/weather?lat=%f&lon=%f&units=metric&appid=%s",
		lat, lon, settings.OpenWeatherMapKey,
	)

	// Fetch weather data from OpenWeatherMap
	resp, err := http.Get(owmURL)
	if err != nil {
		log.Printf("Error fetching weather data from OpenWeatherMap: %v", err)
		http.Error(w, "Error fetching weather data", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("OpenWeatherMap returned status %d", resp.StatusCode)
		http.Error(w, fmt.Sprintf("OpenWeatherMap error: %s", resp.Status), resp.StatusCode)
		return
	}

	// Read the weather data
	weatherData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading weather data: %v", err)
		http.Error(w, "Error reading weather data", http.StatusInternalServerError)
		return
	}

	// Parse the JSON to validate and potentially enhance it
	var weatherJson map[string]interface{}
	if err := json.Unmarshal(weatherData, &weatherJson); err != nil {
		log.Printf("Error parsing weather data: %v", err)
		http.Error(w, "Error processing weather data", http.StatusInternalServerError)
		return
	}

	// Add a timestamp field
	weatherJson["timestamp"] = time.Now().Format(time.RFC3339)

	// Re-encode the enhanced data
	enhancedData, err := json.Marshal(weatherJson)
	if err != nil {
		log.Printf("Error encoding enhanced weather data: %v", err)
		http.Error(w, "Error processing weather data", http.StatusInternalServerError)
		return
	}

	// Store in Redis cache with TTL
	err = redisClient.Set(ctx, cacheKey, enhancedData, time.Duration(settings.TileTTL)*time.Second).Err()
	if err != nil {
		log.Printf("Error caching weather data in Redis: %v", err)
	}

	// Serve the weather data
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", settings.TileTTL))
	w.Write(enhancedData)
}
