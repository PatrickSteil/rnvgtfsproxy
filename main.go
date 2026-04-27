package main

import (
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	TenantID     string
	ClientID     string
	ClientSecret string
	Resource     string
	Hostname     string
	PollInterval time.Duration
	Port         string
}

type TokenCache struct {
	AccessToken string
	Expiry      time.Time
	mu          sync.Mutex
}

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in,string"`
}

type FeedEntry struct {
	Data        []byte
	LastUpdate  time.Time
	ETag        string
	ContentType string
	EntityCount int
}

type FeedCache struct {
	data map[string]*FeedEntry
	mu   sync.RWMutex
}

var config Config
var tokenCache TokenCache
var feedCache = FeedCache{data: make(map[string]*FeedEntry)}

var endpoints = map[string]struct {
	Path        string
	ContentType string
}{
	"tripupdates.pb":   {"/tripupdates", "application/octet-stream"},
	"alerts.pb":        {"/alerts", "application/octet-stream"},
	"tripupdates.json": {"/tripupdates/decoded", "application/json"},
	"alerts.json":      {"/alerts/decoded", "application/json"},
}

func loadConfig() Config {
	_ = godotenv.Load()

	pollStr := getEnv("POLL_INTERVAL", "30")
	poll, err := strconv.Atoi(pollStr)
	if err != nil {
		log.Printf("invalid POLL_INTERVAL %q, using 30s: %v", pollStr, err)
		poll = 30
	}

	return Config{
		TenantID:     os.Getenv("TENANT_ID"),
		ClientID:     os.Getenv("CLIENT_ID"),
		ClientSecret: os.Getenv("CLIENT_SECRET"),
		Resource:     os.Getenv("RESOURCE"),
		Hostname:     os.Getenv("HOSTNAME"),
		PollInterval: time.Duration(poll) * time.Second,
		Port:         getEnv("PORT", "8000"),
	}
}

func getEnv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

func extractEntityCountJSON(data []byte) int {
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return -1
	}
	entities, ok := parsed["entity"].([]interface{})
	if !ok {
		return -1
	}
	return len(entities)
}

func getAccessToken() (string, error) {
	tokenCache.mu.Lock()
	defer tokenCache.mu.Unlock()

	if tokenCache.AccessToken != "" && time.Now().Before(tokenCache.Expiry.Add(-60*time.Second)) {
		return tokenCache.AccessToken, nil
	}

	url := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/token", config.TenantID)
	resp, err := http.PostForm(url, map[string][]string{
		"grant_type":    {"client_credentials"},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
		"resource":      {config.Resource},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request failed: %s", string(body))
	}

	var tr TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", err
	}

	tokenCache.AccessToken = tr.AccessToken
	tokenCache.Expiry = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return tokenCache.AccessToken, nil
}

func fetch(endpoint string) ([]byte, error) {
	token, err := getAccessToken()
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("GET", config.Hostname+endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func computeETag(data []byte) string {
	hash := sha1.Sum(data)
	return hex.EncodeToString(hash[:])
}

func updateFeeds(ctx context.Context) {
	for {
		var wg sync.WaitGroup

		for key, meta := range endpoints {
			wg.Add(1)
			go func(k string, m struct {
				Path        string
				ContentType string
			}) {
				defer wg.Done()

				data, err := fetch(m.Path)
				if err != nil {
					log.Printf("poll error [%s]: %v", k, err)
					return
				}

				entry := &FeedEntry{
					Data:        data,
					LastUpdate:  time.Now(),
					ETag:        computeETag(data),
					ContentType: m.ContentType,
				}

				if m.ContentType == "application/json" {
					entry.EntityCount = extractEntityCountJSON(data)
				} else {
					entry.EntityCount = -1
				}

				feedCache.mu.Lock()
				feedCache.data[k] = entry
				feedCache.mu.Unlock()

				if entry.EntityCount >= 0 {
					log.Printf("[%s] entities=%d size=%dB", k, entry.EntityCount, len(data))
				} else {
					log.Printf("[%s] size=%dB (protobuf)", k, len(data))
				}
			}(key, meta)
		}

		wg.Wait()
		log.Println("feeds updated")

		select {
		case <-ctx.Done():
			return
		case <-time.After(config.PollInterval):
		}
	}
}

func serveFeed(key string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		feedCache.mu.RLock()
		entry, ok := feedCache.data[key]
		feedCache.mu.RUnlock()

		if !ok {
			http.Error(w, "No data yet", http.StatusServiceUnavailable)
			return
		}

		if match := r.Header.Get("If-None-Match"); match != "" && match == entry.ETag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set("Content-Type", entry.ContentType)
		w.Header().Set("ETag", entry.ETag)
		w.Header().Set("Last-Modified", entry.LastUpdate.UTC().Format(http.TimeFormat))
		w.Header().Set("Cache-Control", "public, max-age=5")

		if acceptsGzip(r) {
			w.Header().Set("Content-Encoding", "gzip")
			if err := writeGzip(w, entry.Data); err != nil {
				http.Error(w, "gzip error", http.StatusInternalServerError)
				return
			}
		} else {
			w.Write(entry.Data)
		}

		log.Printf("%s %s %d %s", r.Method, r.URL.Path, http.StatusOK, time.Since(start))
	}
}

func acceptsGzip(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
}

var gzipPool = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}

func writeGzip(w http.ResponseWriter, data []byte) error {
	gz := gzipPool.Get().(*gzip.Writer)
	defer gzipPool.Put(gz)
	gz.Reset(w)
	_, err := gz.Write(data)
	return errors.Join(err, gz.Close())
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out := make(map[string]interface{})

	feedCache.mu.RLock()
	for k, v := range feedCache.data {
		out[k] = map[string]interface{}{
			"last_update": v.LastUpdate,
			"etag":        v.ETag,
			"size":        len(v.Data),
			"entities":    v.EntityCount,
		}
	}
	feedCache.mu.RUnlock()

	json.NewEncoder(w).Encode(out)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	feedCache.mu.RLock()
	defer feedCache.mu.RUnlock()

	if len(feedCache.data) == 0 {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok"))
}

func main() {
	config = loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go updateFeeds(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/tripupdates.pb", serveFeed("tripupdates.pb"))
	mux.HandleFunc("/alerts.pb", serveFeed("alerts.pb"))
	mux.HandleFunc("/tripupdates.json", serveFeed("tripupdates.json"))
	mux.HandleFunc("/alerts.json", serveFeed("alerts.json"))
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/healthz", healthHandler)

	server := &http.Server{
		Addr:    ":" + config.Port,
		Handler: mux,
	}

	go func() {
		log.Printf("server running on :%s\n", config.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	server.Shutdown(ctxShutdown)
}
