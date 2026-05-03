package main

import (
	"bytes"
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

	gtfsrt "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/joho/godotenv"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	accessToken string
	expiry      time.Time
	mu          sync.Mutex
}

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in,string"`
}

// FeedEntry holds both raw and pre-compressed representations of a feed.
// Pre-compressing once at write time avoids redundant gzip work per request.
type FeedEntry struct {
	Data        []byte
	GzipData    []byte // pre-compressed; nil if compression failed
	LastUpdate  time.Time
	ETag        string
	ContentType string
	EntityCount int
}

type FeedCache struct {
	data map[string]*FeedEntry
	mu   sync.RWMutex
}

// feedSource describes a single upstream protobuf endpoint and the cache keys
// it populates. Each fetch produces two entries: one raw protobuf (.pb) and
// one JSON-decoded (.json), so we only hit the upstream once per feed type.
type feedSource struct {
	path    string // upstream path, e.g. "/tripupdates"
	pbKey   string // cache key for the protobuf entry
	jsonKey string // cache key for the JSON entry
}

var feedSources = []feedSource{
	{"/tripupdates", "tripupdates.pb", "tripupdates.json"},
	{"/alerts", "alerts.pb", "alerts.json"},
}

var (
	config     Config
	tokenCache TokenCache
	feedCache  = FeedCache{data: make(map[string]*FeedEntry)}

	// httpClient is shared across all upstream fetches so TCP/TLS connections
	// are reused between poll cycles.
	httpClient = &http.Client{Timeout: 10 * time.Second}

	// pjson marshals proto messages to JSON using proto field names (snake_case)
	// and omitting unpopulated fields for a clean output.
	pjson = protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
)

// allCacheKeys returns the full set of cache keys produced by feedSources,
// used for health-check completeness validation.
func allCacheKeys() []string {
	keys := make([]string, 0, len(feedSources)*2)
	for _, s := range feedSources {
		keys = append(keys, s.pbKey, s.jsonKey)
	}
	return keys
}

// maxFeedAge is the threshold beyond which a feed is considered stale for
// health-check purposes.
const maxFeedAge = 2 * time.Minute

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

func validateConfig(c Config) error {
	var missing []string
	for _, pair := range []struct{ name, val string }{
		{"TENANT_ID", c.TenantID},
		{"CLIENT_ID", c.ClientID},
		{"CLIENT_SECRET", c.ClientSecret},
		{"RESOURCE", c.Resource},
		{"HOSTNAME", c.Hostname},
	} {
		if pair.val == "" {
			missing = append(missing, pair.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return nil
}

func getEnv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

// getAccessToken returns a valid bearer token, refreshing via OAuth2 client
// credentials when the cached token is within 60s of expiry.
//
// The mutex is held only for the cache read/write, not across the HTTP call,
// so concurrent callers may briefly race to refresh — a harmless thundering
// herd for a low-cardinality poller like this. If stricter single-flight
// behaviour is needed, use golang.org/x/sync/singleflight.
func getAccessToken() (string, error) {
	tokenCache.mu.Lock()
	if tokenCache.accessToken != "" && time.Now().Before(tokenCache.expiry.Add(-60*time.Second)) {
		tok := tokenCache.accessToken
		tokenCache.mu.Unlock()
		return tok, nil
	}
	tokenCache.mu.Unlock()

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
		return "", fmt.Errorf("token request failed (%d): %s", resp.StatusCode, string(body))
	}

	var tr TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	tokenCache.mu.Lock()
	tokenCache.accessToken = tr.AccessToken
	tokenCache.expiry = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	tokenCache.mu.Unlock()

	return tr.AccessToken, nil
}

func fetchFeed(endpoint string) ([]byte, error) {
	token, err := getAccessToken()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, config.Hostname+endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upstream %s: status %d: %s", endpoint, resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// pbToJSON unmarshals a GTFS-RT FeedMessage from protobuf wire format and
// re-encodes it as JSON using protojson so field names and enum values match
// the official GTFS-RT JSON representation.
// Returns the JSON bytes and the number of entities in the message.
func pbToJSON(pbData []byte) ([]byte, int, error) {
	msg := &gtfsrt.FeedMessage{}
	if err := proto.Unmarshal(pbData, msg); err != nil {
		return nil, 0, fmt.Errorf("unmarshal protobuf: %w", err)
	}

	jsonData, err := pjson.Marshal(msg)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal to JSON: %w", err)
	}

	return jsonData, len(msg.Entity), nil
}

func computeETag(data []byte) string {
	hash := sha1.Sum(data)
	return `"` + hex.EncodeToString(hash[:]) + `"`
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	gz, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}

	if _, err := gz.Write(data); err != nil {
		gz.Close()
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func makeCacheEntry(data []byte, contentType string, entityCount int) *FeedEntry {
	entry := &FeedEntry{
		Data:        data,
		LastUpdate:  time.Now(),
		ETag:        computeETag(data),
		ContentType: contentType,
		EntityCount: entityCount,
	}
	if gz, err := compressGzip(data); err == nil {
		entry.GzipData = gz
	} else {
		log.Printf("gzip compress: %v", err)
	}
	return entry
}

func updateFeeds(ctx context.Context) {
	pollOnce()

	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollOnce()
		}
	}
}

func pollOnce() {
	var wg sync.WaitGroup

	for _, src := range feedSources {
		wg.Add(1)
		go func(s feedSource) {
			defer wg.Done()

			pbData, err := fetchFeed(s.path)
			if err != nil {
				log.Printf("poll error [%s]: %v", s.pbKey, err)
				return
			}

			jsonData, entityCount, err := pbToJSON(pbData)
			if err != nil {
				log.Printf("decode error [%s]: %v", s.pbKey, err)
				pbEntry := makeCacheEntry(pbData, "application/octet-stream", -1)
				feedCache.mu.Lock()
				feedCache.data[s.pbKey] = pbEntry
				feedCache.mu.Unlock()
				log.Printf("[%s] size=%dB gzip=%dB (protobuf only; JSON decode failed)",
					s.pbKey, len(pbData), len(pbEntry.GzipData))
				return
			}

			pbEntry := makeCacheEntry(pbData, "application/octet-stream", -1)
			jsonEntry := makeCacheEntry(jsonData, "application/json", entityCount)

			feedCache.mu.Lock()
			feedCache.data[s.pbKey] = pbEntry
			feedCache.data[s.jsonKey] = jsonEntry
			feedCache.mu.Unlock()

			log.Printf("[%s] size=%dB gzip=%dB (protobuf)", s.pbKey, len(pbData), len(pbEntry.GzipData))
			log.Printf("[%s] entities=%d size=%dB gzip=%dB (json)", s.jsonKey, entityCount, len(jsonData), len(jsonEntry.GzipData))
		}(src)
	}

	wg.Wait()
	log.Println("feeds updated")
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		token := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if strings.EqualFold(token, "gzip") {
			return true
		}
	}
	return false
}

func serveFeed(key string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		feedCache.mu.RLock()
		entry, ok := feedCache.data[key]
		feedCache.mu.RUnlock()

		if !ok {
			http.Error(w, "feed not yet available", http.StatusServiceUnavailable)
			return
		}

		if match := r.Header.Get("If-None-Match"); match != "" && match == entry.ETag {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		if ims := r.Header.Get("If-Modified-Since"); ims != "" {
			if t, err := http.ParseTime(ims); err == nil && !entry.LastUpdate.After(t) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}

		h := w.Header()
		h.Set("Content-Type", entry.ContentType)
		h.Set("ETag", entry.ETag)
		h.Set("Last-Modified", entry.LastUpdate.UTC().Format(http.TimeFormat))
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=5, stale-while-revalidate=%d",
			int(config.PollInterval.Seconds())-5))
		h.Set("Vary", "Accept-Encoding")

		if acceptsGzip(r) && entry.GzipData != nil {
			h.Set("Content-Encoding", "gzip")
			h.Set("Content-Length", strconv.Itoa(len(entry.GzipData)))
			w.WriteHeader(http.StatusOK)
			w.Write(entry.GzipData)
		} else {
			h.Set("Content-Length", strconv.Itoa(len(entry.Data)))
			w.WriteHeader(http.StatusOK)
			w.Write(entry.Data)
		}

		log.Printf("%s %s 200 %s", r.Method, r.URL.Path, time.Since(start))
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out := make(map[string]interface{})

	feedCache.mu.RLock()
	for k, v := range feedCache.data {
		out[k] = map[string]interface{}{
			"last_update": v.LastUpdate,
			"etag":        v.ETag,
			"size_bytes":  len(v.Data),
			"gzip_bytes":  len(v.GzipData),
			"entities":    v.EntityCount,
			"stale":       time.Since(v.LastUpdate) > maxFeedAge,
		}
	}
	feedCache.mu.RUnlock()

	json.NewEncoder(w).Encode(out)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	feedCache.mu.RLock()
	defer feedCache.mu.RUnlock()

	for _, k := range allCacheKeys() {
		v, ok := feedCache.data[k]
		if !ok {
			http.Error(w, fmt.Sprintf("not ready: feed %q not yet populated", k), http.StatusServiceUnavailable)
			return
		}
		if time.Since(v.LastUpdate) > maxFeedAge {
			http.Error(w, fmt.Sprintf("not ready: feed %q is stale", k), http.StatusServiceUnavailable)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	config = loadConfig()
	if err := validateConfig(config); err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go updateFeeds(ctx)

	mux := http.NewServeMux()

	for _, src := range feedSources {
		pbKey := src.pbKey
		jsonKey := src.jsonKey
		mux.HandleFunc("/"+pbKey, serveFeed(pbKey))
		mux.HandleFunc("/"+jsonKey, serveFeed(jsonKey))
	}

	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/healthz", healthHandler)

	server := &http.Server{
		Addr:         ":" + config.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("server running on :%s", config.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
