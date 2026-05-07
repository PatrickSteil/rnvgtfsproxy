package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
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
	TenantID       string
	ClientID       string
	ClientSecret   string
	Resource       string
	Hostname       string
	PollInterval   time.Duration
	Port           string
	RateLimitRPS   float64 // requests per second per IP
	RateLimitBurst int     // burst size per IP
}

type ipBucket struct {
	tokens    float64
	lastRefil time.Time
}

type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*ipBucket
	rps     float64
	burst   float64
}

func newRateLimiter(rps float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*ipBucket),
		rps:     rps,
		burst:   float64(burst),
	}
	go func() {
		for range time.Tick(5 * time.Minute) {
			rl.mu.Lock()
			cutoff := time.Now().Add(-10 * time.Minute)
			for ip, b := range rl.buckets {
				if b.lastRefil.Before(cutoff) {
					delete(rl.buckets, ip)
				}
			}
			rl.mu.Unlock()
		}
	}()
	return rl
}

func (rl *RateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[ip]
	if !ok {
		rl.buckets[ip] = &ipBucket{tokens: rl.burst - 1, lastRefil: now}
		return true
	}

	elapsed := now.Sub(b.lastRefil).Seconds()
	b.tokens += elapsed * rl.rps
	if b.tokens > rl.burst {
		b.tokens = rl.burst
	}
	b.lastRefil = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

func rateLimitMiddleware(rl *RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := remoteIP(r)
		if !rl.allow(ip) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			log.Printf("rate limited %s %s from %s", r.Method, r.URL.Path, ip)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func remoteIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip, _, err := net.SplitHostPort(strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])); err == nil {
			return ip
		}
		return strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func validationMiddleware(next http.Handler) http.Handler {
	// Feed endpoints only accept these content types in Accept headers.
	feedPaths := map[string]bool{}
	for _, s := range feedSources {
		feedPaths["/"+s.pbKey] = true
		feedPaths["/"+s.jsonKey] = true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			// allowed
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if len(r.URL.Path) > 128 {
			http.Error(w, "request path too long", http.StatusBadRequest)
			return
		}

		if strings.Contains(r.URL.Path, "..") || strings.Contains(r.URL.Path, "//") {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}

		if feedPaths[r.URL.Path] && r.Method == http.MethodGet {
			accept := r.Header.Get("Accept")
			if accept != "" && !acceptableFeedAccept(accept) {
				http.Error(w, "not acceptable", http.StatusNotAcceptable)
				return
			}
		}

		if r.ContentLength > 0 {
			http.Error(w, "request body not allowed", http.StatusBadRequest)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func acceptableFeedAccept(accept string) bool {
	for _, part := range strings.Split(accept, ",") {
		mt := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		switch mt {
		case "*/*", "application/*",
			"application/json", "application/x-protobuf",
			"application/octet-stream":
			return true
		}
	}
	return false
}

type TokenCache struct {
	accessToken string
	expiry      time.Time
	mu          sync.RWMutex
}

type TokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in,string"`
}

type FeedEntry struct {
	Data        []byte
	GzipData    []byte
	LastUpdate  time.Time
	ETag        string
	ContentType string
	EntityCount int
}

type FeedCache struct {
	data map[string]*FeedEntry
	mu   sync.RWMutex
}

type feedSource struct {
	path    string
	pbKey   string
	jsonKey string
}

var feedSources = []feedSource{
	{"/tripupdates", "tripupdates.pb", "tripupdates.json"},
	{"/alerts", "alerts.pb", "alerts.json"},
}

var (
	config     Config
	tokenCache TokenCache
	feedCache  = FeedCache{data: make(map[string]*FeedEntry)}

	httpClient = &http.Client{Timeout: 10 * time.Second}

	pjson = protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
)

func allCacheKeys() []string {
	keys := make([]string, 0, len(feedSources)*2)
	for _, s := range feedSources {
		keys = append(keys, s.pbKey, s.jsonKey)
	}
	return keys
}

const maxFeedAge = 2 * time.Minute

func loadConfig() Config {
	_ = godotenv.Load()

	pollStr := getEnv("POLL_INTERVAL", "30")
	poll, err := strconv.Atoi(pollStr)
	if err != nil {
		log.Printf("invalid POLL_INTERVAL %q, using 30s: %v", pollStr, err)
		poll = 30
	}

	rpsStr := getEnv("RATE_LIMIT_RPS", "10")
	rps, err := strconv.ParseFloat(rpsStr, 64)
	if err != nil || rps <= 0 {
		log.Printf("invalid RATE_LIMIT_RPS %q, using 10: %v", rpsStr, err)
		rps = 10
	}

	burstStr := getEnv("RATE_LIMIT_BURST", "30")
	burst, err := strconv.Atoi(burstStr)
	if err != nil || burst <= 0 {
		log.Printf("invalid RATE_LIMIT_BURST %q, using 30: %v", burstStr, err)
		burst = 30
	}

	return Config{
		TenantID:       os.Getenv("TENANT_ID"),
		ClientID:       os.Getenv("CLIENT_ID"),
		ClientSecret:   os.Getenv("CLIENT_SECRET"),
		Resource:       os.Getenv("RESOURCE"),
		Hostname:       os.Getenv("HOSTNAME"),
		PollInterval:   time.Duration(poll) * time.Second,
		Port:           getEnv("PORT", "8000"),
		RateLimitRPS:   rps,
		RateLimitBurst: burst,
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

func getAccessToken() (string, error) {
	tokenCache.mu.RLock()
	if tokenCache.accessToken != "" && time.Now().Before(tokenCache.expiry.Add(-60*time.Second)) {
		tok := tokenCache.accessToken
		tokenCache.mu.RUnlock()
		return tok, nil
	}
	tokenCache.mu.RUnlock()

	tokenCache.mu.Lock()
	defer tokenCache.mu.Unlock()

	if tokenCache.accessToken != "" && time.Now().Before(tokenCache.expiry.Add(-60*time.Second)) {
		return tokenCache.accessToken, nil
	}

	url := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/token", config.TenantID)
	resp, err := httpClient.PostForm(url, map[string][]string{
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

	tokenCache.accessToken = tr.AccessToken
	tokenCache.expiry = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)

	return tokenCache.accessToken, nil
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
	hash := sha256.Sum256(data)
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
				pbEntry := makeCacheEntry(pbData, "application/x-protobuf", -1)
				feedCache.mu.Lock()
				feedCache.data[s.pbKey] = pbEntry
				feedCache.mu.Unlock()
				log.Printf("[%s] size=%dB gzip=%dB (protobuf only; JSON decode failed)",
					s.pbKey, len(pbData), len(pbEntry.GzipData))
				return
			}

			pbEntry := makeCacheEntry(pbData, "application/x-protobuf", -1)
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
		swr := int(config.PollInterval.Seconds()) - 5
		if swr < 0 {
			swr = 0
		}
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=5, stale-while-revalidate=%d", swr))
		h.Set("Vary", "Accept-Encoding")

		if acceptsGzip(r) && entry.GzipData != nil {
			h.Set("Content-Encoding", "gzip")
			h.Set("Content-Length", strconv.Itoa(len(entry.GzipData)))
			if _, err := w.Write(entry.GzipData); err != nil {
				log.Printf("write error: %v", err)
			}
		} else {
			h.Set("Content-Length", strconv.Itoa(len(entry.Data)))
			if _, err := w.Write(entry.Data); err != nil {
				log.Printf("write error: %v", err)
			}
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
	noRateLimit := flag.Bool("no-rate-limit", false, "disable per-IP rate limiting")
	flag.Parse()

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

	var handler http.Handler
	if *noRateLimit {
		log.Println("rate limiter disabled")
		handler = validationMiddleware(mux)
	} else {
		rl := newRateLimiter(config.RateLimitRPS, config.RateLimitBurst)
		log.Printf("rate limiter: %.1f req/s per IP, burst %d", config.RateLimitRPS, config.RateLimitBurst)
		handler = validationMiddleware(rateLimitMiddleware(rl, mux))
	}

	server := &http.Server{
		Addr:         ":" + config.Port,
		Handler:      handler,
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
