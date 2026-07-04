package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

type proxyStats struct {
	mu            sync.Mutex
	requests      int64
	requestBytes  int64
	responseBytes int64
	statuses      map[int]int64
}

func newProxyStats() *proxyStats {
	return &proxyStats{
		statuses: make(map[int]int64),
	}
}

func (s *proxyStats) record(requestBytes, responseBytes int64, status int) {
	s.mu.Lock()
	s.requests++
	s.requestBytes += requestBytes
	s.responseBytes += responseBytes
	s.statuses[status]++
	s.mu.Unlock()
}

func (s *proxyStats) drain() (requests, requestBytes, responseBytes int64, statuses map[int]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	requests = s.requests
	requestBytes = s.requestBytes
	responseBytes = s.responseBytes

	statuses = make(map[int]int64, len(s.statuses))
	maps.Copy(statuses, s.statuses)

	s.requests = 0
	s.requestBytes = 0
	s.responseBytes = 0
	s.statuses = make(map[int]int64)

	return requests, requestBytes, responseBytes, statuses
}

func formatStatusCounts(statuses map[int]int64) string {
	if len(statuses) == 0 {
		return "-"
	}

	codes := make([]int, 0, len(statuses))
	for code := range statuses {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, fmt.Sprintf("%d=%d", code, statuses[code]))
	}
	return strings.Join(parts, ",")
}

func logStats(prefix string, s *proxyStats, elapsed time.Duration) {
	reqs, reqBytes, respBytes, statuses := s.drain()
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}

	totalBytes := reqBytes + respBytes
	throughput := float64(totalBytes) / seconds
	log.Printf(
		"%s requests=%d req_bytes=%d resp_bytes=%d throughput=%.2fB/s statuses=%s",
		prefix,
		reqs,
		reqBytes,
		respBytes,
		throughput,
		formatStatusCounts(statuses),
	)
}

type countingReader struct {
	r     io.Reader
	count int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.count += int64(n)
	return n, err
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	if !r.wroteHeader {
		r.status = statusCode
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func newProxy(targetURL *url.URL, username, password string) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(preq *httputil.ProxyRequest) {
			preq.SetURL(targetURL)
			preq.Out.Host = preq.In.Host
			preq.Out.Header.Del("Accept-Encoding")
			if username != "" {
				preq.Out.SetBasicAuth(username, password)
			}
			preq.SetXForwarded()
		},
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("no .env found, using environment")
	}
	var (
		listenAddr = flag.String("addr", ":8080", "address to listen on")
		upstream   = flag.String("upstream", getEnv("ENDPOINT", "https://httpbun.com"), "upstream origin to proxy to")
		username   = flag.String("username", getEnv("USERNAME", ""), "basic auth username for upstream")
		password   = flag.String("password", getEnv("PASSWORD", ""), "basic auth password for upstream")
		interval   = flag.Duration("interval", 10*time.Second, "stats logging interval")
	)
	flag.Parse()

	targetURL, err := url.Parse(*upstream)
	if err != nil {
		log.Fatalf("parse upstream: %v", err)
	}

	statsCollector := newProxyStats()
	proxy := newProxy(targetURL, *username, *password)
	proxy.Transport = &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured := &responseRecorder{ResponseWriter: w}
		countingBody := &countingReader{r: r.Body}
		r.Body = io.NopCloser(countingBody)

		proxy.ServeHTTP(captured, r)

		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		statsCollector.record(countingBody.count, captured.bytes, status)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", handler)

	server := &http.Server{
		Addr:    *listenAddr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()

		lastReport := time.Now()
		for {
			select {
			case <-ctx.Done():
				logStats("final", statsCollector, time.Since(lastReport))
				return
			case now := <-ticker.C:
				logStats("stats", statsCollector, now.Sub(lastReport))
				lastReport = now
			}
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s proxying to %s", *listenAddr, targetURL.String())
	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
}
