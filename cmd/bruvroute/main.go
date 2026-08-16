// BruvRoute — low-resource single-user AI gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/alisa/bruvroute/internal/auth"
	"github.com/alisa/bruvroute/internal/config"
	"github.com/alisa/bruvroute/internal/health"
	"github.com/alisa/bruvroute/internal/logring"
	"github.com/alisa/bruvroute/internal/router"
	"github.com/alisa/bruvroute/internal/server"
	"github.com/alisa/bruvroute/internal/telemetry"
)

const (
	envAdminKey  = "ADMIN_KEY"
	envAPIKeys   = "API_KEYS"
	flushEvery   = 30 * time.Second
	defaultDBDir = ".bruvroute"
)

func main() {
	log.SetPrefix("bruvroute: ")
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	dataDir := flag.String("data", dbDir(), "data directory (config override + SQLite)")
	healthCheck := flag.Bool("healthcheck", false, "check /healthz of a running instance and exit 0/1")
	flag.Parse()

	if *healthCheck {
		os.Exit(doHealthCheck(*configPath))
	}

	log.Printf("starting, config=%s", *configPath)

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	adminKey := os.Getenv(envAdminKey)
	if adminKey == "" {
		log.Fatalf("%s env var required", envAdminKey)
	}
	var clientKeys []string
	for _, k := range strings.Split(os.Getenv(envAPIKeys), ",") {
		if k = strings.TrimSpace(k); k != "" {
			clientKeys = append(clientKeys, k)
		}
	}
	if len(clientKeys) == 0 {
		log.Fatalf("%s env var required (comma-separated client keys)", envAPIKeys)
	}

	dbPath := filepath.Join(*dataDir, "bruvroute.db")
	if err := os.MkdirAll(*dataDir, 0o700); err != nil {
		log.Fatalf("data dir: %v", err)
	}
	tm, err := telemetry.Open(dbPath, flushEvery)
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	defer tm.Close()

	rtr := router.New(cfg)
	authn := auth.New(adminKey, clientKeys)
	hlth := health.New()
	ring := logring.New(512)
	log.SetOutput(io.MultiWriter(os.Stderr, ring))
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           server.New(cfg, rtr, authn, tm, hlth, ring).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("listening on %s (db=%s)", srv.Addr, dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

// doHealthCheck probes a running instance's /healthz. Distroless images have
// no shell or curl, so the binary itself is the container healthcheck.
func doHealthCheck(configPath string) int {
	port := 20128
	if cfg, err := loadConfig(configPath); err == nil {
		port = cfg.Port
	} else {
		log.Printf("healthcheck: config ignored (%v), probing :%d", err, port)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		log.Printf("healthcheck: %v", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("healthcheck: status %d", resp.StatusCode)
		return 1
	}
	return 0
}

// loadConfig prefers ~/.bruvroute/config.yaml; falls back to the flag path.
func loadConfig(flagPath string) (*config.Config, error) {
	homeCfg := filepath.Join(dbDir(), "config.yaml")
	path := flagPath
	if _, err := os.Stat(homeCfg); err == nil {
		path = homeCfg
	}
	return config.Load(path)
}

func dbDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return defaultDBDir
	}
	return filepath.Join(home, defaultDBDir)
}
