// Guvna — low-resource single-user AI gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/creamy-ghost/guvna/internal/auth"
	"github.com/creamy-ghost/guvna/internal/chains"
	"github.com/creamy-ghost/guvna/internal/cli"
	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/localadmin"
	"github.com/creamy-ghost/guvna/internal/logring"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/server"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

const (
	envAdminKey = "ADMIN_KEY"
	envAPIKeys  = "API_KEYS"
	flushEvery  = 30 * time.Second
)

// version is set from the release tag with -ldflags '-X main.version=<tag>'.
var version = "dev"

func main() {
	log.SetPrefix("guvna: ")
	if err := run(os.Args[1:], os.Stdout, os.Stderr, serve); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(os.Stderr, "guvna: %v\n", err)
		os.Exit(1)
	}
}

// run dispatches before reading configuration or credentials, so management,
// help and version invocations cannot accidentally start a gateway.
func run(args []string, stdout, stderr io.Writer, start func([]string, io.Writer) error) error {
	if len(args) == 0 {
		return start(args, stderr)
	}
	switch args[0] {
	case "serve":
		return start(args[1:], stderr)
	case "status", "logs", "chains":
		return cli.Run(args, stdout, stderr)
	case "help", "-h", "--help":
		if len(args) != 1 {
			return fmt.Errorf("%s accepts no arguments", args[0])
		}
		usage(stdout)
		return nil
	case "--version":
		if len(args) != 1 {
			return fmt.Errorf("--version accepts no arguments")
		}
		_, err := fmt.Fprintf(stdout, "guvna %s\n", version)
		return err
	default:
		// Existing deployments pass service flags without a subcommand.
		if strings.HasPrefix(args[0], "-") {
			return start(args, stderr)
		}
		return fmt.Errorf("unknown command %q (want serve|status|logs|chains)", args[0])
	}
}

func usage(out io.Writer) {
	fmt.Fprint(out, `guvna — single-user AI gateway and management CLI

usage:
  guvna serve [-config path] [-data directory]
  guvna [-config path] [-data directory]  (legacy service startup)
  guvna --healthcheck [-config path]
  guvna status [flags]
  guvna status --scoring [--since timestamp --until timestamp] [flags]
  guvna logs [-n 100] [flags]
  guvna chains [list] [flags]
  guvna chains add NAME --step provider:model [--step ...] [flags]
  guvna chains rm NAME [flags]
  guvna --version

management flags:
  --socket local management socket (default: ~/.guvna/admin.sock or $GUVNA_ADMIN_SOCKET)
  --token  admin key (default: $GUVNA_ADMIN_KEY)
  --json   emit raw JSON instead of a table

Run guvna serve --help for service flags, or guvna COMMAND --help for management flags.
`)
}

type serveOptions struct {
	configPath  string
	dataDir     string
	healthCheck bool
}

func parseServe(args []string, stderr io.Writer) (serveOptions, error) {
	var opts serveOptions
	fs := flag.NewFlagSet("guvna serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.configPath, "config", "config.yaml", "path to config.yaml")
	fs.StringVar(&opts.dataDir, "data", dbDir(), "data directory (config override + SQLite)")
	fs.BoolVar(&opts.healthCheck, "healthcheck", false, "check /healthz of a running instance and exit 0/1")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, fmt.Errorf("serve accepts no positional arguments")
	}
	return opts, nil
}

func serve(args []string, stderr io.Writer) (retErr error) {
	opts, err := parseServe(args, stderr)
	if err != nil {
		return err
	}
	if opts.healthCheck {
		return doHealthCheck(opts.configPath)
	}
	log.Printf("starting, config=%s", opts.configPath)

	cfg, err := loadConfig(opts.configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	adminKey := os.Getenv(envAdminKey)
	if adminKey == "" {
		return fmt.Errorf("%s env var required", envAdminKey)
	}
	var clientKeys []string
	for _, k := range strings.Split(os.Getenv(envAPIKeys), ",") {
		if k = strings.TrimSpace(k); k != "" {
			clientKeys = append(clientKeys, k)
		}
	}
	if len(clientKeys) == 0 {
		return fmt.Errorf("%s env var required (comma-separated client keys)", envAPIKeys)
	}

	dbPath := filepath.Join(opts.dataDir, "guvna.db")
	if err := os.MkdirAll(opts.dataDir, 0o700); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	tm, err := telemetry.Open(dbPath, flushEvery)
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer finishTelemetry(tm.Close, &retErr)

	rtr := router.New(cfg)
	chainStore := chains.New(opts.dataDir)
	if err := chainStore.Load(rtr); err != nil {
		log.Printf("chains: %v", err)
	}
	authn := auth.New(adminKey, clientKeys)
	hlth := health.New()
	ring := logring.New(512)
	log.SetOutput(io.MultiWriter(stderr, ring))
	api := server.New(cfg, rtr, authn, tm, hlth, ring, chainStore)
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	publicListener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("public listener: %w", err)
	}
	defer publicListener.Close()
	socketPath := localadmin.SocketPath(opts.dataDir)
	adminListener, err := localadmin.Listen(socketPath)
	if err != nil {
		return err
	}
	defer adminListener.Close()
	adminServer := &http.Server{Handler: api.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
	api.StartAutoDecider()
	log.Printf("public API listening on %s (db=%s); local management socket=%s", publicListener.Addr(), dbPath, socketPath)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serveEndpoints(ctx, srv, adminServer, publicListener, adminListener)
}

// Always expose final telemetry flush failures. Losing the last buffered events
// at shutdown must not turn an otherwise clean exit into a silent success.
func finishTelemetry(closeFn func() error, result *error) {
	if err := closeFn(); err != nil {
		log.Printf("telemetry close: %v", err)
		*result = errors.Join(*result, fmt.Errorf("telemetry close: %w", err))
	}
}

// Both listeners are opened before startup succeeds. Any serving failure stops
// the other endpoint too; management never falls back to the public listener.
func serveEndpoints(ctx context.Context, public, admin *http.Server, publicListener, adminListener net.Listener) error {
	serveErrors := make(chan error, 2)
	for _, endpoint := range []struct {
		name string
		srv  *http.Server
		ln   net.Listener
	}{{"public API", public, publicListener}, {"local management", admin, adminListener}} {
		go func() {
			err := endpoint.srv.Serve(endpoint.ln)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			if err != nil {
				err = fmt.Errorf("%s: %w", endpoint.name, err)
			}
			serveErrors <- err
		}()
	}
	var result error
	select {
	case <-ctx.Done():
	case result = <-serveErrors:
	}
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdownErrors := make(chan error, 2)
	for _, srv := range []*http.Server{public, admin} {
		go func() {
			err := srv.Shutdown(shutdownCtx)
			if err != nil {
				srv.Close()
			}
			shutdownErrors <- err
		}()
	}
	return errors.Join(result, <-shutdownErrors, <-shutdownErrors)
}

// doHealthCheck probes a running instance's /healthz. Distroless images have
// no shell or curl, so the binary itself is the container healthcheck.
func doHealthCheck(configPath string) error {
	port := 20128
	if cfg, err := loadConfig(configPath); err == nil {
		port = cfg.Port
	} else {
		log.Printf("healthcheck: config ignored (%v), probing :%d", err, port)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: status %d", resp.StatusCode)
	}
	return nil
}

// loadConfig prefers ~/.guvna/config.yaml; falls back to the flag path.
func loadConfig(flagPath string) (*config.Config, error) {
	homeCfg := filepath.Join(dbDir(), "config.yaml")
	path := flagPath
	if _, err := os.Stat(homeCfg); err == nil {
		path = homeCfg
	}
	return config.Load(path)
}

func dbDir() string {
	return localadmin.DefaultDataDir()
}
