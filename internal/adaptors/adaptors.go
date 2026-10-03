// Package adaptors relays chat completion requests to upstream providers.
//
// The hot path is raw passthrough: the request body is forwarded byte-identical
// to the upstream endpoint with a Bearer key. No JSON parsing or rewriting in
// the relay path. All providers currently speak the OpenAI protocol — Gemini is
// reached via its official OpenAI-compatible endpoint. A native Gemini format
// adaptor can be added behind this interface without touching the relay.
package adaptors

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

const (
	openAIPath = "/v1"
	geminiPath = "/v1beta/openai"
	chatPath   = "/chat/completions"
	embedPath  = "/embeddings"
)

// Adaptor forwards a raw chat completion request to one provider.
type Adaptor interface {
	// Chat sends body to the upstream and returns the raw response.
	// Caller owns closing resp.Body.
	Chat(ctx context.Context, body []byte) (*http.Response, error)
	// Embed sends an embeddings request to the upstream and returns the raw
	// response. Caller owns closing resp.Body.
	Embed(ctx context.Context, body []byte) (*http.Response, error)
}

// Dial timeout lives on the Dialer, not the Transport: http.Transport no longer
// exposes DialTimeout, and DialContext is what's actually consulted. 10s is
// plenty for the handshake to a host that answers; a host that doesn't answer
// at TCP level should fail fast so the chain can move on.
var defaultDialer = &net.Dialer{Timeout: envDuration("GUVNA_DIAL_TIMEOUT", 10*time.Second), KeepAlive: 30 * time.Second}

// preferV4DialContext dials IPv4 when A records exist. Some networks serve
// dead AAAA records (e.g. a VPS behind a DNS virtual gateway: AAAA
// TCP connects but TLS never completes), which Go's Happy Eyeballs prefers and
// turns into EOF/timeouts. Falling back to the default dialer when the host is
// an IP literal or has no A records keeps v6-only hosts working.
func preferV4DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) != nil {
		return defaultDialer.DialContext(ctx, network, addr)
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil || len(ips) == 0 {
		return defaultDialer.DialContext(ctx, network, addr)
	}
	return defaultDialer.DialContext(ctx, "tcp4", net.JoinHostPort(ips[0].String(), port))
}

// Client is shared across adaptors and reused for keep-alive.
//
// Timeout stays 0 on purpose: a streaming response may run for minutes and a
// whole-request deadline would cut it off mid-stream. Instead the transport
// bounds the wait for response headers (ResponseHeaderTimeout) — that is the
// failure mode that actually happens, an upstream that accepts the TCP/TLS
// handshake and then never answers. That deadline does not touch a body that
// has already started flowing, so a long stream is unaffected.
//
// A per-attempt context timeout is deliberately not used: net/http ties the
// request context to body reads, and on the 2xx path the body is handed to the
// caller, so there is no safe moment to cancel without risking a mid-read cut.
var Client = &http.Client{
	Timeout:   0, // no overall timeout; streaming may run long
	Transport: preferV4Transport(),
}

// DeciderClient is a dedicated client for /v1/auto decider calls. Thinking
// models (glm-5.3, deepseek) need more time to reason about a candidate list
// than the global GUVNA_HEADER_TIMEOUT (20s on the live server). The decider
// is a single small JSON ranking, not a streaming completion, so a longer
// header timeout is safe — it only bounds the wait for the first byte.
var DeciderClient = &http.Client{
	Timeout:   0,
	Transport: deciderTransport(),
}

func deciderTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = preferV4DialContext
	t.TLSHandshakeTimeout = envDuration("GUVNA_TLS_TIMEOUT", 15*time.Second)
	t.ExpectContinueTimeout = envDuration("GUVNA_EXPECT_TIMEOUT", time.Second)
	t.IdleConnTimeout = 90 * time.Second
	// 90s: generous for glm-5.3 reasoning over 10+ candidates with health stats.
	t.ResponseHeaderTimeout = envDuration("GUVNA_DECIDER_HEADER_TIMEOUT", 90*time.Second)
	return t
}

// AutoClient (streaming steps) and AutoClientNonStream bound each /v1/auto
// serving step's header wait. The two exist because the header means different
// things: for a stream it is the first-token budget (short); for a non-stream
// completion the upstream sends headers only after generating the whole body,
// so the same 5s would fail every inference model — and feed timeouts into
// health/keypool, marking healthy providers down for the /v1/chat chain too.
// The auto router's total budget is the client's patience (tens of seconds); a
// chain-sized 45s wait lets one hung upstream eat it all.
var AutoClient = &http.Client{
	Timeout:   0,
	Transport: autoTransport(envDuration("GUVNA_AUTO_HEADER_TIMEOUT", 5*time.Second)),
}

var AutoClientNonStream = &http.Client{
	Timeout:   0,
	Transport: autoTransport(envDuration("GUVNA_AUTO_HEADER_TIMEOUT_NONSTREAM", 30*time.Second)),
}

func autoTransport(headerTimeout time.Duration) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = preferV4DialContext
	t.TLSHandshakeTimeout = envDuration("GUVNA_TLS_TIMEOUT", 15*time.Second)
	t.ExpectContinueTimeout = envDuration("GUVNA_EXPECT_TIMEOUT", time.Second)
	t.IdleConnTimeout = 90 * time.Second
	t.ResponseHeaderTimeout = headerTimeout
	return t
}

// envDuration reads a duration override from the environment, falling back to
// def on any empty, unparsable, or non-positive value.
func envDuration(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func preferV4Transport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = preferV4DialContext
	t.TLSHandshakeTimeout = envDuration("GUVNA_TLS_TIMEOUT", 15*time.Second)
	t.ExpectContinueTimeout = envDuration("GUVNA_EXPECT_TIMEOUT", 1*time.Second)
	t.IdleConnTimeout = envDuration("GUVNA_IDLE_TIMEOUT", 90*time.Second)
	// The critical one. Without this a hung upstream blocks a step forever and
	// chain failover can never happen. 45s default: generous enough for slow
	// first-token inference, tight enough to move on within a sane budget.
	t.ResponseHeaderTimeout = envDuration("GUVNA_HEADER_TIMEOUT", 45*time.Second)
	return t
}

type openAICompat struct {
	name     string
	url      string
	embedURL string
	key      string
	client   *http.Client // nil uses the shared Client
}

// NewOpenAICompat builds an OpenAI-protocol adaptor (type "openai").
func NewOpenAICompat(p config.Provider, key string) Adaptor {
	return newCompat(p, key, openAIPath, nil)
}

// NewGemini builds the Gemini adaptor. Uses Gemini's official OpenAI-compatible
// endpoint so the body passes through unchanged.
func NewGemini(p config.Provider, key string) Adaptor {
	return newCompat(p, key, geminiPath, nil)
}

// newCompat keeps endpoint construction identical across the normal, decider,
// and auto clients. Gemini's compatibility API has no extra /v1 segment.
func newCompat(p config.Provider, key, apiPath string, c *http.Client) Adaptor {
	base := strings.TrimRight(p.BaseURL, "/") + apiPath
	return &openAICompat{name: p.Name, url: base + chatPath, embedURL: base + embedPath, key: key, client: c}
}

// New returns the adaptor matching the provider's type.
func New(p config.Provider, key string) (Adaptor, error) {
	switch p.Type {
	case "openai":
		return NewOpenAICompat(p, key), nil
	case "gemini":
		return NewGemini(p, key), nil
	default:
		return nil, fmt.Errorf("provider %q: unknown type %q", p.Name, p.Type)
	}
}

// NewDecider builds an adaptor that uses DeciderClient (longer header timeout)
// instead of the shared Client. For the /v1/auto decider call only.
func NewDecider(p config.Provider, key string) (Adaptor, error) {
	return withClient(p, key, DeciderClient)
}

// NewAuto builds an adaptor for /v1/auto serving steps: AutoClient bounds each
// step's header wait to a few seconds so failover stays inside the client's
// patience instead of the chain's.
// NewAuto picks the client by stream-ness: streams get the short first-token
// budget, non-stream completions the longer whole-generation budget.
func NewAuto(p config.Provider, key string, stream bool) (Adaptor, error) {
	if stream {
		return withClient(p, key, AutoClient)
	}
	return withClient(p, key, AutoClientNonStream)
}

func withClient(p config.Provider, key string, c *http.Client) (Adaptor, error) {
	switch p.Type {
	case "openai":
		return newCompat(p, key, openAIPath, c), nil
	case "gemini":
		return newCompat(p, key, geminiPath, c), nil
	default:
		return nil, fmt.Errorf("provider %q: unknown type %q", p.Name, p.Type)
	}
}

func (a *openAICompat) Chat(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.key)
	req.Header.Set("User-Agent", "guvna/0.1")
	if a.client != nil {
		return a.client.Do(req)
	}
	return Client.Do(req)
}

func (a *openAICompat) Embed(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.embedURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.key)
	req.Header.Set("User-Agent", "guvna/0.1")
	if a.client != nil {
		return a.client.Do(req)
	}
	return Client.Do(req)
}
