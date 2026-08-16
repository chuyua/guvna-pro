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
	"net/http"
	"strings"

	"github.com/alisa/bruvroute/internal/config"
)

const chatPath = "/v1/chat/completions"

// Adaptor forwards a raw chat completion request to one provider.
type Adaptor interface {
	// Chat sends body to the upstream and returns the raw response.
	// Caller owns closing resp.Body.
	Chat(ctx context.Context, body []byte) (*http.Response, error)
}

// Client is shared across adaptors and reused for keep-alive.
var Client = &http.Client{Timeout: 0} // no overall timeout; streaming may run long

type openAICompat struct {
	name string
	url  string
	key  string
}

// NewOpenAICompat builds an OpenAI-protocol adaptor (type "openai").
func NewOpenAICompat(p config.Provider, key string) Adaptor {
	url := strings.TrimRight(p.BaseURL, "/") + chatPath
	return &openAICompat{name: p.Name, url: url, key: key}
}

// NewGemini builds the Gemini adaptor. Uses Gemini's official OpenAI-compatible
// endpoint so the body passes through unchanged.
func NewGemini(p config.Provider, key string) Adaptor {
	base := strings.TrimRight(p.BaseURL, "/")
	return &openAICompat{name: p.Name, url: base + "/v1beta/openai" + chatPath, key: key}
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

func (a *openAICompat) Chat(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.key)
	req.Header.Set("User-Agent", "bruvroute/0.1")
	return Client.Do(req)
}
