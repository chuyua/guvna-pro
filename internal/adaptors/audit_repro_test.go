package adaptors

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

// Expected paths come from https://ai.google.dev/gemini-api/docs/openai.
func TestAuditGeminiOfficialPaths(t *testing.T) {
	testProviderPaths(t, "gemini", "/v1beta/openai", NewGemini)
}

func TestOpenAIPathsAcrossConstructors(t *testing.T) {
	testProviderPaths(t, "openai", "/v1", NewOpenAICompat)
}

func testProviderPaths(t *testing.T, providerType, apiPath string, direct func(config.Provider, string) Adaptor) {
	t.Helper()
	constructors := []struct {
		name string
		make func(config.Provider, string) (Adaptor, error)
	}{
		{"direct", func(p config.Provider, k string) (Adaptor, error) { return direct(p, k), nil }},
		{"normal", New}, {"decider", NewDecider},
		{"auto_stream", func(p config.Provider, k string) (Adaptor, error) { return NewAuto(p, k, true) }},
		{"auto_nonstream", func(p config.Provider, k string) (Adaptor, error) { return NewAuto(p, k, false) }},
	}
	for _, ctor := range constructors {
		for _, trailing := range []string{"", "/", "///"} {
			t.Run(ctor.name+"/trailing="+trailing, func(t *testing.T) {
				for _, embed := range []bool{false, true} {
					name := "chat"
					want := apiPath + "/chat/completions"
					if embed {
						name = "embed"
						want = apiPath + "/embeddings"
					}
					t.Run(name, func(t *testing.T) {
						paths := make(chan string, 1)
						ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							paths <- r.URL.Path
							if r.URL.Path != want {
								http.NotFound(w, r)
								return
							}
							body, err := io.ReadAll(r.Body)
							if err != nil || string(body) != `{"model":"m","input":"unchanged"}` ||
								r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer dummy" ||
								r.Header.Get("Content-Type") != "application/json" {
								http.Error(w, "request contract changed", http.StatusBadRequest)
								return
							}
							w.Write([]byte(`{}`))
						}))
						defer ts.Close()
						a, err := ctor.make(config.Provider{Name: providerType, Type: providerType, BaseURL: ts.URL + trailing}, "dummy")
						if err != nil {
							t.Fatal(err)
						}
						var resp *http.Response
						if embed {
							resp, err = a.Embed(context.Background(), []byte(`{"model":"m","input":"unchanged"}`))
						} else {
							resp, err = a.Chat(context.Background(), []byte(`{"model":"m","input":"unchanged"}`))
						}
						if err != nil {
							t.Fatal(err)
						}
						defer resp.Body.Close()
						got := <-paths
						t.Logf("got=%s official=%s status=%d", got, want, resp.StatusCode)
						if got != want {
							t.Fatalf("wrong %s endpoint %q, want %q", providerType, got, want)
						}
						if resp.StatusCode != http.StatusOK {
							t.Fatalf("request contract failed: status=%d", resp.StatusCode)
						}
					})
				}
			})
		}
	}
}
