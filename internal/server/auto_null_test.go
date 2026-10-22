package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestAutoRejectsNullInsteadOfPanicking(t *testing.T) {
	upstream, hits := countingUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	s, _ := autoServer(t,
		[]config.Provider{{Name: "a", Type: "openai", BaseURL: upstream.URL, KeyEnv: "A"}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}},
		map[string]string{"A": "dummy"},
	)
	for _, input := range []string{"null", "  null  ", "[]", `"null"`} {
		t.Run(input, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/auto", strings.NewReader(input))
			request.Header.Set("Authorization", "Bearer client-1")
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("input=%q: status=%d body=%s", input, response.Code, response.Body.String())
			}
		})
	}
	if hits.Load() != 0 {
		t.Fatalf("invalid requests were forwarded to upstream %d times", hits.Load())
	}
}
