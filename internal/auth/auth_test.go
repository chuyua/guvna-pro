package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidKeys(t *testing.T) {
	a := New("admin-secret", []string{"client-1", "client-2"})
	cases := map[string]bool{
		"admin-secret": true,
		"client-1":     true,
		"client-2":     true,
		"wrong":        false,
		"":             false,
	}
	for token, want := range cases {
		if got := a.Valid(token); got != want {
			t.Errorf("Valid(%q) = %v, want %v", token, got, want)
		}
	}
}

func TestMiddleware(t *testing.T) {
	a := New("admin-secret", []string{"client-1"})
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no auth -> %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Authorization", "Bearer client-1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("client key -> %d, want 200", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("bad key -> %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Authorization", "Basic abc")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("basic auth -> %d, want 401", rec.Code)
	}
}

func TestNoAdminKeyStillRequiresClientKey(t *testing.T) {
	a := New("", []string{"client-1"})
	if a.Valid("") {
		t.Fatal("empty token must never be valid")
	}
	if !a.Valid("client-1") {
		t.Fatal("client key must be valid")
	}
}
