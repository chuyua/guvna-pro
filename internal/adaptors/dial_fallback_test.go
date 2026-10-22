package adaptors

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func twoIPv4(context.Context, string, string) ([]net.IP, error) {
	return []net.IP{net.ParseIP("192.0.2.1"), net.ParseIP("192.0.2.2")}, nil
}

func TestDialPreferV4TriesSecondAddress(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	var addresses []string
	lookup := func(ctx context.Context, network, host string) ([]net.IP, error) {
		if network != "ip4" || host != "provider.example" {
			t.Fatalf("lookup = %s/%s", network, host)
		}
		return twoIPv4(ctx, network, host)
	}
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if network != "tcp4" {
			t.Fatalf("IPv4 preference lost: %s", network)
		}
		addresses = append(addresses, addr)
		if len(addresses) == 1 {
			return nil, errors.New("refused")
		}
		return client, nil
	}
	got, err := dialPreferV4(context.Background(), "tcp", "provider.example:443", time.Second, lookup, dial)
	if err != nil || got != client {
		t.Fatalf("fallback conn=%v err=%v", got, err)
	}
	if !reflect.DeepEqual(addresses, []string{"192.0.2.1:443", "192.0.2.2:443"}) {
		t.Fatalf("addresses = %v", addresses)
	}
}

func TestDialPreferV4AllFailuresJoined(t *testing.T) {
	first, second := errors.New("first refused"), errors.New("second unreachable")
	count := 0
	dial := func(context.Context, string, string) (net.Conn, error) {
		count++
		if count == 1 {
			return nil, first
		}
		return nil, second
	}
	conn, err := dialPreferV4(context.Background(), "tcp", "provider.example:443", time.Second, twoIPv4, dial)
	if conn != nil || !errors.Is(err, first) || !errors.Is(err, second) || count != 2 {
		t.Fatalf("all failures: conn=%v err=%v attempts=%d", conn, err, count)
	}
	for _, addr := range []string{"192.0.2.1:443", "192.0.2.2:443"} {
		if !strings.Contains(err.Error(), addr) {
			t.Fatalf("missing address %s in %v", addr, err)
		}
	}
}

func TestDialPreferV4SharesRemainingBudget(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	parentDeadline := time.Now().Add(150 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()
	var deadlines []time.Time
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("attempt has no budget")
		}
		deadlines = append(deadlines, deadline)
		if len(deadlines) == 1 {
			// Simulate a blackholed first A record rather than immediate refusal.
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if ctx.Err() != nil {
			t.Fatal("second address inherited the first address's expired deadline")
		}
		return client, nil
	}
	got, err := dialPreferV4(ctx, "tcp", "provider.example:443", time.Hour, twoIPv4, dial)
	if err != nil || got != client || len(deadlines) != 2 {
		t.Fatalf("budgeted fallback conn=%v err=%v deadlines=%v", got, err, deadlines)
	}
	if !deadlines[0].Before(parentDeadline.Add(-40*time.Millisecond)) || !deadlines[1].Equal(parentDeadline) {
		t.Fatalf("per-address deadlines = %v, parent=%s", deadlines, parentDeadline)
	}
}

func TestDialPreferV4TotalTimeoutDoesNotMultiply(t *testing.T) {
	const timeout = 80 * time.Millisecond
	start := time.Now()
	var firstTotalDeadline time.Time
	attempts := 0
	lookup := func(ctx context.Context, network, host string) ([]net.IP, error) {
		firstTotalDeadline, _ = ctx.Deadline()
		return twoIPv4(ctx, network, host)
	}
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		attempts++
		deadline, _ := ctx.Deadline()
		if deadline.After(firstTotalDeadline) {
			t.Fatal("address attempt extended total timeout")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	_, err := dialPreferV4(context.Background(), "tcp", "provider.example:443", timeout, lookup, dial)
	if !errors.Is(err, context.DeadlineExceeded) || attempts != 2 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
	if time.Since(start) > time.Second {
		t.Fatal("dial did not honor one bounded timeout")
	}
}

func TestDialPreferV4CancellationStopsFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		attempts++
		cancel()
		return nil, ctx.Err()
	}
	_, err := dialPreferV4(ctx, "tcp", "provider.example:443", time.Second, twoIPv4, dial)
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("cancellation err=%v attempts=%d", err, attempts)
	}
	// A canceled resolution must not restart DNS through the fallback dialer.
	_, err = dialPreferV4(ctx, "tcp", "provider.example:443", time.Second, twoIPv4, dial)
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("already canceled err=%v attempts=%d", err, attempts)
	}
}

func TestDialPreferV4LiteralAndIPv6OnlyFallback(t *testing.T) {
	for _, tc := range []struct {
		name, addr, network string
		ips                 []net.IP
		lookupErr           error
		wantLookups         int
	}{
		{"ipv4_literal", "127.0.0.1:443", "tcp", nil, nil, 0},
		{"ipv6_literal", "[::1]:443", "tcp6", nil, nil, 0},
		{"ipv6_only", "provider.example:443", "tcp", nil, nil, 1},
		{"lookup_error", "provider.example:443", "tcp", nil, errors.New("A lookup failed"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookups, dials := 0, 0
			lookup := func(context.Context, string, string) ([]net.IP, error) {
				lookups++
				return tc.ips, tc.lookupErr
			}
			sentinel := errors.New("native dial")
			dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
				dials++
				if network != tc.network || addr != tc.addr {
					t.Fatalf("fallback dial=%s/%s", network, addr)
				}
				return nil, sentinel
			}
			_, err := dialPreferV4(context.Background(), tc.network, tc.addr, time.Second, lookup, dial)
			if !errors.Is(err, sentinel) || lookups != tc.wantLookups || dials != 1 {
				t.Fatalf("err=%v lookups=%d dials=%d", err, lookups, dials)
			}
		})
	}
}

func TestAdaptorBodyLengthAndRedirectReplay(t *testing.T) {
	body := []byte(`{"input":"` + strings.Repeat("内容", 4096) + `"}`)
	for _, embed := range []bool{false, true} {
		name := "chat"
		if embed {
			name = "embed"
		}
		t.Run(name, func(t *testing.T) {
			received := make(chan []byte, 2)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, err := io.ReadAll(r.Body)
				if err != nil || r.ContentLength != int64(len(body)) || len(r.TransferEncoding) != 0 || r.Method != http.MethodPost {
					t.Errorf("body contract: length=%d encoding=%v method=%s err=%v", r.ContentLength, r.TransferEncoding, r.Method, err)
				}
				received <- got
				if r.URL.Path != "/replayed" {
					http.Redirect(w, r, "/replayed", http.StatusTemporaryRedirect)
					return
				}
				io.WriteString(w, `{}`)
			}))
			defer up.Close()
			a := NewOpenAICompat(config.Provider{Name: "p", BaseURL: up.URL}, "dummy")
			var resp *http.Response
			var err error
			if embed {
				resp, err = a.Embed(context.Background(), body)
			} else {
				resp, err = a.Chat(context.Background(), body)
			}
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || len(received) != 2 {
				t.Fatalf("redirect status=%d sends=%d", resp.StatusCode, len(received))
			}
			for i := 0; i < 2; i++ {
				if got := <-received; !bytes.Equal(got, body) {
					t.Fatalf("send %d altered body", i+1)
				}
			}
		})
	}
}
