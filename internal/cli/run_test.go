package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRunStatusAndLogsRetainAuthenticationAndJSON(t *testing.T) {
	for _, tc := range []struct {
		args []string
		path string
	}{
		{[]string{"status", "--json"}, "/admin/status"},
		{[]string{"logs", "-n", "7", "--json"}, "/admin/logs?n=7"},
	} {
		t.Run(tc.args[0], func(t *testing.T) {
			var requests atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method != http.MethodGet || r.URL.RequestURI() != tc.path || r.Header.Get("Authorization") != "Bearer env-admin" {
					t.Errorf("request changed: %s %s auth=%q", r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"))
				}
				io.WriteString(w, `{"lines":["one","two"],"future_field":true}`)
			}))
			defer gateway.Close()
			t.Setenv("GUVNA_URL", gateway.URL)
			t.Setenv("GUVNA_ADMIN_KEY", "env-admin")
			var output bytes.Buffer
			if err := Run(tc.args, &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 || !json.Valid(output.Bytes()) {
				t.Fatalf("requests=%d output=%q", requests.Load(), output.String())
			}
		})
	}
}

func TestRunHelpAndInvalidFlagsNeverSendRequests(t *testing.T) {
	for _, tc := range []struct {
		args []string
		help bool
	}{
		{[]string{"status", "--help"}, true},
		{[]string{"logs", "--help"}, true},
		{[]string{"chains", "list", "--help"}, true},
		{[]string{"chains", "add", "--help"}, true},
		{[]string{"chains", "rm", "--help"}, true},
		{[]string{"status", "--url"}, false},
		{[]string{"status", "--token", "--json"}, false},
		{[]string{"status", "unexpected"}, false},
		{[]string{"logs", "--unknown"}, false},
		{[]string{"logs", "-n", "not-a-number"}, false},
		{[]string{"logs", "unexpected"}, false},
		{[]string{"bogus"}, false},
		{nil, false},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			gateway, requests := cliTestGateway(t)
			t.Setenv("GUVNA_URL", gateway.URL)
			t.Setenv("GUVNA_ADMIN_KEY", "private-help-token")
			var stderr bytes.Buffer
			err := Run(tc.args, io.Discard, &stderr)
			if err == nil || errors.Is(err, flag.ErrHelp) != tc.help {
				t.Fatalf("err=%v help=%v", err, tc.help)
			}
			if strings.Contains(stderr.String(), "private-help-token") || strings.Contains(err.Error(), "private-help-token") {
				t.Fatal("argument/help handling exposed environment credential")
			}
			if tc.help && !strings.Contains(stderr.String(), "-token") {
				t.Fatalf("help output missing flags: %q", stderr.String())
			}
			select {
			case req := <-requests:
				t.Fatalf("invalid/help invocation sent a request: %+v", req)
			default:
			}
		})
	}
}
