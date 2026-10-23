package localadmin

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func shortSocket(t *testing.T) string {
	t.Helper()
	root := os.TempDir()
	if runtime.GOOS == "windows" {
		// Windows AF_UNIX needs an ASCII path on the supported test host.
		root = os.Getenv("PUBLIC")
		if root == "" {
			t.Fatal("PUBLIC is required for a short ASCII Windows socket test path")
		}
	}
	dir, err := os.MkdirTemp(root, "ga-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "admin.sock")
}

func serveSocket(t *testing.T, path string, handler http.Handler) net.Listener {
	t.Helper()
	listener, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close(); listener.Close() })
	return listener
}

func TestSocketOnlyClientAndListenerLifecycle(t *testing.T) {
	path := shortSocket(t)
	var remoteCalls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
	}))
	defer remote.Close()
	t.Setenv("HTTP_PROXY", remote.URL)
	t.Setenv("HTTPS_PROXY", remote.URL)
	listener := serveSocket(t, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-secret" {
			t.Error("local request lost authentication")
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, remote.URL, http.StatusTemporaryRedirect)
			return
		}
		io.WriteString(w, "local")
	}))
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("socket permissions: info=%v err=%v", info, err)
		}
	}
	client := Client(path, time.Second)
	defer client.CloseIdleConnections()
	for _, endpoint := range []string{"/status", "/redirect"} {
		req, _ := http.NewRequest(http.MethodGet, "http://local-admin"+endpoint, nil)
		req.Header.Set("Authorization", "Bearer local-secret")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if endpoint == "/status" && string(body) != "local" {
			t.Fatalf("wrong local response %q", body)
		}
		if endpoint == "/redirect" && resp.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("followed redirect: %s", resp.Status)
		}
	}
	if remoteCalls.Load() != 0 {
		t.Fatal("management contacted a TCP proxy or redirect target")
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("active socket was replaced")
	}
	client.CloseIdleConnections()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("closed socket was not removed: %v", err)
	}
}

func TestListenRefusesExistingAndInvalidPaths(t *testing.T) {
	path := shortSocket(t)
	if err := os.WriteFile(path, []byte("preserve me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Fatal("regular file was replaced")
	}
	if body, _ := os.ReadFile(path); string(body) != "preserve me" {
		t.Fatal("existing file was changed")
	}
	for _, invalid := range []string{"", "@abstract", "bad\x00path"} {
		if _, err := Listen(invalid); err == nil {
			t.Fatalf("accepted nonfilesystem path %q", invalid)
		}
	}
	if _, err := Listen(strings.Repeat("a", 300)); err == nil {
		t.Fatal("overlong socket path was accepted")
	}
	link := filepath.Join(filepath.Dir(path), "link.sock")
	if err := os.Symlink(path, link); err == nil {
		if _, err := Listen(link); err == nil {
			t.Fatal("symlink was replaced")
		}
		if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("symlink was changed: %v", err)
		}
	}
}

func TestSocketDefaultsAndOverride(t *testing.T) {
	t.Setenv("GUVNA_ADMIN_SOCKET", "")
	if got := SocketPath("custom-data"); got != filepath.Join("custom-data", "admin.sock") {
		t.Fatalf("custom data ignored: %s", got)
	}
	t.Setenv("GUVNA_ADMIN_SOCKET", "custom.sock")
	if got := SocketPath("ignored"); got != "custom.sock" {
		t.Fatalf("socket override ignored: %s", got)
	}
}
