// Package localadmin provides filesystem-socket-only management transport.
package localadmin

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func DefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".guvna"
	}
	return filepath.Join(home, ".guvna")
}

func SocketPath(dataDir string) string {
	if path := os.Getenv("GUVNA_ADMIN_SOCKET"); path != "" {
		return path
	}
	return filepath.Join(dataDir, "admin.sock")
}

func filesystemPath(path string) (string, error) {
	if path == "" || strings.HasPrefix(path, "@") || strings.ContainsRune(path, 0) {
		return "", fmt.Errorf("management requires a filesystem socket path")
	}
	return filepath.Abs(path)
}

// Listen refuses every existing path, including stale sockets. It never removes
// another process's socket or a user file to make room for a new listener.
func Listen(path string) (net.Listener, error) {
	path, err := filesystemPath(path)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, fmt.Errorf("management socket path already exists: %s (check for a running instance or stale socket)", path)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("management socket: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("management socket %s: %w", path, err)
	}
	// Windows AF_UNIX objects do not implement POSIX chmod; their access uses
	// the containing directory's ACL. Bearer authentication is still required.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o600); err != nil {
			listener.Close()
			return nil, fmt.Errorf("management socket permissions: %w", err)
		}
	}
	return listener, nil
}

// Client never consults proxy settings, dials TCP, or follows a redirect.
func Client(path string, timeout time.Duration) *http.Client {
	dialer := &net.Dialer{}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				path, err := filesystemPath(path)
				if err != nil {
					return nil, err
				}
				return dialer.DialContext(ctx, "unix", path)
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
