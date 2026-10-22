package vision

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestRegistryReadErrorsAreNotSilenced(t *testing.T) {
	old := readFile
	t.Cleanup(func() { readFile = old })
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"missing", os.ErrNotExist},
		{"permission", fmt.Errorf("read denied: %w", os.ErrPermission)},
		{"io", errors.New("device I/O failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readFile = func(string) ([]byte, error) { return nil, tc.err }
			r, err := Load("vision-registry.yaml")
			if errors.Is(tc.err, os.ErrNotExist) {
				if err != nil || r == nil || r.Has("deepseek-v4-flash") {
					t.Fatalf("missing registry should use builtins: r=%v err=%v", r, err)
				}
			} else if !errors.Is(err, tc.err) || r != nil {
				t.Fatalf("read failure must surface: r=%v err=%v", r, err)
			}
		})
	}
}
