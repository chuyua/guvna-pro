package config

import (
	"errors"
	"strings"
	"testing"
)

type secretParamMarshaler struct{}

func (secretParamMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New("private-parameter-value")
}

func TestLoadRejectsNonJSONStepParams(t *testing.T) {
	for _, params := range []string{
		"temperature: .nan",
		"temperature: .inf",
		"temperature: -.inf",
		"custom: {1: original}",
	} {
		t.Run(params, func(t *testing.T) {
			cfg := `providers:
  - name: available
    type: openai
    base_url: https://example.invalid
    key_env: TEST_KEY
chains:
  - name: main
    steps:
      - provider: available
        model: original
        params:
          ` + params + "\n"
			_, err := Load(writeTemp(t, cfg))
			if err == nil || !strings.Contains(err.Error(), `chain "main": step for "available"`) || !strings.Contains(err.Error(), "cannot be encoded as JSON") {
				t.Fatalf("unencodable config was not rejected with context: %v", err)
			}
		})
	}
}

func TestStepParamsEncodingErrorDoesNotExposeValues(t *testing.T) {
	err := ValidateStepParams(map[string]any{"custom": secretParamMarshaler{}})
	if err == nil || !strings.Contains(err.Error(), `"custom"`) || strings.Contains(err.Error(), "private-parameter-value") {
		t.Fatalf("unexpected parameter error: %v", err)
	}
}
