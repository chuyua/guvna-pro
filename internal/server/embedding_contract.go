package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
)

// embeddingContract describes only constraints established by the effective
// request. Unknown or invalid provider-specific request values remain upstream's
// responsibility; in particular, no model's default vector size is assumed.
type embeddingContract struct {
	count      int
	dimensions int
	format     string
}

// embeddingRequestContract follows mergeParams' exact-key, fill-missing
// defaults. An explicit null or zero prevents the corresponding step default.
func embeddingRequestContract(body []byte, params map[string]any) embeddingContract {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return embeddingContract{}
	}
	field := func(name string) json.RawMessage {
		if value, present := fields[name]; present {
			return value
		}
		if value, present := params[name]; present {
			encoded, err := json.Marshal(value)
			if err == nil {
				return encoded
			}
		}
		return nil
	}
	contract := embeddingContract{count: embeddingInputCount(field("input"))}
	if raw := field("dimensions"); !isJSONNull(raw) {
		var dimensions int
		if json.Unmarshal(raw, &dimensions) == nil && dimensions > 0 {
			contract.dimensions = dimensions
		}
	}
	var format string
	if json.Unmarshal(field("encoding_format"), &format) == nil && (format == "float" || format == "base64") {
		contract.format = format
	}
	return contract
}

// embeddingInputCount recognizes text and token inputs without interpreting
// arbitrary provider extensions as a batch. Zero means the count is unknown.
func embeddingInputCount(raw json.RawMessage) int {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0
	}
	if raw[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) == nil && text != "" {
			return 1
		}
		return 0
	}
	if raw[0] != '[' {
		return 0
	}
	first := bytes.TrimSpace(raw[1:])
	if len(first) == 0 {
		return 0
	}
	if first[0] == '"' {
		var texts []string
		if json.Unmarshal(raw, &texts) != nil {
			return 0
		}
		for _, text := range texts {
			if text == "" {
				return 0
			}
		}
		return len(texts)
	}
	if first[0] == '[' {
		var inputs [][]embeddingToken
		if json.Unmarshal(raw, &inputs) != nil {
			return 0
		}
		for _, tokens := range inputs {
			if len(tokens) == 0 {
				return 0
			}
		}
		return len(inputs)
	}
	var tokens []embeddingToken
	if json.Unmarshal(raw, &tokens) == nil && len(tokens) > 0 {
		return 1
	}
	return 0
}

// A token's value is irrelevant to response count. A zero-size element validates
// each integer without retaining a RawMessage and allocation for every token.
type embeddingToken struct{}

func (*embeddingToken) UnmarshalJSON(raw []byte) error {
	token, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || token < 0 {
		return errors.New("invalid embedding input token")
	}
	return nil
}
