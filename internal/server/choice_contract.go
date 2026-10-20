package server

import "encoding/json"

// jsonFieldPresence records top-level keys without copying their potentially
// large values. Parameter defaults use exact key names, as mergeParams does.
type jsonFieldPresence struct{}

func (*jsonFieldPresence) UnmarshalJSON([]byte) error { return nil }

// withChoiceDefault returns a per-step request description. Only the n default
// is needed for response validation; the original body is still forwarded by
// tryStepAdaptor. In particular, an explicit null or zero is not a missing key.
func withChoiceDefault(req chatRequest, body []byte, params map[string]any) chatRequest {
	value, ok := params["n"]
	if !ok {
		return req
	}
	var fields map[string]jsonFieldPresence
	if json.Unmarshal(body, &fields) != nil {
		return req
	}
	if _, present := fields["n"]; present {
		return req
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return req // mergeParams also skips defaults that cannot be marshaled.
	}
	var n int
	if json.Unmarshal(encoded, &n) == nil {
		req.N = n
	}
	return req
}
