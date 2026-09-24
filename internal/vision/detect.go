package vision

import "encoding/json"

type message struct {
	Content json.RawMessage `json:"content"`
}

type contentPart struct {
	Type string `json:"type"`
}

// RequestHasImages reports whether an OpenAI chat-completion body carries any
// image content part. It only needs the shape of the request, not its
// semantics, so it decodes content as raw JSON and inspects part types.
//
// Every failure mode returns false. The result only ever narrows the /v1/auto
// candidate pool, so a false negative costs one 400 from an optimistic
// registry entry, while a false positive would silently exclude models that
// could have served the request.
func RequestHasImages(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var req struct {
		Messages []message `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	for _, m := range req.Messages {
		if len(m.Content) == 0 {
			continue
		}
		// String content fails this unmarshal and is skipped — that is the
		// common text-only case, not an error worth reporting.
		var parts []contentPart
		if err := json.Unmarshal(m.Content, &parts); err != nil {
			continue
		}
		for _, p := range parts {
			switch p.Type {
			case "image_url", "image", "input_image":
				return true
			}
		}
	}
	return false
}
