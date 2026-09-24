package vision

import "testing"

func TestHasKnownTextOnlyModels(t *testing.T) {
	r := mustLoad("")
	// Full upstream ids must read false...
	for _, m := range []string{
		"nvidia/deepseek-ai/deepseek-v4.1-flash",
		"nvidia/deepseek-ai/deepseek-v4-flash",
		"xkiro/deepseek/deepseek-v4-flash",
		"xkiro/deepseek/deepseek-v4-pro",
		"xkiro/deepseek/deepseek-v4.1-flash:free",
		"mistralai/mistral-nemotron",
	} {
		if r.Has(m) {
			t.Fatalf("Has(%q) = true, want false (known text-only)", m)
		}
	}
	// ...and so must the bare-tail forms the gateway sees after namespace
	// stripping or from upstreams that publish bare ids.
	for _, m := range []string{
		"DeepSeek-V4.1-Flash",
		"DeepSeek-V4-Flash",
		"deepseek-v4.1-flash",
		"deepseek-v4-flash",
		"deepseek-v4-pro",
		"deepseek-v4.1-flash:free",
		"mistral-nemotron",
	} {
		if r.Has(m) {
			t.Fatalf("Has(%q) = true, want false (known text-only tail)", m)
		}
	}
}

// The whole point of the negative-list design: an unknown model must not read
// as false, or an image-bearing request would 404 whenever the pool is all
// models this table has never heard of.
func TestHasUnknownModelDefaultsTrue(t *testing.T) {
	r := mustLoad("")
	for _, m := range []string{"", "unknown", "some-brand-new-model", "qwen/qwen3.8-vl"} {
		if !r.Has(m) {
			t.Fatalf("Has(%q) = false; unknown models must default optimistic", m)
		}
	}
	var nilRegistry *Registry
	if !nilRegistry.Has("anything") {
		t.Fatal("nil registry must default optimistic")
	}
}

func TestHasTailOfNamespacedModel(t *testing.T) {
	r := mustLoad("")
	// deepseek-v4-flash is registered under both the nvidia and xkiro
	// namespaces; either full id must resolve to the bare false entry.
	if r.Has("nvidia/deepseek-ai/deepseek-v4-flash") || r.Has("xkiro/deepseek/deepseek-v4-flash") {
		t.Fatal("namespaced text-only models must resolve via tail fallback")
	}
}

func TestLoadEmptyPathReturnsBuiltins(t *testing.T) {
	r, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if r.Has("deepseek-v4-flash") {
		t.Fatal("empty path must return builtins")
	}
	r2, err := Load("/nonexistent/path/to/registry.yaml")
	if err != nil {
		t.Fatalf("Load(missing) must not error: %v", err)
	}
	if r2.Has("mistral-nemotron") {
		t.Fatal("missing file must fall back to builtins")
	}
}

func TestLoadMergesExternalOverBuiltins(t *testing.T) {
	old := readFile
	defer func() { readFile = old }()
	readFile = func(string) ([]byte, error) {
		// Flip a built-in text-only entry optimistic and add a new negative —
		// both directions of the override must stick.
		return []byte("models:\n  deepseek-v4-flash: true\n  brand-new-text-model: false\n"), nil
	}
	r, err := Load("external.yaml")
	if err != nil {
		t.Fatalf("Load(external): %v", err)
	}
	if !r.Has("deepseek-v4-flash") {
		t.Fatal("external override lost: deepseek-v4-flash should read true")
	}
	if r.Has("brand-new-text-model") {
		t.Fatal("external addition lost: brand-new-text-model should read false")
	}
	if r.Has("mistral-nemotron") {
		t.Fatal("builtin dropped by partial external file")
	}
}

func TestRequestHasImagesStringContent(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hello world"}]}`)
	if RequestHasImages(body) {
		t.Fatal("string content must not read as image-bearing")
	}
}

func TestRequestHasImagesTextArray(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"text","text":"there"}]}]}`)
	if RequestHasImages(body) {
		t.Fatal("text-only parts must not read as image-bearing")
	}
}

func TestRequestHasImagesDetectsImageParts(t *testing.T) {
	cases := map[string]string{
		"image_url":   `{"messages":[{"content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"https://x/y.png"}}]}]}`,
		"image":       `{"messages":[{"content":[{"type":"image","source":"base64"}]}]}`,
		"input_image": `{"messages":[{"content":[{"type":"input_image","image_url":"https://x/y.png"}]}]}`,
	}
	for name, body := range cases {
		if !RequestHasImages([]byte(body)) {
			t.Fatalf("%s part not detected: RequestHasImages = false, want true", name)
		}
	}
	// Images in an earlier message count too, not just the last one.
	multi := `{"messages":[{"content":[{"type":"image_url","image_url":{"url":"https://x/y.png"}}]},{"content":"and this?"}]}`
	if !RequestHasImages([]byte(multi)) {
		t.Fatal("image in an earlier message must be detected")
	}
}

func TestRequestHasImagesEmptyOrBrokenBody(t *testing.T) {
	for _, body := range [][]byte{nil, {}, []byte("not json"), []byte(`{"messages":`), []byte(`[]`)} {
		if RequestHasImages(body) {
			t.Fatalf("RequestHasImages(%q) = true; broken bodies must read false", body)
		}
	}
}

func mustLoad(path string) *Registry {
	r, err := Load(path)
	if err != nil {
		panic(err)
	}
	return r
}
