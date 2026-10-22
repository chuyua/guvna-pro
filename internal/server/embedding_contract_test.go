package server

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestEmbeddingRequestContract(t *testing.T) {
	defaults := map[string]any{"input": []string{"a", "b"}, "dimensions": 2, "encoding_format": "base64"}
	for _, tc := range []struct {
		name, body string
		params     map[string]any
		want       embeddingContract
	}{
		{"text", `{"input":"hello"}`, nil, embeddingContract{count: 1}},
		{"text batch", `{"input":["a","b"]}`, nil, embeddingContract{count: 2}},
		{"tokens", `{"input":[0,12,345]}`, nil, embeddingContract{count: 1}},
		{"token batch", `{"input":[[0,12],[345]]}`, nil, embeddingContract{count: 2}},
		{"whitespace tokens", "{\"input\": [ \n 0 , 12 \t ]}", nil, embeddingContract{count: 1}},
		{"empty text", `{"input":""}`, nil, embeddingContract{}},
		{"empty batch", `{"input":[]}`, nil, embeddingContract{}},
		{"empty token input", `{"input":[[]]}`, nil, embeddingContract{}},
		{"empty batch text", `{"input":["a",""]}`, nil, embeddingContract{}},
		{"mixed batch", `{"input":["a",1]}`, nil, embeddingContract{}},
		{"mixed tokens", `{"input":[1,[2]]}`, nil, embeddingContract{}},
		{"mixed nested tokens", `{"input":[[1],2]}`, nil, embeddingContract{}},
		{"deep nested tokens", `{"input":[[[1]]]}`, nil, embeddingContract{}},
		{"null batch text", `{"input":["a",null]}`, nil, embeddingContract{}},
		{"null batch tokens", `{"input":[[1],null]}`, nil, embeddingContract{}},
		{"boolean token", `{"input":[true]}`, nil, embeddingContract{}},
		{"null token", `{"input":[1,null]}`, nil, embeddingContract{}},
		{"negative token", `{"input":[-1]}`, nil, embeddingContract{}},
		{"fractional token", `{"input":[1.5]}`, nil, embeddingContract{}},
		{"exponent token", `{"input":[1e0]}`, nil, embeddingContract{}},
		{"overflow token", `{"input":[9223372036854775808]}`, nil, embeddingContract{}},
		{"extension input", `{"input":{"text":"a"}}`, nil, embeddingContract{}},
		{"defaults", `{}`, defaults, embeddingContract{2, 2, "base64"}},
		{"explicit wins", `{"input":"a","dimensions":3,"encoding_format":"float"}`, defaults, embeddingContract{1, 3, "float"}},
		{"explicit nulls", `{"input":null,"dimensions":null,"encoding_format":null}`, defaults, embeddingContract{}},
		{"explicit zero", `{"input":[],"dimensions":0,"encoding_format":"other"}`, defaults, embeddingContract{}},
		{"case exact", `{"INPUT":"a","DIMENSIONS":3,"ENCODING_FORMAT":"float"}`, defaults, embeddingContract{2, 2, "base64"}},
		{"escaped keys", `{"inpu\u0074":"a","dimens\u0069ons":3,"encoding_\u0066ormat":"float"}`, defaults, embeddingContract{1, 3, "float"}},
		{"duplicate keys", `{"input":["a","b"],"input":"c","dimensions":2,"dimensions":3}`, nil, embeddingContract{1, 3, ""}},
		{"duplicate null", `{"input":"c","input":null,"dimensions":3,"dimensions":null}`, defaults, embeddingContract{format: "base64"}},
		{"nested fields", `{"extra":{"input":"a","dimensions":3}}`, defaults, embeddingContract{2, 2, "base64"}},
		{"invalid dimensions", `{"input":"a","dimensions":"2"}`, nil, embeddingContract{count: 1}},
		{"negative dimensions", `{"dimensions":-1}`, nil, embeddingContract{}},
		{"fractional dimensions", `{"dimensions":1.5}`, nil, embeddingContract{}},
		{"unmarshalable defaults", `{}`, map[string]any{"input": func() {}, "dimensions": func() {}, "encoding_format": func() {}}, embeddingContract{}},
		{"numeric defaults", `{}`, map[string]any{"input": []any{float64(1), float64(2)}, "dimensions": float64(2)}, embeddingContract{1, 2, ""}},
		{"malformed request", `{"input":`, defaults, embeddingContract{}},
		{"null request", `null`, defaults, embeddingContract{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := embeddingRequestContract([]byte(tc.body), tc.params)
			if got != tc.want {
				t.Fatalf("contract=%+v, want %+v", got, tc.want)
			}
			if tc.name == "malformed request" || tc.name == "null request" {
				return
			}
			forwarded, err := mergeParams([]byte(tc.body), tc.params)
			if tc.name == "unmarshalable defaults" {
				if err == nil {
					t.Fatal("unencodable defaults should fail before forwarding")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if actual := embeddingRequestContract(forwarded, nil); actual != got {
				t.Fatalf("contract=%+v differs from forwarded payload contract=%+v", got, actual)
			}
		})
	}
}

func BenchmarkEmbeddingRequestContractTokens(b *testing.B) {
	body := []byte(`{"input":[` + strings.Repeat("0,", (1<<19)-1) + `0]}`)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := embeddingRequestContract(body, nil); got.count != 1 {
			b.Fatalf("token input count=%d, want 1", got.count)
		}
	}
}

func TestEmbeddingResponseContract(t *testing.T) {
	encoded := func(values ...float32) string {
		bytes := make([]byte, len(values)*4)
		for i, value := range values {
			binary.LittleEndian.PutUint32(bytes[i*4:], math.Float32bits(value))
		}
		return `"` + base64.StdEncoding.EncodeToString(bytes) + `"`
	}
	one := encoded(1)
	two := encoded(1, -2)
	batch := embeddingContract{count: 2, dimensions: 2}
	for _, tc := range []struct {
		name, data string
		contract   embeddingContract
		failed     bool
	}{
		{"single legacy", `{"embedding":[1,2]}`, embeddingContract{count: 1}, false},
		{"single index", `{"index":0,"embedding":[1,2]}`, embeddingContract{count: 1}, false},
		{"single null index", `{"index":null,"embedding":[1,2]}`, embeddingContract{count: 1}, true},
		{"single wrong index", `{"index":1,"embedding":[1,2]}`, embeddingContract{}, true},
		{"batch complete reversed", `{"index":1,"embedding":[1,2]},{"index":0,"embedding":[3,4]}`, batch, false},
		{"batch missing result", `{"index":0,"embedding":[1,2]}`, batch, true},
		{"batch extra result", `{"index":0,"embedding":[1,2]},{"index":1,"embedding":[3,4]}`, embeddingContract{count: 1}, true},
		{"batch missing index", `{"embedding":[1,2]},{"index":1,"embedding":[3,4]}`, batch, true},
		{"batch duplicate index", `{"index":0,"embedding":[1,2]},{"index":0,"embedding":[3,4]}`, batch, true},
		{"batch negative index", `{"index":-1,"embedding":[1,2]},{"index":1,"embedding":[3,4]}`, batch, true},
		{"batch out of range", `{"index":0,"embedding":[1,2]},{"index":2,"embedding":[3,4]}`, batch, true},
		{"batch fractional index", `{"index":0.5,"embedding":[1,2]},{"index":1,"embedding":[3,4]}`, batch, true},
		{"batch string index", `{"index":"0","embedding":[1,2]},{"index":1,"embedding":[3,4]}`, batch, true},
		{"batch null index", `{"index":null,"embedding":[1,2]},{"index":1,"embedding":[3,4]}`, batch, true},
		{"unequal dimensions", `{"index":0,"embedding":[1]},{"index":1,"embedding":[3,4]}`, embeddingContract{}, true},
		{"requested dimensions", `{"embedding":[1]}`, embeddingContract{dimensions: 2}, true},
		{"unknown dimensions", `{"embedding":[1,2,3]}`, embeddingContract{}, false},
		{"base64", `{"embedding":` + two + `}`, embeddingContract{dimensions: 2, format: "base64"}, false},
		{"base64 legacy", `{"embedding":` + two + `}`, embeddingContract{}, false},
		{"base64 dimension mismatch", `{"embedding":` + one + `}`, embeddingContract{dimensions: 2}, true},
		{"base64 batch mismatch", `{"index":0,"embedding":` + one + `},{"index":1,"embedding":` + two + `}`, embeddingContract{}, true},
		{"base64 infinity", `{"embedding":` + encoded(float32(math.Inf(1))) + `}`, embeddingContract{}, true},
		{"base64 nan", `{"embedding":` + encoded(float32(math.NaN())) + `}`, embeddingContract{}, true},
		{"base64 empty", `{"embedding":""}`, embeddingContract{}, true},
		{"base64 partial float", `{"embedding":"AAAA"}`, embeddingContract{}, true},
		{"base64 invalid bits", `{"embedding":"AAAAAB=="}`, embeddingContract{}, true},
		{"float format", `{"embedding":[1,2]}`, embeddingContract{format: "float"}, false},
		{"float requested base64", `{"embedding":[1,2]}`, embeddingContract{format: "base64"}, true},
		{"base64 requested float", `{"embedding":` + two + `}`, embeddingContract{format: "float"}, true},
		{"mixed formats", `{"index":0,"embedding":[1,2]},{"index":1,"embedding":` + two + `}`, batch, true},
		{"numeric empty", `{"embedding":[]}`, embeddingContract{}, true},
		{"numeric null", `{"embedding":[1,null]}`, embeddingContract{}, true},
		{"numeric string", `{"embedding":[1,"2"]}`, embeddingContract{}, true},
		{"numeric overflow", `{"embedding":[1e999]}`, embeddingContract{}, true},
		{"no vector", `{"index":0}`, embeddingContract{}, true},
		{"null entry", `null`, embeddingContract{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEmbeddings([]byte(`{"data":[`+tc.data+`]}`), tc.contract)
			if (err != nil) != tc.failed {
				t.Fatalf("error=%v, want failure=%v", err, tc.failed)
			}
		})
	}
	for _, body := range []string{`null`, `{}`, `{"data":[]}`, `{"data":[{"embedding":[1]}],"error":{"message":"bad"}}`, `{"data":`} {
		if err := validateEmbeddings([]byte(body)); err == nil {
			t.Errorf("accepted unusable body %s", body)
		}
	}
	// Keep the original optional-contract API usable by existing callers.
	if err := validateEmbeddings(json.RawMessage(`{"data":[{"embedding":[1]}]}`)); err != nil {
		t.Fatalf("legacy response rejected: %v", err)
	}
}
