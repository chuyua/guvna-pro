// Package ctxsize provides the compiled-in context-length table.
package ctxsize

var builtIn = Registry{
	DefaultContext: DefaultContext,
	Models: map[string]int{
		// amd (Radeon Cloud). Qwen3.8-27B and Qwen3.8-Flash-Next are the only two
		// amd models that have returned 200 from this host; DeepSeek and GLM on
		// amd time out at the header stage, so they never get far enough to care
		// about context length.
		"Qwen3.8-27B":         131072,
		"Qwen3.8-Flash-Next":  131072,
		"DeepSeek-V4-Flash":   163840,
		"DeepSeek-V4.1-Flash": 163840,
		"GLM-5.3-Flash":       204800,
		"GLM-5.3":             204800,
		"MiMo-V2.6-Flash":     65536,
		"MiniCPM5-2B":         32768,
		"MinerU2.5-Pro":       131072,

		// nvidia (integrate.api.nvidia.com). Keys are the upstream-verbatim ids,
		// so both the namespaced and bare forms are listed.
		"nvidia/nemotron-3-ultra-550b-a55b":             524288,
		"nemotron-3-ultra-550b-a55b":                    524288,
		"nvidia/nemotron-3-super-120b-a12b":             262144,
		"nemotron-3-super-120b-a12b":                    262144,
		"nvidia/nemotron-3.5-lightning-30b-a3b":         262144,
		"nemotron-3.5-lightning-30b-a3b":                262144,
		"nvidia/nemotron-3-nano-omni-30b-a3b-reasoning": 128000,
		"nemotron-3-nano-omni-30b-a3b-reasoning":        128000,
		"nvidia/mistralai/mistral-nemotron":             262144,
		"mistral-nemotron":                              262144,
		"nvidia/moonshotai/kimi-k3":                     262144,
		"kimi-k3":                                       262144,
		"nvidia/z-ai/glm-5.3":                           204800,
		"glm-5.3":                                       204800,
		"nvidia/z-ai/glm-5.3-flash":                     204800,
		"glm-5.3-flash":                                 204800,
		"nvidia/deepseek-ai/deepseek-v4.1-flash":        163840,
		"deepseek-v4.1-flash":                           163840,
		"deepseek-v4-flash":                             163840,

		// kira — the only free endpoint that returns 402 when its daily token
		// quota is exhausted, which is effectively every observation of it.
		"ling-3.0-flash": 32768,
	},
}
