// Package vision provides the compiled-in text-only model table.
package vision

var builtIn = Registry{
	Models: map[string]bool{
		// amd (Radeon Cloud). DeepSeek on amd is text-only; the Qwen and GLM
		// entries are absent because they have not been observed rejecting
		// images, and absence means optimistic.
		"DeepSeek-V4-Flash":   false,
		"DeepSeek-V4.1-Flash": false,

		// nvidia (integrate.api.nvidia.com). Keys are the upstream-verbatim
		// ids, so both the namespaced and bare forms are listed.
		"nvidia/deepseek-ai/deepseek-v4.1-flash": false,
		"deepseek-v4.1-flash":                    false,
		"nvidia/deepseek-ai/deepseek-v4-flash":   false,
		"deepseek-v4-flash":                      false,

		// xkiro. The bare tails of the nvidia deepseek entries overlap here
		// ("deepseek-v4-flash"), which is harmless: both upstreams are false.
		"xkiro/deepseek/deepseek-v4-flash":        false,
		"deepseek-v4-pro":                         false,
		"xkiro/deepseek/deepseek-v4-pro":          false,
		"deepseek-v4.1-flash:free":                false,
		"xkiro/deepseek/deepseek-v4.1-flash:free": false,

		// mistralai. Listed under its own namespace rather than nvidia's —
		// the nvidia mirror tail "mistral-nemotron" still resolves here.
		"mistralai/mistral-nemotron": false,
		"mistral-nemotron":           false,
	},
}
