package router

import "github.com/alisa/bruvroute/internal/config"

type Step struct {
	Provider string
	Model    string
}

type Router struct {
	chains      map[string][]Step
	modelIndex  map[string]string
	defaultName string
}

func New(cfg *config.Config) *Router {
	r := &Router{
		chains:      make(map[string][]Step, len(cfg.Chains)),
		modelIndex:  make(map[string]string),
		defaultName: cfg.DefaultChain,
	}
	for _, ch := range cfg.Chains {
		steps := make([]Step, 0, len(ch.Steps))
		for _, s := range ch.Steps {
			steps = append(steps, Step{Provider: s.Provider, Model: s.Model})
			r.modelIndex[s.Model] = ch.Name
		}
		r.chains[ch.Name] = steps
	}
	return r
}

// Resolve maps a requested model name to a chain.
// Precedence: exact chain name -> any model inside a chain's steps -> default chain.
func (r *Router) Resolve(model string) (name string, steps []Step, ok bool) {
	if _, found := r.chains[model]; found {
		return model, r.chains[model], true
	}
	if chain, found := r.modelIndex[model]; found {
		return chain, r.chains[chain], true
	}
	steps, ok = r.chains[r.defaultName]
	return r.defaultName, steps, ok
}

func (r *Router) ChainNames() []string {
	names := make([]string, 0, len(r.chains))
	for name := range r.chains {
		names = append(names, name)
	}
	return names
}
