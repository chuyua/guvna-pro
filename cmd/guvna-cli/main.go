// guvna-cli — local and remote status, logs, and chain management for a
// Guvna gateway. Local: talks to http://127.0.0.1:20128 (or GUVNA_URL).
// Remote: point --url at the gateway's HTTPS endpoint (caddy) — the admin
// key travels as a Bearer token; no new port is exposed.
//
//	guvna-cli status [--url URL] [--token KEY]
//	guvna-cli logs [-n 100] [--url URL] [--token KEY]
//	guvna-cli chains [list]
//	guvna-cli chains add NAME --step provider:model [--step ...]
//	guvna-cli chains rm NAME
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultURL = "http://127.0.0.1:20128"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "status":
		err = statusCmd(os.Args[2:])
	case "logs":
		err = logsCmd(os.Args[2:])
	case "chains":
		err = chainsCmd(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(1)
	}
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "guvna-cli: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `guvna-cli — gateway status, logs, chain management

usage:
  guvna-cli status [flags]
  guvna-cli logs [-n lines] [flags]
  guvna-cli chains [list] [flags]
  guvna-cli chains add NAME --step provider:model [--step ...] [flags]
  guvna-cli chains rm NAME [flags]

flags:
  --url    gateway base URL (default: %s or $GUVNA_URL)
  --token  admin key (default: $GUVNA_ADMIN_KEY)
  --json   emit raw JSON instead of a table
`, defaultURL)
}

type commonFlags struct {
	url   string
	token string
	json  bool
}

func parseCommon(fs *flag.FlagSet, cf *commonFlags) {
	fs.StringVar(&cf.url, "url", envOr("GUVNA_URL", defaultURL), "gateway base URL")
	fs.StringVar(&cf.token, "token", os.Getenv("GUVNA_ADMIN_KEY"), "admin key (Bearer token)")
	// Keep the environment default active without displaying a credential in help.
	fs.Lookup("token").DefValue = ""
	fs.BoolVar(&cf.json, "json", false, "raw JSON output")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (cf commonFlags) get(path string, out any) error {
	return cf.do(http.MethodGet, path, nil, out)
}

// do performs an HTTP request against the gateway with the Bearer token and
// decodes a JSON response body into out (when out is non-nil).
func (cf commonFlags) do(method, path string, body any, out any) error {
	if cf.token == "" {
		return fmt.Errorf("no admin key: pass --token or set GUVNA_ADMIN_KEY")
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(cf.url, "/")+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cf.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		eb, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %s %s", method, path, resp.Status, strings.TrimSpace(string(eb)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type statusResp struct {
	Uptime    string   `json:"uptime"`
	Chains    []string `json:"chains"`
	Steps     []step   `json:"steps"`
	StepsDown int      `json:"steps_down"`
	Usage     struct {
		Requests   int            `json:"requests"`
		Streams    int            `json:"streams"`
		Errors     int            `json:"errors"`
		TokensIn   int64          `json:"tokens_in"`
		TokensOut  int64          `json:"tokens_out"`
		ByProvider []providerStat `json:"by_provider"`
	} `json:"usage"`
}

type step struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	Failures int    `json:"failures"`
	Down     bool   `json:"down"`
	DownFor  int64  `json:"down_for_ns"`
}

type providerStat struct {
	Provider  string `json:"provider"`
	Requests  int    `json:"requests"`
	Errors    int    `json:"errors"`
	TokensIn  int64  `json:"tokens_in"`
	TokensOut int64  `json:"tokens_out"`
}

func statusCmd(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	fs.Parse(args)

	var st statusResp
	if err := cf.get("/admin/status", &st); err != nil {
		return err
	}
	if cf.json {
		return printJSON(st)
	}

	fmt.Printf("Guvna %s  (uptime %s)\n", cf.url, st.Uptime)
	fmt.Printf("chains: %s\n\n", strings.Join(st.Chains, "  "))

	fmt.Printf("%-12s %-28s %8s  %s\n", "PROVIDER", "MODEL", "FAILURES", "STATE")
	for _, s := range st.Steps {
		state := "ok"
		if s.Down {
			state = fmt.Sprintf("DOWN %v", time.Duration(s.DownFor))
		}
		fmt.Printf("%-12s %-28s %8d  %s\n", s.Provider, s.Model, s.Failures, state)
	}
	if st.StepsDown > 0 {
		fmt.Printf("\n%d step(s) down — skipped in chains until cool-off expires\n", st.StepsDown)
	}

	fmt.Printf("\nUSAGE (persisted)\n")
	fmt.Printf("  requests %d   streams %d   errors %d\n", st.Usage.Requests, st.Usage.Streams, st.Usage.Errors)
	fmt.Printf("  tokens %d in / %d out\n", st.Usage.TokensIn, st.Usage.TokensOut)
	if len(st.Usage.ByProvider) > 0 {
		fmt.Printf("\n  %-12s %8s %8s %12s %12s\n", "PROVIDER", "REQUESTS", "ERRORS", "TOKENS IN", "TOKENS OUT")
		for _, p := range st.Usage.ByProvider {
			fmt.Printf("  %-12s %8d %8d %12d %12d\n", p.Provider, p.Requests, p.Errors, p.TokensIn, p.TokensOut)
		}
	}
	return nil
}

func logsCmd(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	n := fs.Int("n", 100, "number of lines to fetch (max 512)")
	fs.Parse(args)

	var out struct {
		Lines []string `json:"lines"`
	}
	if err := cf.get(fmt.Sprintf("/admin/logs?n=%d", *n), &out); err != nil {
		return err
	}
	if cf.json {
		return printJSON(out)
	}
	for _, l := range out.Lines {
		fmt.Println(l)
	}
	return nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

type chainResp struct {
	Chains []chainInfo `json:"chains"`
}

type chainInfo struct {
	Name   string      `json:"name"`
	Source string      `json:"source"`
	Steps  []chainStep `json:"steps"`
}

type chainStep struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

func chainsCmd(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return chainsListCmd(args)
	}
	switch args[0] {
	case "list":
		return chainsListCmd(args[1:])
	case "add":
		return chainsAddCmd(args[1:])
	case "rm", "remove":
		return chainsRmCmd(args[1:])
	default:
		return fmt.Errorf("unknown chains subcommand %q (want list|add|rm)", args[0])
	}
}

func chainsListCmd(args []string) error {
	fs := flag.NewFlagSet("chains", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	ordered, err := reorderArgs(fs, args)
	if err != nil {
		return err
	}
	if err := fs.Parse(ordered); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("chains list accepts no positional arguments")
	}

	var out chainResp
	if err := cf.get("/v1/chains", &out); err != nil {
		return err
	}
	if cf.json {
		return printJSON(out)
	}
	fmt.Printf("%-20s %-8s  %s\n", "NAME", "SOURCE", "STEPS")
	for _, c := range out.Chains {
		steps := make([]string, 0, len(c.Steps))
		for _, s := range c.Steps {
			steps = append(steps, s.Provider+":"+s.Model)
		}
		fmt.Printf("%-20s %-8s  %s\n", c.Name, c.Source, strings.Join(steps, " -> "))
	}
	return nil
}

// reorderArgs moves flag tokens (and their values) before positional args so
// Go's flag package, which stops at the first non-flag argument, still parses
// commands like "chains add mychain --step a:b".
func reorderArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if a == "-" || !strings.HasPrefix(a, "-") {
			pos = append(pos, a)
			continue
		}
		name, _, hasValue := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-"), "=")
		f := fs.Lookup(name)
		if f == nil {
			if name == "h" || name == "help" {
				flags = append(flags, a)
				continue
			}
			return nil, fmt.Errorf("flag provided but not defined: -%s", name)
		}
		flags = append(flags, a)
		if hasValue {
			continue
		}
		if boolean, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && boolean.IsBoolFlag() {
			continue
		}
		if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
			return nil, fmt.Errorf("flag needs an argument: -%s", name)
		}
		i++
		flags = append(flags, args[i])
	}
	// Preserve positional values after an explicit separator, including a leading
	// dash, instead of allowing flag.Parse to reinterpret them as options.
	return append(append(flags, "--"), pos...), nil
}

func chainsAddCmd(args []string) error {
	var steps []chainStep
	fs := flag.NewFlagSet("chains add", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	fs.Func("step", "provider:model step (repeat for fallback order)", func(value string) error {
		p, m, found := strings.Cut(value, ":")
		if !found || p == "" || m == "" {
			return fmt.Errorf("want provider:model")
		}
		steps = append(steps, chainStep{Provider: p, Model: m})
		return nil
	})
	ordered, err := reorderArgs(fs, args)
	if err != nil {
		return err
	}
	if err := fs.Parse(ordered); err != nil {
		return err
	}
	if len(steps) == 0 {
		return fmt.Errorf("chains add needs at least one --step")
	}

	name := fs.Arg(0)
	if name == "" || fs.NArg() != 1 {
		return fmt.Errorf("chains add NAME --step provider:model [--step ...]")
	}
	var created chainInfo
	if err := cf.do(http.MethodPost, "/v1/chains", map[string]any{"name": name, "steps": steps}, &created); err != nil {
		return err
	}
	if cf.json {
		return printJSON(created)
	}
	fmt.Printf("created chain %q (%d steps, persisted)\n", created.Name, len(created.Steps))
	return nil
}

func chainsRmCmd(args []string) error {
	fs := flag.NewFlagSet("chains rm", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	ordered, err := reorderArgs(fs, args)
	if err != nil {
		return err
	}
	if err := fs.Parse(ordered); err != nil {
		return err
	}

	name := fs.Arg(0)
	if name == "" || fs.NArg() != 1 {
		return fmt.Errorf("chains rm NAME")
	}
	if err := cf.do(http.MethodDelete, "/v1/chains/"+name, nil, nil); err != nil {
		return err
	}
	fmt.Printf("removed chain %q\n", name)
	return nil
}
