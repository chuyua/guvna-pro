// Package cli provides local status, logs and chain management over a
// filesystem socket. Management never connects to the public HTTP listener.
//
//	guvna status [--scoring] [--socket PATH] [--token KEY]
//	guvna logs [-n 100] [--socket PATH] [--token KEY]
//	guvna chains [list]
//	guvna chains add NAME --step provider:model [--step ...]
//	guvna chains rm NAME
package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/localadmin"
)

// Run executes a management command against an already-running gateway.
// It never starts a server or exits the process.
func Run(args []string, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	c := command{out: stdout, errOut: stderr}
	if len(args) == 0 {
		return fmt.Errorf("management command required (want status|logs|chains)")
	}
	switch args[0] {
	case "status":
		return c.statusCmd(args[1:])
	case "logs":
		return c.logsCmd(args[1:])
	case "chains":
		return c.chainsCmd(args[1:])
	default:
		return fmt.Errorf("unknown management command %q (want status|logs|chains)", args[0])
	}
}

type command struct {
	out    io.Writer
	errOut io.Writer
}

func (c command) parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(c.errOut)
	ordered, err := reorderArgs(fs, args)
	if err != nil {
		return err
	}
	return fs.Parse(ordered)
}

type commonFlags struct {
	socket string
	token  string
	json   bool
}

func parseCommon(fs *flag.FlagSet, cf *commonFlags) {
	fs.StringVar(&cf.socket, "socket", localadmin.SocketPath(localadmin.DefaultDataDir()), "local management socket path ($GUVNA_ADMIN_SOCKET)")
	fs.StringVar(&cf.token, "token", os.Getenv("GUVNA_ADMIN_KEY"), "admin key (Bearer token)")
	// Keep the environment default active without displaying a credential in help.
	fs.Lookup("token").DefValue = ""
	fs.BoolVar(&cf.json, "json", false, "raw JSON output")
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
	req, err := http.NewRequest(method, "http://local-admin"+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cf.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := localadmin.Client(cf.socket, 10*time.Second)
	defer client.CloseIdleConnections()
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
	Telemetry struct {
		FlushesOK      uint64 `json:"flushes_ok"`
		FlushesFailed  uint64 `json:"flushes_failed"`
		RetriedEvents  uint64 `json:"retried_events"`
		DroppedEvents  uint64 `json:"dropped_events"`
		BufferLen      int    `json:"buffer_len"`
		InflightEvents int    `json:"inflight_events"`
	} `json:"telemetry"`
	UsageError string `json:"usage_error,omitempty"`
	Usage      struct {
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

func (c command) statusCmd(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	scoring := fs.Bool("scoring", false, "show passive Auto and provider observations")
	since := fs.String("since", "", "inclusive RFC3339Nano observation window start (with --scoring)")
	until := fs.String("until", "", "exclusive RFC3339Nano observation window end (with --scoring)")
	if err := c.parse(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s accepts no positional arguments", fs.Name())
	}
	if *scoring {
		return c.scoringStatus(cf, *since, *until)
	}
	if *since != "" || *until != "" {
		return fmt.Errorf("--since and --until require --scoring")
	}

	var raw json.RawMessage
	if err := cf.get("/admin/status", &raw); err != nil {
		return err
	}
	if cf.json {
		return c.printJSON(raw)
	}
	var st statusResp
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}

	fmt.Fprintf(c.out, "Guvna local %s  (uptime %s)\n", cf.socket, st.Uptime)
	fmt.Fprintf(c.out, "chains: %s\n\n", strings.Join(st.Chains, "  "))

	fmt.Fprintf(c.out, "%-12s %-28s %8s  %s\n", "PROVIDER", "MODEL", "FAILURES", "STATE")
	for _, s := range st.Steps {
		state := "ok"
		if s.Down {
			state = fmt.Sprintf("DOWN %v", time.Duration(s.DownFor))
		}
		fmt.Fprintf(c.out, "%-12s %-28s %8d  %s\n", s.Provider, s.Model, s.Failures, state)
	}
	if st.StepsDown > 0 {
		fmt.Fprintf(c.out, "\n%d step(s) down — skipped in chains until cool-off expires\n", st.StepsDown)
	}

	fmt.Fprintf(c.out, "\nUSAGE (persisted)\n")
	if st.UsageError != "" {
		fmt.Fprintf(c.out, "  unavailable: %s\n", st.UsageError)
	}
	fmt.Fprintf(c.out, "  requests %d   streams %d   errors %d\n", st.Usage.Requests, st.Usage.Streams, st.Usage.Errors)
	fmt.Fprintf(c.out, "  tokens %d in / %d out\n", st.Usage.TokensIn, st.Usage.TokensOut)
	if len(st.Usage.ByProvider) > 0 {
		fmt.Fprintf(c.out, "\n  %-12s %8s %8s %12s %12s\n", "PROVIDER", "REQUESTS", "ERRORS", "TOKENS IN", "TOKENS OUT")
		for _, p := range st.Usage.ByProvider {
			fmt.Fprintf(c.out, "  %-12s %8d %8d %12d %12d\n", p.Provider, p.Requests, p.Errors, p.TokensIn, p.TokensOut)
		}
	}
	fmt.Fprintf(c.out, "\nTELEMETRY\n  buffer %d   in-flight %d   dropped %d\n", st.Telemetry.BufferLen, st.Telemetry.InflightEvents, st.Telemetry.DroppedEvents)
	fmt.Fprintf(c.out, "  flushes %d ok / %d failed   retained for retry %d\n", st.Telemetry.FlushesOK, st.Telemetry.FlushesFailed, st.Telemetry.RetriedEvents)
	return nil
}

func (c command) logsCmd(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	n := fs.Int("n", 100, "number of lines to fetch (max 512)")
	if err := c.parse(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("%s accepts no positional arguments", fs.Name())
	}

	var out struct {
		Lines []string `json:"lines"`
	}
	if err := cf.get(fmt.Sprintf("/admin/logs?n=%d", *n), &out); err != nil {
		return err
	}
	if cf.json {
		return c.printJSON(out)
	}
	for _, l := range out.Lines {
		fmt.Fprintln(c.out, l)
	}
	return nil
}

func (c command) printJSON(v any) error {
	enc := json.NewEncoder(c.out)
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

func (c command) chainsCmd(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return c.chainsListCmd(args)
	}
	switch args[0] {
	case "list":
		return c.chainsListCmd(args[1:])
	case "add":
		return c.chainsAddCmd(args[1:])
	case "rm", "remove":
		return c.chainsRmCmd(args[1:])
	default:
		return fmt.Errorf("unknown chains subcommand %q (want list|add|rm)", args[0])
	}
}

func (c command) chainsListCmd(args []string) error {
	fs := flag.NewFlagSet("chains", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	if err := c.parse(fs, args); err != nil {
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
		return c.printJSON(out)
	}
	fmt.Fprintf(c.out, "%-20s %-8s  %s\n", "NAME", "SOURCE", "STEPS")
	for _, ch := range out.Chains {
		steps := make([]string, 0, len(ch.Steps))
		for _, s := range ch.Steps {
			steps = append(steps, s.Provider+":"+s.Model)
		}
		fmt.Fprintf(c.out, "%-20s %-8s  %s\n", ch.Name, ch.Source, strings.Join(steps, " -> "))
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

func (c command) chainsAddCmd(args []string) error {
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
	if err := c.parse(fs, args); err != nil {
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
		return c.printJSON(created)
	}
	fmt.Fprintf(c.out, "created chain %q (%d steps, persisted)\n", created.Name, len(created.Steps))
	return nil
}

func (c command) chainsRmCmd(args []string) error {
	fs := flag.NewFlagSet("chains rm", flag.ContinueOnError)
	var cf commonFlags
	parseCommon(fs, &cf)
	if err := c.parse(fs, args); err != nil {
		return err
	}

	name := fs.Arg(0)
	if name == "" || fs.NArg() != 1 {
		return fmt.Errorf("chains rm NAME")
	}
	if err := cf.do(http.MethodDelete, "/v1/chains/"+name, nil, nil); err != nil {
		return err
	}
	fmt.Fprintf(c.out, "removed chain %q\n", name)
	return nil
}
