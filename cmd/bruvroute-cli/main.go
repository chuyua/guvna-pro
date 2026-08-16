// bruvroute-cli — local and remote status/logs for a BruvRoute gateway.
// Local: talks to http://127.0.0.1:20128 (or BRUVROUTE_URL).
// Remote: point --url at the gateway's HTTPS endpoint (caddy) — the admin
// key travels as a Bearer token; no new port is exposed.
//
//	bruvroute-cli status [--url URL] [--token KEY]
//	bruvroute-cli logs [-n 100] [--url URL] [--token KEY]
package main

import (
	"encoding/json"
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
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "bruvroute-cli: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `bruvroute-cli — gateway status and logs

usage:
  bruvroute-cli status [flags]
  bruvroute-cli logs [-n lines] [flags]

flags:
  --url    gateway base URL (default: %s or $BRUVROUTE_URL)
  --token  admin key (default: $BRUVROUTE_ADMIN_KEY)
  --json   emit raw JSON instead of a table
`, defaultURL)
}

type commonFlags struct {
	url   string
	token string
	json  bool
}

func parseCommon(fs *flag.FlagSet, cf *commonFlags) {
	fs.StringVar(&cf.url, "url", envOr("BRUVROUTE_URL", defaultURL), "gateway base URL")
	fs.StringVar(&cf.token, "token", os.Getenv("BRUVROUTE_ADMIN_KEY"), "admin key (Bearer token)")
	fs.BoolVar(&cf.json, "json", false, "raw JSON output")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (cf commonFlags) get(path string, out any) error {
	if cf.token == "" {
		return fmt.Errorf("no admin key: pass --token or set BRUVROUTE_ADMIN_KEY")
	}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(cf.url, "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cf.token)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: %s %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type statusResp struct {
	Uptime    string   `json:"uptime"`
	Chains    []string `json:"chains"`
	Default   string   `json:"default_chain"`
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

	fmt.Printf("BruvRoute %s  (uptime %s)\n", cf.url, st.Uptime)
	fmt.Printf("default chain: %s\n\n", st.Default)
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
