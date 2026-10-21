package dashboard

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/requestbody"
)

const (
	defaultFormMaxBytes    = 1 << 20
	defaultFormReadTimeout = 30 * time.Second
)

func dashboardFormMaxBytes() int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(os.Getenv("GUVNA_DASHBOARD_MAX_FORM_BYTES")), 10, 64)
	if err != nil || n <= 0 {
		return defaultFormMaxBytes
	}
	return n
}

func dashboardFormReadTimeout() time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(os.Getenv("GUVNA_DASHBOARD_FORM_READ_TIMEOUT")))
	if err != nil || d < 0 {
		return defaultFormReadTimeout
	}
	return d
}

// readForm consumes the transport body once, with a deadline that ends before
// parsing or gateway work. ParseForm retains its normal body/query semantics.
func (d *Dashboard) readForm(w http.ResponseWriter, r *http.Request) error {
	body, err := requestbody.Read(w, r, d.formMaxBytes, d.formReadTimeout)
	if err != nil {
		return err
	}
	if r.Body != nil {
		_ = r.Body.Close()
	}
	// Mark the replay as explicitly bounded, so ParseForm does not introduce
	// its own 10 MiB default when an operator allows a larger form.
	r.Body = http.MaxBytesReader(w, io.NopCloser(bytes.NewReader(body)), d.formMaxBytes)
	return r.ParseForm()
}

// writeFormError never echoes submitted fields or an underlying read error,
// and renders directly so a rejected form cannot even fetch the chain list.
func (d *Dashboard) writeFormError(w http.ResponseWriter, r *http.Request, view string, err error) {
	if errors.Is(err, requestbody.ErrClientGone) || r.Context().Err() != nil && !errors.Is(err, requestbody.ErrTimeout) {
		return
	}
	status, message := http.StatusBadRequest, "bad form submission"
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		status, message = http.StatusRequestEntityTooLarge, "form submission is too large"
	case errors.Is(err, requestbody.ErrTimeout):
		status, message = http.StatusRequestTimeout, "form submission timed out"
	}
	if r.ProtoMajor < 2 {
		w.Header().Set("Connection", "close")
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Guvna-Form-Error", "true")
	w.WriteHeader(status)
	if view == "login" {
		d.tmpl.ExecuteTemplate(w, "login", loginView{Error: message})
		return
	}
	d.tmpl.ExecuteTemplate(w, "chains", chainsView{Error: message})
}
