// The /v1/responses surface: the same chain loop as /v1/chat/completions,
// with protocol translation on both edges. See package responses for why
// translation is required at all (no upstream implements the Responses API).
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/creamy-ghost/guvna/internal/keypool"
	"github.com/creamy-ghost/guvna/internal/responses"
	"github.com/creamy-ghost/guvna/internal/router"
)

// responsesErr writes an OpenAI-shaped error body: {"error":{"message","type","code"}}.
// Clients that speak either protocol read this shape, so it is used on every
// failure path of the responses surface.
func responsesErr(w http.ResponseWriter, status int, typ, code, msg string) {
	body := map[string]any{"message": msg}
	if typ != "" {
		body["type"] = typ
	}
	if code != "" {
		body["code"] = code
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": body})
}

// handleResponses is POST /v1/responses. The request is translated to the chat
// protocol once, then walked through the identical chain / health / key-pool
// loop that serves /v1/chat/completions. Only the final response shape differs.
func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		responsesErr(w, http.StatusBadRequest, "invalid_request_error", "read_body_failed", "read body")
		return
	}
	req, err := responses.ParseRequest(body)
	if err != nil {
		responsesErr(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "invalid json")
		return
	}
	if req.Model == "" {
		responsesErr(w, http.StatusBadRequest, "invalid_request_error", "missing_model", `{"model":"..."} is required`)
		return
	}
	chain, steps, err := s.rtr.Resolve(req.Model)
	if err != nil {
		if errors.Is(err, router.ErrNotFound) {
			responsesErr(w, http.StatusNotFound, "invalid_request_error", "model_not_found",
				fmt.Sprintf("unknown chain or model %q — create a chain via POST /v1/chains or use a provider-prefixed model", req.Model))
			return
		}
		responsesErr(w, http.StatusInternalServerError, "api_error", "resolve_failed", "resolve failed")
		return
	}
	log.Printf("responses: chain=%s model=%q steps=%d stream=%v", chain, req.Model, len(steps), req.Stream)

	chat, err := responses.ToChat(req)
	if err != nil {
		responsesErr(w, http.StatusInternalServerError, "api_error", "translate_failed", err.Error())
		return
	}

	var lastErr error
	var lastStatus int
	var lastBody []byte
	var lastCT string
	for _, step := range steps {
		if s.hlth.IsDown(step.Provider, step.Model) {
			log.Printf("responses: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		// tryStep rewrites "model" to the step's model and applies the step's
		// params, exactly as it does for chat completions.
		payload, err := chat.Marshal()
		if err != nil {
			responsesErr(w, http.StatusInternalServerError, "api_error", "marshal_failed", err.Error())
			return
		}
		// A stream_only provider's non-streaming endpoint is broken: rewrite
		// the body to upstream streaming and aggregate below.
		agg := !req.Stream && s.streamOnly(step.Provider)
		if agg {
			payload = forceStreamBody(payload)
		}
		resp, keyEnv, err := s.tryStep(r, step, payload)
		if err != nil {
			log.Printf("responses: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.hlth.Mark(step.Provider, step.Model, 0, err)
			s.record(step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if agg {
				aggResp, aerr := s.aggregateStreamed(resp)
				if aerr != nil {
					log.Printf("responses: step %s/%s aggregate failed: %v", step.Provider, step.Model, aerr)
					s.hlth.Mark(step.Provider, step.Model, 0, aerr)
					s.record(step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 0, aerr, keyEnv)
					continue
				}
				// relayResponsesBody translates the chat.completion into a
				// Responses object and marks health after the body lands.
				s.relayResponsesBody(w, r, step, chain, req, aggResp, keyEnv)
				return
			}
			// Health is marked after the body lands — same rule as handleChat.
			if req.Stream {
				s.relayResponsesStream(w, r, step, chain, req, resp, keyEnv)
			} else {
				s.relayResponsesBody(w, r, step, chain, req, resp, keyEnv)
			}
			return
		}
		lastStatus = resp.StatusCode
		lastBody, _ = io.ReadAll(resp.Body)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		if retryable(resp.StatusCode, nil) {
			s.hlth.Mark(step.Provider, step.Model, resp.StatusCode, nil)
		}
		log.Printf("responses: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
	}

	log.Printf("responses: all steps failed for chain %q: %v", chain, lastErr)
	if lastStatus == 0 {
		responsesErr(w, http.StatusServiceUnavailable, "api_error", "no_providers_available", "no providers available")
		return
	}
	// Propagate the last upstream response as-is: chat-style error bodies are
	// already {"error":{...}}, which is the shape responses clients expect.
	if lastCT != "" {
		w.Header().Set("Content-Type", lastCT)
	}
	w.WriteHeader(lastStatus)
	w.Write(lastBody)
}

// relayResponsesBody translates a whole chat completions body back into a
// Responses response object.
func (s *Server) relayResponsesBody(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req *responses.Request, resp *http.Response, keyEnv string) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		s.hlth.Mark(step.Provider, step.Model, 0, err)
		s.poolRecordResult(keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 502, 0, 0, keypool.ClassTransient, err)
		responsesErr(w, http.StatusBadGateway, "api_error", "upstream_read_failed", "upstream read failed")
		return
	}
	s.hlth.MarkSuccess(step.Provider, step.Model)
	var cc responses.ChatCompletion
	if err := json.Unmarshal(body, &cc); err != nil {
		// Not a chat completion object — pass the body straight through so the
		// client still sees the upstream's own error.
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
		s.poolRecord(keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, resp.StatusCode, 0, 0, "")
		return
	}
	// The upstream echoes the model that actually served the request, e.g.
	// "self-dploy/GLM-5.3-Flash" for a step that asked for "GLM-5.3-Flash".
	served := responses.Served{ChatModel: cc.Model, ChainModel: step.Model}
	in, out := int64(0), int64(0)
	if cc.Usage != nil {
		in, out = cc.Usage.PromptTokens, cc.Usage.CompletionTokens
	}
	outResp := responses.FromChat(&cc, req, served)
	s.poolRecord(keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, resp.StatusCode, in, out, "")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(outResp)
}

// relayResponsesStream translates complete SSE events. Blocking reads run in a
// separate goroutine so keep-alive and cancellation remain responsive; only this
// goroutine writes the client response. Once opened, this stream never fails over.
func (s *Server) relayResponsesStream(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req *responses.Request, resp *http.Response, keyEnv string) {
	defer resp.Body.Close()
	fl, ok := w.(http.Flusher)
	if !ok {
		responsesErr(w, http.StatusInternalServerError, "api_error", "streaming_unsupported", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
	served := responses.Served{ChatModel: step.Model, ChainModel: step.Model}
	output := &streamOutput{ResponseWriter: w}
	st, err := responses.NewStream(output, fl, req, served)
	if err != nil {
		return
	} // a downstream write failure says nothing about upstream health
	lines, stop := readStreamLines(r.Context(), resp.Body)
	defer stop()
	timer := time.NewTimer(KeepAliveInterval)
	defer timer.Stop()
	var data sseData
	var usage *responses.ChatUsage
	var finish string
	chatReq := chatRequest{Model: req.Model, Stream: req.Stream}
	tokens := func() (int64, int64) {
		if usage == nil {
			return 0, 0
		}
		return usage.PromptTokens, usage.CompletionTokens
	}
	fail := func(err error) {
		if r.Context().Err() != nil {
			return
		}
		in, out := tokens()
		s.hlth.Mark(step.Provider, step.Model, 0, err)
		s.poolRecordResult(keyEnv, step, chain, chatReq, 502, in, out, keypool.ClassTransient, err)
		_ = st.Fail("upstream stream invalid or interrupted")
	}
	complete := func() {
		if r.Context().Err() != nil {
			return
		}
		if err := st.Finish(finish, usage); err != nil {
			return
		}
		s.hlth.MarkSuccess(step.Provider, step.Model)
		in, out := tokens()
		s.poolRecord(keyEnv, step, chain, chatReq, resp.StatusCode, in, out, "")
	}
	// Parse errors are upstream failures; Chunk write errors are downstream
	// failures. Keep these distinct so disconnects cannot quarantine a good key.
	consume := func(payload string) (done bool, upstreamErr, writeErr error) {
		if strings.TrimSpace(payload) == "[DONE]" {
			return true, nil, nil
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(payload), &fields); err != nil || fields == nil {
			return false, errors.New("invalid upstream SSE JSON"), nil
		}
		if raw, ok := fields["error"]; ok && string(raw) != "null" {
			return false, errors.New("upstream SSE error payload"), nil
		}
		var cc responses.ChatChunk
		if err := json.Unmarshal([]byte(payload), &cc); err != nil {
			return false, errors.New("invalid upstream chat chunk"), nil
		}
		if len(cc.Choices) == 0 && cc.Usage == nil {
			return false, errors.New("upstream SSE payload has no choices or usage"), nil
		}
		if cc.Usage != nil {
			usage = cc.Usage
		}
		if len(cc.Choices) > 0 && cc.Choices[0].FinishReason != nil {
			reason := *cc.Choices[0].FinishReason
			switch reason {
			case "stop", "length", "tool_calls", "function_call", "content_filter":
				finish = reason
			case "": // some providers emit an empty reason before the terminal chunk
			default:
				return false, errors.New("unknown upstream finish_reason"), nil
			}
		}
		output.wrote = false
		return false, nil, st.Chunk(&cc)
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			fl.Flush()
			timer.Reset(KeepAliveInterval)
		case next := <-lines:
			if r.Context().Err() != nil {
				return
			}
			if len(next.line) > 0 {
				if payload, ok := data.line(next.line); ok {
					done, upstreamErr, writeErr := consume(payload)
					if upstreamErr != nil {
						fail(upstreamErr)
						return
					}
					if writeErr != nil {
						return
					}
					if output.wrote {
						resetKeepAlive(timer)
					}
					if done {
						complete()
						return
					}
				}
			}
			if next.err != nil {
				if !errors.Is(next.err, io.EOF) {
					fail(next.err)
					return
				}
				// EOF dispatches a pending event too, but it is not itself a
				// successful terminator. A valid finish_reason or DONE is required.
				if payload, ok := data.flush(); ok {
					done, upstreamErr, writeErr := consume(payload)
					if upstreamErr != nil {
						fail(upstreamErr)
						return
					}
					if writeErr != nil {
						return
					}
					if done {
						complete()
						return
					}
				}
				if finish == "" {
					fail(errors.New("upstream SSE ended without a terminator"))
					return
				}
				complete()
				return
			}
		}
	}
}

// handleModelInfo is GET /v1/models/{id}: a single model descriptor. OpenAI
// does not expose this, but Azure does, and a couple of clients probe it to
// learn the reasoning levels a model supports before they send an effort.
func (s *Server) handleModelInfo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, _, err := s.rtr.Resolve(id); err != nil {
		responsesErr(w, http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("unknown model %q", id))
		return
	}
	resp := map[string]any{
		"id":         id,
		"object":     "model",
		"created":    time.Now().Unix(),
		"owned_by":   "guvna",
		"root":       id,
		"parent":     nil,
		"capability": map[string]any{"input": map[string]any{"text": true, "image": true}, "output": map[string]any{"text": true}},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleResponseLookup is GET /v1/responses/{id}. The gateway stores no
// responses: translation is stateless, so there is nothing to look up.
func (s *Server) handleResponseLookup(w http.ResponseWriter, r *http.Request) {
	responsesErr(w, http.StatusNotFound, "invalid_request_error", "response_not_found",
		"response storage is disabled on this gateway — every request is translated statelessly")
}

// handleResponseDelete is DELETE /v1/responses/{id}. There is nothing to delete;
// answering 200 keeps clients that delete eagerly from logging a failure.
func (s *Server) handleResponseDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": r.PathValue("id"), "object": "response.deleted", "deleted": true, "model": "", "error": nil,
	})
}

// handleResponseCancel is POST /v1/responses/{id}/cancel. Nothing is stored, so
// there is no response to cancel — but the client's own request is already
// aborted on its side, so report success.
func (s *Server) handleResponseCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&responses.Response{
		ID: id, Object: "response", CreatedAt: time.Now().Unix(), Model: "", Status: "cancelled", Output: []map[string]any{},
	})
}
