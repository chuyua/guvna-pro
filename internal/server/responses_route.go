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
	body, err := readRequestBody(w, r)
	if err != nil {
		writeRequestBodyError(w, r, err)
		return
	}
	r = withRequestID(r, s.newRequestID())
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
				fmt.Sprintf("unknown chain or model %q — create a chain with the local guvna chains add command or use a provider-prefixed model", req.Model))
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
	deadline := walkDeadline(chatTotalBudget)
	for _, step := range steps {
		if retryBudgetExhausted(r) {
			break
		}
		if r.Context().Err() != nil {
			return
		}
		permit, admitted := s.hlth.TryAcquire(step.Provider, step.Model)
		if !admitted {
			log.Printf("responses: skipping %s/%s (cool-off)", step.Provider, step.Model)
			continue
		}
		defer permit.Release()
		r = withPermit(r, permit)
		if walkExpired(deadline) {
			log.Printf("responses: request budget exhausted, stopping the walk")
			break
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
		r = withAttempt(r, chain, chatRequest{Model: req.Model, Stream: req.Stream}, payload)
		defer releaseAttempt(r)
		stepCtx, cancel := stepContext(r, deadline, req.Stream, chatStepBudget)
		resp, keyEnv, err := s.tryStepCtx(stepCtx, r, step, payload)
		if err != nil {
			cancel()
			if r.Context().Err() != nil {
				return
			}
			if errors.Is(err, errNoAvailableKeys) {
				permit.Release()
				continue
			}
			if isClientGone(err) {
				log.Printf("responses: client gone, stopping the walk at %s/%s: %v", step.Provider, step.Model, err)
				return
			}
			log.Printf("responses: step %s/%s failed: %v", step.Provider, step.Model, err)
			lastErr = err
			s.mark(r, step.Provider, step.Model, 0, err)
			s.record(r, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 0, err, keyEnv)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var res relayResult
			if agg {
				aggResp, aerr := s.aggregateStreamed(resp)
				if aerr != nil {
					cancel()
					if r.Context().Err() != nil {
						return
					}
					log.Printf("responses: step %s/%s aggregate failed: %v", step.Provider, step.Model, aerr)
					s.mark(r, step.Provider, step.Model, http.StatusBadGateway, aerr)
					s.poolRecordResult(r, keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 502, 0, 0, aerr)
					failure := invalidResponsesResult(aerr)
					lastStatus, lastBody, lastCT, lastErr = failure.status, failure.body, failure.ct, failure.err
					continue
				}
				// relayResponsesBody translates the chat.completion into a
				// Responses object and marks health after the body lands.
				res = s.relayResponsesBody(w, r, step, chain, req, aggResp, keyEnv)
			} else if req.Stream {
				// Health is marked after the body lands — same rule as handleChat.
				s.relayResponsesStream(w, r, step, chain, req, resp, keyEnv)
				res = relayResult{outcome: relayCommitted}
			} else {
				res = s.relayResponsesBody(w, r, step, chain, req, resp, keyEnv)
			}
			cancel()
			if res.clientGone {
				return
			}
			if res.outcome == relayCommitted {
				return
			}
			lastStatus, lastBody, lastCT, lastErr = res.status, res.body, res.ct, res.err
			continue
		}
		lastStatus = resp.StatusCode
		lastBody, _ = readErrorBody(resp, req.Stream)
		lastCT = resp.Header.Get("Content-Type")
		resp.Body.Close()
		cancel()
		if r.Context().Err() != nil {
			return
		}
		s.mark(r, step.Provider, step.Model, resp.StatusCode, nil)
		log.Printf("responses: step %s/%s failed: status %d", step.Provider, step.Model, resp.StatusCode)
		s.record(r, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, resp.StatusCode, errors.New(http.StatusText(resp.StatusCode)), keyEnv)
		permit.Release()
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

// Invalid 2xx responses are gateway failures when no fallback can serve them.
func invalidResponsesResult(err error) relayResult {
	return relayResult{outcome: relayUncommitted, status: http.StatusBadGateway, ct: "application/json",
		body: []byte(`{"error":{"type":"api_error","code":"upstream_invalid_response","message":"upstream returned an invalid or incomplete response"}}`), err: err}
}

// relayResponsesBody translates a whole chat completions body back into a
// Responses response object.
func (s *Server) relayResponsesBody(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req *responses.Request, resp *http.Response, keyEnv string) relayResult {
	defer resp.Body.Close()
	body, err := readLimited(resp.Body, maxResponseBytes)
	ct := resp.Header.Get("Content-Type")
	if r.Context().Err() != nil {
		return relayResult{outcome: relayUncommitted, clientGone: true, err: r.Context().Err()}
	}
	if err != nil {
		if r.Context().Err() != nil {
			return relayResult{outcome: relayUncommitted, clientGone: true, status: resp.StatusCode, ct: ct, err: err}
		}
		s.mark(r, step.Provider, step.Model, http.StatusBadGateway, err)
		s.poolRecordResult(r, keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 502, 0, 0, err)
		log.Printf("responses: %s/%s body read failed: %v", step.Provider, step.Model, err)
		return invalidResponsesResult(err)
	}
	var cc responses.ChatCompletion
	verr := validateChatCompletion(body)
	if verr == nil {
		verr = json.Unmarshal(body, &cc)
	}
	if verr != nil {
		// A 2xx that is not a chat completion cannot be translated into a
		// Responses object, so this attempt failed: report it and let the walk
		// try the next step instead of handing the caller an unusable success.
		verr = fmt.Errorf("upstream 2xx body is not a chat completion: %w", verr)
		log.Printf("responses: %s/%s unusable 2xx body: %v", step.Provider, step.Model, verr)
		s.mark(r, step.Provider, step.Model, http.StatusBadGateway, verr)
		s.poolRecordResult(r, keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 502, 0, 0, verr)
		return invalidResponsesResult(verr)
	}
	if len(cc.Choices) == 0 {
		verr := errors.New("upstream 2xx body has no choices")
		log.Printf("responses: %s/%s %v", step.Provider, step.Model, verr)
		s.mark(r, step.Provider, step.Model, http.StatusBadGateway, verr)
		s.poolRecordResult(r, keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, 502, 0, 0, verr)
		return invalidResponsesResult(verr)
	}
	// The upstream echoes the model that actually served the request, e.g.
	// "self-dploy/GLM-5.3-Flash" for a step that asked for "GLM-5.3-Flash".
	served := responses.Served{ChatModel: cc.Model, ChainModel: step.Model}
	in, out := int64(0), int64(0)
	if cc.Usage != nil {
		in, out = cc.Usage.PromptTokens, cc.Usage.CompletionTokens
	}
	outResp := responses.FromChat(&cc, req, served)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Guvna-Step", step.Provider+"/"+step.Model)
	w.WriteHeader(resp.StatusCode)
	if err := json.NewEncoder(w).Encode(outResp); err != nil || r.Context().Err() != nil {
		return relayResult{outcome: relayCommitted, clientGone: true}
	}
	s.markSuccess(r, step.Provider, step.Model)
	s.poolRecord(r, keyEnv, step, chain, chatRequest{Model: req.Model, Stream: req.Stream}, resp.StatusCode, in, out)
	return relayResult{outcome: relayCommitted}
}

// relayResponsesStream translates complete SSE events. Blocking reads run in a
// separate goroutine so keep-alive and cancellation remain responsive; only this
// goroutine writes the client response. Once opened, this stream never fails over.
func (s *Server) relayResponsesStream(w http.ResponseWriter, r *http.Request, step router.Step, chain string, req *responses.Request, resp *http.Response, keyEnv string) {
	streamDeadline, durationC, stopDuration := streamDurationTimer()
	defer stopDuration()
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
	streamCalls := make(map[int]*toolCallAgg)
	chatReq := chatRequest{Model: req.Model, Stream: req.Stream}
	var idleC <-chan time.Time
	idleTimer := time.NewTimer(time.Hour)
	defer idleTimer.Stop()
	if streamIdle > 0 {
		idleTimer.Reset(streamIdle)
		idleC = idleTimer.C
	}
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
		s.mark(r, step.Provider, step.Model, http.StatusBadGateway, err)
		s.poolRecordResult(r, keyEnv, step, chain, chatReq, 502, in, out, err)
		message := "upstream stream invalid or interrupted"
		var budgetErr *stepTimeoutError
		if errors.As(err, &budgetErr) {
			message = "upstream stream exceeded maximum duration"
		}
		_ = st.Fail(message)
	}
	complete := func() {
		if r.Context().Err() != nil {
			return
		}
		if finish == "" {
			fail(errors.New("upstream SSE ended without finish_reason"))
			return
		}
		if finish == "tool_calls" || finish == "function_call" {
			if len(streamCalls) == 0 {
				fail(errors.New("upstream stream reported tool_calls without any call"))
				return
			}
		}
		if finish != "length" && finish != "content_filter" {
			for _, call := range streamCalls {
				if !call.valid() {
					fail(errors.New("upstream stream carried an incomplete tool call"))
					return
				}
			}
		}
		if err := st.Finish(finish, usage); err != nil {
			return
		}
		s.markSuccess(r, step.Provider, step.Model)
		in, out := tokens()
		s.poolRecord(r, keyEnv, step, chain, chatReq, resp.StatusCode, in, out)
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
		if len(cc.Choices) > 0 {
			for _, raw := range cc.Choices[0].Delta.ToolCalls {
				var tc streamToolCall
				if err := json.Unmarshal(raw, &tc); err != nil || tc.Index == nil || *tc.Index < 0 {
					return false, errors.New("invalid upstream tool-call delta"), nil
				}
				call := streamCalls[*tc.Index]
				if call == nil {
					call = &toolCallAgg{}
					streamCalls[*tc.Index] = call
				}
				call.append(tc)
			}
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
		if walkExpired(streamDeadline) {
			fail(streamDurationError())
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-durationC:
			fail(streamDurationError())
			return
		case <-idleC:
			fail(fmt.Errorf("no upstream data for %s", streamIdle))
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
				if streamIdle > 0 {
					idleTimer.Reset(streamIdle)
				}
				payload, ok, err := data.line(next.line)
				if err != nil {
					fail(err)
					return
				}
				if ok {
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
				// successful terminator. A valid finish_reason is required.
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
