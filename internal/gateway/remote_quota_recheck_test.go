package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// #4202: two gateways retain separate rests. A renewed account there
// must be reachable before the downstream's old Retry-After expires.
// Both HTTP hops use Front, including the running server's guards.
func TestRemoteQuotaRecheckedBeforeRetryAfter(t *testing.T) {
	for _, preset := range []string{provider.RemoteMagpiePreset, ""} {
		t.Run("preset="+preset, func(t *testing.T) {
			fresh(t)
			noRests()
			t.Cleanup(noRests)
			var ready atomic.Bool
			var vendorCalls, hops atomic.Int32
			entered, finish := make(chan struct{}), make(chan struct{})
			var once, reached sync.Once
			defer once.Do(func() { close(finish) })
			vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				vendorCalls.Add(1)
				if !ready.Load() {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Retry-After", "3600")
					w.WriteHeader(429)
					io.WriteString(w, `{"type":"error","error":{"type":"rate_limit_error","message":"You've hit your session limit · resets 3pm (Asia/Singapore)"}}`)
					return
				}
				reached.Do(func() { close(entered) })
				select {
				case <-finish:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, sse(
					"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\",\"model\":\"claude-sonnet-5-5\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}",
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"OK"}}`,
					`data: {"type":"content_block_stop","index":0}`,
					`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
					`data: {"type":"message_stop"}`))
			}))
			defer vendor.Close()
			save := func(p provider.Provider) {
				t.Helper()
				if err := provider.Save(p); err != nil {
					t.Fatal(err)
				}
			}
			save(provider.Provider{ID: "vendor", Name: "Claude Code", Key: "k", Anthropic: vendor.URL, Models: []string{"claude-sonnet-5-5"}})
			if err := provider.SaveGroup(provider.Group{Name: "Inner", Members: []string{"vendor/claude-sonnet-5-5"}, Routing: provider.Ordered}); err != nil {
				t.Fatal(err)
			}
			upstream := New()
			front := upstream.Front()
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hops.Add(1); front.ServeHTTP(w, r) }))
			defer remote.Close()
			save(provider.Provider{ID: "remote", Name: "Remote Magpie", Preset: preset, Key: "magpie", Anthropic: remote.URL, Models: []string{"group/inner"}})
			if err := provider.SaveGroup(provider.Group{Name: "Outer", Members: []string{"remote/group/inner"}, Routing: provider.Ordered}); err != nil {
				t.Fatal(err)
			}
			downstream := httptest.NewServer(New().Front())
			defer func() { once.Do(func() { close(finish) }); downstream.Close() }()
			client := &http.Client{Timeout: 20 * time.Second}
			type reply struct {
				code        int
				body, retry string
				err         error
			}
			send := func() reply {
				req, _ := http.NewRequest("POST", downstream.URL+"/v1/messages", strings.NewReader(`{"model":"group/outer","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer magpie")
				res, err := client.Do(req)
				if err != nil {
					return reply{err: err}
				}
				defer res.Body.Close()
				b, err := io.ReadAll(res.Body)
				return reply{res.StatusCode, string(b), res.Header.Get("Retry-After"), err}
			}
			held := func() {
				t.Helper()
				got := send()
				if got.err != nil || got.code != 429 {
					t.Fatalf("want quota refusal: %+v", got)
				}
				seconds, err := strconv.Atoi(got.retry)
				if err != nil || seconds < 1 || seconds > 60 {
					t.Fatalf("recheck Retry-After = %q, want at most a minute", got.retry)
				}
			}
			held()
			const key = "remote/group/inner"
			rest, ok := restOf(key)
			if !ok || time.Until(rest.Until) < 50*time.Minute {
				t.Fatalf("original long rest lost: %+v", rest)
			}
			held()
			if hops.Load() != 1 || vendorCalls.Load() != 1 {
				t.Fatalf("immediate retry: hops=%d vendor=%d", hops.Load(), vendorCalls.Load())
			}
			due := func() {
				restingUntil.Lock()
				r := restingUntil.note[key]
				if !r.askAt.IsZero() {
					r.askAt = time.Now().Add(-time.Minute)
				}
				restingUntil.note[key] = r
				restingUntil.Unlock()
			}
			due()
			held()
			if hops.Load() != 2 || vendorCalls.Load() != 1 {
				t.Fatalf("remote still guards the real quota: hops=%d vendor=%d", hops.Load(), vendorCalls.Load())
			}
			held()
			if hops.Load() != 2 {
				t.Fatal("a refused recovery check was not held again")
			}
			// The remote's account renewal clears only its own rest.
			upstream.UnrestAll("vendor")
			ready.Store(true)
			due()
			result := make(chan reply, 1)
			go func() { result <- send() }()
			select {
			case <-entered:
			case got := <-result:
				t.Fatalf("recovery never reached vendor: %+v", got)
			case <-time.After(10 * time.Second):
				t.Fatal("recovery never reached vendor")
			}
			// Even a check running longer than its interval admits no second ask.
			due()
			var wg sync.WaitGroup
			replies := make(chan reply, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); replies <- send() }()
			}
			wg.Wait()
			close(replies)
			for got := range replies {
				if got.err != nil || got.code != 429 {
					t.Errorf("concurrent retry: %+v", got)
				}
			}
			if hops.Load() != 3 || vendorCalls.Load() != 2 {
				t.Errorf("checks not coalesced: hops=%d vendor=%d", hops.Load(), vendorCalls.Load())
			}
			once.Do(func() { close(finish) })
			got := <-result
			if got.err != nil || got.code != 200 || !strings.Contains(got.body, "OK") || !strings.Contains(got.body, "message_stop") {
				t.Fatalf("recovered: %+v", got)
			}
			if r, ok := restOf(key); ok {
				t.Fatalf("successful recovery retained rest: %+v", r)
			}
		})
	}
}

func TestRemoteQuotaRecheckHonorsOtherLimits(t *testing.T) {
	for _, tc := range []struct {
		name, why string
		header    http.Header
		own       int
		remote    bool
	}{
		{"remote quota", failQuota, http.Header{"Retry-After": {"3600"}}, 0, true},
		{"user rest", failQuota, http.Header{"Retry-After": {"3600"}}, 120, true},
		{"plain vendor", failQuota, http.Header{"Retry-After": {"3600"}}, 0, false},
		{"remote rate", failRate, http.Header{"Retry-After": {"3600"}}, 0, true},
		{"remote credit", failCredit, http.Header{"Retry-After": {"3600"}}, 0, true},
		{"remote key limit", failQuota, http.Header{"Retry-After": {"3600"}, remoteKeyLimitHeader: {"1"}}, 0, true},
		{"noted key limit", failQuota, http.Header{"Retry-After": {"3600"}, keyLimitThereHeader: {"1"}}, 0, true},
		{"explicit reset", failQuota, http.Header{"Retry-After": {"3600"}, resetsHeader: {strconv.FormatInt(time.Now().Add(2*time.Hour).Unix(), 10)}}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh(t)
			noRests()
			t.Cleanup(noRests)
			p := provider.Provider{ID: "remote", Key: "k", QuotaRest: tc.own}
			if tc.remote {
				p.Preset = provider.RemoteMagpiePreset
			}
			c := candidate{p: p, model: "group/m", rest: "remote"}
			msg := map[string]string{failQuota: "usage limit reached", failRate: "too many requests", failCredit: "insufficient balance"}[tc.why]
			r := New().restAfter(c, 429, tc.header, []byte(`{"error":{"message":"`+msg+`"}}`))
			at := r.asksAt(time.Now())
			if tc.name == "remote quota" {
				if time.Until(at) > time.Minute {
					t.Fatalf("remote quota waits until %v", at)
				}
			} else if r.remoteCheck || tc.why != failCredit && !at.Equal(r.Until) {
				t.Fatalf("%s rechecked early: %+v", tc.name, r)
			}
		})
	}
}

// A cancelled check releases its reservation without clearing the rest;
// a new refusal in the meantime belongs to that newer request.
func TestRemoteQuotaCheckRelease(t *testing.T) {
	fresh(t)
	noRests()
	t.Cleanup(noRests)
	c := candidate{p: provider.Provider{ID: "remote", Preset: provider.RemoteMagpiePreset, Key: "k"}, model: "group/m", rest: "remote"}
	s := New()
	refuse := func() Rest {
		return s.restAfter(c, 429, http.Header{"Retry-After": {"3600"}}, []byte(`{"error":{"message":"usage limit reached"}}`))
	}
	due := func() {
		restingUntil.Lock()
		r := restingUntil.note[c.restID()]
		r.askAt = time.Now().Add(-time.Minute)
		restingUntil.note[c.restID()] = r
		restingUntil.Unlock()
	}
	refuse()
	due()
	_, _, held, release := quotaHeldForTry([]candidate{c})
	if held || release == nil {
		t.Fatal("due check not reserved")
	}
	release()
	r, _ := restOf(c.restID())
	if r.checking || time.Until(r.asksAt(time.Now())) > time.Minute {
		t.Fatalf("cancelled check retained reservation: %+v", r)
	}
	due()
	_, _, held, release = quotaHeldForTry([]candidate{c})
	if held || release == nil {
		t.Fatal("cancelled check cannot retry")
	}
	newer := refuse()
	release()
	r, _ = restOf(c.restID())
	if !r.Until.Equal(newer.Until) || !r.askAt.Equal(newer.askAt) {
		t.Fatalf("release changed newer refusal: %+v", r)
	}
}

// Quota words on another status do not turn an otherwise unheld error
// into a locally replayed quota refusal.
func TestRemoteQuotaCheckOnly429(t *testing.T) {
	fresh(t)
	noRests()
	t.Cleanup(noRests)
	c := candidate{p: provider.Provider{ID: "remote", Preset: provider.RemoteMagpiePreset, Key: "k"}, model: "group/m", rest: "remote"}
	for _, status := range []int{400, 402, 403, 500} {
		r := New().restAfter(c, status, http.Header{"Retry-After": {"3600"}}, []byte(`{"error":{"message":"usage limit reached","type":"usage_limit_reached"}}`))
		if r.remoteCheck {
			t.Errorf("status %d enabled recovery checks: %+v", status, r)
		}
	}
}
