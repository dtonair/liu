package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dtonair/liu/engine"
	"github.com/dtonair/liu/model"
	"github.com/dtonair/liu/security"
	"github.com/dtonair/liu/store"
)

func testServer(t *testing.T) (*httptest.Server, *engine.Engine, store.Store) {
	t.Helper()
	st := store.NewMemStore()
	eng := engine.New(st)
	// Drive RUNNABLE instances on a fast background scheduler so the HTTP flow
	// advances without manual ticking.
	sched := engine.NewScheduler(eng, 10*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = sched.Run(ctx) }()
	t.Cleanup(cancel)

	srv := NewServer(eng, st, Options{
		Auth:         &security.Authenticator{Disabled: true},
		PollInterval: 5 * time.Millisecond,
	})
	hs := httptest.NewServer(srv.Router())
	t.Cleanup(hs.Close)
	return hs, eng, st
}

func do(t *testing.T, method, url, tenant string, body any) (*http.Response, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rdr)
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

func registerOrderApproval(t *testing.T, base string) {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join("..", "workflows", "order_approval.json"))
	var def map[string]any
	_ = json.Unmarshal(b, &def)
	resp, body := do(t, http.MethodPost, base+"/v1/definitions", "demo", def)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register def: %d %s", resp.StatusCode, body)
	}
}

func TestEndToEndOverHTTP(t *testing.T) {
	hs, _, _ := testServer(t)
	registerOrderApproval(t, hs.URL)

	// Start an instance.
	resp, body := do(t, http.MethodPost, hs.URL+"/v1/workflows/order_approval/instances", "demo",
		map[string]any{"idempotency_key": "order-1", "input": map[string]any{"order_id": 7}})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("start: %d %s", resp.StatusCode, body)
	}
	var started struct {
		InstanceID string `json:"instance_id"`
	}
	_ = json.Unmarshal(body, &started)
	id := started.InstanceID

	// Poll + complete reserve_inventory.
	pollComplete(t, hs.URL, "reserve_inventory")

	// Wait until parked on the signal, then send approval.
	waitStatus(t, hs.URL, id, model.StatusWaiting)
	resp, body = do(t, http.MethodPost, hs.URL+"/v1/instances/"+id+"/signals/manager_approval", "demo", nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signal: %d %s", resp.StatusCode, body)
	}

	// Poll + complete capture_payment.
	pollComplete(t, hs.URL, "capture_payment")
	waitStatus(t, hs.URL, id, model.StatusSucceeded)

	// History endpoint reflects the run.
	resp, body = do(t, http.MethodGet, hs.URL+"/v1/instances/"+id+"/history", "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("history: %d %s", resp.StatusCode, body)
	}
	var hist struct {
		Events []model.Event `json:"events"`
	}
	_ = json.Unmarshal(body, &hist)
	if len(hist.Events) < 6 {
		t.Fatalf("expected rich history, got %d events", len(hist.Events))
	}
}

func pollComplete(t *testing.T, base, activity string) {
	t.Helper()
	resp, body := do(t, http.MethodPost, base+"/v1/tasks/poll", "demo",
		pollRequest{ActivityType: activity, WorkerID: "w1", WaitSeconds: 2, LeaseSeconds: 30})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll %s: %d %s", activity, resp.StatusCode, body)
	}
	var task model.Task
	_ = json.Unmarshal(body, &task)
	resp, body = do(t, http.MethodPost, base+"/v1/tasks/"+task.ID+"/complete", "demo",
		completeRequest{WorkerID: "w1", LeaseToken: task.LeaseToken})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete %s: %d %s", activity, resp.StatusCode, body)
	}
}

func waitStatus(t *testing.T, base, id string, want model.InstanceStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, body := do(t, http.MethodGet, base+"/v1/instances/"+id, "demo", nil)
		if resp.StatusCode == http.StatusOK {
			var inst model.Instance
			_ = json.Unmarshal(body, &inst)
			if inst.Status == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("instance %s did not reach %s in time", id, want)
}

func TestAuthRequired(t *testing.T) {
	hs, _, _ := testServer(t)
	// No X-Tenant-ID header -> 401.
	resp, _ := do(t, http.MethodGet, hs.URL+"/v1/instances", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without tenant, got %d", resp.StatusCode)
	}
}

func TestCrossTenantIsolation(t *testing.T) {
	hs, _, _ := testServer(t)
	registerOrderApproval(t, hs.URL)
	_, body := do(t, http.MethodPost, hs.URL+"/v1/workflows/order_approval/instances", "tenant-a",
		map[string]any{"idempotency_key": "x"})
	var started struct {
		InstanceID string `json:"instance_id"`
	}
	_ = json.Unmarshal(body, &started)

	// tenant-b must not see tenant-a's instance.
	resp, _ := do(t, http.MethodGet, hs.URL+"/v1/instances/"+started.InstanceID, "tenant-b", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant read: want 404, got %d", resp.StatusCode)
	}
}

func TestLongPollReturns204(t *testing.T) {
	hs, _, _ := testServer(t)
	start := time.Now()
	resp, _ := do(t, http.MethodPost, hs.URL+"/v1/tasks/poll", "demo",
		pollRequest{ActivityType: "nothing", WorkerID: "w1", WaitSeconds: 1})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204 on empty long-poll, got %d", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("long-poll returned too early: %v", elapsed)
	}
}

func TestInvalidDefinitionRejected(t *testing.T) {
	hs, _, _ := testServer(t)
	resp, _ := do(t, http.MethodPost, hs.URL+"/v1/definitions", "demo",
		map[string]any{"name": "bad", "version": 1, "initial": "ghost", "steps": []any{}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for invalid def, got %d", resp.StatusCode)
	}
}
