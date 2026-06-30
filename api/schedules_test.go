package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/dtonair/liu/model"
)

func createSchedule(t *testing.T, base, tenant string, body map[string]any) model.Schedule {
	t.Helper()
	resp, out := do(t, http.MethodPost, base+"/v1/schedules", tenant, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create schedule: %d %s", resp.StatusCode, out)
	}
	var sched model.Schedule
	if err := json.Unmarshal(out, &sched); err != nil {
		t.Fatal(err)
	}
	return sched
}

func TestScheduleAPIManageLifecycle(t *testing.T) {
	hs, _, _ := testServer(t)
	registerOrderApproval(t, hs.URL)

	sched := createSchedule(t, hs.URL, "demo", map[string]any{
		"workflow_name": "order_approval",
		"version":       1,
		"cron":          "*/5 * * * *",
		"input":         map[string]any{"source": "api-test"},
	})
	if sched.ID == "" || sched.TenantID != "demo" || sched.Timezone != "UTC" || sched.NextRunAt.IsZero() || !sched.Enabled {
		t.Fatalf("unexpected schedule: %+v", sched)
	}

	resp, body := do(t, http.MethodGet, hs.URL+"/v1/schedules", "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list schedules: %d %s", resp.StatusCode, body)
	}
	var listed struct {
		Schedules []model.Schedule `json:"schedules"`
	}
	_ = json.Unmarshal(body, &listed)
	if len(listed.Schedules) != 1 || listed.Schedules[0].ID != sched.ID {
		t.Fatalf("listed schedules: %+v", listed.Schedules)
	}

	resp, body = do(t, http.MethodGet, hs.URL+"/v1/schedules/"+sched.ID, "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get schedule: %d %s", resp.StatusCode, body)
	}

	resp, body = do(t, http.MethodPost, hs.URL+"/v1/schedules/"+sched.ID+"/pause", "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause schedule: %d %s", resp.StatusCode, body)
	}
	var paused model.Schedule
	_ = json.Unmarshal(body, &paused)
	if paused.Enabled {
		t.Fatalf("schedule should be paused: %+v", paused)
	}

	resp, body = do(t, http.MethodPost, hs.URL+"/v1/schedules/"+sched.ID+"/resume", "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume schedule: %d %s", resp.StatusCode, body)
	}
	var resumed model.Schedule
	_ = json.Unmarshal(body, &resumed)
	if !resumed.Enabled || resumed.NextRunAt.IsZero() {
		t.Fatalf("schedule should be resumed: %+v", resumed)
	}

	resp, body = do(t, http.MethodDelete, hs.URL+"/v1/schedules/"+sched.ID, "demo", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete schedule: %d %s", resp.StatusCode, body)
	}
	resp, _ = do(t, http.MethodGet, hs.URL+"/v1/schedules/"+sched.ID, "demo", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get deleted schedule: want 404 got %d", resp.StatusCode)
	}
}

func TestScheduleAPIValidation(t *testing.T) {
	hs, _, _ := testServer(t)
	registerOrderApproval(t, hs.URL)

	cases := []struct {
		name string
		body map[string]any
		code int
	}{
		{
			name: "missing workflow",
			body: map[string]any{"cron": "* * * * *"},
			code: http.StatusBadRequest,
		},
		{
			name: "invalid cron",
			body: map[string]any{"workflow_name": "order_approval", "cron": "* * *"},
			code: http.StatusBadRequest,
		},
		{
			name: "invalid timezone",
			body: map[string]any{"workflow_name": "order_approval", "cron": "* * * * *", "timezone": "No/SuchZone"},
			code: http.StatusBadRequest,
		},
		{
			name: "missing definition",
			body: map[string]any{"workflow_name": "missing", "cron": "* * * * *"},
			code: http.StatusNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := do(t, http.MethodPost, hs.URL+"/v1/schedules", "demo", tc.body)
			if resp.StatusCode != tc.code {
				t.Fatalf("want %d got %d", tc.code, resp.StatusCode)
			}
		})
	}
}

func TestScheduleAPICrossTenantIsolation(t *testing.T) {
	hs, _, _ := testServer(t)
	registerOrderApproval(t, hs.URL)
	sched := createSchedule(t, hs.URL, "tenant-a", map[string]any{
		"workflow_name": "order_approval",
		"cron":          "0 9 * * *",
	})

	resp, _ := do(t, http.MethodGet, hs.URL+"/v1/schedules/"+sched.ID, "tenant-b", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant get: want 404 got %d", resp.StatusCode)
	}
	resp, _ = do(t, http.MethodPost, hs.URL+"/v1/schedules/"+sched.ID+"/pause", "tenant-b", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant pause: want 404 got %d", resp.StatusCode)
	}
	resp, _ = do(t, http.MethodDelete, hs.URL+"/v1/schedules/"+sched.ID, "tenant-b", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant delete: want 404 got %d", resp.StatusCode)
	}

	resp, body := do(t, http.MethodGet, hs.URL+"/v1/schedules", "tenant-b", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tenant-b list: %d %s", resp.StatusCode, body)
	}
	var listed struct {
		Schedules []model.Schedule `json:"schedules"`
	}
	_ = json.Unmarshal(body, &listed)
	if len(listed.Schedules) != 0 {
		t.Fatalf("tenant-b saw schedules: %+v", listed.Schedules)
	}
}
