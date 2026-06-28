// Package worker is the out-of-engine task execution SDK: an HTTP client for
// the engine's worker protocol and a Runner that polls, dispatches to handlers
// with lease heartbeating and bounded concurrency, and reports results
// (spec FR5, FR6, FR10, FR11).
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/dtonair/liu/internal/model"
)

// Client speaks the engine's HTTP worker protocol.
type Client struct {
	BaseURL  string
	WorkerID string
	// Auth: set Token for JWT bearer auth, or TenantID for the engine's
	// header-based local mode.
	Token    string
	TenantID string
	HTTP     *http.Client
}

// NewClient returns a Client with sensible HTTP defaults.
func NewClient(baseURL, workerID string) *Client {
	return &Client{
		BaseURL:  baseURL,
		WorkerID: workerID,
		HTTP:     &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.TenantID != "" {
		req.Header.Set("X-Tenant-ID", c.TenantID)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("engine %s %s: %d %s", method, path, resp.StatusCode, string(msg))
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

// Poll long-polls for the next task of an activity type. ok is false when no
// task became available within waitSeconds (HTTP 204).
func (c *Client) Poll(ctx context.Context, activityType string, waitSeconds, leaseSeconds int) (*model.Task, bool, error) {
	var task model.Task
	status, err := c.do(ctx, http.MethodPost, "/v1/tasks/poll", map[string]any{
		"activity_type": activityType,
		"worker_id":     c.WorkerID,
		"wait_seconds":  waitSeconds,
		"lease_seconds": leaseSeconds,
	}, &task)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNoContent {
		return nil, false, nil
	}
	return &task, true, nil
}

// Complete reports a successful task result.
func (c *Client) Complete(ctx context.Context, taskID, leaseToken string, output json.RawMessage) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID+"/complete", map[string]any{
		"worker_id":   c.WorkerID,
		"lease_token": leaseToken,
		"output":      output,
	}, nil)
	return err
}

// Fail reports a task failure with retry intent.
func (c *Client) Fail(ctx context.Context, taskID, leaseToken, errMsg, errClass string, retryable bool) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID+"/fail", map[string]any{
		"worker_id":   c.WorkerID,
		"lease_token": leaseToken,
		"error":       errMsg,
		"error_class": errClass,
		"retryable":   retryable,
	}, nil)
	return err
}

// Heartbeat extends a task lease for a long-running activity.
func (c *Client) Heartbeat(ctx context.Context, taskID, leaseToken string, leaseSeconds int) error {
	_, err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID+"/heartbeat", map[string]any{
		"worker_id":     c.WorkerID,
		"lease_token":   leaseToken,
		"lease_seconds": leaseSeconds,
	}, nil)
	return err
}
