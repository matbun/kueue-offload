package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/interlink-hq/interlink-dispatch/pkg/agentapi"
)

// AgentError is a non-2xx answer from an agent.
type AgentError struct {
	Code      int
	Permanent bool
	Message   string
}

func (e *AgentError) Error() string {
	return fmt.Sprintf("agent answered %d: %s", e.Code, e.Message)
}

// IsPermanent reports whether err is an agent answer that will never change.
func IsPermanent(err error) bool {
	var ae *AgentError
	return errors.As(err, &ae) && ae.Permanent
}

// IsNotFound reports whether err is the agent saying it has no record.
func IsNotFound(err error) bool {
	var ae *AgentError
	return errors.As(err, &ae) && ae.Code == http.StatusNotFound
}

// TokenSource reads a bearer token from a file and re-reads it when the file
// changes, as a projected ServiceAccount token is rotated in place.
type TokenSource struct {
	Path string

	mu      sync.Mutex
	modTime time.Time
	size    int64
	token   string
}

// Token returns the current token.
func (t *TokenSource) Token() (string, error) {
	fi, err := os.Stat(t.Path)
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && fi.ModTime().Equal(t.modTime) && fi.Size() == t.size {
		return t.token, nil
	}
	b, err := os.ReadFile(t.Path)
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", t.Path)
	}
	t.token, t.modTime, t.size = tok, fi.ModTime(), fi.Size()
	return tok, nil
}

// AgentClient speaks the agent HTTP API.
type AgentClient struct {
	HTTP  *http.Client
	Token *TokenSource
}

// Submit sends POST /submit. created is false when the agent answered 200,
// meaning it already had a job for the Workload.
func (a *AgentClient) Submit(ctx context.Context, endpoint string, req *agentapi.SubmitRequest) (resp *agentapi.SubmitResponse, created bool, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, false, err
	}
	code, data, err := a.do(ctx, http.MethodPost, endpoint+"/submit", req.WorkloadUID, body)
	if err != nil {
		return nil, false, err
	}
	if code != http.StatusOK && code != http.StatusCreated {
		return nil, false, agentError(code, data)
	}
	resp = &agentapi.SubmitResponse{}
	if err := json.Unmarshal(data, resp); err != nil {
		return nil, false, fmt.Errorf("decoding submit answer: %w", err)
	}
	if resp.JobID == "" {
		return nil, false, fmt.Errorf("agent answered %d without a jobID", code)
	}
	return resp, code == http.StatusCreated, nil
}

// GetJob sends GET /jobs/{uid}.
func (a *AgentClient) GetJob(ctx context.Context, endpoint, uid string) (*agentapi.JobStatus, error) {
	code, data, err := a.do(ctx, http.MethodGet, endpoint+"/jobs/"+url.PathEscape(uid), uid, nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, agentError(code, data)
	}
	st := &agentapi.JobStatus{}
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("decoding job status: %w", err)
	}
	return st, nil
}

// DeleteJob sends DELETE /jobs/{uid}. A 404 counts as done.
func (a *AgentClient) DeleteJob(ctx context.Context, endpoint, uid string) error {
	code, data, err := a.do(ctx, http.MethodDelete, endpoint+"/jobs/"+url.PathEscape(uid), uid, nil)
	if err != nil {
		return err
	}
	if code == http.StatusNoContent || code == http.StatusOK || code == http.StatusNotFound {
		return nil
	}
	return agentError(code, data)
}

func (a *AgentClient) do(ctx context.Context, method, u, uid string, body []byte) (int, []byte, error) {
	tok, err := a.Token.Token()
	if err != nil {
		return 0, nil, err
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set(agentapi.SessionHeader, uid)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}

func agentError(code int, data []byte) error {
	e := &agentapi.ErrorResponse{}
	if json.Unmarshal(data, e) != nil || e.Message == "" {
		e.Message = strings.TrimSpace(string(data))
	}
	// Only a 4xx can be permanent: a 5xx is transient whatever its body says.
	return &AgentError{Code: code, Permanent: e.Permanent && code >= 400 && code < 500, Message: e.Message}
}
