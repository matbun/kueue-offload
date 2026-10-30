package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"strings"

	commonIL "github.com/interlink-hq/interlink/pkg/interlink"
	v1 "k8s.io/api/core/v1"

	"github.com/matbun/kueue-offload/pkg/executorapi"
)

// maxErrorText bounds the text of a refusal the executor keeps.
const maxErrorText = 4096

// InterLink calls an interLink API server, as the virtual kubelet does
// (pkg/virtualkubelet/execute.go): /create, /status, /getLogs, /delete and
// /pinglink, with interLink's own wire types.
type InterLink struct {
	URL  string
	HTTP *http.Client
}

// PluginError is an answer other than 200. The API server forwards the plugin's
// status code but wraps its text, so only the code is a reliable signal.
type PluginError struct {
	Call    string
	Code    int
	Message string
}

func (e *PluginError) Error() string {
	return fmt.Sprintf("%s answered %d: %s", e.Call, e.Code, e.Message)
}

// pluginText is the plugin's own text out of the API server's wrapping, which
// escapes it and puts it after "Body: " (pkg/interlink/api/handler.go).
func pluginText(raw []byte) string {
	text := html.UnescapeString(strings.TrimSpace(string(raw)))
	if i := strings.LastIndex(text, "Body: "); i >= 0 {
		text = text[i+len("Body: "):]
	}
	if len(text) > maxErrorText {
		text = text[:maxErrorText]
	}
	return strings.TrimSpace(text)
}

// do sends body as JSON and returns the answer for the caller to close. A status
// other than 200 is a *PluginError.
func (c *InterLink) do(ctx context.Context, method, path, session string, body any) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set(executorapi.SessionHeader, session)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, &PluginError{Call: strings.TrimPrefix(path, "/"), Code: resp.StatusCode, Message: pluginText(text)}
	}
	return resp, nil
}

func (c *InterLink) call(ctx context.Context, method, path, session string, body, out any) error {
	resp, err := c.do(ctx, method, path, session, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decoding the answer of %s: %w", path, err)
	}
	return nil
}

// Create sends one pod with its data.
func (c *InterLink) Create(ctx context.Context, session string, req commonIL.PodCreateRequests) (commonIL.CreateStruct, error) {
	var out commonIL.CreateStruct
	err := c.call(ctx, http.MethodPost, "/create", session, req, &out)
	return out, err
}

// Status asks for the status of pods. The API server asks the plugin again only
// for pods whose phase is Pending or Running, so the caller sets one.
func (c *InterLink) Status(ctx context.Context, session string, pods []*v1.Pod) ([]commonIL.PodStatus, error) {
	var out []commonIL.PodStatus
	err := c.call(ctx, http.MethodGet, "/status", session, pods, &out)
	return out, err
}

// Delete removes a pod: the plugin cancels its job unless it ended and removes
// its files.
func (c *InterLink) Delete(ctx context.Context, session string, pod *v1.Pod) error {
	return c.call(ctx, http.MethodDelete, "/delete", session, pod, nil)
}

// Logs returns a container's log, streamed while it runs with Opts.Follow.
func (c *InterLink) Logs(ctx context.Context, session string, req commonIL.LogStruct) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, "/getLogs", session, req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// pingAnswer is interLink's PingResponse with the capabilities field it does not
// have upstream yet (backends.md 3.1).
type pingAnswer struct {
	commonIL.PingResponse
	Capabilities []string `json:"capabilities,omitempty"`
}

// Ping asks the API server whether the plugin answers, and returns the
// capabilities the plugin lists in its answer, which the API server passes on
// unchanged. An answer without the field, or not in JSON as an older plugin's,
// lists none.
func (c *InterLink) Ping(ctx context.Context) ([]string, error) {
	resp, err := c.do(ctx, http.MethodPost, "/pinglink", "", struct{}{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var answer pingAnswer
	if json.Unmarshal(raw, &answer) != nil {
		return nil, nil
	}
	return answer.Capabilities, nil
}

// pluginCode is the status code of a plugin's answer, 0 when there was none.
func pluginCode(err error) int {
	var pe *PluginError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return 0
}
