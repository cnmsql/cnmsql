package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestControlClientRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		response   string
		call       func(*ControlClient) error
		wantMethod string
		wantPath   string
		wantBody   map[string]string
		wantErr    string
	}{
		{
			name: "get decodes response", status: http.StatusOK, response: `{"state":"ready"}`,
			call: func(c *ControlClient) error {
				var out map[string]string
				if err := c.Get(context.Background(), "/status", &out); err != nil {
					return err
				}
				if out["state"] != "ready" {
					return &testError{"response not decoded"}
				}
				return nil
			},
			wantMethod: http.MethodGet, wantPath: "/status",
		},
		{
			name: "post encodes body", status: http.StatusOK,
			call: func(c *ControlClient) error {
				return c.Post(context.Background(), "/reload", map[string]string{"mode": "all"}, nil)
			},
			wantMethod: http.MethodPost, wantPath: "/reload", wantBody: map[string]string{"mode": "all"},
		},
		{
			name: "json error is surfaced", status: http.StatusConflict, response: `{"error":"primary is busy"}`,
			call:       func(c *ControlClient) error { return c.Get(context.Background(), "/status", nil) },
			wantMethod: http.MethodGet, wantPath: "/status", wantErr: "409 Conflict on /status: primary is busy",
		},
		{
			name: "plain error uses status", status: http.StatusBadGateway, response: "bad gateway",
			call:       func(c *ControlClient) error { return c.Get(context.Background(), "/status", nil) },
			wantMethod: http.MethodGet, wantPath: "/status", wantErr: "502 Bad Gateway on /status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := testControlClient(func(r *http.Request) *http.Response {
				if r.Method != tt.wantMethod {
					t.Errorf("method = %s, want %s", r.Method, tt.wantMethod)
				}
				if r.URL.Path != tt.wantPath {
					t.Errorf("path = %s, want %s", r.URL.Path, tt.wantPath)
				}
				if tt.wantBody != nil {
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decoding body: %v", err)
					}
					if body["mode"] != tt.wantBody["mode"] {
						t.Errorf("body = %v", body)
					}
					if got := r.Header.Get("Content-Type"); got != "application/json" {
						t.Errorf("content type = %q", got)
					}
				}
				return &http.Response{
					StatusCode: tt.status,
					Status:     fmt.Sprintf("%d %s", tt.status, http.StatusText(tt.status)),
					Body:       io.NopCloser(strings.NewReader(tt.response)),
				}
			})
			err := tt.call(client)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("call error = %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestControlClientGetText(t *testing.T) {
	t.Parallel()
	client := testControlClient(func(_ *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK",
			Body: io.NopCloser(strings.NewReader("metric 1\n")),
		}
	})
	got, err := client.GetText(context.Background(), "/metrics")
	if err != nil {
		t.Fatalf("GetText() error = %v", err)
	}
	if got != "metric 1\n" {
		t.Errorf("GetText() = %q", got)
	}
}

func TestControlClientResponseFailures(t *testing.T) {
	t.Parallel()

	client := testControlClient(func(_ *http.Request) *http.Response {
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("not-json"))}
	})
	var out map[string]string
	if err := client.Get(context.Background(), "/status", &out); err == nil {
		t.Error("Get() accepted malformed JSON")
	}

	client.http.Transport = roundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return nil, &testError{"transport failed"}
	})
	err := client.Get(context.Background(), "/status", nil)
	if err == nil || !strings.Contains(err.Error(), "transport failed") {
		t.Errorf("Get() transport error = %v", err)
	}
}

func TestControlClientClose(t *testing.T) {
	t.Parallel()

	stop := make(chan struct{})
	done := make(chan struct{})
	close(done)
	client := &ControlClient{forward: &PortForward{stopCh: stop, doneCh: done}}
	client.Close()
	select {
	case <-stop:
	default:
		t.Error("Close did not stop port-forward")
	}
	var nilClient *ControlClient
	nilClient.Close()
}

func testControlClient(response func(*http.Request) *http.Response) *ControlClient {
	return &ControlClient{
		scheme: "http", forward: &PortForward{Local: "instance.test"},
		http: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return response(request), nil
		})},
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type testError struct{ message string }

func (e *testError) Error() string { return e.message }
