package polling

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPClient defines the interface for making HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client fetches telemetry from device HTTP endpoints with bounded timeouts.
type Client struct {
	httpClient HTTPClient
	timeout    time.Duration
}

// NewClient initializes a telemetry polling HTTP client with the specified timeout.
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

// NewClientWithHTTPClient allows injecting a custom HTTPClient (useful for testing).
func NewClientWithHTTPClient(httpClient HTTPClient, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &Client{
		httpClient: httpClient,
		timeout:    timeout,
	}
}

// Poll fetches and decodes Telemetry from the given metricsURL.
func (c *Client) Poll(ctx context.Context, metricsURL string) (Telemetry, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return Telemetry{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Telemetry{}, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Telemetry{}, fmt.Errorf("unexpected http status %d", resp.StatusCode)
	}

	// Limit body read to 1MB to guard against malformed or unbounded responses
	bodyReader := io.LimitReader(resp.Body, 1<<20)

	var t Telemetry
	if err := json.NewDecoder(bodyReader).Decode(&t); err != nil {
		return Telemetry{}, fmt.Errorf("decode telemetry json: %w", err)
	}

	return t, nil
}
