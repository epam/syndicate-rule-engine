package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (c *Client) fetchAPI(ctx context.Context, path string) (json.RawMessage, error) {
	return c.fetchAPIWithRetry(ctx, path, false)
}

func (c *Client) fetchAPIWithRetry(ctx context.Context, path string, retried bool) (json.RawMessage, error) {
	if err := c.ensureAccessToken(ctx); err != nil {
		return nil, err
	}

	apiURL := strings.TrimRight(c.cfg.APIURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	if token := c.accessToken(); token != "" {
		req.Header.Set("Authorization", token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusUnauthorized && !retried {
		if err := c.refreshOrSignIn(ctx); err != nil {
			return nil, fmt.Errorf("SRE API %s returned 401 and re-auth failed: %w", path, err)
		}
		return c.fetchAPIWithRetry(ctx, path, true)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("SRE API %s returned %d: %s", path, resp.StatusCode, string(body))
	}
	return body, nil
}
