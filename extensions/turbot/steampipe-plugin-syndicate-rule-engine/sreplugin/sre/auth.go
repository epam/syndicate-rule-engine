package sre

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type authTokens struct {
	accessToken  string
	refreshToken string
}

type signInRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type authResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func (c *Client) hasCredentials() bool {
	return strings.TrimSpace(c.cfg.Username) != "" && strings.TrimSpace(c.cfg.Password) != ""
}

func (c *Client) accessToken() string {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.tokens.accessToken
}

func (c *Client) ensureAccessToken(ctx context.Context) error {
	if strings.TrimSpace(c.cfg.Username) == "" || strings.TrimSpace(c.cfg.Password) == "" {
		return fmt.Errorf("SRE API credentials required: set username and password")
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.tokens.accessToken != "" {
		return nil
	}
	return c.signInLocked(ctx)
}

func (c *Client) refreshOrSignIn(ctx context.Context) error {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.tokens.refreshToken != "" {
		if err := c.refreshLocked(ctx); err == nil {
			return nil
		}
	}
	return c.signInLocked(ctx)
}

func (c *Client) signInLocked(ctx context.Context) error {
	body, err := json.Marshal(signInRequest{
		Username: c.cfg.Username,
		Password: c.cfg.Password,
	})
	if err != nil {
		return err
	}
	var resp authResponse
	if err := c.postJSON(ctx, "/signin", body, &resp); err != nil {
		return fmt.Errorf("SRE signin failed: %w", err)
	}
	if resp.AccessToken == "" {
		return fmt.Errorf("SRE signin returned empty access_token")
	}
	c.tokens.accessToken = resp.AccessToken
	if resp.RefreshToken != "" {
		c.tokens.refreshToken = resp.RefreshToken
	}
	return nil
}

func (c *Client) refreshLocked(ctx context.Context) error {
	body, err := json.Marshal(refreshRequest{RefreshToken: c.tokens.refreshToken})
	if err != nil {
		return err
	}
	var resp authResponse
	if err := c.postJSON(ctx, "/refresh", body, &resp); err != nil {
		return err
	}
	if resp.AccessToken == "" {
		return fmt.Errorf("SRE refresh returned empty access_token")
	}
	c.tokens.accessToken = resp.AccessToken
	if resp.RefreshToken != "" {
		c.tokens.refreshToken = resp.RefreshToken
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, path string, body []byte, dest interface{}) error {
	url := strings.TrimRight(c.cfg.APIURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("returned %d: %s", resp.StatusCode, string(raw))
	}
	return json.Unmarshal(raw, dest)
}
