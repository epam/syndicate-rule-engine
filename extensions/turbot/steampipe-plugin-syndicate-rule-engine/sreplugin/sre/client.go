package sre

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type ClientConfig struct {
	APIURL   string
	Username string
	Password string
}

type Client struct {
	cfg      ClientConfig
	http     *http.Client
	cache    sync.Map
	jobCache sync.Map
	authMu   sync.Mutex
	tokens   authTokens
}

func NewClient(cfg ClientConfig) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) fetchCached(ctx context.Context, path, customerID string) (json.RawMessage, error) {
	key := cacheKey(path, customerID)
	if val, ok := c.cache.Load(key); ok {
		if raw, ok := val.(json.RawMessage); ok {
			return raw, nil
		}
	}

	raw, err := c.fetchAPI(ctx, path)
	if err != nil {
		return nil, err
	}
	c.cache.Store(key, raw)
	return raw, nil
}

func (c *Client) get(ctx context.Context, path, customerID string, dest interface{}) error {
	raw, err := c.fetchCached(ctx, path, customerID)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dest)
}
