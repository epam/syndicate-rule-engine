package sre

import (
	"context"
	"net/url"
)

func listItems[T any](c *Client, ctx context.Context, path, customerID string) ([]T, error) {
	var wrap struct {
		Items []T `json:"items"`
	}
	if err := c.get(ctx, path, customerID, &wrap); err != nil {
		return nil, err
	}
	return wrap.Items, nil
}

func (c *Client) ListCustomers(ctx context.Context) ([]Customer, error) {
	return listItems[Customer](c, ctx, "/customers", "")
}

func (c *Client) ListTenants(ctx context.Context, customerID string) ([]Tenant, error) {
	return listItems[Tenant](c, ctx, withCustomerID("/tenants", customerID), customerID)
}

func (c *Client) ListPlatforms(ctx context.Context, customerID, tenantName string) ([]K8sPlatform, error) {
	params := url.Values{}
	if tenantName != "" {
		params.Set("tenant_name", tenantName)
	}
	path := withCustomerID(buildPath("/platforms/k8s", params), customerID)
	return listItems[K8sPlatform](c, ctx, path, customerID)
}

func (c *Client) ListJobs(ctx context.Context, customerID, tenantName, platformID, status string) ([]Job, error) {
	params := url.Values{}
	if tenantName != "" {
		params.Set("tenant_name", tenantName)
	}
	if status != "" {
		params.Set("status", status)
	}
	path := withCustomerID(buildPath("/jobs", params), customerID)

	items, err := listItems[Job](c, ctx, path, customerID)
	if err != nil {
		return nil, err
	}
	if platformID == "" {
		return items, nil
	}

	filtered := make([]Job, 0, len(items))
	for _, job := range items {
		if job.PlatformID == platformID {
			filtered = append(filtered, job)
		}
	}
	return filtered, nil
}
