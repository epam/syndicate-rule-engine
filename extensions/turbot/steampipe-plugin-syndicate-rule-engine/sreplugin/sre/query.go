package sre

import (
	"fmt"
	"net/url"
	"strings"
)

var defaultJobTypes = []string{"standard", "scheduled", "reactive"}

func buildPath(base string, params url.Values) string {
	if len(params) == 0 {
		return base
	}
	return base + "?" + params.Encode()
}

func mergeQueryParams(path string, params url.Values) string {
	if len(params) == 0 {
		return path
	}
	base, existing, hasQuery := strings.Cut(path, "?")
	merged := url.Values{}
	if hasQuery {
		if parsed, err := url.ParseQuery(existing); err == nil {
			merged = parsed
		}
	}
	for key, values := range params {
		for _, value := range values {
			merged.Add(key, value)
		}
	}
	return buildPath(base, merged)
}

func withCustomerID(path, customerID string) string {
	if customerID == "" {
		return path
	}
	params := url.Values{}
	params.Set("customer_id", customerID)
	return mergeQueryParams(path, params)
}

func withJobTypes(path, jobType string) string {
	seen := map[string]struct{}{}
	types := append([]string{jobType}, defaultJobTypes...)
	params := url.Values{}
	for _, t := range types {
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		params.Add("job_types", t)
	}
	return mergeQueryParams(path, params)
}

func jobReportPath(segment, jobID, customerID, jobType string) string {
	path := fmt.Sprintf("/reports/%s/jobs/%s", segment, jobID)
	return withJobTypes(withCustomerID(path, customerID), jobType)
}

func jobPath(jobID, customerID string) string {
	return withCustomerID(fmt.Sprintf("/jobs/%s", jobID), customerID)
}

func cacheKey(path, customerID string) string {
	if customerID == "" {
		return path
	}
	return path + "|customer_id=" + customerID
}
