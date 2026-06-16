package sre

import "context"

func jobCacheKey(jobID, customerID string) string {
	if customerID == "" {
		return jobID
	}
	return jobID + "|customer_id=" + customerID
}

func (c *Client) GetJob(ctx context.Context, jobID, customerID string) (*Job, error) {
	key := jobCacheKey(jobID, customerID)
	if val, ok := c.jobCache.Load(key); ok {
		if job, ok := val.(*Job); ok {
			return job, nil
		}
	}

	var wrap struct {
		Data Job `json:"data"`
	}
	if err := c.get(ctx, jobPath(jobID, customerID), customerID, &wrap); err != nil {
		return nil, err
	}
	job := wrap.Data
	c.jobCache.Store(key, &job)
	return &job, nil
}
