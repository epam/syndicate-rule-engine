package common

import (
	"context"

	"github.com/epam/steampipe-plugin-syndicate-rule-engine/sreplugin/sre"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func JobFromQuery(ctx context.Context, client *sre.Client, d *plugin.QueryData) (jobID, customerID string, job *sre.Job, err error) {
	jobID, err = RequireQualString(d, "job_id")
	if err != nil {
		return "", "", nil, err
	}
	customerID = CustomerID(d)
	job, err = client.GetJob(ctx, jobID, customerID)
	if err != nil {
		return jobID, customerID, nil, err
	}
	return jobID, customerID, job, nil
}
