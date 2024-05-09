## SaaS scheme
![SaaS retry scheme](../../../docs/pics/saas_retry.png)
## Onprem scheme (lambda icons = python function)
![OnPrem retry scheme](../../../docs/pics/onprem_retry.png)

## Testing tips
- To enable/disable setting that blocks reports sending on the Custodian side, use these CLI commands:
   `c7n setting report enable_sending --confirm`
   `c7n setting report disable_sending --confirm`
- To raise ReportNotSendException to test retry functionality, set environment variable `CAAS_TESTING` as `true` 
(for saas: in `custodian_report_generation_handler` lambda) and run any report
Only for onprem:
- To remove all active schedules, set environment variable `TESTING_DROP_SCHEDULES` as `true` and try to trigger any report
- To set max number of crons at the same time, use setting `MAX_CRON_NUMBER` in `CaaSSettings` table (min=2, max=20, default=10)

### NOTE: 
`by any "/reports/" endpoint` I mean any of these endpoints: `/reports/operational`, `/reports/project`, `/reports/department`, `/reports/clevel` 

## Use cases
1. Trigger any `/reports/*` endpoint via CLI or API while setting `SEND_REPORTS`=`false`, request will be saved to `CaaSReportStatistics` table with status `PENDING`. 
2. Trigger any `/reports/*` endpoint via CLI or API with setting `SEND_REPORTS`=`true`:
   1. Report was sent successfully and saved to `CaaSReportStatistics` table with `SUCCEEDED` status.
   2. Report was not sent because of any RabbitMQ or Maestro server issues:
      - OnPrem: request information is stored to `CaaSReportStatistics` table, cron created that will retry sending request in 15 minutes. Was saved to `CaaSReportStatistics` table with `FAILED` status;
      - SaaS: request information is stored to `CaaSReportStatistics` table, step function will retry sending request in 15 minutes. Was saved to `CaaSReportStatistics` table with `FAILED` status;
   3. Report was not sent because of any other issues and saved to `CaaSReportStatistics` table with `FAILED` status.
3. Trigger any `/reports/*` endpoint via cron or step function for the 1-3rd times:
   1. Report was sent successfully and saved to `CaaSReportStatistics` table with `SUCCEEDED` status.
   2. Report was not sent, request information is stored to `CaaSReportStatistics` table with new attempt number and:
      - OnPrem: existing cron related to this request has be updated with bigger interval (15 min * attempt number). Item saved to `CaaSReportStatistics` table with `FAILED` status;
      - SaaS: step function retrying send the request with a longer interval. Item saved to `CaaSReportStatistics` table with `FAILED` status;
4. Trigger any `/reports/*` endpoint via cron or step function for the 4th time:
   1. Report was sent successfully.
   2. Report was not sent, value of `SEND_REPORTS` setting changed to `false`, all subsequent requests to trigger reports will be saved to `CaaSReportStatistics` table with status `PENDING` and:
      - OnPrem: removed cron job of current retry. All subsequent cron jobs (that were created previously) will be removed at the start of execution.
5. Trigger `/reports/retry` endpoint while there are no items with `PENDING` status in `CaaSReportStatistics` table;
6. Trigger `/reports/retry` endpoint while there is one item with `PENDING` status in `CaaSReportStatistics` table:
   - OnPrem: the report sending function will run asynchronously;
   - SaaS: lambda will trigger the report sending step function;
7. Trigger `/reports/retry` endpoint and multiple items with `PENDING` status in `CaaSReportStatistics` table:
   - OnPrem: the report sending function will run asynchronously for each item in loop, excluding duplicates;
   - SaaS: lambda will trigger the report sending step function for each item in loop, excluding duplicates;
8. Trigger `/reports/retry` endpoint with setting `SEND_REPORTS`=`false`


### Items status:
- `FAILED` - failed to trigger reports
- `PENDING` - the request was created when the `SEND_REPORTS` setting was disabled
- `DUPLICATE` - the request for this specific entity and report type has already been retried within one "retry session" (retry lambda execution)
- `SUCCEEDED` - the report triggered successfully
- `RETRIED` - the report was successfully rerun


### Step function input examples:
For retry-send-report:
```json
{ 
   "requestContext": {
      "authorizer": { 
         "claims": {
            "cognito:username":"system_user", 
            "custom:role":"system_role", 
            "custom:customer":"CUSTODIAN_SYSTEM"
         }
      }, 
      "resourcePath": "/reports/retry", 
      "path": "/caas/reports/retry"
   }, 
   "headers": {
      "Host": "${ID}.execute-api.eu-west-1.amazonaws.com"
   }, 
   "httpMethod": "POST"
}
```


For send-report:
```json
{
   "pathParameters": {}, 
   "path": "/reports/department", 
   "httpMethod": "POST", 
   "headers": {
      "Content-Length": "16", "Content-Type": "application/json", "Host": "host", 
      "User-Agent": "python-requests/2.28.2", "Accept-Encoding": "gzip, deflate", "Accept": "*/*", 
      "Connection": "keep-alive"}, 
   "requestContext": {
      "stage": "caas", 
      "resourcePath": "/reports/department", "path": "caas/reports/department", 
      "authorizer": {"claims": {"cognito:username": "system", "custom:customer": "CUSTODIAN_SYSTEM", 
         "custom:role": "system_role", "custom:tenants": []}
      }
   }, 
   "tenant_names": "NAMES", // only for operational report
   "tenant_display_names": "NAMES", // only for project report
}
```