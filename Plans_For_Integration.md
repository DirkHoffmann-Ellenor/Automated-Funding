For your setup (Static Web App + Azure Container App API), the best pattern is:

Use a scheduled Azure Container Apps Job to run monthly discovery.
Have that job call a protected internal API endpoint that enqueues scrape URLs.
Store monthly artifacts in Azure Blob Storage with lifecycle rules.
Keep API app scale-to-zero and stateless.
This is the most reliable + cost-efficient way to run “first of month” logic.

Azure Resource Plan

Create one storage account in East US (Standard GPv2, LRS) for monthly artifacts/state.
Create blob containers:
monthly-state
monthly-artifacts
Use prefixes and lifecycle:
raw/ keep 7-14 days, then delete.
runs/ move to Cool after 30 days, delete after 365 days.
latest/ keep indefinitely (small files only).
Reuse your existing managed identity (af-containerapps-identity) and grant Storage Blob Data Contributor on this storage account.
Add one Container Apps Job (schedule trigger) with cron 15 3 1 * * (UTC).
Add a secret token for internal endpoint auth (Key Vault-backed secret preferred).
Important operational note

Your API currently uses in-memory job tracking and 2 gunicorn workers. That can cause missing job status across workers.
For now, set API to one worker for reliability of in-memory job store, or move job state to Redis in this same project update.
Step-by-Step Code Update Plan (Start to Finish)

Add monthly config env vars in api/config.py:
MONTHLY_ENABLED
MONTHLY_INTERNAL_TOKEN
MONTHLY_STORAGE_ACCOUNT_URL
MONTHLY_STATE_CONTAINER
MONTHLY_ARTIFACTS_CONTAINER
MONTHLY_TIMEZONE (recommend UTC)
Add dependency versions for blob + identity SDK in requirements.txt.
Create monthly_state_store.py:
month key generation (YYYY-MM)
lock acquire/release
state read/write (started, completed, failed, counts)
idempotency check (“already completed this month?”)
Create scrape_prepare.py and move shared dedupe prep logic from api/routes/scrape.py so both normal scrape and monthly ingestion use exactly one dedupe path.
Create monthly_ingest.py:
parse to_scrape_pipeline_latest.csv
dedupe by registered_charity_number
dedupe by normalized URL
check existing processed URLs (current sheet-backed logic)
enqueue via existing job_store.create(...)
Add monthly.py with protected internal endpoints:
POST /internal/monthly/ingest (job sends discovered URLs)
POST /internal/monthly/run (optional manual admin trigger)
GET /internal/monthly/status/{month_key}
Register router in api/main.py.
Create monthly_job_runner.py:
run automate_grant_making_pipeline.py with --required-only
read generated to_scrape CSV
call /internal/monthly/ingest with token
upload artifacts (delta, snapshot, to_scrape, run_metadata) to blob
write month state (completed/failed)
Keep grant_prospector.py mostly as-is; it already has charity-id dedupe and uniqueness checks.
Add URL canonicalization hardening in utils/tools.py for Charity Commission URLs to reduce query-string duplicates.
Add tests:
monthly idempotency (run once per month)
CSV dedupe (same charity repeated)
URL dedupe with variant query params
existing-results exclusion
ingest endpoint auth rejection without token
Add deployment wiring:
API env vars/secrets
Job env vars/secrets
Job schedule
identity RBAC
Staging dry run:
manual run job
verify produced artifacts in blob
verify to_scrape counts
verify only new URLs queued
Production cutover:
deploy API update
deploy monthly job
run one manual “force” execution
enable cron schedule
Monitoring/runbook:
log summary per monthly run
alert if monthly state is failed
alert if zero records for two consecutive months (sanity check)
Duplicate-Protection Model (final)

Charity-level dedupe in monthly pipeline output.
URL-level dedupe before queueing.
Existing-results dedupe against sheet URLs.
Month-level idempotency lock/state so reruns do not re-enqueue.
If you want, next I can convert this plan into a concrete task checklist mapped to exact PR chunks (PR1 infra, PR2 backend services, PR3 tests/deploy).

Sources:

Azure Container Apps Jobs: https://learn.microsoft.com/en-us/azure/container-apps/jobs
Container Apps scaling/scale-to-zero: https://learn.microsoft.com/en-us/azure/container-apps/scale-app
Container Apps billing: https://learn.microsoft.com/en-us/azure/container-apps/billing
Container Apps storage mounts: https://learn.microsoft.com/en-us/azure/container-apps/storage-mounts
Blob lifecycle policies: https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-structure
Blob access tiers and early deletion behavior: https://learn.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview
Managed identities in Container Apps: https://learn.microsoft.com/azure/container-apps/managed-identity