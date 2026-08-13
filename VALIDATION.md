# Validation Results — uc_saas_churn_analysis

**Date:** 2026-08-13  
**Pipeline ID:** 983a6ad2-2859-4f2f-b183-14af92b1372b  
**Catalog:** uc_saas_churn_analysis  

## Validation Ladder

| # | Rung | Result | Evidence |
|---|------|--------|----------|
| 1 | `bundle validate` | PASS | `Validation OK!` |
| 2 | Deploy to target workspace | PASS | `Deployment complete!` Pipeline and job created. |
| 3 | Run the pipeline | PASS | Update `3f5a44f4` reached COMPLETED state. All 18 flows completed successfully. |
| 4 | Non-empty gold | PASS | dim_date: 731, dim_customer: 500, dim_subscription: 600, fact_mrr_movement: 1420, fact_churn_event: 108, fact_cohort_retention: 290, fact_customer_activity: 12000 |
| 5 | Grain holds | PASS | All PKs unique and non-null. Zero duplicates across all 7 gold tables. |
| 6 | No orphans | PASS | All FK relationships verified: fact_churn_event->dim_subscription (0), fact_mrr_movement->dim_subscription (0), fact_customer_activity->dim_customer (0), dim_subscription->dim_customer (0), fact_churn_event->dim_date (0) |
| 7 | Catalog conformance | PASS | Gold tables match catalog exactly: dim_date, dim_customer, dim_subscription, fact_mrr_movement, fact_churn_event, fact_cohort_retention, fact_customer_activity. No extra, no missing. |
| 8 | Idempotent | PASS | Second full refresh produced identical row counts across all tables. |
| 9 | No secrets committed | PASS | grep for tokens/keys/secrets found no actual credentials in committed files. |

## Commands Used

```bash
# Rung 1
databricks bundle validate

# Rung 2
databricks bundle deploy

# Rung 3
databricks api post /api/2.0/pipelines/<pipeline_id>/updates --json '{"full_refresh": true}'

# Rungs 4-8
# SQL queries via databricks api post /api/2.0/sql/statements
# (see individual rung descriptions above)

# Rung 9
grep -rn "token|secret|password|key" --include="*.py" --include="*.yml"
```
