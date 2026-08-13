# uc_saas_churn_analysis — SaaS Churn Analysis Datamodel

## Overview

| Field | Value |
|-------|-------|
| **Use-case** | `uc_saas_churn_analysis` |
| **Industry** | `ind_technology` |
| **Datamodels** | `dm_saas_subscription`, `dm_customer_360` |
| **Catalog version** | `deployment_catalog_v1` |
| **Skill version** | `datamodel-build-and-validate` v1 |
| **Deployed catalog** | `uc_saas_churn_analysis` |
| **Pipeline name** | `uc_saas_churn_analysis_pipeline` |
| **Job name** | `uc_saas_churn_analysis_refresh` |
| **Pipeline ID** | `983a6ad2-2859-4f2f-b183-14af92b1372b` |
| **Workspace** | `https://dbc-0f232e4d-3ed4.cloud.databricks.com` |

## Description

Analyzes subscription churn and net revenue retention to target at-risk accounts. Built on a medallion architecture (bronze → silver → gold) using Databricks Delta Live Tables (DLT) with serverless compute.

## Gold Tables

| Table | Description | Row Count |
|-------|-------------|-----------|
| `dim_date` | Calendar date dimension (2023-01-01 to 2024-12-31) | 731 |
| `dim_customer` | Unified customer dimension with CLV and churn propensity score | 500 |
| `dim_subscription` | Subscription dimension with plan, status, and tenure | 600 |
| `fact_mrr_movement` | Monthly MRR movements (new, expansion, contraction, churn, reactivation) | 1,420 |
| `fact_churn_event` | One row per churn event with reason, MRR lost, and tenure | 108 |
| `fact_cohort_retention` | Monthly cohort retention rates | 290 |
| `fact_customer_activity` | Monthly customer activity metrics (tickets, logins, usage) | 12,000 |

## Synthetic Data

| Parameter | Value |
|-----------|-------|
| **Seed** | 42 |
| **Date window** | 2023-01-01 to 2024-12-31 |
| **Customers** | 500 |
| **Subscriptions** | 600 |
| **Invoices** | 5,000 |
| **Events** | 3,000 |
| **Contacts** | 700 |
| **Support cases** | 1,200 |

Data is deterministic — re-running with the same seed produces identical results.

### Source Systems (Synthetic)

- **Stripe**: Subscriptions, invoices, subscription lifecycle events
- **Salesforce**: Accounts (companies), contacts (people), cases (support tickets)

## How to Re-run

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run the pipeline (full refresh)
databricks bundle run churn_analysis_pipeline

# Or trigger via API
databricks api post /api/2.0/pipelines/<pipeline_id>/updates --json '{"full_refresh": true}'
```

## Project Structure

```
├── databricks.yml              # Bundle configuration
├── docs/
│   └── datamodel.md            # Gold-first datamodel design
├── src/
│   ├── bronze/
│   │   └── generate_bronze.py  # Synthetic bronze data generation
│   ├── silver/
│   │   └── silver_transforms.py # Silver layer transformations
│   └── gold/
│       └── gold_transforms.py  # Gold layer aggregations
├── VALIDATION.md               # Validation ladder results
└── README.md                   # This file
```

## Status

**Working** — All 9 validation rungs pass. The datamodel is fully deployed and operational.
