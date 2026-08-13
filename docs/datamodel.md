# Datamodel Design — uc_saas_churn_analysis

**Use-case:** `uc_saas_churn_analysis`  
**Industry:** `ind_technology`  
**Datamodels:** `dm_saas_subscription`, `dm_customer_360`  
**Catalog version:** `deployment_catalog_v1`

---

## Gold Layer

### dm_saas_subscription

| Table | Grain | Primary Key | Measures | Dimensions |
|-------|-------|-------------|----------|------------|
| `fact_mrr_movement` | One row per customer per month per movement type | `mrr_movement_id` | mrr_amount, movement_delta | dim_customer, dim_date, movement_type (new, expansion, contraction, churn, reactivation) |
| `fact_churn_event` | One row per churn event | `churn_event_id` | mrr_lost, days_as_customer | dim_customer, dim_date, churn_reason |
| `fact_cohort_retention` | One row per cohort per period | `cohort_retention_id` | customers_retained, retention_rate, revenue_retained | dim_date (cohort month), period_number |
| `dim_subscription` | One row per subscription | `subscription_id` | - | customer_id, plan, status, start_date, end_date, mrr |

### dm_customer_360

| Table | Grain | Primary Key | Measures | Dimensions |
|-------|-------|-------------|----------|------------|
| `dim_customer` | One row per customer | `customer_id` | clv, churn_propensity_score | segment, industry, company_size, acquisition_channel, account_owner |
| `fact_customer_activity` | One row per customer per month | `customer_activity_id` | support_tickets, logins, feature_usage_score, nps_score | dim_customer, dim_date |

### Shared

| Table | Grain | Primary Key |
|-------|-------|-------------|
| `dim_date` | One row per calendar date | `date_key` |

---

## Silver Layer

| Table | Grain | Upstream Bronze |
|-------|-------|-----------------|
| `silver.customers` | One row per customer (deduplicated, conformed) | `bronze.salesforce.account`, `bronze.salesforce.contact` |
| `silver.subscriptions` | One row per subscription (conformed) | `bronze.stripe.subscription` |
| `silver.invoices` | One row per invoice | `bronze.stripe.invoice` |
| `silver.subscription_events` | One row per subscription lifecycle event | `bronze.stripe.event` |
| `silver.support_tickets` | One row per support ticket | `bronze.salesforce.case` |

---

## Bronze Layer

| Table | Source System | Entity | Grain |
|-------|--------------|--------|-------|
| `bronze.stripe.subscription` | Stripe | Subscriptions | One row per subscription record |
| `bronze.stripe.invoice` | Stripe | Invoices | One row per invoice |
| `bronze.stripe.event` | Stripe | Events (subscription lifecycle) | One row per event |
| `bronze.salesforce.account` | Salesforce | Accounts (companies) | One row per account |
| `bronze.salesforce.contact` | Salesforce | Contacts (people) | One row per contact |
| `bronze.salesforce.case` | Salesforce | Cases (support tickets) | One row per case |
