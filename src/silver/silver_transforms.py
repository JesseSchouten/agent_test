# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Conformed, Cleaned Entities
# MAGIC Cleaning, deduplication, type casting, and conforming business entities.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Conformed customer dimension from Salesforce accounts"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "company_name IS NOT NULL")
def silver_customers():
    accounts = dlt.read("bronze_salesforce_account")

    return (
        accounts
        .select(
            F.col("Id").alias("customer_id"),
            F.col("Name").alias("company_name"),
            F.col("Segment__c").alias("segment"),
            F.col("Industry").alias("industry"),
            F.col("Company_Size__c").alias("company_size"),
            F.col("Acquisition_Channel__c").alias("acquisition_channel"),
            F.col("OwnerId").alias("account_owner"),
            F.to_date(F.col("CreatedDate")).alias("created_date"),
            F.col("Status__c").alias("status"),
            F.to_timestamp(F.col("LastModifiedDate")).alias("last_modified_at")
        )
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_subscriptions",
    comment="Conformed subscription records from Stripe"
)
@dlt.expect_or_drop("valid_subscription_id", "subscription_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_ref", "customer_id IS NOT NULL")
@dlt.expect("valid_mrr", "mrr >= 0")
def silver_subscriptions():
    subs = dlt.read("bronze_stripe_subscription")

    return (
        subs
        .select(
            F.col("id").alias("subscription_id"),
            F.col("customer").alias("customer_id"),  # aligned with salesforce account ID
            F.col("plan_id").alias("plan"),
            F.col("plan_amount").cast("int").alias("plan_amount"),
            F.col("plan_interval").alias("billing_interval"),
            F.col("status"),
            F.to_date(F.col("start_date")).alias("start_date"),
            F.when(F.col("ended_at") == "", None).otherwise(F.to_date(F.col("ended_at"))).alias("end_date"),
            F.when(F.col("canceled_at") == "", None).otherwise(F.to_date(F.col("canceled_at"))).alias("canceled_date"),
            F.when(F.col("cancellation_reason") == "", None).otherwise(F.col("cancellation_reason")).alias("cancel_reason"),
            F.col("mrr").cast("int").alias("mrr"),
            F.to_timestamp(F.col("updated_at")).alias("updated_at")
        )
        .dropDuplicates(["subscription_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_invoices",
    comment="Conformed invoice records from Stripe"
)
@dlt.expect_or_drop("valid_invoice_id", "invoice_id IS NOT NULL")
@dlt.expect("valid_amount", "amount_cents >= 0")
def silver_invoices():
    invoices = dlt.read("bronze_stripe_invoice")

    return (
        invoices
        .select(
            F.col("id").alias("invoice_id"),
            F.col("customer").alias("customer_id"),
            F.col("subscription").alias("subscription_id"),
            F.col("amount_due").cast("int").alias("amount_cents"),
            F.col("status").alias("payment_status"),
            F.to_date(F.col("created")).alias("invoice_date"),
            F.to_date(F.col("due_date")).alias("due_date"),
            F.col("currency")
        )
        .dropDuplicates(["invoice_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_subscription_events",
    comment="Conformed subscription lifecycle events from Stripe"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_type", "event_type IS NOT NULL")
def silver_subscription_events():
    events = dlt.read("bronze_stripe_event")

    return (
        events
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("subscription_id"),
            F.col("customer_id").alias("customer_id"),
            F.to_timestamp(F.col("created")).alias("event_timestamp")
        )
        .dropDuplicates(["event_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_support_tickets",
    comment="Conformed support ticket records from Salesforce cases"
)
@dlt.expect_or_drop("valid_ticket_id", "ticket_id IS NOT NULL")
@dlt.expect_or_drop("valid_account_ref", "customer_id IS NOT NULL")
def silver_support_tickets():
    cases = dlt.read("bronze_salesforce_case")

    return (
        cases
        .select(
            F.col("Id").alias("ticket_id"),
            F.col("AccountId").alias("customer_id"),
            F.col("Subject").alias("subject"),
            F.col("Priority").alias("priority"),
            F.col("Status").alias("status"),
            F.col("Type").alias("ticket_type"),
            F.to_timestamp(F.col("CreatedDate")).alias("created_at"),
            F.col("Resolution_Hours__c").cast("int").alias("resolution_hours")
        )
        .dropDuplicates(["ticket_id"])
    )
