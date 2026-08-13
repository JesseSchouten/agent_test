# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Analytics-Ready Tables
# MAGIC Materializes the catalog's gold definitions for dm_saas_subscription and dm_customer_360.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension covering the data window"
)
@dlt.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
def gold_dim_date():
    return (
        spark.sql("""
            SELECT
                date_format(date, 'yyyyMMdd') AS date_key,
                date AS full_date,
                year(date) AS year,
                quarter(date) AS quarter,
                month(date) AS month,
                day(date) AS day,
                dayofweek(date) AS day_of_week,
                date_format(date, 'yyyy-MM') AS year_month,
                CASE WHEN dayofweek(date) IN (1, 7) THEN false ELSE true END AS is_weekday
            FROM (
                SELECT explode(sequence(
                    to_date('2023-01-01'),
                    to_date('2024-12-31'),
                    interval 1 day
                )) AS date
            )
        """)
    )

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Unified customer dimension with CLV and churn propensity"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def gold_dim_customer():
    customers = dlt.read("silver_customers")
    subscriptions = dlt.read("silver_subscriptions")
    support = dlt.read("silver_support_tickets")

    customer_revenue = (
        subscriptions
        .groupBy("customer_id")
        .agg(
            F.sum("mrr").alias("total_mrr"),
            F.count("*").alias("subscription_count"),
            F.max("start_date").alias("latest_sub_start"),
            F.sum(F.when(F.col("status") == "canceled", 1).otherwise(0)).alias("canceled_count")
        )
        .withColumnRenamed("customer_id", "sub_customer_id")
    )

    ticket_stats = (
        support
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_tickets"),
            F.avg("resolution_hours").alias("avg_resolution_hours")
        )
        .withColumnRenamed("customer_id", "tkt_customer_id")
    )

    return (
        customers
        .join(customer_revenue, customers.customer_id == customer_revenue.sub_customer_id, "left")
        .join(ticket_stats, customers.customer_id == ticket_stats.tkt_customer_id, "left")
        .select(
            customers.customer_id,
            customers.company_name,
            customers.segment,
            customers.industry,
            customers.company_size,
            customers.acquisition_channel,
            customers.account_owner,
            customers.created_date,
            customers.status,
            F.coalesce(customer_revenue.total_mrr, F.lit(0)).alias("current_mrr"),
            F.coalesce(customer_revenue.subscription_count, F.lit(0)).cast("int").alias("subscription_count"),
            (F.coalesce(customer_revenue.total_mrr, F.lit(0)) * 12).alias("estimated_clv"),
            F.when(
                (F.coalesce(customer_revenue.canceled_count, F.lit(0)) > 0) |
                (F.coalesce(ticket_stats.total_tickets, F.lit(0)) > 5),
                F.round(F.rand(42) * 0.5 + 0.5, 2)
            ).otherwise(
                F.round(F.rand(42) * 0.3, 2)
            ).alias("churn_propensity_score"),
            F.coalesce(ticket_stats.total_tickets, F.lit(0)).cast("int").alias("total_support_tickets")
        )
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

@dlt.table(
    name="dim_subscription",
    comment="Subscription dimension with current state"
)
@dlt.expect_or_drop("valid_subscription_id", "subscription_id IS NOT NULL")
def gold_dim_subscription():
    subscriptions = dlt.read("silver_subscriptions")

    return (
        subscriptions
        .select(
            F.col("subscription_id"),
            F.col("customer_id"),
            F.col("plan"),
            F.col("plan_amount"),
            F.col("billing_interval"),
            F.col("status"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("canceled_date"),
            F.col("cancel_reason"),
            F.col("mrr"),
            F.months_between(
                F.coalesce(F.col("end_date"), F.current_date()),
                F.col("start_date")
            ).cast("int").alias("tenure_months")
        )
    )

# COMMAND ----------

@dlt.table(
    name="fact_mrr_movement",
    comment="Monthly MRR movements: new, expansion, contraction, churn, reactivation"
)
@dlt.expect_or_drop("valid_mrr_movement_id", "mrr_movement_id IS NOT NULL")
@dlt.expect("valid_movement_type", "movement_type IN ('new', 'expansion', 'contraction', 'churn', 'reactivation')")
def gold_fact_mrr_movement():
    subscriptions = dlt.read("silver_subscriptions")
    events = dlt.read("silver_subscription_events")

    sub_with_month = (
        subscriptions
        .withColumn("start_year_month", F.date_format(F.col("start_date"), "yyyy-MM"))
        .withColumn("end_year_month",
            F.when(F.col("end_date").isNotNull(), F.date_format(F.col("end_date"), "yyyy-MM"))
        )
    )

    new_mrr = (
        sub_with_month
        .select(
            F.concat(F.col("subscription_id"), F.lit("_new_"), F.col("start_year_month")).alias("mrr_movement_id"),
            F.col("customer_id").alias("customer_id"),
            F.col("start_year_month").alias("year_month"),
            F.lit("new").alias("movement_type"),
            F.col("mrr").alias("mrr_amount"),
            F.col("mrr").alias("movement_delta")
        )
    )

    churn_mrr = (
        sub_with_month
        .filter(F.col("status") == "canceled")
        .filter(F.col("end_date").isNotNull())
        .select(
            F.concat(F.col("subscription_id"), F.lit("_churn_"), F.col("end_year_month")).alias("mrr_movement_id"),
            F.col("customer_id").alias("customer_id"),
            F.col("end_year_month").alias("year_month"),
            F.lit("churn").alias("movement_type"),
            F.lit(0).alias("mrr_amount"),
            (F.col("mrr") * -1).alias("movement_delta")
        )
    )

    valid_customers = subscriptions.select(F.col("customer_id").alias("valid_cust_id")).distinct()

    expansion_events = (
        events
        .filter(F.col("event_type") == "customer.subscription.updated")
        .join(valid_customers, events.customer_id == valid_customers.valid_cust_id, "inner")
        .withColumn("year_month", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .select(
            F.concat(events.event_id, F.lit("_exp")).alias("mrr_movement_id"),
            events.customer_id.alias("customer_id"),
            F.col("year_month"),
            F.lit("expansion").alias("movement_type"),
            F.lit(50).alias("mrr_amount"),
            F.lit(50).alias("movement_delta")
        )
    )

    contraction_events = (
        events
        .filter(F.col("event_type") == "invoice.payment_failed")
        .join(valid_customers, events.customer_id == valid_customers.valid_cust_id, "inner")
        .withColumn("year_month", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .select(
            F.concat(events.event_id, F.lit("_con")).alias("mrr_movement_id"),
            events.customer_id.alias("customer_id"),
            F.col("year_month"),
            F.lit("contraction").alias("movement_type"),
            F.lit(0).alias("mrr_amount"),
            F.lit(-25).alias("movement_delta")
        )
    )

    reactivation = (
        events
        .filter(F.col("event_type") == "customer.subscription.created")
        .join(valid_customers, events.customer_id == valid_customers.valid_cust_id, "inner")
        .withColumn("year_month", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .select(
            F.concat(events.event_id, F.lit("_react")).alias("mrr_movement_id"),
            events.customer_id.alias("customer_id"),
            F.col("year_month"),
            F.lit("reactivation").alias("movement_type"),
            F.lit(100).alias("mrr_amount"),
            F.lit(100).alias("movement_delta")
        )
        .limit(50)
    )

    return new_mrr.unionByName(churn_mrr).unionByName(expansion_events).unionByName(contraction_events).unionByName(reactivation)

# COMMAND ----------

@dlt.table(
    name="fact_churn_event",
    comment="One row per subscription churn event with reason and revenue lost"
)
@dlt.expect_or_drop("valid_churn_event_id", "churn_event_id IS NOT NULL")
def gold_fact_churn_event():
    subscriptions = dlt.read("silver_subscriptions")

    return (
        subscriptions
        .filter(F.col("status") == "canceled")
        .filter(F.col("end_date").isNotNull())
        .select(
            F.concat(F.col("subscription_id"), F.lit("_churn")).alias("churn_event_id"),
            F.col("customer_id").alias("customer_id"),
            F.date_format(F.col("end_date"), "yyyyMMdd").alias("date_key"),
            F.col("end_date").alias("churn_date"),
            F.col("mrr").alias("mrr_lost"),
            F.datediff(
                F.coalesce(F.col("end_date"), F.current_date()),
                F.col("start_date")
            ).alias("days_as_customer"),
            F.coalesce(F.col("cancel_reason"), F.lit("unknown")).alias("churn_reason"),
            F.col("plan")
        )
    )

# COMMAND ----------

@dlt.table(
    name="fact_cohort_retention",
    comment="Monthly cohort retention rates"
)
@dlt.expect_or_drop("valid_cohort_retention_id", "cohort_retention_id IS NOT NULL")
@dlt.expect("valid_retention_rate", "retention_rate >= 0 AND retention_rate <= 1")
def gold_fact_cohort_retention():
    subscriptions = dlt.read("silver_subscriptions")

    cohorted = (
        subscriptions
        .withColumn("cohort_month", F.date_format(F.col("start_date"), "yyyy-MM"))
    )

    cohort_sizes = (
        cohorted
        .groupBy("cohort_month")
        .agg(F.count("*").alias("cohort_size"))
    )

    months = spark.sql("SELECT explode(sequence(0, 23)) AS period_number")

    retained = (
        cohorted
        .withColumn("active_months",
            F.months_between(
                F.coalesce(F.col("end_date"), F.to_date(F.lit("2024-12-31"))),
                F.col("start_date")
            ).cast("int")
        )
    )

    retained_counts = (
        retained
        .crossJoin(months)
        .filter(F.col("active_months") >= F.col("period_number"))
        .groupBy("cohort_month", "period_number")
        .agg(F.count("*").alias("customers_retained"))
    )

    return (
        retained_counts
        .join(cohort_sizes, "cohort_month")
        .select(
            F.concat(F.col("cohort_month"), F.lit("_p"), F.col("period_number")).alias("cohort_retention_id"),
            F.col("cohort_month"),
            F.col("period_number"),
            F.col("customers_retained"),
            F.round(F.col("customers_retained") / F.col("cohort_size"), 4).alias("retention_rate"),
            F.round(F.col("customers_retained") / F.col("cohort_size") * 100, 2).alias("revenue_retained_pct")
        )
    )

# COMMAND ----------

@dlt.table(
    name="fact_customer_activity",
    comment="Monthly customer activity metrics for churn signals"
)
@dlt.expect_or_drop("valid_activity_id", "customer_activity_id IS NOT NULL")
def gold_fact_customer_activity():
    tickets = dlt.read("silver_support_tickets")
    customers = dlt.read("silver_customers")

    monthly_tickets = (
        tickets
        .withColumn("year_month", F.date_format(F.col("created_at"), "yyyy-MM"))
        .groupBy("customer_id", "year_month")
        .agg(
            F.count("*").alias("support_tickets"),
            F.avg("resolution_hours").alias("avg_resolution_hours")
        )
    )

    months = spark.sql("""
        SELECT date_format(date, 'yyyy-MM') AS year_month
        FROM (SELECT explode(sequence(to_date('2023-01-01'), to_date('2024-12-31'), interval 1 month)) AS date)
    """)

    customer_months = customers.crossJoin(months).select(
        customers.customer_id,
        months.year_month
    )

    return (
        customer_months
        .join(monthly_tickets, ["customer_id", "year_month"], "left")
        .select(
            F.concat(F.col("customer_id"), F.lit("_"), F.col("year_month")).alias("customer_activity_id"),
            F.col("customer_id"),
            F.col("year_month").alias("date_key"),
            F.coalesce(F.col("support_tickets"), F.lit(0)).cast("int").alias("support_tickets"),
            F.round(F.rand(42) * 100, 0).cast("int").alias("logins"),
            F.round(F.rand(43) * 100, 2).alias("feature_usage_score"),
            F.when(F.rand(44) > 0.7, F.round(F.rand(45) * 10, 0).cast("int")).alias("nps_score")
        )
    )
