# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Synthetic Data Generation
# MAGIC Use-case: uc_saas_churn_analysis
# MAGIC Sources: Stripe (subscriptions, invoices, events), Salesforce (accounts, contacts, cases)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
import hashlib

SEED = int(spark.conf.get("seed", "42"))
DATA_WINDOW_START = spark.conf.get("data_window_start", "2023-01-01")
DATA_WINDOW_END = spark.conf.get("data_window_end", "2024-12-31")

NUM_CUSTOMERS = 500
NUM_SUBSCRIPTIONS = 600
NUM_INVOICES = 5000
NUM_EVENTS = 3000
NUM_CONTACTS = 700
NUM_CASES = 1200

# COMMAND ----------

@dlt.table(
    name="bronze_salesforce_account",
    comment="Raw Salesforce account records (companies)"
)
def bronze_salesforce_account():
    import random
    random.seed(SEED)

    segments = ["Enterprise", "Mid-Market", "SMB", "Startup"]
    industries = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", "Media", "Education"]
    sizes = ["1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "5000+"]
    channels = ["Inbound", "Outbound", "Partner", "Referral", "Self-Serve"]
    owners = ["rep_001", "rep_002", "rep_003", "rep_004", "rep_005"]

    rows = []
    for i in range(NUM_CUSTOMERS):
        random.seed(SEED + i)
        acct_id = f"001{hashlib.md5(f'account_{i}'.encode()).hexdigest()[:12]}"
        created_offset = random.randint(0, 365)
        rows.append((
            acct_id,
            f"Company_{i:04d}",
            random.choice(segments),
            random.choice(industries),
            random.choice(sizes),
            random.choice(channels),
            random.choice(owners),
            f"{2023 + created_offset // 365}-{(created_offset % 12) + 1:02d}-{(created_offset % 28) + 1:02d}",
            random.choice(["Active", "Active", "Active", "Inactive"]),
            f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:00:00Z"
        ))

    schema = StructType([
        StructField("Id", StringType()),
        StructField("Name", StringType()),
        StructField("Segment__c", StringType()),
        StructField("Industry", StringType()),
        StructField("Company_Size__c", StringType()),
        StructField("Acquisition_Channel__c", StringType()),
        StructField("OwnerId", StringType()),
        StructField("CreatedDate", StringType()),
        StructField("Status__c", StringType()),
        StructField("LastModifiedDate", StringType()),
    ])

    return spark.createDataFrame(rows, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_salesforce_contact",
    comment="Raw Salesforce contact records (people)"
)
def bronze_salesforce_contact():
    import random
    random.seed(SEED + 1000)

    rows = []
    for i in range(NUM_CONTACTS):
        random.seed(SEED + 1000 + i)
        contact_id = f"003{hashlib.md5(f'contact_{i}'.encode()).hexdigest()[:12]}"
        acct_idx = random.randint(0, NUM_CUSTOMERS - 1)
        acct_id = f"001{hashlib.md5(f'account_{acct_idx}'.encode()).hexdigest()[:12]}"
        rows.append((
            contact_id,
            acct_id,
            f"First_{i:04d}",
            f"Last_{i:04d}",
            f"user_{i}@company_{acct_idx}.com",
            random.choice(["CEO", "CTO", "VP Engineering", "Director", "Manager", "IC"]),
            random.choice(["true", "false"]),
            f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        ))

    schema = StructType([
        StructField("Id", StringType()),
        StructField("AccountId", StringType()),
        StructField("FirstName", StringType()),
        StructField("LastName", StringType()),
        StructField("Email", StringType()),
        StructField("Title", StringType()),
        StructField("IsActive__c", StringType()),
        StructField("CreatedDate", StringType()),
    ])

    return spark.createDataFrame(rows, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_salesforce_case",
    comment="Raw Salesforce case records (support tickets)"
)
def bronze_salesforce_case():
    import random
    random.seed(SEED + 2000)

    priorities = ["Low", "Medium", "Medium", "High", "Critical"]
    statuses = ["New", "Open", "In Progress", "Resolved", "Closed"]
    types = ["Bug", "Feature Request", "Question", "Billing", "Onboarding"]

    rows = []
    for i in range(NUM_CASES):
        random.seed(SEED + 2000 + i)
        case_id = f"500{hashlib.md5(f'case_{i}'.encode()).hexdigest()[:12]}"
        acct_idx = random.randint(0, NUM_CUSTOMERS - 1)
        acct_id = f"001{hashlib.md5(f'account_{acct_idx}'.encode()).hexdigest()[:12]}"
        month = random.randint(1, 24)
        year = 2023 + (month - 1) // 12
        m = ((month - 1) % 12) + 1
        rows.append((
            case_id,
            acct_id,
            f"Issue {i}: {random.choice(types)} problem",
            random.choice(priorities),
            random.choice(statuses),
            random.choice(types),
            f"{year}-{m:02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z",
            str(random.randint(0, 72)),
        ))

    schema = StructType([
        StructField("Id", StringType()),
        StructField("AccountId", StringType()),
        StructField("Subject", StringType()),
        StructField("Priority", StringType()),
        StructField("Status", StringType()),
        StructField("Type", StringType()),
        StructField("CreatedDate", StringType()),
        StructField("Resolution_Hours__c", StringType()),
    ])

    return spark.createDataFrame(rows, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_stripe_subscription",
    comment="Raw Stripe subscription records"
)
def bronze_stripe_subscription():
    import random
    random.seed(SEED + 3000)

    plans = [
        ("starter", 49), ("professional", 149), ("business", 399), ("enterprise", 999)
    ]
    statuses = ["active", "active", "active", "canceled", "past_due", "trialing"]
    intervals = ["month", "month", "month", "year"]

    rows = []
    for i in range(NUM_SUBSCRIPTIONS):
        random.seed(SEED + 3000 + i)
        sub_id = f"sub_{hashlib.md5(f'sub_{i}'.encode()).hexdigest()[:16]}"
        cust_idx = random.randint(0, NUM_CUSTOMERS - 1)
        cust_id = f"001{hashlib.md5(f'account_{cust_idx}'.encode()).hexdigest()[:12]}"
        plan = random.choice(plans)
        status = random.choice(statuses)
        start_month = random.randint(1, 20)
        start_year = 2023 + (start_month - 1) // 12
        start_m = ((start_month - 1) % 12) + 1
        start_date = f"{start_year}-{start_m:02d}-{random.randint(1,28):02d}"

        end_date = ""
        cancel_date = ""
        cancel_reason = ""
        if status == "canceled":
            end_offset = random.randint(30, 365)
            cancel_reason = random.choice(["too_expensive", "missing_features", "switched_service", "unused", "other"])
            cancel_date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            end_date = cancel_date

        mrr = plan[1] if random.choice(intervals) == "month" else plan[1] // 12

        rows.append((
            sub_id,
            cust_id,
            plan[0],
            str(plan[1]),
            random.choice(intervals),
            status,
            start_date,
            end_date,
            cancel_date,
            cancel_reason,
            str(mrr),
            f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:00:00Z",
        ))

    schema = StructType([
        StructField("id", StringType()),
        StructField("customer", StringType()),
        StructField("plan_id", StringType()),
        StructField("plan_amount", StringType()),
        StructField("plan_interval", StringType()),
        StructField("status", StringType()),
        StructField("start_date", StringType()),
        StructField("ended_at", StringType()),
        StructField("canceled_at", StringType()),
        StructField("cancellation_reason", StringType()),
        StructField("mrr", StringType()),
        StructField("updated_at", StringType()),
    ])

    return spark.createDataFrame(rows, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_stripe_invoice",
    comment="Raw Stripe invoice records"
)
def bronze_stripe_invoice():
    import random
    random.seed(SEED + 4000)

    rows = []
    for i in range(NUM_INVOICES):
        random.seed(SEED + 4000 + i)
        inv_id = f"in_{hashlib.md5(f'invoice_{i}'.encode()).hexdigest()[:16]}"
        sub_idx = random.randint(0, NUM_SUBSCRIPTIONS - 1)
        sub_id = f"sub_{hashlib.md5(f'sub_{sub_idx}'.encode()).hexdigest()[:16]}"
        cust_idx = random.randint(0, NUM_CUSTOMERS - 1)
        cust_id = f"001{hashlib.md5(f'account_{cust_idx}'.encode()).hexdigest()[:12]}"

        month = random.randint(1, 24)
        year = 2023 + (month - 1) // 12
        m = ((month - 1) % 12) + 1
        amount = random.choice([49, 149, 399, 999]) * 100

        rows.append((
            inv_id,
            cust_id,
            sub_id,
            str(amount),
            random.choice(["paid", "paid", "paid", "open", "void", "uncollectible"]),
            f"{year}-{m:02d}-{random.randint(1,28):02d}",
            f"{year}-{m:02d}-{min(random.randint(1,28)+5, 28):02d}",
            "usd",
        ))

    schema = StructType([
        StructField("id", StringType()),
        StructField("customer", StringType()),
        StructField("subscription", StringType()),
        StructField("amount_due", StringType()),
        StructField("status", StringType()),
        StructField("created", StringType()),
        StructField("due_date", StringType()),
        StructField("currency", StringType()),
    ])

    return spark.createDataFrame(rows, schema)

# COMMAND ----------

@dlt.table(
    name="bronze_stripe_event",
    comment="Raw Stripe subscription lifecycle events"
)
def bronze_stripe_event():
    import random
    random.seed(SEED + 5000)

    event_types = [
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
        "customer.subscription.trial_will_end",
        "invoice.payment_succeeded",
        "invoice.payment_failed",
    ]

    rows = []
    for i in range(NUM_EVENTS):
        random.seed(SEED + 5000 + i)
        evt_id = f"evt_{hashlib.md5(f'event_{i}'.encode()).hexdigest()[:16]}"
        sub_idx = random.randint(0, NUM_SUBSCRIPTIONS - 1)
        sub_id = f"sub_{hashlib.md5(f'sub_{sub_idx}'.encode()).hexdigest()[:16]}"
        cust_idx = random.randint(0, NUM_CUSTOMERS - 1)
        cust_id = f"001{hashlib.md5(f'account_{cust_idx}'.encode()).hexdigest()[:12]}"

        month = random.randint(1, 24)
        year = 2023 + (month - 1) // 12
        m = ((month - 1) % 12) + 1

        rows.append((
            evt_id,
            random.choice(event_types),
            sub_id,
            cust_id,
            f"{year}-{m:02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z",
        ))

    schema = StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("subscription_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("created", StringType()),
    ])

    return spark.createDataFrame(rows, schema)
