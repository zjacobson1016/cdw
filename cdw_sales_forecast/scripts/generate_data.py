"""Generate synthetic sales data for CDW Corp sales forecasting demo."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import holidays
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "mfg_mc_se_sa"
SCHEMA = "cdw_sales_forecast"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

N_SALES_REPS = 35
N_INVOICES = 15000
N_ORDERS = 12000
N_DEALS = 4000

END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

US_HOLIDAYS = holidays.US(years=[START_DATE.year, END_DATE.year])

SEED = 42

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

spark = DatabricksSession.builder.profile("group-demo").serverless().getOrCreate()
# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
print(f"Creating catalog/schema/volume if needed...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")

print(f"Generating: {N_SALES_REPS} reps, {N_INVOICES:,} invoices, {N_ORDERS:,} orders, {N_DEALS:,} deals")

# =============================================================================
# CDW-SPECIFIC REFERENCE DATA
# =============================================================================
PRODUCT_CATEGORIES = [
    "Servers & Storage", "Networking Equipment", "Endpoint Devices",
    "Security Solutions", "Cloud Infrastructure", "Collaboration Tools",
    "Software Licensing", "Managed Services"
]

CDW_REGIONS = ["Central", "East", "West", "Federal", "Canada"]

CUSTOMER_VERTICALS = [
    "Healthcare", "Finance", "Education", "Government",
    "Manufacturing", "Retail", "Technology", "Legal"
]

DEAL_STAGES = [
    "Prospecting", "Discovery", "Proposal Sent", "Negotiation",
    "Verbal Commit", "Closed Won", "Closed Lost"
]

INVOICE_STATUSES = [
    "Invoiced", "Shipped", "Partially Shipped", "Delivered", "Cancelled"
]

# =============================================================================
# 1. SALES REPS (Master Table)
# =============================================================================
print("Generating sales reps...")

reps_data = []
for i in range(N_SALES_REPS):
    region = np.random.choice(CDW_REGIONS, p=[0.30, 0.25, 0.20, 0.15, 0.10])
    role = np.random.choice(
        ["Account Executive", "Senior Account Executive", "Strategic Account Manager"],
        p=[0.50, 0.35, 0.15]
    )
    reps_data.append({
        "rep_id": f"REP-{i+1:04d}",
        "rep_name": fake.name(),
        "email": fake.email(),
        "region": region,
        "role": role,
        "manager_name": fake.name(),
        "hire_date": fake.date_between(start_date="-8y", end_date="-6m").isoformat(),
        "annual_quota": round(np.random.lognormal(13.5, 0.4), 2),
    })

reps_pdf = pd.DataFrame(reps_data)
rep_ids = reps_pdf["rep_id"].tolist()
rep_region_map = dict(zip(reps_pdf["rep_id"], reps_pdf["region"]))

role_weights = reps_pdf["role"].map({
    "Strategic Account Manager": 4.0,
    "Senior Account Executive": 2.5,
    "Account Executive": 1.0
})
rep_weights = (role_weights / role_weights.sum()).tolist()

print(f"  Created {len(reps_pdf)} sales reps")

# =============================================================================
# 2. CUSTOMERS
# =============================================================================
print("Generating customers...")
N_CUSTOMERS = 800

customers_data = []
for i in range(N_CUSTOMERS):
    vertical = np.random.choice(CUSTOMER_VERTICALS, p=[0.18, 0.16, 0.15, 0.14, 0.12, 0.10, 0.10, 0.05])
    tier = np.random.choice(["Enterprise", "Mid-Market", "SMB"], p=[0.15, 0.35, 0.50])
    customers_data.append({
        "customer_id": f"CDW-CUST-{i+1:05d}",
        "customer_name": fake.company(),
        "vertical": vertical,
        "tier": tier,
        "region": np.random.choice(CDW_REGIONS, p=[0.30, 0.25, 0.20, 0.15, 0.10]),
        "assigned_rep_id": np.random.choice(rep_ids, p=rep_weights),
        "created_date": fake.date_between(start_date="-5y", end_date="-1m").isoformat(),
    })

customers_pdf = pd.DataFrame(customers_data)
customer_ids = customers_pdf["customer_id"].tolist()
customer_tier_map = dict(zip(customers_pdf["customer_id"], customers_pdf["tier"]))
customer_rep_map = dict(zip(customers_pdf["customer_id"], customers_pdf["assigned_rep_id"]))

tier_weights_c = customers_pdf["tier"].map({"Enterprise": 5.0, "Mid-Market": 2.0, "SMB": 1.0})
customer_weights = (tier_weights_c / tier_weights_c.sum()).tolist()

print(f"  Created {len(customers_pdf)} customers")

# =============================================================================
# 3. INVOICES (Revenue records with statuses)
# =============================================================================
print("Generating invoices...")

invoices_data = []
for i in range(N_INVOICES):
    cid = np.random.choice(customer_ids, p=customer_weights)
    tier = customer_tier_map[cid]
    rep_id = customer_rep_map[cid]
    invoice_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

    if tier == "Enterprise":
        amount = round(np.random.lognormal(9.5, 0.8), 2)
    elif tier == "Mid-Market":
        amount = round(np.random.lognormal(8.0, 0.7), 2)
    else:
        amount = round(np.random.lognormal(6.5, 0.6), 2)

    status = np.random.choice(
        INVOICE_STATUSES,
        p=[0.30, 0.35, 0.15, 0.15, 0.05]
    )

    recognized_pct = {
        "Invoiced": np.random.choice([0.0, 0.25, 0.50], p=[0.50, 0.30, 0.20]),
        "Shipped": 1.0,
        "Partially Shipped": round(np.random.uniform(0.25, 0.75), 2),
        "Delivered": 1.0,
        "Cancelled": 0.0,
    }[status]

    product_category = np.random.choice(PRODUCT_CATEGORIES, p=[0.20, 0.15, 0.18, 0.12, 0.10, 0.08, 0.10, 0.07])

    invoices_data.append({
        "invoice_id": f"INV-{i+1:06d}",
        "customer_id": cid,
        "rep_id": rep_id,
        "invoice_date": invoice_date.isoformat(),
        "invoice_amount": amount,
        "status": status,
        "revenue_recognized_pct": recognized_pct,
        "product_category": product_category,
        "po_number": f"PO-{fake.random_int(min=100000, max=999999)}",
        "payment_terms": np.random.choice(["Net 30", "Net 45", "Net 60", "Net 90"], p=[0.40, 0.30, 0.20, 0.10]),
    })

invoices_pdf = pd.DataFrame(invoices_data)
print(f"  Created {len(invoices_pdf):,} invoices")

# =============================================================================
# 4. CONFIRMED ORDERS (Hardware shipped)
# =============================================================================
print("Generating confirmed orders...")

orders_data = []
for i in range(N_ORDERS):
    cid = np.random.choice(customer_ids, p=customer_weights)
    tier = customer_tier_map[cid]
    rep_id = customer_rep_map[cid]
    order_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

    if tier == "Enterprise":
        amount = round(np.random.lognormal(9.0, 0.7), 2)
    elif tier == "Mid-Market":
        amount = round(np.random.lognormal(7.5, 0.6), 2)
    else:
        amount = round(np.random.lognormal(6.0, 0.5), 2)

    ship_delay_days = int(np.random.exponential(scale=5))
    ship_date = order_date + timedelta(days=ship_delay_days)
    if ship_date > END_DATE.date():
        ship_date = None
        ship_status = "Pending Shipment"
    else:
        ship_status = np.random.choice(["Shipped", "Delivered", "In Transit"], p=[0.35, 0.45, 0.20])

    product_category = np.random.choice(PRODUCT_CATEGORIES, p=[0.25, 0.18, 0.20, 0.10, 0.08, 0.05, 0.08, 0.06])

    orders_data.append({
        "order_id": f"ORD-{i+1:06d}",
        "customer_id": cid,
        "rep_id": rep_id,
        "order_date": order_date.isoformat(),
        "order_amount": amount,
        "ship_date": ship_date.isoformat() if ship_date else None,
        "ship_status": ship_status,
        "product_category": product_category,
        "quantity": int(np.random.lognormal(1.5, 0.8)) + 1,
        "is_confirmed": True,
    })

orders_pdf = pd.DataFrame(orders_data)
print(f"  Created {len(orders_pdf):,} confirmed orders")

# =============================================================================
# 5. DEALS / PIPELINE (Salesforce-style opportunities)
# =============================================================================
print("Generating deals/pipeline...")

deals_data = []
for i in range(N_DEALS):
    cid = np.random.choice(customer_ids, p=customer_weights)
    tier = customer_tier_map[cid]
    rep_id = customer_rep_map[cid]

    stage = np.random.choice(
        DEAL_STAGES,
        p=[0.15, 0.20, 0.20, 0.15, 0.10, 0.12, 0.08]
    )

    if tier == "Enterprise":
        deal_amount = round(np.random.lognormal(10.5, 0.9), 2)
    elif tier == "Mid-Market":
        deal_amount = round(np.random.lognormal(8.5, 0.7), 2)
    else:
        deal_amount = round(np.random.lognormal(7.0, 0.6), 2)

    stage_probability = {
        "Prospecting": round(np.random.uniform(0.05, 0.15), 2),
        "Discovery": round(np.random.uniform(0.15, 0.30), 2),
        "Proposal Sent": round(np.random.uniform(0.35, 0.55), 2),
        "Negotiation": round(np.random.uniform(0.55, 0.75), 2),
        "Verbal Commit": round(np.random.uniform(0.80, 0.95), 2),
        "Closed Won": 1.0,
        "Closed Lost": 0.0,
    }[stage]

    created_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)
    if stage in ("Closed Won", "Closed Lost"):
        close_date = created_date + timedelta(days=int(np.random.exponential(scale=30)))
        if close_date > END_DATE.date():
            close_date = END_DATE.date()
    else:
        close_date = created_date + timedelta(days=int(np.random.exponential(scale=45)) + 14)

    product_category = np.random.choice(PRODUCT_CATEGORIES, p=[0.20, 0.15, 0.15, 0.13, 0.12, 0.08, 0.10, 0.07])

    forecasted_amount = round(deal_amount * stage_probability, 2)

    deals_data.append({
        "deal_id": f"OPP-{i+1:06d}",
        "customer_id": cid,
        "rep_id": rep_id,
        "deal_name": f"{fake.company()} - {product_category}",
        "stage": stage,
        "deal_amount": deal_amount,
        "forecasted_amount": forecasted_amount,
        "probability": stage_probability,
        "product_category": product_category,
        "created_date": created_date.isoformat(),
        "expected_close_date": close_date.isoformat(),
        "deal_type": np.random.choice(["New Business", "Expansion", "Renewal"], p=[0.40, 0.35, 0.25]),
        "next_step": np.random.choice([
            "Schedule demo", "Send proposal", "Executive meeting",
            "Technical review", "Contract negotiation", "Awaiting PO",
            "Follow up call", "Reference check"
        ]),
    })

deals_pdf = pd.DataFrame(deals_data)
print(f"  Created {len(deals_pdf):,} deals")

# =============================================================================
# 6. SAVE TO VOLUME
# =============================================================================
print(f"\nSaving to {VOLUME_PATH}...")

spark.createDataFrame(reps_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/sales_reps")
spark.createDataFrame(customers_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/customers")
spark.createDataFrame(invoices_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/invoices")
spark.createDataFrame(orders_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/orders")
spark.createDataFrame(deals_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/deals")

print("Done!")

# =============================================================================
# 7. VALIDATION
# =============================================================================
print("\n=== VALIDATION ===")
print(f"Sales Reps: {len(reps_pdf)} | Regions: {reps_pdf['region'].value_counts().to_dict()}")
print(f"Customers: {len(customers_pdf)} | Tiers: {customers_pdf['tier'].value_counts().to_dict()}")
print(f"Invoices: {len(invoices_pdf):,} | Statuses: {invoices_pdf['status'].value_counts().to_dict()}")
print(f"Orders: {len(orders_pdf):,} | Ship Status: {orders_pdf['ship_status'].value_counts().to_dict()}")
print(f"Deals: {len(deals_pdf):,} | Stages: {deals_pdf['stage'].value_counts().to_dict()}")

total_invoiced = invoices_pdf["invoice_amount"].sum()
total_recognized = (invoices_pdf["invoice_amount"] * invoices_pdf["revenue_recognized_pct"]).sum()
total_pipeline = deals_pdf[~deals_pdf["stage"].isin(["Closed Won", "Closed Lost"])]["deal_amount"].sum()

print(f"\nTotal Invoiced: ${total_invoiced:,.2f}")
print(f"Total Revenue Recognized: ${total_recognized:,.2f}")
print(f"Open Pipeline Value: ${total_pipeline:,.2f}")
