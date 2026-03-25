"""
Set up Lakebase instance and configure snapshot syncs from gold tables.

Run this script AFTER the pipeline has completed successfully.
Uses the Databricks SDK to:
  1. Create or get the Lakebase Provisioned instance
  2. Register it with Unity Catalog
  3. Create synced tables (SNAPSHOT) from gold layer
"""
from databricks.sdk import WorkspaceClient
import uuid
import time

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "cdw_sales"
SCHEMA = "sales_forecast"
LAKEBASE_INSTANCE = "cdw-sales-lakebase"
CAPACITY = "CU_1"

GOLD_TABLES_TO_SYNC = [
    f"{CATALOG}.{SCHEMA}.gold_rep_monthly_summary",
    f"{CATALOG}.{SCHEMA}.gold_active_deals",
    f"{CATALOG}.{SCHEMA}.gold_invoice_details",
    f"{CATALOG}.{SCHEMA}.gold_category_summary",
]

# =============================================================================
# SETUP
# =============================================================================
w = WorkspaceClient()

# 1. Create Lakebase instance (idempotent)
print(f"Creating Lakebase instance '{LAKEBASE_INSTANCE}'...")
try:
    instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    print(f"  Instance already exists: {instance.name} (state: {instance.state})")
except Exception:
    instance = w.database.create_database_instance(
        name=LAKEBASE_INSTANCE,
        capacity=CAPACITY,
        stopped=False,
    )
    print(f"  Instance created: {instance.name}")
    print("  Waiting for instance to be ready...")
    while True:
        instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
        if instance.state and str(instance.state).upper() == "RUNNING":
            break
        print(f"    State: {instance.state}... waiting 30s")
        time.sleep(30)
    print("  Instance is RUNNING.")

print(f"  DNS: {instance.read_write_dns}")

# 2. Register with Unity Catalog
print(f"\nRegistering with Unity Catalog as '{CATALOG}'...")
try:
    w.database.register_database_instance(
        name=LAKEBASE_INSTANCE,
        catalog=CATALOG,
        schema=SCHEMA,
    )
    print("  Registered successfully.")
except Exception as e:
    print(f"  Registration skipped (may already exist): {e}")

# 3. Create synced tables (SNAPSHOT mode)
print("\nCreating synced tables...")
for source_table in GOLD_TABLES_TO_SYNC:
    table_name = source_table.split(".")[-1]
    target_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"  Syncing {source_table} -> {table_name}...")
    try:
        w.database.create_synced_table(
            instance_name=LAKEBASE_INSTANCE,
            source_table_name=source_table,
            target_table_name=table_name,
            sync_mode="FULL",
        )
        print(f"    Synced table created.")
    except Exception as e:
        print(f"    Skipped (may already exist): {e}")

print("\nLakebase setup complete!")
print(f"Instance: {LAKEBASE_INSTANCE}")
print(f"DNS: {instance.read_write_dns}")
print(f"Synced tables: {len(GOLD_TABLES_TO_SYNC)}")
