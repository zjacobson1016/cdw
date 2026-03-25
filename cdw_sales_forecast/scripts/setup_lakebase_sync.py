"""
Set up Lakebase instance and configure snapshot syncs from gold tables.

Run this script AFTER the pipeline has completed successfully.
Uses the Databricks SDK to:
  1. Create or get the Lakebase Provisioned instance
  2. Create synced tables (FULL/snapshot sync) from gold layer

Prerequisites:
  - pip install "databricks-sdk>=0.81.0"
  - Authenticated via `databricks auth login -p group-demo`
"""
from databricks.sdk import WorkspaceClient
import uuid
import time

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "mfg_mc_se_sa"
SCHEMA = "cdw_sales_forecast"
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
w = WorkspaceClient(profile="group-demo")

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
    for _ in range(20):
        instance = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
        if instance.state and str(instance.state).upper() == "RUNNING":
            break
        print(f"    State: {instance.state}... waiting 30s")
        time.sleep(30)
    else:
        print("  WARNING: Timed out waiting for instance. Check status manually.")
    print("  Instance is RUNNING.")

print(f"  DNS: {instance.read_write_dns}")

# 2. Create synced tables (FULL sync = snapshot replacement each run)
print("\nCreating synced tables...")
for source_table in GOLD_TABLES_TO_SYNC:
    table_name = source_table.split(".")[-1]
    print(f"  Syncing {source_table} -> {table_name}...")
    try:
        synced_table = w.database.create_synced_table(
            instance_name=LAKEBASE_INSTANCE,
            source_table_name=source_table,
            target_table_name=table_name,
            sync_mode="FULL",
        )
        print(f"    Synced table created: {synced_table.target_table_name}")
    except Exception as e:
        print(f"    Skipped (may already exist): {e}")

# 3. Verify connection
print("\nVerifying connection...")
try:
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    print(f"  OAuth token generated (length: {len(cred.token)})")
    username = w.current_user.me().user_name
    print(f"  Connect with: host={instance.read_write_dns} user={username} sslmode=require")
except Exception as e:
    print(f"  Token generation failed: {e}")

print("\nLakebase setup complete!")
print(f"Instance: {LAKEBASE_INSTANCE}")
print(f"DNS: {instance.read_write_dns}")
print(f"Synced tables: {len(GOLD_TABLES_TO_SYNC)}")
print("\nNext steps:")
print("  1. Add Lakebase as an app resource in the Databricks UI")
print(f"     databricks apps add-resource cdw-sales-forecast-dev --resource-type database --resource-name lakebase --database-instance {LAKEBASE_INSTANCE} -p group-demo")
print("  2. Deploy and start the app: databricks bundle run cdw_sales_app -t dev")
