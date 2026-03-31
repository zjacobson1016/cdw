e"""
Set up Lakebase Provisioned instance, register UC catalog, and sync gold tables.

Run this script AFTER the pipeline has completed successfully.
Uses the Databricks CLI to:
  1. Create or verify the Lakebase Provisioned instance
  2. Register it as a Unity Catalog catalog
  3. Create synced tables (SNAPSHOT) from gold layer

Prerequisites:
  - Databricks CLI installed
  - Authenticated via `databricks auth login -p group-demo`
"""
import subprocess
import json
import sys
import time

PROFILE = "group-demo"
CATALOG = "mfg_mc_se_sa"
SCHEMA = "cdw_sales_forecast"
LAKEBASE_INSTANCE = "cdw-sales-lakebase"
LAKEBASE_CATALOG = "cdw_sales_lakebase"
LOGICAL_DB = "databricks_postgres"

GOLD_TABLES_TO_SYNC = {
    "gold_rep_monthly_summary": ["rep_id", "month_date"],
    "gold_active_deals": ["deal_id"],
    "gold_invoice_details": ["invoice_id"],
    "gold_category_summary": ["month_date", "product_category"],
}


def run_cli(args, parse_json=True):
    """Run a Databricks CLI command and return (output, error)."""
    cmd = ["databricks"] + args + ["-p", PROFILE]
    if parse_json:
        cmd += ["-o", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None, result.stderr.strip() or result.stdout.strip()
    if not parse_json or not result.stdout.strip():
        return result.stdout.strip(), None
    try:
        return json.loads(result.stdout), None
    except json.JSONDecodeError:
        return result.stdout.strip(), None


# =========================================================================
# 1. Verify Lakebase instance exists and is running
# =========================================================================
print(f"Checking Lakebase instance '{LAKEBASE_INSTANCE}'...")
data, err = run_cli(["database", "get-database-instance", "--name", LAKEBASE_INSTANCE])
if err:
    print(f"  Instance not found. Creating with CU_1 capacity...")
    data, err = run_cli([
        "database", "create-database-instance",
        "--name", LAKEBASE_INSTANCE,
        "--capacity", "CU_1",
    ])
    if err:
        print(f"  ERROR creating instance: {err}")
        sys.exit(1)
    print(f"  Instance created. Waiting for RUNNING state...")
    for i in range(20):
        data, _ = run_cli(["database", "get-database-instance", "--name", LAKEBASE_INSTANCE])
        state = data.get("state", "UNKNOWN") if data else "UNKNOWN"
        if "AVAILABLE" in state.upper() or "RUNNING" in state.upper():
            break
        print(f"    State: {state}... waiting 30s ({i+1}/20)")
        time.sleep(30)
else:
    state = data.get("state", "UNKNOWN") if data else "UNKNOWN"
    dns = data.get("read_write_dns", "N/A") if data else "N/A"
    print(f"  Instance exists. State: {state}")
    print(f"  DNS: {dns}")

# =========================================================================
# 2. Register Lakebase as a Unity Catalog catalog
# =========================================================================
print(f"\nRegistering UC catalog '{LAKEBASE_CATALOG}'...")
data, err = run_cli([
    "database", "create-database-catalog",
    LAKEBASE_CATALOG, LAKEBASE_INSTANCE, LOGICAL_DB,
    "--create-database-if-not-exists",
])
if err and "already exists" in err.lower():
    print(f"  Catalog already registered.")
elif err:
    print(f"  Note: {err}")
else:
    print(f"  Catalog '{LAKEBASE_CATALOG}' registered.")

# =========================================================================
# 3. Create synced tables (Reverse ETL from gold layer)
# =========================================================================
print("\nCreating synced tables...")
for table_name, pk_columns in GOLD_TABLES_TO_SYNC.items():
    source = f"{CATALOG}.{SCHEMA}.{table_name}"
    target = f"{LAKEBASE_CATALOG}.{SCHEMA}.{table_name}"
    print(f"  {source} -> {target}")

    result = subprocess.run([
        "databricks", "database", "create-synced-database-table",
        "--json", json.dumps({
            "name": target,
            "database_instance_name": LAKEBASE_INSTANCE,
            "logical_database_name": LOGICAL_DB,
            "spec": {
                "source_table_full_name": source,
                "primary_key_columns": pk_columns,
                "scheduling_policy": "SNAPSHOT",
            },
        }),
        "-p", PROFILE, "-o", "json",
    ], capture_output=True, text=True)

    if result.returncode == 0:
        resp = json.loads(result.stdout)
        state = resp.get("data_synchronization_status", {}).get("detailed_state", "CREATED")
        print(f"    Created ({state})")
    else:
        msg = result.stderr.strip() or result.stdout.strip()
        if "already exists" in msg.lower():
            print(f"    Already exists.")
        else:
            print(f"    Error: {msg}")

# =========================================================================
# 4. Connection info
# =========================================================================
data, _ = run_cli(["database", "get-database-instance", "--name", LAKEBASE_INSTANCE])
dns = data.get("read_write_dns", "N/A") if data else "N/A"

user_data, _ = run_cli(["current-user", "me"])
email = user_data.get("userName", "unknown") if isinstance(user_data, dict) else "unknown"

print("\n" + "=" * 60)
print("Lakebase sync setup complete!")
print("=" * 60)
print(f"  Instance:  {LAKEBASE_INSTANCE}")
print(f"  UC Catalog: {LAKEBASE_CATALOG}")
print(f"  DNS:       {dns}")
print(f"  User:      {email}")
print(f"  Database:  {LOGICAL_DB}")
print(f"  Synced:    {len(GOLD_TABLES_TO_SYNC)} gold tables")
print(f"\nNext steps:")
print(f"  1. Add Lakebase as app resource after deployment:")
print(f"     databricks apps add-resource cdw-sales-forecast-dev \\")
print(f"       --resource-type database --resource-name lakebase \\")
print(f"       --database-instance {LAKEBASE_INSTANCE} -p {PROFILE}")
print(f"  2. Deploy app: databricks bundle run cdw_sales_app -t dev")
