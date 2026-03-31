"""Copy gold tables from Unity Catalog into Lakebase PostgreSQL via Spark.

Reads each gold materialized view as a Spark DataFrame using Databricks
Connect, then bulk-inserts the rows into the corresponding Lakebase
PostgreSQL table using psycopg COPY for fast loading.

Usage:
    python scripts/copy_gold_to_lakebase.py

Prerequisites:
    - databricks-connect, databricks-sdk, psycopg[binary] installed
    - Authenticated via Databricks CLI profile or environment variables
"""

import os
import io
import csv
import uuid

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
import psycopg

PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "group-demo")
CATALOG = os.getenv("UC_CATALOG", "mfg_mc_se_sa")
SCHEMA = os.getenv("UC_SCHEMA", "cdw_sales_forecast")
LAKEBASE_DATABASE = os.getenv("LAKEBASE_DATABASE_NAME", "databricks_postgres")

LAKEBASE_PROJECT = "projects/cdw-sales-forecast"
LAKEBASE_ENDPOINT = f"{LAKEBASE_PROJECT}/branches/production/endpoints/primary"
LAKEBASE_HOST = "ep-broad-firefly-d2lmogie.database.us-east-1.cloud.databricks.com"

GOLD_TABLES = [
    "gold_rep_monthly_summary",
    "gold_active_deals",
    "gold_invoice_details",
    "gold_category_summary",
]

SPARK_TO_PG = {
    "string": "TEXT",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "timestamp_ntz": "TIMESTAMP",
}


def spark_type_to_pg(spark_type: str) -> str:
    t = spark_type.lower()
    if t.startswith("decimal"):
        return "NUMERIC"
    return SPARK_TO_PG.get(t, "TEXT")


def connect_lakebase():
    """Connect to Lakebase autoscaling instance via the postgres SDK API."""
    w = WorkspaceClient(profile=PROFILE)
    username = w.current_user.me().user_name

    cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)

    conn_str = (
        f"host={LAKEBASE_HOST} "
        f"dbname={LAKEBASE_DATABASE} "
        f"user={username} "
        f"password={cred.token} "
        f"sslmode=require"
    )
    conn = psycopg.connect(conn_str)
    print(f"Connected to Lakebase: {LAKEBASE_DATABASE}@{LAKEBASE_HOST}")
    return conn


def copy_table(spark, conn, table_name: str):
    """Read a single gold table from UC and bulk-load it into Lakebase."""
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    print(f"\n{'=' * 60}")
    print(f"  {full_name}  ->  {table_name}")
    print(f"{'=' * 60}")
    df = spark.read.table(full_name)
    schema = df.schema
    row_count = df.count()
    print(f"  {len(schema.fields)} columns, {row_count:,} rows")

    col_defs = []
    col_names = []
    for field in schema.fields:
        pg_type = spark_type_to_pg(field.dataType.simpleString())
        col_defs.append(f"  {field.name} {pg_type}")
        col_names.append(field.name)

    create_ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(col_defs) + "\n);"

    with conn.cursor() as cur:
        cur.execute(create_ddl)
        cur.execute(f"TRUNCATE TABLE {table_name};")
    conn.commit()
    print(f"  Table created / truncated")

    pdf = df.toPandas()
    buf = io.StringIO()
    pdf.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)

    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} ({', '.join(col_names)}) FROM STDIN WITH (FORMAT csv, NULL '')"
        ) as copy:
            for line in buf:
                copy.write(line)
    conn.commit()
    print(f"  Loaded {len(pdf):,} rows")


def main():
    print(f"Initializing Spark session via Databricks Connect (profile={PROFILE}) ...")
    spark = DatabricksSession.builder.profile(PROFILE).serverless().getOrCreate()

    conn = connect_lakebase()

    try:
        for table_name in GOLD_TABLES:
            copy_table(spark, conn, table_name)
    finally:
        conn.close()
        spark.stop()

    print(f"\nAll {len(GOLD_TABLES)} gold tables copied to Lakebase.")


if __name__ == "__main__":
    main()
