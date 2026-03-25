"""Backend abstraction — Lakebase (real) or in-memory mock for local dev."""
import os
import json
from datetime import datetime

USE_MOCK = os.getenv("USE_MOCK_BACKEND", "true").lower() == "true"


def get_backend():
    if USE_MOCK:
        return MockBackend()
    return LakebaseBackend()


# ---------------------------------------------------------------------------
# Lakebase Backend (production)
# ---------------------------------------------------------------------------
class LakebaseBackend:
    def __init__(self):
        import psycopg2
        self._conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            database=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            port=os.getenv("PGPORT", "5432"),
        )
        self._ensure_app_tables()

    def _ensure_app_tables(self):
        """Create app-specific tables for feedback and overrides (not synced from gold)."""
        with self._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_feedback (
                    id SERIAL PRIMARY KEY,
                    rep_id VARCHAR(20),
                    rep_name VARCHAR(200),
                    submitted_at TIMESTAMP DEFAULT NOW(),
                    confidence VARCHAR(20),
                    risks TEXT,
                    upside TEXT,
                    notes TEXT,
                    adjustments JSONB
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS manager_overrides (
                    id SERIAL PRIMARY KEY,
                    manager_override_pct INTEGER,
                    manager_notes TEXT,
                    approved_at TIMESTAMP DEFAULT NOW(),
                    approved_by VARCHAR(200)
                );
            """)
            self._conn.commit()

    def _query(self, sql, params=None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _build_filter(self, rep_id=None, region=None):
        clauses, params = [], []
        if rep_id:
            clauses.append("rep_id = %s")
            params.append(rep_id)
        if region and region != "All":
            clauses.append("rep_region = %s")
            params.append(region)
        where = " AND ".join(clauses)
        return (f" WHERE {where}" if where else ""), params

    def get_sales_reps(self):
        return self._query("SELECT rep_id, rep_name, region, role, annual_quota FROM gold_rep_monthly_summary GROUP BY rep_id, rep_name, region, role, annual_quota ORDER BY rep_name")

    def get_monthly_summary(self, rep_id=None, region=None):
        where, params = self._build_filter(rep_id, region)
        sql = f"""SELECT rep_id, rep_name, region, month_date,
                    total_invoiced, total_recognized, total_confirmed_orders,
                    shipped_revenue, total_pipeline_value, total_weighted_pipeline,
                    blended_forecast, annual_quota
                 FROM gold_rep_monthly_summary{where.replace('rep_region', 'region')}
                 ORDER BY month_date"""
        return self._query(sql, params)

    def get_active_deals(self, rep_id=None, region=None):
        where, params = self._build_filter(rep_id, region)
        return self._query(
            f"SELECT * FROM gold_active_deals{where} ORDER BY expected_close_date", params
        )

    def get_invoice_details(self, rep_id=None, region=None):
        where, params = self._build_filter(rep_id, region)
        return self._query(
            f"SELECT * FROM gold_invoice_details{where} ORDER BY invoice_date DESC LIMIT 500", params
        )

    def get_category_summary(self, region=None):
        if region and region != "All":
            return self._query(
                """SELECT cs.* FROM gold_category_summary cs
                   JOIN gold_invoice_details id ON cs.product_category = id.product_category
                   WHERE id.rep_region = %s
                   GROUP BY cs.month_date, cs.product_category, cs.total_invoiced, cs.total_recognized, cs.invoice_count, cs.avg_invoice_amount""",
                [region]
            )
        return self._query("SELECT * FROM gold_category_summary ORDER BY month_date")

    def save_feedback(self, feedback):
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sales_feedback (rep_id, rep_name, submitted_at, confidence, risks, upside, notes, adjustments)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (feedback["rep_id"], feedback["rep_name"], feedback["submitted_at"],
                 feedback["confidence"], feedback["risks"], feedback["upside"],
                 feedback["notes"], json.dumps(feedback["adjustments"])),
            )
            self._conn.commit()

    def get_all_feedback(self, rep_id=None):
        if rep_id:
            rows = self._query("SELECT * FROM sales_feedback WHERE rep_id = %s ORDER BY submitted_at DESC", [rep_id])
        else:
            rows = self._query("SELECT * FROM sales_feedback ORDER BY submitted_at DESC")
        for r in rows:
            if isinstance(r.get("adjustments"), str):
                r["adjustments"] = json.loads(r["adjustments"])
        return rows

    def save_manager_override(self, override):
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manager_overrides (manager_override_pct, manager_notes, approved_at, approved_by) VALUES (%s, %s, %s, %s)",
                (override["manager_override_pct"], override["manager_notes"],
                 override["approved_at"], override["approved_by"]),
            )
            self._conn.commit()


# ---------------------------------------------------------------------------
# Mock Backend (local development)
# ---------------------------------------------------------------------------
class MockBackend:
    """In-memory mock for local testing without Lakebase."""

    def __init__(self):
        import random
        random.seed(42)
        self._reps = self._gen_reps()
        self._feedback_store = []
        self._override_store = []

    @staticmethod
    def _gen_reps():
        regions = ["Central", "East", "West", "Federal", "Canada"]
        roles = ["Account Executive", "Senior Account Executive", "Strategic Account Manager"]
        names = [
            "Alice Johnson", "Bob Martinez", "Carol Davis", "Dan Wilson", "Eva Brown",
            "Frank Lee", "Grace Kim", "Henry Chen", "Irene Patel", "Jack Thompson",
        ]
        reps = []
        for i, name in enumerate(names):
            reps.append({
                "rep_id": f"REP-{i+1:04d}",
                "rep_name": name,
                "region": regions[i % len(regions)],
                "role": roles[i % len(roles)],
                "annual_quota": 1_500_000 + (i * 200_000),
            })
        return reps

    def get_sales_reps(self):
        return self._reps

    def get_monthly_summary(self, rep_id=None, region=None):
        import random
        random.seed(42)
        rows = []
        months = pd.date_range("2025-10-01", periods=6, freq="MS") if "pd" in dir() else []
        try:
            import pandas as _pd
            months = _pd.date_range("2025-10-01", periods=6, freq="MS")
        except Exception:
            from datetime import date as _d
            months = [_d(2025, m, 1) for m in range(10, 13)] + [_d(2026, m, 1) for m in range(1, 4)]

        for rep in self._reps:
            if rep_id and rep["rep_id"] != rep_id:
                continue
            if region and region != "All" and rep["region"] != region:
                continue
            for m in months:
                rows.append({
                    "rep_id": rep["rep_id"],
                    "rep_name": rep["rep_name"],
                    "region": rep["region"],
                    "month_date": str(m)[:10] if hasattr(m, "strftime") else str(m),
                    "total_invoiced": random.randint(80_000, 350_000),
                    "total_recognized": random.randint(60_000, 280_000),
                    "total_confirmed_orders": random.randint(50_000, 300_000),
                    "shipped_revenue": random.randint(40_000, 250_000),
                    "total_pipeline_value": random.randint(200_000, 800_000),
                    "total_weighted_pipeline": random.randint(100_000, 400_000),
                    "blended_forecast": random.randint(150_000, 500_000),
                    "annual_quota": rep["annual_quota"],
                })
        return rows

    def get_active_deals(self, rep_id=None, region=None):
        import random
        random.seed(99)
        stages = ["Prospecting", "Discovery", "Proposal Sent", "Negotiation", "Verbal Commit"]
        cats = ["Servers & Storage", "Networking Equipment", "Endpoint Devices", "Security Solutions", "Cloud Infrastructure"]
        deals = []
        for i in range(30):
            rep = self._reps[i % len(self._reps)]
            if rep_id and rep["rep_id"] != rep_id:
                continue
            if region and region != "All" and rep["region"] != region:
                continue
            amt = random.randint(20_000, 500_000)
            prob = round(random.uniform(0.1, 0.9), 2)
            deals.append({
                "deal_id": f"OPP-{i+1:06d}",
                "deal_name": f"Mock Deal {i+1} - {cats[i % len(cats)]}",
                "rep_id": rep["rep_id"],
                "rep_name": rep["rep_name"],
                "rep_region": rep["region"],
                "customer_id": f"CDW-CUST-{i+1:05d}",
                "customer_name": f"Customer {i+1}",
                "customer_vertical": "Technology",
                "customer_tier": "Enterprise",
                "stage": stages[i % len(stages)],
                "deal_amount": amt,
                "forecasted_amount": round(amt * prob, 2),
                "probability": prob,
                "product_category": cats[i % len(cats)],
                "created_date": "2026-01-15",
                "expected_close_date": "2026-04-30",
                "deal_type": "New Business",
                "next_step": "Schedule demo",
            })
        return deals

    def get_invoice_details(self, rep_id=None, region=None):
        import random
        random.seed(77)
        rows = []
        statuses = ["Invoiced", "Shipped", "Partially Shipped", "Delivered"]
        for i in range(50):
            rep = self._reps[i % len(self._reps)]
            if rep_id and rep["rep_id"] != rep_id:
                continue
            if region and region != "All" and rep["region"] != region:
                continue
            rows.append({
                "invoice_id": f"INV-{i+1:06d}",
                "rep_id": rep["rep_id"],
                "rep_name": rep["rep_name"],
                "rep_region": rep["region"],
                "customer_id": f"CDW-CUST-{i+1:05d}",
                "customer_name": f"Customer {i+1}",
                "invoice_date": "2026-03-01",
                "invoice_amount": random.randint(5_000, 200_000),
                "status": statuses[i % len(statuses)],
                "revenue_recognized_pct": round(random.uniform(0, 1), 2),
                "product_category": "Servers & Storage",
            })
        return rows

    def get_category_summary(self, region=None):
        import random
        random.seed(55)
        cats = ["Servers & Storage", "Networking Equipment", "Endpoint Devices", "Security Solutions", "Cloud Infrastructure", "Software Licensing"]
        rows = []
        for m in range(10, 13):
            for cat in cats:
                rows.append({
                    "month_date": f"2025-{m:02d}-01",
                    "product_category": cat,
                    "total_invoiced": random.randint(50_000, 500_000),
                    "total_recognized": random.randint(30_000, 400_000),
                    "invoice_count": random.randint(10, 100),
                    "avg_invoice_amount": random.randint(5_000, 50_000),
                })
        return rows

    def save_feedback(self, feedback):
        self._feedback_store.append(feedback)

    def get_all_feedback(self, rep_id=None):
        if rep_id:
            return [f for f in self._feedback_store if f["rep_id"] == rep_id]
        return self._feedback_store

    def save_manager_override(self, override):
        self._override_store.append(override)
