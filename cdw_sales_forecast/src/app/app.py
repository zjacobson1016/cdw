"""CDW Sales Forecast App — Streamlit + Lakebase backend."""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from backend import get_backend

st.set_page_config(
    page_title="CDW Sales Forecast",
    page_icon="📊",
    layout="wide",
    initial_sidebar_config="expanded",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main .block-container { padding-top: 1rem; }
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 1.2rem; border-radius: 10px; color: white; text-align: center;
    }
    .metric-card h3 { margin: 0; font-size: 0.85rem; opacity: 0.85; }
    .metric-card h1 { margin: 0.3rem 0 0 0; font-size: 1.8rem; }
    .section-header { border-bottom: 2px solid #e74c3c; padding-bottom: 0.3rem; margin-top: 1.5rem; }
    .forecast-positive { color: #27ae60; font-weight: bold; }
    .forecast-negative { color: #e74c3c; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------
backend = get_backend()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/CDW_Logo.svg/200px-CDW_Logo.svg.png", width=160)
st.sidebar.title("CDW Sales Forecast")

reps = backend.get_sales_reps()
rep_options = {r["rep_name"]: r["rep_id"] for r in reps}

page = st.sidebar.radio("Navigation", [
    "📊 Dashboard",
    "📝 Sales Feedback",
    "🔮 Forecast Builder",
    "👔 Manager Review",
    "📄 Generate Report",
])

selected_region = st.sidebar.selectbox("Region", ["All"] + sorted({r["region"] for r in reps}))
selected_rep_name = st.sidebar.selectbox(
    "Sales Rep",
    ["All"] + sorted(rep_options.keys()),
)

if selected_rep_name != "All":
    selected_rep_id = rep_options[selected_rep_name]
else:
    selected_rep_id = None


def fmt_currency(val):
    if val >= 1_000_000:
        return f"${val/1_000_000:,.1f}M"
    if val >= 1_000:
        return f"${val/1_000:,.0f}K"
    return f"${val:,.0f}"


# ===================================================================
# PAGE: Dashboard
# ===================================================================
if page == "📊 Dashboard":
    st.title("Sales Performance Dashboard")

    summary = backend.get_monthly_summary(rep_id=selected_rep_id, region=selected_region)
    if not summary:
        st.info("No data available for the selected filters.")
        st.stop()

    current_month = max(r["month_date"] for r in summary)
    current = [r for r in summary if r["month_date"] == current_month]
    agg = lambda key: sum(r[key] for r in current)

    invoiced = agg("total_invoiced")
    recognized = agg("total_recognized")
    confirmed = agg("total_confirmed_orders")
    pipeline = agg("total_pipeline_value")
    weighted = agg("total_weighted_pipeline")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f'<div class="metric-card"><h3>Invoiced This Month</h3><h1>{fmt_currency(invoiced)}</h1></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><h3>Confirmed Orders</h3><h1>{fmt_currency(confirmed)}</h1></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><h3>Revenue Recognized</h3><h1>{fmt_currency(recognized)}</h1></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card"><h3>Open Pipeline</h3><h1>{fmt_currency(pipeline)}</h1></div>', unsafe_allow_html=True)
    with c5:
        st.markdown(f'<div class="metric-card"><h3>Weighted Pipeline</h3><h1>{fmt_currency(weighted)}</h1></div>', unsafe_allow_html=True)

    # Trend chart
    st.markdown('<h3 class="section-header">Monthly Revenue Trend</h3>', unsafe_allow_html=True)
    df_trend = pd.DataFrame(summary)
    df_trend["month_date"] = pd.to_datetime(df_trend["month_date"])
    trend_agg = df_trend.groupby("month_date").agg(
        total_invoiced=("total_invoiced", "sum"),
        total_recognized=("total_recognized", "sum"),
        total_confirmed_orders=("total_confirmed_orders", "sum"),
    ).reset_index().sort_values("month_date")

    fig = go.Figure()
    fig.add_trace(go.Bar(x=trend_agg["month_date"], y=trend_agg["total_invoiced"], name="Invoiced", marker_color="#2d5a87"))
    fig.add_trace(go.Bar(x=trend_agg["month_date"], y=trend_agg["total_recognized"], name="Recognized", marker_color="#27ae60"))
    fig.add_trace(go.Scatter(x=trend_agg["month_date"], y=trend_agg["total_confirmed_orders"], name="Confirmed Orders", line=dict(color="#e74c3c", width=3)))
    fig.update_layout(barmode="group", height=350, margin=dict(t=10, b=30), legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(fig, use_container_width=True)

    # Deal pipeline + category split
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<h3 class="section-header">Deal Pipeline by Stage</h3>', unsafe_allow_html=True)
        deals = backend.get_active_deals(rep_id=selected_rep_id, region=selected_region)
        if deals:
            df_deals = pd.DataFrame(deals)
            stage_order = ["Prospecting", "Discovery", "Proposal Sent", "Negotiation", "Verbal Commit"]
            df_stage = df_deals.groupby("stage").agg(total=("deal_amount", "sum"), count=("deal_id", "count")).reset_index()
            df_stage["stage"] = pd.Categorical(df_stage["stage"], categories=stage_order, ordered=True)
            df_stage = df_stage.sort_values("stage")
            fig2 = px.bar(df_stage, x="stage", y="total", text="count", color="total",
                          color_continuous_scale="Blues", labels={"total": "Pipeline Value", "stage": "Stage"})
            fig2.update_layout(height=300, margin=dict(t=10, b=30), showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

    with col_right:
        st.markdown('<h3 class="section-header">Revenue by Product Category</h3>', unsafe_allow_html=True)
        categories = backend.get_category_summary(region=selected_region)
        if categories:
            df_cat = pd.DataFrame(categories)
            cat_agg = df_cat.groupby("product_category")["total_invoiced"].sum().reset_index()
            fig3 = px.pie(cat_agg, values="total_invoiced", names="product_category", hole=0.4)
            fig3.update_layout(height=300, margin=dict(t=10, b=30))
            st.plotly_chart(fig3, use_container_width=True)

# ===================================================================
# PAGE: Sales Feedback
# ===================================================================
elif page == "📝 Sales Feedback":
    st.title("Sales Rep Feedback & Adjustments")
    st.markdown("Update your forecast with qualitative insights and deal-level adjustments.")

    if selected_rep_id is None:
        st.warning("Please select a specific sales rep from the sidebar.")
        st.stop()

    deals = backend.get_active_deals(rep_id=selected_rep_id)
    if not deals:
        st.info("No active deals for this rep.")
        st.stop()

    with st.form("feedback_form"):
        st.subheader("Deal-Level Adjustments")
        adjustments = []
        for deal in deals[:15]:
            col1, col2, col3 = st.columns([3, 1, 2])
            with col1:
                st.text(f"{deal['deal_name'][:50]} | {deal['stage']}")
            with col2:
                st.text(fmt_currency(deal["deal_amount"]))
            with col3:
                adjusted = st.number_input(
                    "Adjusted Amount",
                    value=float(deal["forecasted_amount"]),
                    min_value=0.0,
                    step=1000.0,
                    key=f"adj_{deal['deal_id']}",
                    label_visibility="collapsed",
                )
                adjustments.append({
                    "deal_id": deal["deal_id"],
                    "original_forecast": deal["forecasted_amount"],
                    "adjusted_forecast": adjusted,
                })

        st.divider()
        st.subheader("Qualitative Feedback")
        confidence = st.select_slider("Overall Forecast Confidence", options=["Very Low", "Low", "Medium", "High", "Very High"], value="Medium")
        risks = st.text_area("Key Risks or Blockers", placeholder="E.g., Customer budget freeze, competitor engagement...")
        upside = st.text_area("Upside Opportunities", placeholder="E.g., Expansion deal likely to close early...")
        notes = st.text_area("Additional Notes", placeholder="Any context for your manager...")

        submitted = st.form_submit_button("💾 Submit Feedback", type="primary", use_container_width=True)

    if submitted:
        feedback = {
            "rep_id": selected_rep_id,
            "rep_name": selected_rep_name,
            "submitted_at": datetime.now().isoformat(),
            "confidence": confidence,
            "risks": risks,
            "upside": upside,
            "notes": notes,
            "adjustments": adjustments,
        }
        backend.save_feedback(feedback)
        st.success("Feedback submitted successfully!")

# ===================================================================
# PAGE: Forecast Builder
# ===================================================================
elif page == "🔮 Forecast Builder":
    st.title("Forecast Builder")
    st.markdown("Combines actual invoiced revenue with sales-adjusted pipeline to project monthly outcomes.")

    summary = backend.get_monthly_summary(rep_id=selected_rep_id, region=selected_region)
    feedback_list = backend.get_all_feedback(rep_id=selected_rep_id)

    if not summary:
        st.info("No summary data available.")
        st.stop()

    df = pd.DataFrame(summary)
    df["month_date"] = pd.to_datetime(df["month_date"])
    monthly = df.groupby("month_date").agg(
        total_invoiced=("total_invoiced", "sum"),
        total_recognized=("total_recognized", "sum"),
        total_weighted_pipeline=("total_weighted_pipeline", "sum"),
        blended_forecast=("blended_forecast", "sum"),
    ).reset_index().sort_values("month_date")

    total_adj = 0
    if feedback_list:
        for fb in feedback_list:
            for adj in fb.get("adjustments", []):
                total_adj += adj["adjusted_forecast"] - adj["original_forecast"]

    monthly["rep_adjusted_forecast"] = monthly["blended_forecast"] + (total_adj / max(len(monthly), 1))

    st.markdown('<h3 class="section-header">Forecast vs Actuals</h3>', unsafe_allow_html=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=monthly["month_date"], y=monthly["total_invoiced"], name="Actual Invoiced", marker_color="#2d5a87"))
    fig.add_trace(go.Bar(x=monthly["month_date"], y=monthly["total_recognized"], name="Recognized Revenue", marker_color="#27ae60"))
    fig.add_trace(go.Scatter(x=monthly["month_date"], y=monthly["blended_forecast"], name="System Forecast", line=dict(color="#f39c12", width=3, dash="dash")))
    fig.add_trace(go.Scatter(x=monthly["month_date"], y=monthly["rep_adjusted_forecast"], name="Rep-Adjusted Forecast", line=dict(color="#e74c3c", width=3)))
    fig.update_layout(barmode="group", height=400, margin=dict(t=10, b=30), legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        total_actual = monthly["total_invoiced"].sum()
        st.metric("Total Invoiced (All Months)", fmt_currency(total_actual))
    with col2:
        total_forecast = monthly["blended_forecast"].sum()
        st.metric("System Forecast", fmt_currency(total_forecast))
    with col3:
        total_rep = monthly["rep_adjusted_forecast"].sum()
        delta = total_rep - total_forecast
        st.metric("Rep-Adjusted Forecast", fmt_currency(total_rep), delta=fmt_currency(delta))

    st.markdown('<h3 class="section-header">Monthly Breakdown</h3>', unsafe_allow_html=True)
    display_df = monthly.copy()
    display_df["month_date"] = display_df["month_date"].dt.strftime("%b %Y")
    for col in ["total_invoiced", "total_recognized", "total_weighted_pipeline", "blended_forecast", "rep_adjusted_forecast"]:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.0f}")
    display_df.columns = ["Month", "Invoiced", "Recognized", "Weighted Pipeline", "System Forecast", "Rep Forecast"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ===================================================================
# PAGE: Manager Review
# ===================================================================
elif page == "👔 Manager Review":
    st.title("Manager Review & Approval")
    st.markdown("Review rep forecasts, adjustments, and approve or modify the team forecast.")

    feedback_list = backend.get_all_feedback()
    if not feedback_list:
        st.info("No feedback submissions to review yet.")
        st.stop()

    for fb in feedback_list:
        with st.expander(f"📋 {fb['rep_name']} — submitted {fb['submitted_at'][:10]}", expanded=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                confidence_color = {"Very Low": "🔴", "Low": "🟠", "Medium": "🟡", "High": "🟢", "Very High": "🟣"}
                st.markdown(f"**Confidence:** {confidence_color.get(fb['confidence'], '⚪')} {fb['confidence']}")
            with col2:
                adj_total = sum(a["adjusted_forecast"] - a["original_forecast"] for a in fb.get("adjustments", []))
                direction = "↑" if adj_total >= 0 else "↓"
                st.markdown(f"**Net Adjustment:** {direction} {fmt_currency(abs(adj_total))}")
            with col3:
                st.markdown(f"**Submitted:** {fb['submitted_at'][:16]}")

            if fb.get("risks"):
                st.markdown(f"**Risks:** {fb['risks']}")
            if fb.get("upside"):
                st.markdown(f"**Upside:** {fb['upside']}")
            if fb.get("notes"):
                st.markdown(f"**Notes:** {fb['notes']}")

            if fb.get("adjustments"):
                adj_df = pd.DataFrame(fb["adjustments"])
                adj_df["delta"] = adj_df["adjusted_forecast"] - adj_df["original_forecast"]
                adj_df = adj_df[adj_df["delta"] != 0]
                if not adj_df.empty:
                    st.markdown("**Deal Adjustments:**")
                    st.dataframe(adj_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Manager Override")
    with st.form("manager_override"):
        override_pct = st.slider("Forecast Adjustment (%)", min_value=-30, max_value=30, value=0, step=1)
        manager_notes = st.text_area("Manager Notes", placeholder="Rationale for override...")
        approve = st.form_submit_button("✅ Approve Forecast", type="primary")

    if approve:
        override = {
            "manager_override_pct": override_pct,
            "manager_notes": manager_notes,
            "approved_at": datetime.now().isoformat(),
            "approved_by": "Manager",
        }
        backend.save_manager_override(override)
        st.success(f"Forecast approved with {override_pct:+d}% adjustment.")

# ===================================================================
# PAGE: Generate Report
# ===================================================================
elif page == "📄 Generate Report":
    st.title("Generate Sales Report")
    st.markdown("Create a downloadable report combining actuals, pipeline, and forecast data.")

    report_type = st.selectbox("Report Type", [
        "Monthly Sales Summary",
        "Deal Pipeline Report",
        "Invoice Detail Report",
        "Forecast vs Actuals",
    ])

    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("Start Date", value=date.today().replace(day=1) - pd.DateOffset(months=2))
    with col2:
        end = st.date_input("End Date", value=date.today())

    if st.button("📥 Generate Report", type="primary", use_container_width=True):
        with st.spinner("Generating report..."):
            if report_type == "Monthly Sales Summary":
                data = backend.get_monthly_summary(rep_id=selected_rep_id, region=selected_region)
            elif report_type == "Deal Pipeline Report":
                data = backend.get_active_deals(rep_id=selected_rep_id, region=selected_region)
            elif report_type == "Invoice Detail Report":
                data = backend.get_invoice_details(rep_id=selected_rep_id, region=selected_region)
            else:
                data = backend.get_monthly_summary(rep_id=selected_rep_id, region=selected_region)

            if data:
                df = pd.DataFrame(data)
                st.success(f"Report generated: {len(df)} rows")
                st.dataframe(df, use_container_width=True, hide_index=True)

                csv = df.to_csv(index=False)
                st.download_button(
                    "⬇️ Download CSV",
                    csv,
                    f"cdw_{report_type.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                    "text/csv",
                    use_container_width=True,
                )
            else:
                st.warning("No data found for the selected filters.")
