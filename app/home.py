from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from helpers import render_jobs_overview
from sepex import Job, SepexAPI

# ---------- Page config ----------
st.set_page_config(layout="wide")
# st.title("SEPEX Dashboard")
api = SepexAPI()

# ---------- Session state ----------
if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None
if "dashboard_page" not in st.session_state:
    st.session_state.dashboard_page = 0
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False

# ---------- Fetch all jobs once for overview ----------
raw_all = api.fetch_table("jobs", Job, params={"limit": 500, "offset": 0})
df_all = raw_all if isinstance(raw_all, pd.DataFrame) else pd.DataFrame()

# Ensure datetime is parsed on the full dataset first
if not df_all.empty and "updated" in df_all.columns:
    df_all["updated"] = pd.to_datetime(df_all["updated"], utc=True)


# ---------- Get filter inputs for early application ----------
# Create placeholder columns to capture filter values before rendering charts
filter_placeholder = st.container()

with filter_placeholder:
    st.subheader("Dashboard")
    time_col1, time_col2, time_col3, search_col = st.columns([1.5, 1.5, 1.5, 1.5])

    # Get search input first
    with search_col:
        search_query = st.text_input("🔍 Search by ID or submitter", placeholder="Enter job ID or email...")

    with time_col1:
        time_filter = st.selectbox(
            "Select time range:", ["Last 24 Hours", "All Time", "Last 7 Days", "Last 30 Days", "Custom"], index=0
        )

    # Placeholder for custom date inputs
    start_date = None
    end_date = None
    if time_filter == "Custom":
        now = datetime.now()
        with time_col2:
            start_date = st.date_input("Start date", value=now - timedelta(days=7))
        with time_col3:
            end_date = st.date_input("End date", value=now)

# Apply filters to create df_filtered
df_filtered = df_all.copy()

# Apply search filter first
if search_query and not df_filtered.empty:
    df_filtered = df_filtered[
        (df_filtered["jobID"].astype(str).str.contains(search_query, case=False))
        | (df_filtered["submitter"].astype(str).str.contains(search_query, case=False))
    ]

# Then apply time filter
if time_filter != "All Time" and not df_filtered.empty:
    now = pd.Timestamp.now(tz="UTC")

    if time_filter == "Last 24 Hours":
        cutoff = now - timedelta(days=1)
        df_filtered = df_filtered[df_filtered["updated"] >= cutoff]
    elif time_filter == "Last 7 Days":
        cutoff = now - timedelta(days=7)
        df_filtered = df_filtered[df_filtered["updated"] >= cutoff]
    elif time_filter == "Last 30 Days":
        cutoff = now - timedelta(days=30)
        df_filtered = df_filtered[df_filtered["updated"] >= cutoff]
    elif time_filter == "Custom" and start_date and end_date:
        cutoff = pd.Timestamp(start_date, tz="UTC")
        end = pd.Timestamp(end_date, tz="UTC") + timedelta(days=1)  # Include end date
        df_filtered = df_filtered[(df_filtered["updated"] >= cutoff) & (df_filtered["updated"] < end)]

# ---------- FETCH PROCESSES ----------
processes_dict = api.fetch_processes_dict()
process_ids = ["All"] + sorted(processes_dict.keys())

# ---------- ADDITIONAL FILTERS SECTION ----------
st.subheader("Additional Filters")

col1, col2, col3, col4 = st.columns(4)

with col1:
    selected_process_id = st.selectbox("Select processID:", process_ids)

with col2:
    status_options = ["All", "accepted", "dismissed", "running", "failed", "successful"]
    selected_status = st.selectbox("Status", status_options)

# Fetch jobs for filter options
limit = 1000
params = {"limit": limit}
if selected_process_id != "All":
    params["processID"] = selected_process_id
df_for_filters = api.fetch_table("jobs", Job, params=params)

with col3:
    if df_for_filters is not None and not df_for_filters.empty:
        submitter_filter_options = ["All"] + sorted(df_for_filters["submitter"].dropna().astype(str).unique().tolist())
        selected_submitter_filter = st.selectbox("Submitter", submitter_filter_options)
    else:
        selected_submitter_filter = "All"

# Export button
with col4:
    if st.button("📥 Export to CSV", key="export_btn"):
        if not df_filtered.empty:
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"jobs_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )
        else:
            st.warning("No data to export")

# Apply additional filters to df_filtered
if selected_process_id != "All":
    df_filtered = df_filtered[df_filtered["processID"].astype(str) == selected_process_id]

if selected_status != "All":
    df_filtered = df_filtered[df_filtered["status"] == selected_status]

if selected_submitter_filter != "All":
    df_filtered = df_filtered[df_filtered["submitter"] == selected_submitter_filter]

st.markdown("---")

# ---------- Jobs overview (existing chart) with auto-refresh ----------
overview_col1, overview_col2 = st.columns([4, 1])

with overview_col1:
    nothing = None  # Placeholder to keep layout consistent

with overview_col2:
    auto_refresh = st.checkbox(
        "🔄 Auto-refresh (5s)", key="dashboard_auto_refresh", value=st.session_state.auto_refresh
    )
    if auto_refresh != st.session_state.auto_refresh:
        st.session_state.auto_refresh = auto_refresh
        if auto_refresh:
            st.toast("Auto-refresh enabled")

# Auto-refresh timer
if st.session_state.auto_refresh:
    import time

    time.sleep(5)
    st.rerun()

# Render charts with filtered data
render_jobs_overview(df_filtered.to_dict(orient="records"), api=api)

# ---------- Process Info ----------
st.markdown("---")
if selected_process_id and selected_process_id != "All":
    info = processes_dict[selected_process_id]
    col1, col2, col3, col4 = st.columns(4)
    col1.write(f"**Title:** {info['title']}")
    col2.write(f"**Version:** {info['version']}")
    col3.write(f"**Job Control Options:** {info['jobControlOptions']}")
    col4.write(f"**Description:** {info['description']}")

st.markdown("---")

# ---------- BOTTOM: KPIs Section ----------
st.subheader("Key Performance Indicators")
if not df_all.empty:
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)

    total_jobs = len(df_all)
    successful_jobs = len(df_all[df_all["status"] == "successful"])
    failed_jobs = len(df_all[df_all["status"] == "failed"])
    running_jobs = len(df_all[df_all["status"] == "running"])
    success_rate = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0

    with kpi_col1:
        st.metric("Total Jobs", total_jobs)
    with kpi_col2:
        st.metric("✅ Successful", successful_jobs)
    with kpi_col3:
        st.metric("❌ Failed", failed_jobs)
    with kpi_col4:
        st.metric("⏳ Running", running_jobs)
    with kpi_col5:
        st.metric("Success Rate", f"{success_rate:.1f}%")

# ---------- ALERTS & NOTIFICATIONS (Below KPIs) ----------
st.subheader("⚠️ Failed Jobs Alert")
if not df_filtered.empty:
    failed_jobs_alert = df_filtered[df_filtered["status"] == "failed"]
    if not failed_jobs_alert.empty:
        st.warning(f"⚠️ {len(failed_jobs_alert)} failed job(s) detected!")
        with st.expander("View Failed Jobs"):
            st.dataframe(
                failed_jobs_alert[["jobID", "status", "processID", "updated", "submitter"]].head(10),
                use_container_width=True,
            )
    else:
        st.success("✅ No failed jobs!")
else:
    st.info("No jobs in selected time range")
