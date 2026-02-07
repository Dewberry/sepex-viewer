import pandas as pd
import streamlit as st
from helpers import render_jobs_overview
from sepex import Job, SepexAPI

# ---------- Page config ----------
st.set_page_config(layout="wide")
st.title("SEPEX Dashboard")
api = SepexAPI()

# ---------- Session state ----------
if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None
if "dashboard_page" not in st.session_state:
    st.session_state.dashboard_page = 0

# ---------- Fetch all jobs once for overview ----------
raw_all = api.fetch_table("jobs", Job, params={"limit": 500, "offset": 0})
df_all = raw_all if isinstance(raw_all, pd.DataFrame) else pd.DataFrame()

# ---------- TOP: Jobs overview (KPIs + plots) ----------
st.subheader("Jobs overview")
render_jobs_overview(df_all.to_dict(orient="records"), api=api)

st.markdown("---")

# ---------- Fetch processes ----------
processes_dict = api.fetch_processes_dict()
process_ids = ["All"] + sorted(processes_dict.keys())

# ---------- FILTERS SECTION ----------
st.subheader("Filters")

col1, col2, col3 = st.columns(3)

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
