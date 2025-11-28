import pandas as pd
import requests
import streamlit as st
import yaml
from helpers import render_jobs_overview
from pydantic import ValidationError
from sepex import (
    Job,
    JobLogsResponse,
    JobMetadataResponse,
    JobResultsResponse,
    Process,
    SepexAPI,
)

# ---------- Page config ----------
st.set_page_config(layout="wide")
st.title("SEPEX Dashboard")

api = SepexAPI()

# ---------- Session state ----------
if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None

# ---------- Fetch all jobs once for overview ----------
raw_all = api.fetch_table("jobs", Job, params={"limit": 500, "offset": 0})
df_all = raw_all if isinstance(raw_all, pd.DataFrame) else pd.DataFrame()

# ---------- TOP: Jobs overview (KPIs + plots) ----------
st.subheader("Jobs overview")
render_jobs_overview(df_all.to_dict(orient="records"), api=api)

st.markdown("---")

# ---------- Fetch processes ----------
processes_dict = api.fetch_processes_dict()
process_ids = sorted(processes_dict.keys())

# Use full-width columns
col1, col2, col3 = st.columns([1, 2, 2])

# ---------- LEFT: Processes ----------
with col1:
    st.subheader("Processes")

    selected_process_id = st.selectbox("Select processID:", process_ids)

    if selected_process_id:
        # processes_dict is built from Process(**p).model_dump() in SepexAPI
        info = processes_dict[selected_process_id]

        st.write("**Title:**", info["title"])
        st.write("**Description:**", info["description"])
        st.write("**Version:**", info["version"])
        st.write("**Job Control Options:**", info["jobControlOptions"])

        # to show YAML here:
        # st.markdown("**Selected Process as YAML:**")
        # st.code(yaml.dump(info, sort_keys=False), language="yaml")

# ---------- MIDDLE: Jobs table ----------
with col2:
    st.subheader("Jobs")

    # For the jobs table we can fetch per-process (or reuse df_all if you prefer)
    limit = 1000
    params = {"limit": limit, "processID": selected_process_id}
    df = api.fetch_table("jobs", Job, params=params)

    if df is None or df.empty:
        st.info("No jobs found.")
        st.session_state.selected_job_id = None

    else:
        # ---------- Filter row (single row of dropdowns) ----------
        fcol1, fcol2, fcol3 = st.columns([1, 1, 1])

        # Status filter
        status_options = ["All", "accepted", "dismissed", "running", "failed", "successful"]
        selected_status = fcol1.selectbox("Status", status_options)

        # Process ID filter
        process_filter_options = ["All"] + sorted(df["processID"].dropna().astype(str).unique().tolist())
        selected_process_filter = fcol2.selectbox("Process ID", process_filter_options)

        # Submitter filter (guard for missing column)
        if "submitter" not in df.columns:
            df["submitter"] = None
        submitter_filter_options = ["All"] + sorted(df["submitter"].dropna().astype(str).unique().tolist())
        selected_submitter_filter = fcol3.selectbox("Submitter", submitter_filter_options)

        # ---------- Apply filters ----------
        # Status
        if selected_status != "All":
            df = df[df["status"] == selected_status]

        # Process ID
        if selected_process_filter != "All":
            df = df[df["processID"].astype(str) == selected_process_filter]

        # Submitter
        if selected_submitter_filter != "All":
            df = df[df["submitter"].astype(str) == selected_submitter_filter]

        if df.empty:
            st.info("No jobs match your filters.")
            st.session_state.selected_job_id = None
        else:
            # Auto-select first job if none selected
            if st.session_state.selected_job_id is None:
                st.session_state.selected_job_id = str(df.iloc[0]["jobID"])

            # Status color mapping
            STATUS_COLORS = {
                "successful": "green",
                "running": "gold",
                "accepted": "orange",
                "dismissed": "purple",
                "failed": "red",
            }

            # Header
            h1, h2, h3, h4, h5 = st.columns([1, 2, 2, 2, 2])
            h1.write("**Action**")
            h2.write("**Job ID**")
            h3.write("**Status**")
            h4.write("**Updated**")
            h5.write("**ProcessID**")

            # Rows
            for _, row in df.iterrows():
                r1, r2, r3, r4, r5 = st.columns([1, 2, 2, 2, 2])

                job_id_str = str(row["jobID"])
                status_str = str(row["status"])

                # View button (compact)
                if r1.button("View ▸", key=f"view-{job_id_str}", help="View job details"):
                    st.session_state.selected_job_id = job_id_str
                    st.rerun()

                # Selected row highlight
                if job_id_str == st.session_state.selected_job_id:
                    r2.markdown(f"**{job_id_str}**")
                else:
                    r2.write(job_id_str)

                # Colored status text
                color = STATUS_COLORS.get(status_str.lower(), "black")
                r3.markdown(
                    f"<span style='color:{color}; font-weight:600'>{status_str}</span>",
                    unsafe_allow_html=True,
                )

                r4.write(str(row["updated"]))
                r5.write(row["processID"])

# ---------- RIGHT: Job details ----------
with col3:
    st.subheader("Job Details")

    selected_job_id = st.session_state.get("selected_job_id")

    if not selected_job_id:
        st.info("Select a job from the Jobs area to see details here.")
    else:
        st.markdown(f"**Selected job:** `{selected_job_id}`")

        detail_option = st.selectbox(
            "Show:",
            ["Results", "Logs", "Metadata"],
            key="detail_option_box",
        )

        base_url = api.base_url

        # -------- RESULTS ----------
        if detail_option == "Results":
            url = f"{base_url}/jobs/{selected_job_id}/results"
            try:
                resp = requests.get(url)
            except Exception as e:
                st.error(f"Error calling results endpoint: {e}")
            else:
                if resp.status_code == 200:
                    raw = resp.json()
                    # Use JobResultsResponse model if possible
                    try:
                        job_results = JobResultsResponse(**raw)
                        results = job_results.results
                    except ValidationError:
                        # Fallback: use raw data
                        results = raw

                    if isinstance(results, list):
                        try:
                            st.dataframe(
                                pd.DataFrame(results),
                                width="stretch",
                            )
                        except Exception:
                            st.json(results)
                    else:
                        st.json(results)
                else:
                    st.error(f"Failed to fetch results: {resp.status_code}")

        # -------- LOGS ----------
        elif detail_option == "Logs":
            url = f"{base_url}/jobs/{selected_job_id}/logs"
            try:
                resp = requests.get(url)
            except Exception as e:
                st.error(f"Error calling logs endpoint: {e}")
            else:
                if resp.status_code == 200:
                    raw = resp.json()
                    # Use JobLogsResponse if shape matches
                    try:
                        logs_response = JobLogsResponse(**raw)
                        process_logs = logs_response.process_logs
                    except ValidationError:
                        process_logs = raw.get("process_logs", raw)

                    if process_logs and isinstance(process_logs[0], dict) and "msg" in process_logs[0]:
                        lines = [
                            f"[{log.get('level','')}] {log.get('time','')} - {log.get('msg','')}"
                            for log in process_logs
                        ]
                        st.code("\n".join(lines))
                    else:
                        st.json(process_logs)
                else:
                    st.error(f"Failed to fetch logs: {resp.status_code}")

        # -------- METADATA ----------
        elif detail_option == "Metadata":
            urls_to_try = [
                f"{base_url}/jobs/{selected_job_id}/metadata",
                f"{base_url}/jobs/{selected_job_id}",
            ]

            meta_data = None
            last_status = None
            last_error = None

            for url in urls_to_try:
                try:
                    resp = requests.get(url)
                except Exception as e:
                    last_error = str(e)
                    continue

                last_status = resp.status_code
                if resp.status_code == 200:
                    raw = resp.json()
                    try:
                        meta = JobMetadataResponse(**raw)
                        meta_data = meta.model_dump()
                    except ValidationError:
                        meta_data = raw
                    break

            if meta_data is not None:
                st.json(meta_data)
            else:
                if last_error:
                    st.error(f"Failed to fetch metadata. Last error: {last_error}")
                elif last_status is not None:
                    st.error(f"Failed to fetch metadata. Last status: {last_status}")
                else:
                    st.error("Failed to fetch metadata: unknown error.")
