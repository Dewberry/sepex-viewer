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

# File browser and preview layout
with st.expander("Data Files", expanded=True):
    fb_col, preview_col = st.columns([1, 2])

    # LEFT: File browser
    with fb_col:
        st.subheader("Files")
        import os
        import json

        sepex_dir = "/mnt/sepex"
        if os.path.exists(sepex_dir):
            if "current_dir" not in st.session_state:
                st.session_state.current_dir = sepex_dir

            current_dir = st.session_state.current_dir

            try:
                items = sorted(os.listdir(current_dir))
                dirs = [i for i in items if os.path.isdir(os.path.join(current_dir, i))]
                files = [i for i in items if os.path.isfile(os.path.join(current_dir, i))]

                st.caption(f"`{current_dir}`")

                # Parent directory
                parent_dir = os.path.dirname(current_dir)
                if parent_dir != current_dir and parent_dir.startswith(sepex_dir):
                    if st.button("Parent"):
                        st.session_state.current_dir = parent_dir
                        st.rerun()

                # Directories
                if dirs:
                    for dir_name in dirs:
                        if st.button(f"{dir_name}", key=f"dir-{dir_name}", use_container_width=True):
                            st.session_state.current_dir = os.path.join(current_dir, dir_name)
                            st.rerun()

                # Files
                if files:
                    st.divider()
                    for file_name in files:
                        file_path = os.path.join(current_dir, file_name)
                        if st.button(f"{file_name}", key=f"file-{file_path}", use_container_width=True):
                            st.session_state.selected_file = file_path
            except PermissionError:
                st.error(f"Permission denied")
        else:
            st.warning(f"Directory not found")

    # RIGHT: File preview
    with preview_col:
        st.subheader("Preview")
        selected_file = st.session_state.get("selected_file")

        if selected_file:
            st.caption(f"`{os.path.basename(selected_file)}`")
            try:
                with open(selected_file, "r") as f:
                    content = f.read()
                try:
                    st.json(json.loads(content))
                except:
                    st.code(content)
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.info("Select a file to preview")

st.markdown("---")

# ---------- Fetch processes ----------
processes_dict = api.fetch_processes_dict()
process_ids = sorted(processes_dict.keys())

# Use full-width columns
col1, col2 = st.columns([1, 3])

# ---------- LEFT: Processes & Filters ----------
with col1:
    st.subheader("Processes")
    selected_process_id = st.selectbox("Select processID:", process_ids)

    if selected_process_id:
        info = processes_dict[selected_process_id]
        st.write("**Title:**", info["title"])
        st.write("**Description:**", info["description"])
        st.write("**Version:**", info["version"])
        st.write("**Job Control Options:**", info["jobControlOptions"])

    st.divider()

    # Fetch jobs for filter options
    limit = 1000
    params = {"limit": limit, "processID": selected_process_id}
    df_for_filters = api.fetch_table("jobs", Job, params=params)

    # Filters
    st.subheader("Filters")
    status_options = ["All", "accepted", "dismissed", "running", "failed", "successful"]
    selected_status = st.selectbox("Status", status_options)

    if df_for_filters is not None and not df_for_filters.empty:
        process_filter_options = ["All"] + sorted(df_for_filters["processID"].dropna().astype(str).unique().tolist())
        selected_process_filter = st.selectbox("Process ID", process_filter_options)

        if "submitter" not in df_for_filters.columns:
            df_for_filters["submitter"] = None
        submitter_filter_options = ["All"] + sorted(df_for_filters["submitter"].dropna().astype(str).unique().tolist())
        selected_submitter_filter = st.selectbox("Submitter", submitter_filter_options)
    else:
        selected_process_filter = "All"
        selected_submitter_filter = "All"

# ---------- MIDDLE/RIGHT: Jobs table + Details ----------
with col2:
    # Create nested columns for Jobs table and Details
    jobs_col, details_col = st.columns([1.5, 1])

    # JOBS TABLE
    with jobs_col:
        st.subheader("Jobs")

        limit = 1000
        params = {"limit": limit, "processID": selected_process_id}
        df = api.fetch_table("jobs", Job, params=params)

        if df is None or df.empty:
            st.info("No jobs found.")
            st.session_state.selected_job_id = None
        else:

            # Apply filters
            if selected_status != "All":
                df = df[df["status"] == selected_status]
            if selected_process_filter != "All":
                df = df[df["processID"].astype(str) == selected_process_filter]
            if selected_submitter_filter != "All":
                df = df[df["submitter"].astype(str) == selected_submitter_filter]

            if df.empty:
                st.info("No jobs match your filters.")
                st.session_state.selected_job_id = None
            else:
                if st.session_state.selected_job_id is None:
                    st.session_state.selected_job_id = str(df.iloc[0]["jobID"])

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
                    job_id_short = job_id_str[-8:]  # Last 8 characters of UUID
                    status_str = str(row["status"])

                    if r1.button("View ▸", key=f"view-{job_id_str}", help="View job details"):
                        st.session_state.selected_job_id = job_id_str
                        st.rerun()

                    if job_id_str == st.session_state.selected_job_id:
                        r2.markdown(f"**{job_id_short}**")
                    else:
                        r2.write(job_id_short)

                    color = STATUS_COLORS.get(status_str.lower(), "black")
                    r3.markdown(
                        f"<span style='color:{color}; font-weight:600'>{status_str}</span>",
                        unsafe_allow_html=True,
                    )

                    r4.write(str(row["updated"]))
                    r5.write(row["processID"])

    # JOB DETAILS
    with details_col:
        st.subheader("Details")

        selected_job_id = st.session_state.get("selected_job_id")

        if not selected_job_id:
            st.info("Select a job to view details")
        else:
            st.caption(f"`{selected_job_id[-8:]}`")

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
                        try:
                            job_results = JobResultsResponse(**raw)
                            results = job_results.results
                        except ValidationError:
                            results = raw

                        if isinstance(results, list):
                            try:
                                st.dataframe(pd.DataFrame(results), width="stretch")
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
