"""Processes & Jobs - View jobs for selected processes with filtering."""

import pandas as pd
import requests
import streamlit as st
from pydantic import ValidationError
from sepex import (
    Job,
    JobLogsResponse,
    JobMetadataResponse,
    JobResultsResponse,
    SepexAPI,
)

st.set_page_config(layout="wide")
st.title("Jobs Monitor")

api = SepexAPI()

# ---------- Session state ----------
if "selected_job_id" not in st.session_state:
    st.session_state.selected_job_id = None
if "jobs_page" not in st.session_state:
    st.session_state.jobs_page = 0
if "last_selected_process" not in st.session_state:
    st.session_state.last_selected_process = None
if "page_loaded" not in st.session_state:
    st.session_state.page_loaded = False

# Clear selected job on first page load only
if not st.session_state.page_loaded:
    st.session_state.selected_job_id = None
    st.session_state.page_loaded = True

# ---------- Fetch processes ----------
processes_dict = api.fetch_processes_dict()
process_ids = ["All"] + sorted(processes_dict.keys())

# ---------- LEFT: Processes & Filters ----------
with st.container():
    col1, col2, col3 = st.columns([1.5, 1.5, 1.5])

    with col1:
        selected_process_id = st.selectbox("Select Process:", process_ids, key="process_select")

    with col2:
        status_options = ["All", "accepted", "dismissed", "running", "failed", "successful"]
        selected_status = st.selectbox("Status", status_options)

    # Check if process changed and reset filters
    if selected_process_id != st.session_state.last_selected_process:
        st.session_state.jobs_page = 0
        st.session_state.selected_job_id = None
        st.session_state.last_selected_process = selected_process_id
        st.rerun()

    # Fetch jobs for filter options
    limit = 1000
    params = {"limit": limit}
    if selected_process_id != "All":
        params["processID"] = selected_process_id
    df_for_filters = api.fetch_table("jobs", Job, params=params)

    with col3:
        if df_for_filters is not None and not df_for_filters.empty:
            submitter_filter_options = ["All"] + sorted(
                df_for_filters["submitter"].dropna().astype(str).unique().tolist()
            )
            selected_submitter_filter = st.selectbox("Submitter", submitter_filter_options)
        else:
            selected_submitter_filter = "All"

st.markdown("---")

# ---------- Jobs Table + Details ----------
jobs_col, details_col = st.columns([1.5, 1])

# JOBS TABLE
with jobs_col:
    st.subheader("Jobs")

    limit = 1000
    params = {"limit": limit}
    if selected_process_id != "All":
        params["processID"] = selected_process_id
    df = api.fetch_table("jobs", Job, params=params)

    if df is None or df.empty:
        st.info("No jobs found.")
        st.session_state.selected_job_id = None
    else:
        # Apply filters
        if selected_status != "All":
            df = df[df["status"] == selected_status]
        if selected_process_id != "All":
            df = df[df["processID"].astype(str) == selected_process_id]
        if selected_submitter_filter != "All":
            df = df[df["submitter"].astype(str) == selected_submitter_filter]

        if df.empty:
            st.info("No jobs match your filters.")
            st.session_state.selected_job_id = None
        else:
            # Don't auto-select first job - keep it None until user selects one
            # if st.session_state.selected_job_id is None:
            #     st.session_state.selected_job_id = str(df.iloc[0]["jobID"])

            STATUS_COLORS = {
                "successful": "green",
                "running": "gold",
                "accepted": "orange",
                "dismissed": "purple",
                "failed": "red",
            }

            # Reset selected job if it's not in current filtered results
            if st.session_state.selected_job_id and st.session_state.selected_job_id not in df["jobID"].values:
                st.session_state.selected_job_id = None

            # Pagination setup
            jobs_per_page = 10
            total_jobs = len(df)
            total_pages = (total_jobs + jobs_per_page - 1) // jobs_per_page

            if st.session_state.jobs_page >= total_pages:
                st.session_state.jobs_page = total_pages - 1

            start_idx = st.session_state.jobs_page * jobs_per_page
            end_idx = start_idx + jobs_per_page
            df_page = df.iloc[start_idx:end_idx]

            # Pagination controls at top
            pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])

            with pag_col1:
                if st.button("⬅️ Previous", disabled=(st.session_state.jobs_page == 0), key="jobs_prev_top"):
                    st.session_state.jobs_page -= 1
                    st.rerun()

            with pag_col2:
                st.write(f"Page {st.session_state.jobs_page + 1} of {total_pages} | Total jobs: {total_jobs}")

            with pag_col3:
                if st.button("Next ➡️", disabled=(st.session_state.jobs_page >= total_pages - 1), key="jobs_next_top"):
                    st.session_state.jobs_page += 1
                    st.rerun()

            st.markdown("---")

            # Header
            h1, h2, h3, h4, h5 = st.columns([2, 2, 2, 2, 2])
            h1.write("**Job ID**")
            h2.write("**Status**")
            h3.write("**Updated**")
            h4.write("**ProcessID**")
            h5.write("**Submitter**")

            # Rows
            for _, row in df_page.iterrows():
                job_id_str = str(row["jobID"])
                job_id_short = job_id_str[-8:]  # Last 8 characters of UUID
                status_str = str(row["status"])
                is_selected = job_id_str == st.session_state.selected_job_id

                # Use container with background color for selected row
                if is_selected:
                    row_container = st.container(border=True)
                else:
                    row_container = st.container()

                with row_container:
                    r1, r2, r3, r4, r5 = st.columns([2, 2, 2, 2, 2])

                    # Highlight selected row with bold styling
                    if is_selected:
                        if r1.button(f"🔗 {job_id_short}", key=f"view-{job_id_str}", help="View job details"):
                            st.session_state.selected_job_id = job_id_str
                            st.rerun()
                    else:
                        if r1.button(f"🔗 {job_id_short}", key=f"view-{job_id_str}", help="View job details"):
                            st.session_state.selected_job_id = job_id_str
                            st.rerun()

                    color = STATUS_COLORS.get(status_str.lower(), "black")
                    r2.markdown(
                        f"<span style='color:{color}; font-weight:600'>{status_str}</span>",
                        unsafe_allow_html=True,
                    )

                    # Format timestamp to show seconds without nanoseconds
                    updated_time = pd.to_datetime(row["updated"]).strftime("%Y-%m-%d %H:%M:%S")
                    r3.write(updated_time)
                    r4.write(row["processID"])
                    # Display submitter as plain text (not as a link)
                    submitter_val = str(row.get("submitter", "N/A"))
                    r5.text(submitter_val)

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
            ["None", "Results", "Logs", "Metadata"],
            key="detail_option_box",
        )

        base_url = api.base_url

        # -------- NONE ----------
        if detail_option == "None":
            st.info("Select a detail option to view")

        # -------- RESULTS ----------
        elif detail_option == "Results":
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
                            st.dataframe(pd.DataFrame(results), use_container_width=True)
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

                    if process_logs:
                        # Format logs nicely
                        lines = []
                        for log in process_logs:
                            # Handle both dict and object cases
                            if isinstance(log, dict):
                                level = log.get("level", "")
                                msg = log.get("msg", "")
                                time = log.get("time", "")
                            else:
                                # Handle object attributes
                                level = getattr(log, "level", "")
                                msg = getattr(log, "msg", "")
                                time = getattr(log, "time", "")

                            # Only show time if it's not the default zero time
                            if time and time != "0001-01-01T00:00:00Z":
                                lines.append(f"[{level}] {time} - {msg}")
                            else:
                                lines.append(f"[{level}] {msg}")

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
