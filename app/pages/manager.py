"""Input Jobs."""

import json
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import yaml
from sepex import Job, SepexAPI

st.set_page_config(layout="wide")


def app():
    st.title("Jobs Management")

    # Initialize SepexAPI client
    api = SepexAPI()

    # Tab selection
    tab1, tab2, tab3 = st.tabs(["Submit Job", "Dismiss Job", "Dismiss All"])

    # ========== TAB 1: SUBMIT JOB ==========
    with tab1:
        st.subheader("Submit Job")

        # Fetch available processes
        try:
            processes_dict = api.fetch_processes_dict()
            process_ids = list(processes_dict.keys())
        except Exception as e:
            st.error(f"Error fetching processes: {e}")
            process_ids = []

        # Process ID selection
        if process_ids:
            process_id = st.selectbox("Select Process ID", process_ids)
        else:
            st.warning("Could not fetch processes")
            process_id = None

        # Display process information and inputs in two columns
        left_col, right_col = st.columns([1, 2])

        with left_col:
            st.subheader("Process Info")
            if process_id and process_id in processes_dict:
                info = processes_dict[process_id]
                st.write(f"**Title:** {info['title']}")
                st.write(f"**Description:** {info['description']}")
                st.write(f"**Version:** {info['version']}")
                st.write(f"**Job Control:** {info['jobControlOptions']}")
            else:
                st.info("Select a process to see details")

        with right_col:
            # Read file once if uploaded
            uploaded_file = None
            json_data = None
            payload = None

            submitter = st.text_input("Submitter (optional)", placeholder="Enter your name or identifier")

            # File uploader for JSON file
            uploaded_file = st.file_uploader("Choose a JSON file", type="json", key="json_uploader")

            if uploaded_file and process_id:
                try:
                    json_data = json.load(uploaded_file)
                    payload = {"inputs": json_data}

                    # Submit button
                    if st.button("Submit to API", key="submit_job_btn"):
                        url = f"{api.base_url}/processes/{process_id}/execution"

                        try:
                            # Prepare headers with submitter
                            headers = {}
                            if submitter:
                                headers["X-ProcessAPI-User-Email"] = submitter

                            response = requests.post(url, json=payload, headers=headers)

                            # Log response details
                            st.write(f"**Response Status:** {response.status_code}")

                            response.raise_for_status()
                            st.success(f"Successfully submitted! Status: {response.status_code}")
                        except requests.exceptions.HTTPError as e:
                            st.error(f"HTTP Error {response.status_code}: {e}")
                            st.error(f"Response: {response.text}")
                        except Exception as e:
                            st.error(f"Error submitting to API: {str(e)}")
                except json.JSONDecodeError:
                    st.error("Invalid JSON file. Please upload a valid JSON file.")

        # Payload preview below in full width
        st.markdown("---")
        st.subheader("Payload Preview")

        if uploaded_file and payload:
            preview_col1, preview_col2 = st.columns(2)
            try:
                with preview_col1:
                    st.write("**YAML**")
                    yaml_str = yaml.dump(payload, default_flow_style=False, sort_keys=False)
                    st.code(yaml_str, language="yaml")

                with preview_col2:
                    st.write("**JSON**")
                    st.code(json.dumps(payload, indent=2), language="json")
            except Exception as e:
                st.warning(f"Error displaying preview: {str(e)}")
        else:
            st.info("Upload a JSON file to see preview")

    # ========== TAB 2: DISMISS JOB ==========
    with tab2:
        st.subheader("Dismiss Job")

        # Fetch jobs to dismiss
        try:
            jobs_data = api.fetch_table("jobs", Job, params={"limit": 500})
            if jobs_data is not None and not jobs_data.empty:
                # Filter for running/accepted jobs
                dismissible_statuses = ["running", "accepted"]
                jobs_data = jobs_data[jobs_data["status"].isin(dismissible_statuses)]

                if not jobs_data.empty:
                    job_options = [f"{row['jobID'][-8:]} - {row['status']}" for _, row in jobs_data.iterrows()]
                    selected_job_display = st.selectbox("Select Job to Dismiss", job_options, key="dismiss_job_select")

                    if selected_job_display:
                        # Extract job ID
                        selected_idx = job_options.index(selected_job_display)
                        selected_job_id = jobs_data.iloc[selected_idx]["jobID"]

                        st.info(f"Selected Job ID: {selected_job_id}")

                        # Confirmation
                        col1, col2 = st.columns(2)
                        with col1:
                            confirm = st.checkbox("I confirm I want to dismiss this job", key="dismiss_confirm")

                        with col2:
                            if st.button("Dismiss Job", disabled=not confirm, key="dismiss_job_btn"):
                                try:
                                    url = f"{api.base_url}/jobs/{selected_job_id}"
                                    response = requests.delete(url)

                                    st.write(f"**Response Status:** {response.status_code}")
                                    response.raise_for_status()
                                    st.success(f"Job dismissed successfully!")
                                except requests.exceptions.HTTPError as e:
                                    st.error(f"HTTP Error {response.status_code}: {e}")
                                    st.error(f"Response: {response.text}")
                                except Exception as e:
                                    st.error(f"Error dismissing job: {str(e)}")
                else:
                    st.info("No running or accepted jobs to dismiss")
            else:
                st.warning("Could not fetch jobs")
        except Exception as e:
            st.error(f"Error fetching jobs: {e}")

    # ========== TAB 3: DISMISS ALL JOBS ==========
    with tab3:
        st.subheader("Dismiss All Jobs")
        st.warning("⚠️ This action will dismiss ALL running and accepted jobs!")

        # Fetch jobs info
        try:
            jobs_data = api.fetch_table("jobs", Job, params={"limit": 500})
            if jobs_data is not None and not jobs_data.empty:
                # Filter for running/accepted jobs
                dismissible_statuses = ["running", "accepted"]
                dismissible_jobs = jobs_data[jobs_data["status"].isin(dismissible_statuses)]

                if not dismissible_jobs.empty:
                    st.write(f"Found **{len(dismissible_jobs)}** jobs to dismiss:")
                    st.dataframe(
                        dismissible_jobs[["jobID", "status", "processID", "submitter"]], use_container_width=True
                    )

                    # Double confirmation
                    col1, col2 = st.columns(2)
                    with col1:
                        confirm1 = st.checkbox("I understand this will dismiss all jobs", key="dismiss_all_confirm1")

                    with col2:
                        confirm2 = st.checkbox("I confirm I want to proceed", key="dismiss_all_confirm2")

                    if st.button("Dismiss All Jobs", disabled=not (confirm1 and confirm2), key="dismiss_all_btn"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        success_count = 0
                        failed_count = 0

                        for idx, (_, row) in enumerate(dismissible_jobs.iterrows()):
                            job_id = row["jobID"]
                            try:
                                url = f"{api.base_url}/jobs/{job_id}"
                                response = requests.delete(url)
                                response.raise_for_status()
                                success_count += 1
                            except Exception as e:
                                failed_count += 1

                            progress = (idx + 1) / len(dismissible_jobs)
                            progress_bar.progress(progress)
                            status_text.write(f"Dismissed {idx + 1} of {len(dismissible_jobs)} jobs...")

                        progress_bar.empty()
                        status_text.empty()
                        st.success(f"Completed! Successfully dismissed {success_count} jobs. Failed: {failed_count}")
                else:
                    st.info("No running or accepted jobs to dismiss")
            else:
                st.warning("Could not fetch jobs")
        except Exception as e:
            st.error(f"Error: {e}")


if __name__ == "__main__":
    app()
