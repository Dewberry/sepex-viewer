"""Payload Builder - Create and save process execution payloads."""

import streamlit as st
import json
import yaml
import requests
from sepex import SepexAPI

def app():
    st.title("Payload Builder")

    # Initialize SepexAPI client
    api = SepexAPI()

    # Fetch available processes
    try:
        processes_dict = api.fetch_processes_dict()
        process_ids = sorted(list(processes_dict.keys()))
    except Exception as e:
        st.error(f"Error fetching processes: {e}")
        process_ids = []

    if not process_ids:
        st.warning("No processes available")
        return

    # Process ID selection
    selected_process_id = st.selectbox("Select Process", process_ids)

    if selected_process_id:
        # Initialize session state for this process
        session_key = f"payload_{selected_process_id}"
        if session_key not in st.session_state:
            st.session_state[session_key] = {}

        # Fetch process details including inputs
        try:
            url = f"{api.base_url}/processes/{selected_process_id}"
            response = requests.get(url)
            response.raise_for_status()
            process_detail = response.json()

            st.subheader(f"Process: {process_detail.get('info', {}).get('title', selected_process_id)}")
            st.write(process_detail.get('info', {}).get('description', 'No description'))

            # Get inputs from process detail
            inputs = process_detail.get('inputs', [])

            if not inputs:
                st.info("This process has no inputs")
                return

            st.markdown("---")
            st.subheader("Build Payload")

            # Create left/right columns
            left_col, right_col = st.columns([1, 1])

            with left_col:
                st.write("**Editor**")
                # Use JSON/YAML text editor for full flexibility

                # Initialize with example or empty
                current_json = json.dumps(st.session_state[session_key], indent=2) if st.session_state[session_key] else "{}"

                json_text = st.text_area(
                    "Payload (JSON format)",
                    value=current_json,
                    height=400,
                    key=f"{session_key}_json_editor",
                    label_visibility="collapsed"
                )

                try:
                    edited_payload = json.loads(json_text)
                    st.session_state[session_key] = edited_payload
                    json_valid = True
                except json.JSONDecodeError as e:
                    st.error(f"Invalid JSON: {e}")
                    edited_payload = st.session_state[session_key]
                    json_valid = False

            with right_col:
                st.write("**Preview**")
                # Display payload in both formats with tabs
                preview_tab1, preview_tab2 = st.tabs(["JSON", "YAML"])

                with preview_tab1:
                    st.code(json.dumps(edited_payload, indent=2), language="json")

                with preview_tab2:
                    yaml_str = yaml.dump(edited_payload, default_flow_style=False, sort_keys=False, allow_unicode=True)
                    st.code(yaml_str, language="yaml")

            st.markdown("---")

            # Save and download options
            col1, col2 = st.columns(2)

            with col1:
                json_str = json.dumps(edited_payload, indent=2)
                st.download_button(
                    label="Download as JSON",
                    data=json_str,
                    file_name=f"{selected_process_id}_payload.json",
                    mime="application/json"
                )

            with col2:
                yaml_str = yaml.dump(edited_payload, default_flow_style=False, sort_keys=False, allow_unicode=True)
                st.download_button(
                    label="Download as YAML",
                    data=yaml_str,
                    file_name=f"{selected_process_id}_payload.yaml",
                    mime="text/yaml"
                )

        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching process details: {e}")
        except Exception as e:
            st.error(f"Error building payload: {e}")


if __name__ == "__main__":
    app()
