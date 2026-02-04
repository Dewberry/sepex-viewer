"""Input Jobs."""

import streamlit as st
import json
import yaml
from pathlib import Path
from sepex import SepexAPI

def app():
   st.title("Submit Jobs")

   # Initialize SepexAPI client
   api = SepexAPI()

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

   # File uploader for JSON file
   uploaded_file = st.file_uploader("Choose a JSON file", type="json")

   if uploaded_file and process_id:
      # Read the JSON file
      try:
         json_data = json.load(uploaded_file)

         # Wrap payload in {"inputs": {...}}
         payload = {"inputs": json_data}

         # Hideable payload preview with tabs
         with st.expander("Payload Preview"):
            preview_tab1, preview_tab2 = st.tabs(["YAML", "JSON"])

            with preview_tab1:
               yaml_str = yaml.dump(payload["inputs"], default_flow_style=False, sort_keys=False)
               st.code(yaml_str, language="yaml")

            with preview_tab2:
               st.code(json.dumps(payload["inputs"], indent=2), language="json")

         # Submit button
         if st.button("Submit to API"):
            url = f"{api.base_url}/processes/{process_id}/execution"

            try:
               import requests

               response = requests.post(url, json=payload)

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




if __name__ == "__main__":
    app()