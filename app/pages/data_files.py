"""Data Files Preview - Browse and preview data files."""

import json
import os

import streamlit as st

st.set_page_config(layout="wide")
st.title("📁 Data Files")

sepex_dir = "/data"

if "current_dir" not in st.session_state:
    st.session_state.current_dir = sepex_dir

if "selected_file" not in st.session_state:
    st.session_state.selected_file = None

if "files_page" not in st.session_state:
    st.session_state.files_page = 0

# File browser and preview layout
fb_col, preview_col = st.columns([1, 2])

# LEFT: File browser
with fb_col:
    st.subheader("Browser")

    if os.path.exists(sepex_dir):
        current_dir = st.session_state.current_dir

        try:
            items = sorted(os.listdir(current_dir))
            dirs = [i for i in items if os.path.isdir(os.path.join(current_dir, i))]
            files = [i for i in items if os.path.isfile(os.path.join(current_dir, i))]

            st.caption(f"`{current_dir}`")

            # Parent directory
            parent_dir = os.path.dirname(current_dir)
            if parent_dir != current_dir and parent_dir.startswith(sepex_dir):
                if st.button("⬅️ Parent"):
                    st.session_state.current_dir = parent_dir
                    st.session_state.files_page = 0
                    st.rerun()

            # Directories
            if dirs:
                st.write("**Directories:**")
                for dir_name in dirs:
                    if st.button(f"📁 {dir_name}", key=f"dir-{dir_name}", use_container_width=True):
                        st.session_state.current_dir = os.path.join(current_dir, dir_name)
                        st.session_state.files_page = 0
                        st.rerun()

            # Files with pagination
            if files:
                st.divider()
                st.write("**Files:**")

                # Pagination
                files_per_page = 15
                total_files = len(files)
                total_pages = (total_files + files_per_page - 1) // files_per_page

                if st.session_state.files_page >= total_pages:
                    st.session_state.files_page = total_pages - 1

                start_idx = st.session_state.files_page * files_per_page
                end_idx = start_idx + files_per_page
                files_page = files[start_idx:end_idx]

                for file_name in files_page:
                    file_path = os.path.join(current_dir, file_name)
                    if st.button(f"📄 {file_name}", key=f"file-{file_path}", use_container_width=True):
                        st.session_state.selected_file = file_path

                # Pagination controls
                if total_pages > 1:
                    st.markdown("---")
                    pag_col1, pag_col2, pag_col3 = st.columns([1, 2, 1])

                    with pag_col1:
                        if st.button("⬅️ Prev", key="files_prev", disabled=(st.session_state.files_page == 0)):
                            st.session_state.files_page -= 1
                            st.rerun()

                    with pag_col2:
                        st.write(f"Page {st.session_state.files_page + 1} of {total_pages}")

                    with pag_col3:
                        if st.button(
                            "Next ➡️", key="files_next", disabled=(st.session_state.files_page >= total_pages - 1)
                        ):
                            st.session_state.files_page += 1
                            st.rerun()

        except PermissionError:
            st.error("Permission denied")
    else:
        st.warning("Data directory not found")

# RIGHT: File preview
with preview_col:
    st.subheader("Preview")
    selected_file = st.session_state.get("selected_file")

    if selected_file:
        st.caption(f"`{os.path.basename(selected_file)}`")
        try:
            with open(selected_file, "r") as f:
                content = f.read()

            # Try to parse as JSON
            try:
                st.json(json.loads(content))
            except (json.JSONDecodeError, ValueError):
                # If not JSON, display as code
                st.code(content, language="text")

        except Exception as e:
            st.error(f"Error reading file: {e}")
    else:
        st.info("Select a file to preview")
