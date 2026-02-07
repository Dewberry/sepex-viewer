# helpers.py

from datetime import datetime
from typing import Any, Dict, List, Tuple

import altair as alt
import pandas as pd
import streamlit as st
from sepex import SepexAPI


def get_process_maps(api: SepexAPI) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """
    Build:
      - process_map: processID -> title
      - process_status_map: placeholder (kept for compatibility; currently unused)
    """
    processes_dict = api.fetch_processes_dict()  # {id: Process.model_dump()}
    process_map: Dict[str, str] = {}

    for pid, info in processes_dict.items():
        title = info.get("title") or ""
        process_map[str(pid)] = title

    process_status_map: Dict[str, Any] = {}
    return process_map, process_status_map


def _prepare_jobs_dataframe(jobs: List[Dict[str, Any]]) -> pd.DataFrame:
    """Turn raw jobs list into a DataFrame with parsed timestamps."""
    df = pd.DataFrame(jobs or [])

    if "updated" in df.columns:
        try:
            df["updated"] = pd.to_datetime(df["updated"])
        except Exception:
            # Leave as-is if parsing fails
            pass

    return df


def render_jobs_overview(
    jobs: List[Dict[str, Any]],
    api: SepexAPI,
) -> None:
    """
    Render the KPIs and charts for all jobs.
    No raw table here (you already have a filtered jobs table in the main app).
    """
    dfj = _prepare_jobs_dataframe(jobs)

    if dfj.empty:
        st.info("No jobs available for overview charts.")
        return

    # Process mapping via SepexAPI (processID -> title)
    process_map, process_status_map = get_process_maps(api)

    display_df = dfj

    # ---------- KPIs ----------
    total_jobs = len(display_df)
    unique_processes = int(display_df["processID"].nunique()) if "processID" in display_df.columns else 0

    last_seen = None
    if "updated" in display_df.columns and not display_df["updated"].isna().all():
        try:
            last_seen = display_df["updated"].max()
        except Exception:
            last_seen = None

    successful = int(display_df[display_df["status"] == "successful"].shape[0]) if "status" in display_df.columns else 0
    failed = int(display_df[display_df["status"] == "failed"].shape[0]) if "status" in display_df.columns else 0
    running = int(display_df[display_df["status"] == "running"].shape[0]) if "status" in display_df.columns else 0

    k1, k2, k3, k4 = st.columns([1, 1, 1, 2])
    k1.metric("Total jobs", f"{total_jobs}")
    k2.metric("Unique processes", f"{unique_processes}")
    k3.metric("Successful", f"{successful}")
    if last_seen is not None:
        k4.metric("Last seen (UTC)", last_seen.strftime("%Y-%m-%d %H:%M:%S"))
    else:
        k4.metric("Last seen (UTC)", "N/A")

    # ---------- Charts row ----------
    left_col, right_col = st.columns([1.6, 1.4])

    # Ensure "updated" is datetime
    if "updated" in display_df.columns:
        display_df["updated"] = pd.to_datetime(display_df["updated"], utc=True)

    if "updated" in display_df.columns and not display_df["updated"].isna().all():
        # Filter to past 24 hours
        now = pd.Timestamp.now(tz="UTC")
        cutoff = now - pd.Timedelta(days=1)
        chart_df = display_df[display_df["updated"] >= cutoff].copy()

        if not chart_df.empty:
            # Set index for resampling
            chart_df = chart_df.sort_values("updated")
            chart_df.set_index("updated", inplace=True)

            # Resample by 1 hour
            counts = chart_df.groupby([pd.Grouper(freq="1h"), "status"]).size().reset_index(name="count")
            counts.columns = ["updated", "status", "count"]

            if not counts.empty:
                status_domain = ["failed", "running", "successful", "accepted", "dismissed"]
                status_colors = ["#d62728", "#ffbf00", "#2ca02c", "#ff7f0e", "#9467bd"]

                # Filter statuses that actually exist
                existing_statuses = counts["status"].unique().tolist()
                filtered_domain = [s for s in status_domain if s in existing_statuses]
                filtered_colors = [status_colors[i] for i, s in enumerate(status_domain) if s in existing_statuses]

                status_select = alt.selection_multi(fields=["status"], bind="legend")

                max_count = counts["count"].max()

                chart = (
                    alt.Chart(counts)
                    .mark_bar()
                    .encode(
                        x=alt.X("updated:T", title="Time (UTC)", axis=alt.Axis(format="%m/%d %H:%M")),
                        y=alt.Y("count:Q", title="Job Count", scale=alt.Scale(domain=[0, max_count * 1.15])),
                        color=alt.Color(
                            "status:N",
                            title="Status",
                            scale=alt.Scale(domain=filtered_domain, range=filtered_colors),
                        ),
                        opacity=alt.condition(status_select, alt.value(1), alt.value(0.2)),
                        tooltip=[
                            alt.Tooltip("updated:T", title="Time", format="%Y-%m-%d %H:%M:%S"),
                            alt.Tooltip("status:N", title="Status"),
                            alt.Tooltip("count:Q", title="Count"),
                        ],
                    )
                    .add_selection(status_select)
                    .interactive()
                    .properties(width=800, height=300)
                )

                left_col.altair_chart(chart, use_container_width=True)
            else:
                left_col.info("No data available for the selected time range")
        else:
            left_col.info("No data in the past 24 hours")

        # ---------- Process distribution pie ----------
        if "processID" in display_df.columns:
            display_df = display_df.copy()
            display_df["process_title"] = display_df["processID"].astype(str).map(lambda x: process_map.get(x, x))

            proc_counts = display_df["process_title"].value_counts().reset_index()
            proc_counts.columns = ["process_title", "count"]

            top_n = 10
            if proc_counts.shape[0] > top_n:
                others = proc_counts.iloc[top_n:]["count"].sum()
                proc_counts = proc_counts.iloc[:top_n]
                proc_counts = pd.concat(
                    [
                        proc_counts,
                        pd.DataFrame([{"process_title": "other", "count": others}]),
                    ],
                    ignore_index=True,
                )

            proc_pie = (
                alt.Chart(proc_counts)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color("process_title:N", title="Process", scale=alt.Scale(scheme="set2")),
                    tooltip=[
                        alt.Tooltip("process_title:N", title="process"),
                        alt.Tooltip("count:Q", title="jobs"),
                    ],
                )
                .properties(width=300, height=250)
                .configure_legend(orient="right", labelFontSize=9, titleFontSize=10)
            )

            # ---------- Submitter distribution pie ----------
            if "submitter" in display_df.columns:
                sub_counts = display_df["submitter"].value_counts().reset_index()
                sub_counts.columns = ["submitter", "count"]

                top_n_sub = 10
                if sub_counts.shape[0] > top_n_sub:
                    others = sub_counts.iloc[top_n_sub:]["count"].sum()
                    sub_counts = sub_counts.iloc[:top_n_sub]
                    sub_counts = pd.concat(
                        [
                            sub_counts,
                            pd.DataFrame([{"submitter": "other", "count": others}]),
                        ],
                        ignore_index=True,
                    )

                sub_pie = (
                    alt.Chart(sub_counts)
                    .mark_arc(innerRadius=50)
                    .encode(
                        theta=alt.Theta(field="count", type="quantitative"),
                        color=alt.Color("submitter:N", title="Submitter", scale=alt.Scale(scheme="dark2")),
                        tooltip=[
                            alt.Tooltip("submitter:N", title="submitter"),
                            alt.Tooltip("count:Q", title="jobs"),
                        ],
                    )
                    .properties(width=300, height=250)
                    .configure_legend(orient="right", labelFontSize=9, titleFontSize=10)
                )

                # Create two columns for pie charts side by side
                pie_col1, pie_col2 = right_col.columns(2)
                pie_col1.altair_chart(proc_pie, use_container_width=True)
                pie_col2.altair_chart(sub_pie, use_container_width=True)
            else:
                right_col.altair_chart(proc_pie, use_container_width=True)
                right_col.info("No submitter data to show distribution.")
        else:
            right_col.info("No processID field to show distribution.")
    else:
        left_col.info("No timestamped jobs to chart.")
        right_col.info("No processID data.")
