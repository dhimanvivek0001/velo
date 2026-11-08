"""Shared Streamlit UI for the Velo copilot, used by the live app and the public demo.
Depends only on streamlit, pandas and altair, so the demo deploys without Databricks."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

BADGES = {
    "sql": ":blue-background[SQL query]",
    "finding": ":green-background[Certified finding]",
    "refuse": ":orange-background[Refused: out of scope or unsafe]",
    "demo": ":gray-background[Demo mode]",
}
BAR_COLOUR = "#4C78A8"


def _ordered_labels(labels: pd.Series) -> tuple[pd.Series, bool]:
    """Return display labels and whether they have a natural order (numbers, dates)."""
    if pd.api.types.is_numeric_dtype(labels):
        return labels, True
    parsed = pd.to_datetime(labels, errors="coerce", format="ISO8601", utc=True)
    if parsed.notna().all():
        fmt = "%Y-%m" if (parsed.dt.day == 1).all() else "%Y-%m-%d"
        return parsed.dt.strftime(fmt), True
    return labels, False


def chart_if_useful(rows: pd.DataFrame | None) -> None:
    """Bar chart when the result is a small label -> number comparison.
    Rankings -> horizontal bars sorted by value; hours/months -> vertical bars in order."""
    if rows is None or rows.empty or len(rows) > 50 or rows.shape[1] < 2:
        return
    label, value = rows.columns[0], rows.columns[1]
    # Databricks DECIMAL values can arrive as Decimal objects: convert explicitly
    numbers = pd.to_numeric(rows[value].astype(str), errors="coerce")
    if numbers.isna().all():
        return
    labels, ordered = _ordered_labels(rows[label])
    data = pd.DataFrame(
        {
            "label": labels.astype(str).to_numpy(),
            "value": numbers.astype(float).to_numpy(),
        }
    )
    tooltip = ["label", alt.Tooltip("value:Q", format=",.2f", title=str(value))]
    if ordered:
        chart = (
            alt.Chart(data)
            .mark_bar(color=BAR_COLOUR)
            .encode(
                x=alt.X(
                    "label:O", sort=None, title=str(label), axis=alt.Axis(labelAngle=0)
                ),
                y=alt.Y("value:Q", title=str(value)),
                tooltip=tooltip,
            )
        )
    else:
        chart = (
            alt.Chart(data)
            .mark_bar(color=BAR_COLOUR)
            .encode(
                y=alt.Y("label:N", sort="-x", title=str(label)),
                x=alt.X("value:Q", title=str(value)),
                tooltip=tooltip,
            )
            .properties(height=max(160, 32 * len(data)))
        )
    st.altair_chart(chart, use_container_width=True)


def render(item: dict, max_rows: int = 200) -> None:
    """One question and its answer: badge, text, chart, table, SQL, self-corrections."""
    with st.chat_message("user"):
        st.write(item["question"])
    with st.chat_message("assistant"):
        st.markdown(BADGES.get(item.get("type") or "", ":gray-background[No answer]"))
        st.write(item["answer"])
        rows = item.get("rows")
        if rows is not None and not rows.empty:
            chart_if_useful(rows)
            st.dataframe(rows, use_container_width=True, hide_index=True)
        if item.get("sql"):
            with st.expander(f"SQL (guardrail-checked, max {max_rows} rows)"):
                st.code(item["sql"], language="sql")
        if item.get("errors"):
            with st.expander("Self-corrections"):
                for err in item["errors"]:
                    st.caption(err)
