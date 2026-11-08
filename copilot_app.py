"""Velo Copilot web app (Streamlit). Run: streamlit run copilot_app.py

A thin UI over copilot.copilot.ask(): same semantic layer, guardrails and audit log.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from copilot.copilot import LLM_ENDPOINT, MAX_ROWS, ask

EXAMPLES = [
    "Which borough has the longest typical wait time?",
    "What is the typical wait time by hour of day?",
    "How many trips were there per month in 2025?",
    "What share of all trips were Uber trips?",
    "What effect did congestion pricing have on ride-hail demand?",
    "How accurate is our day-ahead demand forecast?",
    "How many yellow taxi trips were there in March 2025?",
]
BADGES = {
    "sql": ":blue-background[SQL query]",
    "finding": ":green-background[Certified finding]",
    "refuse": ":orange-background[Refused: out of scope or unsafe]",
}
BAR_COLOUR = "#4C78A8"

st.set_page_config(page_title="Velo Copilot", page_icon=":taxi:", layout="wide")


def chart_if_useful(rows: pd.DataFrame) -> None:
    """Bar chart when the result is a small label -> number comparison.

    Rankings (text labels) -> horizontal bars sorted by value.
    Ordered labels (hour, month, dates) -> vertical bars in natural order.
    """
    if rows is None or rows.empty or len(rows) > 50 or rows.shape[1] < 2:
        return
    label, value = rows.columns[0], rows.columns[1]
    # Databricks DECIMAL values arrive as Python Decimal objects: convert explicitly
    numbers = pd.to_numeric(rows[value].astype(str), errors="coerce")
    if numbers.isna().all():
        return

    labels = rows[label]
    ordered = pd.api.types.is_numeric_dtype(
        labels
    ) or pd.api.types.is_datetime64_any_dtype(labels)
    if pd.api.types.is_datetime64_any_dtype(labels):
        labels = pd.to_datetime(labels).dt.strftime("%Y-%m")
    data = pd.DataFrame({"label": labels.astype(str), "value": numbers.astype(float)})
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


def render(item: dict) -> None:
    with st.chat_message("user"):
        st.write(item["question"])
    with st.chat_message("assistant"):
        badge = BADGES.get(item["type"] or "", ":gray-background[No answer]")
        st.markdown(badge)
        st.write(item["answer"])
        rows = item["rows"]
        if rows is not None and not rows.empty:
            chart_if_useful(rows)
            st.dataframe(rows, use_container_width=True, hide_index=True)
        if item["sql"]:
            with st.expander(f"SQL (guardrail-checked, max {MAX_ROWS} rows)"):
                st.code(item["sql"], language="sql")
        if item["errors"]:
            with st.expander("Self-corrections"):
                for err in item["errors"]:
                    st.caption(err)


with st.sidebar:
    st.header("Velo Copilot")
    st.caption(
        "Plain-English questions about 360M NYC Uber and Lyft trips (Jan 2024 - Jun 2025), "
        "answered from approved gold tables."
    )
    st.subheader("Try an example")
    clicked = None
    for example in EXAMPLES:
        if st.button(example, use_container_width=True):
            clicked = example
    st.divider()
    st.markdown(
        "**Guardrails**  \n"
        "- Read-only `SELECT`, approved tables only  \n"
        f"- Max {MAX_ROWS} rows per query  \n"
        "- Out-of-range dates refused before the model runs  \n"
        "- Every question and query is audit-logged"
    )
    st.caption(f"Model: {LLM_ENDPOINT}")
    if st.button("Clear conversation"):
        st.session_state.history = []

st.title("Ask Velo")
st.caption(
    "Every answer shows the SQL it ran. Numbers come only from query results or certified findings."
)

if "history" not in st.session_state:
    st.session_state.history = []

for item in st.session_state.history:
    render(item)

question = st.chat_input("Ask a question about NYC ride-hail trips...") or clicked
if question:
    with st.spinner("Writing SQL, checking guardrails, running the query..."):
        result = ask(question)
    st.session_state.history.append(result)
    render(result)
