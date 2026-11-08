"""Velo Copilot web app, LIVE version (Streamlit). Run: streamlit run copilot_app.py

A thin UI over copilot.copilot.ask(): same semantic layer, guardrails and audit log.
Needs Databricks credentials, so it runs locally. Public demo: demo_app/streamlit_app.py
"""

from __future__ import annotations

import streamlit as st

from copilot.copilot import LLM_ENDPOINT, MAX_ROWS, ask
from copilot.ui import render

EXAMPLES = [
    "Which borough has the longest typical wait time?",
    "What is the typical wait time by hour of day?",
    "How many trips were there per month in 2025?",
    "What share of all trips were Uber trips?",
    "What effect did congestion pricing have on ride-hail demand?",
    "How accurate is our day-ahead demand forecast?",
    "How many yellow taxi trips were there in March 2025?",
]

st.set_page_config(page_title="Velo Copilot", page_icon=":taxi:", layout="wide")

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
    render(item, MAX_ROWS)

question = st.chat_input("Ask a question about NYC ride-hail trips...") or clicked
if question:
    with st.spinner("Writing SQL, checking guardrails, running the query..."):
        result = ask(question)
    st.session_state.history.append(result)
    render(result, MAX_ROWS)
