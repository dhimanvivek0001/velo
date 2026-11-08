"""Velo Copilot, PUBLIC DEMO (Streamlit Community Cloud).

Replays REAL answers recorded from the live copilot (copilot/demo_answers.json).
No Databricks connection and no credentials: safe to share publicly.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from copilot.ui import render

DEMO_FILE = ROOT / "copilot" / "demo_answers.json"
REPO_URL = "https://github.com/dhimanvivek0001/velo"

st.set_page_config(page_title="Velo Copilot (demo)", page_icon=":taxi:", layout="wide")


@st.cache_data
def load_demo() -> dict:
    payload = json.loads(DEMO_FILE.read_text(encoding="utf-8"))
    for item in payload["items"]:
        rows = item.get("rows")
        item["rows"] = (
            pd.DataFrame(rows["data"], columns=rows["columns"]) if rows else None
        )
    return payload


demo = load_demo()
by_question = {item["question"].strip().lower(): item for item in demo["items"]}

with st.sidebar:
    st.header("Velo Copilot")
    st.caption(
        "Plain-English questions about 360M NYC Uber and Lyft trips (Jan 2024 - Jun 2025)."
    )
    st.subheader("Recorded questions")
    clicked = None
    for item in demo["items"]:
        if st.button(item["question"], use_container_width=True):
            clicked = item["question"]
    st.divider()
    st.markdown(
        "**Guardrails in the live version**  \n"
        "- Read-only `SELECT`, approved tables only  \n"
        "- Max 200 rows per query  \n"
        "- Out-of-range dates refused before the model runs  \n"
        "- Every question and query is audit-logged"
    )
    st.markdown(f"[Code, evaluation and architecture on GitHub]({REPO_URL})")
    if st.button("Clear conversation"):
        st.session_state.history = []

st.title("Ask Velo")
st.info(
    f"**Public demo:** real answers recorded from the live copilot on {demo['recorded_at']} "
    f"({demo['model']} on Databricks). Pick a question on the left. "
    f"The live version answers any question against the Databricks gold layer; see the [GitHub repo]({REPO_URL})."
)

if "history" not in st.session_state:
    st.session_state.history = []

for entry in st.session_state.history:
    render(entry)

typed = st.chat_input("Type one of the recorded questions, or click one on the left...")
question = typed or clicked
if question:
    item = by_question.get(question.strip().lower())
    if item is None:
        item = {
            "question": question,
            "type": "demo",
            "answer": (
                "This public demo replays recorded answers only, so it can run without database "
                "credentials. Pick one of the questions on the left. The live copilot answers any "
                f"question; the code is on GitHub: {REPO_URL}"
            ),
            "sql": None,
            "rows": None,
            "errors": [],
        }
    st.session_state.history.append(item)
    render(item)
