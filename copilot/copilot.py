"""Phase 11: Velo copilot - business questions in plain English, answered from the gold layer.

Flow: question -> SCOPE GUARD (code) -> LLM (semantic layer + rules) -> JSON plan
      -> SQL GUARDRAILS (code) -> SQL warehouse -> NO-DATA RULE (code)
      -> grounded answer from the result rows only -> audit log.
Lesson from eval v0: prompt rules are suggestions; anything that MUST happen lives in code.
LLM: Databricks Foundation Model API (VELO_LLM_ENDPOINT, default Llama 3.3 70B).
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import pandas as pd
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks.sql.exc import Error as DatabricksSqlError

from copilot.guardrails import MAX_ROWS, GuardrailError, validate_sql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEMANTIC_LAYER = PROJECT_ROOT / "copilot" / "semantic_layer.yml"
CACHE_DIR = PROJECT_ROOT / "data" / "copilot"
SCHEMA_CACHE = CACHE_DIR / "schema.json"
AUDIT_LOG = CACHE_DIR / "audit_log.jsonl"
HTTP_PATH = "/sql/1.0/warehouses/5cf4b93778132e09"
PROFILE = "velo"
LLM_ENDPOINT = os.getenv("VELO_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
MAX_PROMPT_ROWS = 50
DATA_START, DATA_END = (2024, 1), (2025, 6)  # (year, month) covered by the data

_MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name}
_MONTHS |= {abbr.lower(): i for i, abbr in enumerate(calendar.month_abbr) if abbr}
_PERIOD = re.compile(
    r"\b(?:("
    + "|".join(sorted(_MONTHS, key=len, reverse=True))
    + r")\s+)?((?:19|20)\d{2})\b"
)


def out_of_scope_period(question: str) -> str | None:
    """Deterministic scope guard: a month/year in the question outside the data range."""
    for month_txt, year_txt in _PERIOD.findall(question.lower()):
        year = int(year_txt)
        if month_txt:
            if not DATA_START <= (year, _MONTHS[month_txt]) <= DATA_END:
                return f"{month_txt.title()} {year}"
        elif not DATA_START[0] <= year <= DATA_END[0]:
            return str(year)
    return None


@lru_cache(maxsize=1)
def _workspace() -> WorkspaceClient:
    return WorkspaceClient(profile=PROFILE)


def run_sql(sql: str) -> pd.DataFrame:
    """Run SQL on the warehouse. Only ever called with guardrail-checked or system SQL."""
    from databricks import sql as dbsql

    cfg = Config(profile=PROFILE)
    with (
        dbsql.connect(
            server_hostname=cfg.host.replace("https://", ""),
            http_path=HTTP_PATH,
            credentials_provider=lambda: cfg.authenticate,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(sql)
        return cur.fetchall_arrow().to_pandas()


def load_layer() -> dict:
    return yaml.safe_load(SEMANTIC_LAYER.read_text(encoding="utf-8"))


def load_schema(tables: list[str]) -> dict[str, list[str]]:
    """Column names and types, read live from Unity Catalog (cached locally)."""
    if SCHEMA_CACHE.exists():
        cached = json.loads(SCHEMA_CACHE.read_text(encoding="utf-8"))
        if sorted(cached) == sorted(tables):
            return cached
    names = ", ".join(f"'{t}'" for t in tables)
    df = run_sql(
        "select concat(table_catalog, '.', table_schema, '.', table_name) as full_name, "
        "column_name, data_type from velo.information_schema.columns "
        f"where concat(table_catalog, '.', table_schema, '.', table_name) in ({names}) "
        "order by full_name, ordinal_position"
    )
    schema = {
        t: [
            f"{r.column_name} {r.data_type}"
            for r in df[df["full_name"] == t].itertuples()
        ]
        for t in tables
    }
    missing = [t for t, cols in schema.items() if not cols]
    if missing:
        raise SystemExit(f"Tables not found in Unity Catalog: {missing}")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_CACHE.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    return schema


def build_system_prompt(layer: dict, schema: dict[str, list[str]]) -> str:
    lines = [
        "You are Velo Copilot, a careful data analyst for an NYC ride-hailing marketplace.",
        f"Data: {layer['data_range']}",
        "",
        "## Approved tables (Databricks SQL; use these fully qualified names only)",
    ]
    for name, meta in layer["tables"].items():
        lines += [f"### {name}", meta["description"]]
        lines += [f"- note: {n}" for n in meta.get("notes", [])]
        lines.append("columns: " + ", ".join(schema[name]))
    lines += ["", "## Business glossary"]
    lines += [f"- {k}: {v}" for k, v in layer["glossary"].items()]
    lines += ["", "## Certified findings (cite these instead of recomputing them)"]
    lines += [f"- {k}: {v}" for k, v in layer["findings"].items()]
    if layer.get("verified_queries"):
        lines += [
            "",
            "## Verified queries (reviewed, correct patterns: adapt them and keep their metric logic)",
        ]
        for vq in layer["verified_queries"]:
            lines += [f"Q: {vq['question']}", f"SQL: {vq['sql']}"]
    lines += ["", "## Rules"] + [f"- {r}" for r in layer["rules"]]
    lines += [
        "",
        "## Reply with JSON only, exactly one of:",
        '{"type": "sql", "sql": "<one SELECT statement>", "explanation": "<one sentence>"}',
        '{"type": "finding", "finding_id": "<id from Certified findings>"}',
        '{"type": "refuse", "reason": "<why the approved data cannot answer>"}',
    ]
    return "\n".join(lines)


def chat(messages: list[dict], max_tokens: int = 900) -> str:
    resp = _workspace().serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[
            ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
            for m in messages
        ],
        max_tokens=max_tokens,
        temperature=0.0,
    )
    return resp.choices[0].message.content or ""


def parse_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("no JSON object found")
    return json.loads(match.group(0))


def has_no_data(rows: pd.DataFrame) -> bool:
    return rows.empty or bool(rows.isna().all().all())


def summarise(question: str, sql: str, rows: pd.DataFrame, data_range: str) -> str:
    if has_no_data(rows):  # deterministic: never let the model narrate an empty result
        return f"No data matches that question. The dataset covers {data_range}."
    shown = rows.head(MAX_PROMPT_ROWS)
    prompt = (
        f"Question: {question}\n\nSQL that was run:\n{sql}\n\n"
        f"Result ({len(rows)} rows, first {len(shown)} shown, CSV):\n{shown.to_csv(index=False)}\n"
        "Answer the question directly in 2-4 sentences for a business manager. "
        "Lead with the answer, then the key comparison if the result has several rows. "
        "Use ONLY numbers that appear in the result; round sensibly (e.g. 20.4 million, 74%). "
        "Mention a limitation only if it changes the answer. Do not speculate about causes."
    )
    return chat(
        [
            {
                "role": "system",
                "content": "You are a careful data analyst. You never invent numbers.",
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=400,
    ).strip()


def audit(result: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "endpoint": LLM_ENDPOINT,
        **{
            k: result[k]
            for k in (
                "question",
                "type",
                "sql",
                "answer",
                "attempts",
                "errors",
                "guard",
            )
        },
        "n_rows": None if result["rows"] is None else len(result["rows"]),
    }
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def ask(question: str) -> dict:
    layer = load_layer()
    result = {
        "question": question,
        "type": None,
        "sql": None,
        "answer": None,
        "rows": None,
        "attempts": 0,
        "errors": [],
        "guard": None,
    }

    # 1. Scope guard (code, before the model): periods outside the data are refused
    period = out_of_scope_period(question)
    if period:
        result.update(
            type="refuse",
            guard="scope",
            answer=f"I can't answer that: {period} is outside the data, which covers {layer['data_range']}.",
        )
        audit(result)
        return result

    allowed = set(layer["tables"])
    schema = load_schema(sorted(allowed))
    messages = [
        {"role": "system", "content": build_system_prompt(layer, schema)},
        {"role": "user", "content": question},
    ]

    for attempt in (1, 2):  # one self-correction retry
        result["attempts"] = attempt
        raw = chat(messages)
        error = None
        try:
            plan = parse_json(raw)
        except ValueError as exc:
            plan, error = {}, f"invalid JSON: {exc}"
        kind = plan.get("type")
        result["type"] = kind or result["type"]

        if error:
            pass
        elif kind == "finding":
            fid = plan.get("finding_id")
            if fid in layer["findings"]:
                result["answer"] = (
                    f"{layer['findings'][fid]} [certified finding: {fid}]"
                )
                break
            error = f"unknown finding_id {fid}"
        elif kind == "refuse":
            result["answer"] = (
                f"I can't answer that from the approved data: {plan.get('reason', '')}"
            )
            break
        elif kind == "sql":
            try:
                safe_sql = validate_sql(plan.get("sql", ""), allowed)
                rows = run_sql(safe_sql)
            except GuardrailError as exc:
                error = f"guardrail: {exc}"
            except DatabricksSqlError as exc:
                error = f"database: {str(exc)[:400]}"
            else:
                result.update(sql=safe_sql, rows=rows)
                result["answer"] = summarise(
                    question, safe_sql, rows, layer["data_range"]
                )
                break
        else:
            error = f"unknown reply type {kind!r}"

        result["errors"].append(error)
        messages += [
            {"role": "assistant", "content": raw},
            {
                "role": "user",
                "content": f"That failed: {error}. Fix it and reply with JSON only.",
            },
        ]

    if result["answer"] is None:
        last = result["errors"][-1] if result["errors"] else "unknown"
        result["answer"] = (
            f"Sorry, I couldn't produce a safe, working answer. Last error: {last}"
        )
    audit(result)
    return result


def print_result(result: dict) -> None:
    print(f"\nAnswer: {result['answer']}")
    if result["sql"]:
        print(f"\nSQL (guardrail-checked, max {MAX_ROWS} rows):\n{result['sql']}")
    if result["rows"] is not None and not result["rows"].empty:
        print(f"\nResult ({len(result['rows'])} rows, first 10):")
        print(result["rows"].head(10).to_string(index=False))
    for i, err in enumerate(result["errors"], start=1):
        print(f"\n(attempt {i} failed and was self-corrected: {err[:200]})")
    guard = f" | guard={result['guard']}" if result["guard"] else ""
    print(
        f"\n[type={result['type']} | attempts={result['attempts']}{guard} | model={LLM_ENDPOINT}]"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask Velo a business question")
    parser.add_argument("question", nargs="*")
    args = parser.parse_args()
    if args.question:
        print_result(ask(" ".join(args.question)))
        return
    while True:
        question = input("\nAsk Velo (blank to quit): ").strip()
        if not question:
            break
        print_result(ask(question))


if __name__ == "__main__":
    main()
