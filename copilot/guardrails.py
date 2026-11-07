"""Guardrails for LLM-generated SQL. The SQL is PARSED and checked before anything runs:
one statement, read-only SELECT, approved tables only, row LIMIT enforced."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

MAX_ROWS = 200


class GuardrailError(ValueError):
    """Raised when generated SQL breaks a safety rule."""


def validate_sql(sql: str, allowed_tables: set[str], max_rows: int = MAX_ROWS) -> str:
    """Return a safe, LIMIT-capped version of `sql`, or raise GuardrailError."""
    sql = (sql or "").strip().rstrip(";").strip()
    if not sql:
        raise GuardrailError("Empty SQL")
    try:
        statements = [s for s in sqlglot.parse(sql, read="databricks") if s is not None]
    except ParseError as exc:
        raise GuardrailError(f"SQL does not parse: {exc}") from exc
    if len(statements) != 1:
        raise GuardrailError(f"Exactly one statement is allowed, got {len(statements)}")

    root = statements[0]
    if not isinstance(root, exp.Query) or root.find(exp.Command):
        raise GuardrailError(f"Only SELECT queries are allowed, got {root.key.upper()}")

    cte_names = {cte.alias_or_name.lower() for cte in root.find_all(exp.CTE)}
    allowed = {t.lower() for t in allowed_tables}
    for table in root.find_all(exp.Table):
        parts = [p for p in (table.catalog, table.db, table.name) if p]
        full = ".".join(parts).lower()
        if len(parts) == 1 and full in cte_names:
            continue  # reference to a CTE defined in the same query
        if full not in allowed:
            raise GuardrailError(
                f"Table not allowed: {full or table.sql()}. "
                f"Use only fully qualified approved tables: {sorted(allowed)}"
            )

    limit = root.args.get("limit")
    value = limit.expression if limit is not None else None
    if not (
        isinstance(value, exp.Literal) and value.is_int and int(value.this) <= max_rows
    ):
        root = root.limit(max_rows)
    return root.sql(dialect="databricks")
