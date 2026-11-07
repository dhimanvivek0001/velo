"""The copilot's SQL guardrails must block anything that is not a safe, bounded read."""

from __future__ import annotations

import pytest

from copilot.guardrails import GuardrailError, validate_sql

ALLOWED = {"velo.gold.fct_zone_hour", "velo.gold.dim_zone"}


def test_select_gets_row_limit():
    out = validate_sql(
        "select borough, count(*) from velo.gold.dim_zone group by borough", ALLOWED
    )
    assert "LIMIT 200" in out.upper()


def test_large_limit_is_capped():
    out = validate_sql("select * from velo.gold.dim_zone limit 100000", ALLOWED)
    assert "LIMIT 200" in out.upper()
    assert "100000" not in out


def test_small_limit_is_kept():
    assert (
        "LIMIT 5"
        in validate_sql("select * from velo.gold.dim_zone limit 5", ALLOWED).upper()
    )


@pytest.mark.parametrize(
    "sql",
    [
        "delete from velo.gold.dim_zone",
        "drop table velo.gold.dim_zone",
        "insert into velo.gold.dim_zone values (1)",
        "update velo.gold.dim_zone set borough = 'x'",
        "select 1; drop table velo.gold.dim_zone",
    ],
)
def test_writes_and_multiple_statements_are_rejected(sql):
    with pytest.raises(GuardrailError):
        validate_sql(sql, ALLOWED)


def test_unapproved_table_is_rejected():
    with pytest.raises(GuardrailError, match="not allowed"):
        validate_sql("select * from velo.silver.hvfhv_trips", ALLOWED)


def test_unqualified_table_is_rejected():
    with pytest.raises(GuardrailError):
        validate_sql("select * from dim_zone", ALLOWED)


def test_cte_over_approved_table_is_allowed():
    sql = "with z as (select * from velo.gold.dim_zone) select count(*) as n from z"
    assert "LIMIT" in validate_sql(sql, ALLOWED).upper()


def test_table_function_is_rejected():
    with pytest.raises(GuardrailError):
        validate_sql("select * from read_files('s3://bucket/x')", ALLOWED)
