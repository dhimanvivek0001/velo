"""Phase 11.2: score the copilot on questions with known answers, and compare versions.

sql     -> result must contain the reference value (numbers within 1%; a share as 0.73 or 73)
finding -> answer must cite the expected certified finding id
refuse  -> no SQL may be executed (refused by the scope guard, the model, or the guardrails)

LLM output varies from run to run, so each case can be run several times (--repeats):
scores are pass RATES, and cases that pass only sometimes are reported as FLAKY.

Usage: python -m copilot.evaluate_copilot --label v4 --baseline v1 --repeats 3
"""

from __future__ import annotations

import argparse
import time
from decimal import Decimal

import pandas as pd
import yaml

from copilot.copilot import CACHE_DIR, LLM_ENDPOINT, PROJECT_ROOT, ask, run_sql

EVAL_SET = PROJECT_ROOT / "copilot" / "eval_set.yml"
TOLERANCE = 0.01


def _as_python(value):
    return float(value) if isinstance(value, Decimal) else value


def contains_value(expected, rows: pd.DataFrame | None) -> bool:
    if rows is None or rows.empty:
        return False
    for cell in rows.to_numpy().ravel().tolist():
        cell = _as_python(cell)
        if isinstance(expected, str):
            if (
                isinstance(cell, str)
                and cell.strip().lower() == expected.strip().lower()
            ):
                return True
            continue
        try:
            value = float(cell)
        except (TypeError, ValueError):
            continue
        for target in (float(expected), float(expected) * 100):
            if target == 0 and value == 0:
                return True
            if target != 0 and abs(value - target) / abs(target) <= TOLERANCE:
                return True
    return False


def grade(case: dict, result: dict, expected) -> tuple[bool, str]:
    if case["expect"] == "sql":
        ok = result["type"] == "sql" and contains_value(expected, result["rows"])
        return ok, f"expected {expected!r}"
    if case["expect"] == "finding":
        ok = f"[certified finding: {case['finding_id']}]" in (result["answer"] or "")
        return ok, f"expected finding {case['finding_id']}"
    return result["sql"] is None, "expected no SQL executed"


def score(df: pd.DataFrame) -> pd.DataFrame:
    s = df.groupby("category").agg(runs=("passed", "size"), passed=("passed", "sum"))
    s.loc["OVERALL"] = s.sum()
    s["score_pct"] = (s["passed"] / s["runs"] * 100).round(0)
    return s


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="latest", help="version label, e.g. v4")
    parser.add_argument("--baseline", default="v0", help="label to compare against")
    parser.add_argument("--repeats", type=int, default=1, help="runs per case")
    args = parser.parse_args()

    cases = yaml.safe_load(EVAL_SET.read_text(encoding="utf-8"))["cases"]
    expected = {  # reference answers, computed once
        c["id"]: _as_python(run_sql(c["reference_sql"]).iat[0, 0])
        for c in cases
        if c["expect"] == "sql"
    }
    rows = []
    print(
        f"Evaluating {len(cases)} cases x {args.repeats} runs with {LLM_ENDPOINT} "
        f"(label {args.label}) ...\n"
    )
    for run in range(1, args.repeats + 1):
        for case in cases:
            start = time.time()
            result = ask(case["question"])
            passed, detail = grade(case, result, expected.get(case["id"]))
            rows.append(
                {
                    "run": run,
                    "id": case["id"],
                    "category": case["category"],
                    "expect": case["expect"],
                    "got": result["type"],
                    "passed": passed,
                    "attempts": result["attempts"],
                    "guard": result.get("guard"),
                    "seconds": round(time.time() - start, 1),
                    "detail": detail,
                    "errors": " | ".join(result.get("errors", [])),
                    "answer": (result["answer"] or "")[:200],
                    "sql": result["sql"],
                }
            )
            mark = "PASS" if passed else "FAIL"
            guard = f" guard={result['guard']}" if result.get("guard") else ""
            print(
                f"run {run}  {mark}  {case['id']}  [{case['category']}]  got={result['type']}  "
                f"attempts={result['attempts']}{guard}"
            )

    df = pd.DataFrame(rows)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_DIR / f"eval_results_{args.label}.csv", index=False)

    current = score(df)
    print(f"\n=== Score: {args.label} ({args.repeats} run(s) per case, pass rate) ===")
    print(current.to_string())
    first_try = (df["attempts"] <= 1).mean() * 100
    print(
        f"\nFirst-try success: {first_try:.0f}% | median {df['seconds'].median():.0f}s per question"
    )

    per_case = df.groupby("id")["passed"].agg(["sum", "size"])
    flaky = per_case[(per_case["sum"] > 0) & (per_case["sum"] < per_case["size"])]
    broken = per_case[per_case["sum"] == 0]
    print(
        f"Stable passes: {int((per_case['sum'] == per_case['size']).sum())}/{len(per_case)} cases | "
        f"flaky: {list(flaky.index) or 'none'} | always failing: {list(broken.index) or 'none'}"
    )

    baseline_file = CACHE_DIR / f"eval_results_{args.baseline}.csv"
    if baseline_file.exists() and args.label != args.baseline:
        base = pd.read_csv(baseline_file)
        compare = pd.DataFrame(
            {
                f"{args.baseline}_pct": score(base)["score_pct"],
                f"{args.label}_pct": current["score_pct"],
            }
        )
        print(f"\n=== {args.baseline} vs {args.label} (pass rate %) ===")
        print(compare.to_string())
        b_first = (base["attempts"] <= 1).mean() * 100
        print(f"First-try success: {b_first:.0f}% -> {first_try:.0f}%")

    retried = df[df["errors"].fillna("") != ""]
    if not retried.empty:
        print("\n=== Self-corrections (what failed first) ===")
        for r in retried.itertuples():
            print(f"run {r.run} {r.id}: {r.errors[:200]}")
    failed = df[~df["passed"]]
    if not failed.empty:
        print("\n=== Failures (answer / SQL) ===")
        for r in failed.itertuples():
            print(f"\nrun {r.run} {r.id}: {r.answer}\n  SQL: {r.sql}")


if __name__ == "__main__":
    main()
