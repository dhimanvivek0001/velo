"""Phase 8.4 - Event study: congestion pricing effect month by month.

Estimates the CBD/rest-of-NYC log ratio for each month relative to Nov 2024,
controlling for weekday. Pre-policy months should be ~0 (parallel trends);
the shape after Jan 2025 shows whether the effect is a step, a slide, or fading.

    python analysis/event_study.py
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from did_congestion import HAC, IMG_DIR, build_ratio, load_data

REFERENCE_MONTH = -2  # November 2024 (a normal, pre-policy, non-holiday month)
TERM = f"C(event_month, Treatment(reference={REFERENCE_MONTH}))"


def main() -> None:
    wide = build_ratio(load_data(refresh=False))
    sample = wide[~wide["in_holiday_window"]].copy()
    sample["event_month"] = (
        sample["date_day"].dt.year * 12 + sample["date_day"].dt.month - (2025 * 12 + 1)
    )

    model = smf.ols(f"log_ratio ~ {TERM} + C(day_name)", data=sample).fit(**HAC)

    rows = []
    for month in sorted(sample["event_month"].unique()):
        if month == REFERENCE_MONTH:
            rows.append((month, 0.0, 0.0, 0.0))
            continue
        name = f"{TERM}[T.{month}]"
        coef = model.params[name]
        low, high = model.conf_int().loc[name]
        rows.append((month, *((np.exp([coef, low, high]) - 1) * 100)))

    res = pd.DataFrame(rows, columns=["event_month", "effect_pct", "ci_low", "ci_high"])
    res["label"] = [
        (pd.Timestamp("2025-01-01") + pd.DateOffset(months=int(m))).strftime("%b %Y")
        for m in res["event_month"]
    ]

    print("\n=== 8.4 Event study: CBD vs rest of NYC, relative to Nov 2024 ===")
    for r in res.itertuples(index=False):
        tag = (
            "reference"
            if r.event_month == REFERENCE_MONTH
            else ("PRE " if r.event_month < 0 else "POST")
        )
        print(
            f"{r.label}  {tag:<9} {r.effect_pct:+6.2f}%   (95% CI {r.ci_low:+6.2f} to {r.ci_high:+6.2f})"
        )

    pre = res[(res["event_month"] < 0) & (res["event_month"] != REFERENCE_MONTH)][
        "effect_pct"
    ]
    post = res[res["event_month"] >= 0]["effect_pct"]
    print(
        f"\nAverage pre-policy months: {pre.mean():+.2f}%   |   Average post-policy months: {post.mean():+.2f}%"
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    colors = ["tab:blue" if m < 0 else "tab:red" for m in res["event_month"]]
    for i, r in enumerate(res.itertuples(index=False)):
        ax.errorbar(
            i,
            r.effect_pct,
            yerr=[[r.effect_pct - r.ci_low], [r.ci_high - r.effect_pct]],
            fmt="o",
            color=colors[i],
            capsize=4,
        )
    ax.axhline(0, color="grey", linewidth=0.8)
    jan_index = res.index[res["event_month"] == 0][0]
    ax.axvline(
        jan_index - 0.5,
        color="red",
        linestyle="--",
        label="Congestion pricing starts (5 Jan 2025)",
    )
    ax.set_xticks(range(len(res)))
    ax.set_xticklabels(res["label"], rotation=45, ha="right")
    ax.set_ylabel("CBD vs rest of NYC\n(% vs Nov 2024, 95% CI)")
    ax.set_title(
        "Event study: congestion pricing effect on CBD ride-hail demand, by month"
    )
    ax.legend(loc="lower left")
    fig.tight_layout()
    out = IMG_DIR / "did_event_study.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Chart saved: {out}")


if __name__ == "__main__":
    main()
