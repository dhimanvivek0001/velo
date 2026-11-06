# Memo: Did congestion pricing reduce ride-hail demand in Manhattan?

**Project:** Velo — NYC ride-hailing marketplace analytics
**Data:** NYC TLC high-volume for-hire trips (Uber & Lyft), Jan 2024 – Jun 2025, 360 million trips
**Policy:** Manhattan Central Business District (CBD) congestion pricing, effective 5 January 2025

---

## Bottom line

**Congestion pricing reduced ride-hail pickups inside the charging zone by about 6.8% (95% CI: 4.9–8.7%).** That's roughly **12,300 fewer pickups per day**, or about **2.1 million fewer trips** in the first six months. The effect **deepened over time**, from about −5% in January to about −9% by June, consistent with riders gradually adapting.

The result holds under every check: seasonal controls, a same-month year-over-year comparison, placebo dates and an alternative comparison group.

---

## 1. What we measured

- **Treated area:** the 38 TLC pickup zones inside the CBD, identified **from the data**: in these zones, 96–99.5% of pickups paid the CBD fee after 5 January (a trip starting inside the zone is always charged). All 38 lie south of 60th Street.
- **Comparison area:** the rest of New York City (excluding Newark Airport and unknown zones).
- **Outcome:** each day, the CBD's demand **relative to** the rest of the city, `ln(CBD pickups) − ln(rest-of-NYC pickups)`. Anything that hits the whole city equally (weather, seasons, prices, app promotions) cancels out.
- **Method:** difference-in-differences on 18 months of daily data, with weekday and month-of-year controls and autocorrelation-robust (Newey-West) standard errors. Federal holidays and the Christmas–New Year weeks are excluded.

## 2. Results

| Model | Controls | Effect on CBD pickups | 95% CI |
|---|---|---|---|
| A. Simple | Weekday | −6.6% | −8.6% to −4.5% |
| B. Seasonal | + month of year | −6.8% | −8.7% to −4.8% |
| **C. Year-over-year** | **Jan–Jun 2025 vs Jan–Jun 2024** | **−6.8%** | **−8.7% to −4.9%** |

All three are significant at p < 0.0001. Adding seasonal controls doesn't shrink the effect, so **seasonality doesn't explain it.**

**Same month, one year apart** (CBD share of city demand, 2025 vs 2024):

| Jan | Feb | Mar | Apr | May | Jun |
|---|---|---|---|---|---|
| −5.2% | −6.5% | −6.4% | −5.4% | −8.6% | −9.1% |

Every month is lower than the same month a year earlier, and the gap widens through the spring.

**In trips:** after 5 January the CBD averaged 169,244 pickups per non-holiday day; without the fee the model implies about 181,537, a gap of **about 12,300 pickups per day**.

## 3. Why we trust it

| Check | Result |
|---|---|
| **Parallel trends:** did the CBD and the rest of NYC move together before the policy? | Yes. Over the full year before, the CBD/rest ratio drifted −0.13% per month (95% CI −0.67 to +0.41) |
| **Placebo dates:** does the method invent effects when nothing happened? | No. Fake start dates of 7 Apr 2024 (−1.4%, p = 0.44) and 6 Oct 2024 (+3.2%, p = 0.15) show no significant effect |
| **Donut:** are partly-charged boundary zones distorting the comparison? | Removing the 7 boundary zones (around 60th Street) gives −7.0%, slightly larger, so they were diluting the estimate |
| **Data quality** | 360,465,639 raw trips reconcile exactly to clean + quarantined rows; 30 automated tests pass daily |

## 4. What it means

**For the city:** the policy did what it was designed to do for this mode: fewer for-hire vehicle trips starting in the congestion zone, with demand still growing elsewhere in the city. The deepening effect suggests the full impact takes months to appear, so early evaluations understate it.

**For Uber and Lyft:** roughly 1 in 3 ride-hail trips paid the fee once it started, and pickups in the zone are about 7% below where they would otherwise be. Strategies worth testing: dynamic pricing that absorbs part of the fee at off-peak hours, promoting shared rides into the zone, and rebalancing drivers towards the outer boroughs, where demand kept growing.

## 5. Limitations

- **Pickups only.** The outcome counts trips *starting* in the CBD. Trips from elsewhere *into* the CBD also pay the fee, and they sit in the comparison group. If those fell too, the comparison group is partly affected, which makes our estimate **conservative** (a lower bound on the effect inside the zone).
- **Ride-hail only.** The data covers Uber and Lyft, not taxis, private cars or transit, so it says nothing about total traffic or overall welfare.
- **Six months after.** The effect was still growing in June 2025, so the long-run effect may be larger.
- **One city, one policy.** No other region is used as a comparison, so a CBD-specific shock coinciding with January 2025 can't be fully ruled out, although the placebo and year-over-year tests make this unlikely.

---

*Method and code: `analysis/did_congestion.py`, `analysis/event_study.py`, `analysis/robustness.py`. Treatment definition: `dbt/velo/models/causal/`.*
