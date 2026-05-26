# MUW TL Dashboard — Feedback response (data + frontend)

## Summary

| Issue | Root cause | Fix |
|-------|------------|-----|
| **4 weeks not showing** | Base table only had `review_month`; HTML `IN_T4W` expects **weekly** buckets | Added `review_week_start`, `is_in_t4w`; aggregate table `MUW_RF_DQ_BY_RATING` with `period_key = 'IN_T4W'` |
| **DQ by rating — no 4W** | Same as above | Use `MUW_RF_DQ_BY_RATING` filtered to `period_key = 'IN_T4W'` |
| **Calibration heal — 3M empty** | 3M data exists at review level but was never pre-aggregated; mock `RAW_*` had no period slices | `is_in_3mo` on base + `MUW_RF_CALIBRATION_HEAL` |
| **Filters don't filter** | **Frontend**: `applyFilters()` never calls `buildModuleA/B/D`. **Data**: no `program_name` / `uw_type` in mock `RAW_A` | SQL now has `program_name`, `program_group`, `uw_type`; wire JS to rebuild charts |
| **Override matrix lookback** | Lookback used `sl_first_risk_review_time` (deduped SL account), not review date | Grades from `LOAN__C` on review + filter `review_complete_date` / `is_in_*` |
| **DQ by rating inversion** | Often **expected** for Bank Balance; possible bug if all factors invert | See § DQ inversion below |

---

## What we changed in SQL

### `TL Dashboard.sql` (base table)

All columns built in one `CREATE OR REPLACE TABLE` (no `ALTER` / `UPDATE`).

- `uw_type` — spec filter: Onboarding / Ongoing / HVC / AUW Monitoring / Pre-Approval
- `program_group` — Reactive / Proactive (separate from `uw_type`)
- `vantage_at_review` — `COALESCE(INDUS, LOAN__C.vantage_score_last_90_days__c)` in-query
- `review_week_start` — Wed-to-Wed weeks; `is_in_t4w` = last **4 complete** Wed weeks
- `is_in_3mo` / `is_in_6mo` / `is_in_12mo`, `is_mature_6mo`, `is_complete`, `is_ftd`
- `override_lookback_date`, `is_muw_decided_sl`, `is_second_look` on review grain

```sql
review_complete_date >= DATEADD('week', -3, DATE_TRUNC('week', CURRENT_DATE))
AND review_complete_date < DATEADD('day', 1, CURRENT_DATE)
```

### `TL_Dashboard_aggregates.sql` (run after base refresh)

| Table | Use |
|-------|-----|
| `MUW_RF_DQ_BY_RATING` | DQ% by week (T4W) or month (3M/6M/12M), all filter dims |
| `MUW_RF_CALIBRATION_HEAL` | `calibration_heal = dq_pct_high - dq_pct_low` |
| `MUW_RF_PORTFOLIO_COMP` | Module A — Vantage / OG / Revenue by program + uw_type |
| `MUW_RF_OVERRIDE_MATRIX` | Second Look auto vs UW grade by period |

---

## Do we have 3M / 4W data?

**Yes**, at the review grain — after you refresh the base table.

Run the coverage block in `TL_Dashboard_aggregates.sql` (section 0):

```sql
SELECT 'IN_T4W', COUNT(*), COUNT(DISTINCT review_week_start) ...
UNION ALL SELECT 'IN_3MO', ...
```

If `IN_3MO` row count is 0, the problem is upstream refresh, not the dashboard logic.

**Caveats:**

- `rf_residual` — sparse before ~Feb 2026 → heal/DQ for Residual in 3M may be blank
- `rf_quality_of_revenue` — weak before Sep 2024
- Small teams: calibration heal requires `review_count >= 5` per bucket (High and Low)

---

## DQ by rating “inversion”

Ratings are **risk severity** (Low=1, High=3) for trend averages — **except** how UWs use **Bank Balance**:

| Factor | Expected DQ trend (High vs Low) |
|--------|----------------------------------|
| Personal Credit, Industry, Debt Service, Quality of Revenue, Residual | **High rating → higher DQ%** |
| Bank Balance | Often **inverted in charts**: “High” frequently means **strong** balance (lower credit risk), so **lower DQ%** is correct |

**Check:**

```sql
SELECT risk_factor_name, risk_factor_rating,
       SUM(review_count), ROUND(SUM(dq_count)*100.0/NULLIF(SUM(review_count),0),2) AS dq_pct
FROM ANALYTICS.CREDIT.MUW_RF_DQ_BY_RATING
WHERE period_key = 'IN_3MO'
GROUP BY 1,2 ORDER BY 1,2;
```

Use `MUW_RF_CALIBRATION_HEAL.calibration_flag`:

- `Inverted` — High DQ% < Low DQ% (mis-calibration **or** bank-balance semantics)
- `Aligned` — High DQ% > Low DQ%

If **every** non–bank-balance factor shows `Inverted`, investigate `udr35` timing or whether ratings on the review match outcome window.

---

## Frontend (HTML) — still required

The Claude review on `MUW_RiskFactors_Dashboard_v3.html` is correct. SQL alone will not fix:

1. **`applyFilters()`** must read `flt-program`, `flt-uw-type`, TL, UW and call `buildModuleA()`, `buildModuleB1()`, `buildModuleB2()`, `buildModuleD()`, calibration builders.
2. **Data shape** — replace flat `RAW_A.IN_12MO` with nested or filtered queries, e.g.:

```javascript
// Example: portfolio after SQL aggregates exist
function portfolioSlice(periodKey, program, uwType) {
  return PORTFOLIO_DATA.filter(r =>
    r.period_key === periodKey &&
    (program === 'All' || r.program_name === program) &&
    (uwType === 'All' || r.uw_type === uwType)
  );
}
```

3. **T4W charts** — x-axis must use `period_label` / `review_week_label` from weekly aggregate, not `review_month`.

---

## Refresh order

1. `TL Dashboard.sql` → `MUW_RISK_FACTORS_DASHBOARD`
2. `ALTER` vantage (if still separate)
3. `TL_Dashboard_aggregates.sql` → four aggregate tables
4. Point HTML/API at aggregate tables (or embed JSON from those queries)

---

## Override matrix — data in table?

**Yes**, for `program_name = 'Second Look'` where both grades are populated on the review.

Validate:

```sql
SELECT period_key, SUM(override_count) AS cnt
FROM ANALYTICS.CREDIT.MUW_RF_OVERRIDE_MATRIX
GROUP BY 1 ORDER BY 1;
```

If `IN_T4W` is thin, Second Look volume in the last 4 weeks may be small — try `IN_3MO` or `IN_6MO`.
