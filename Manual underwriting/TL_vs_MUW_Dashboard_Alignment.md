# TL Dashboard vs MUW Dashboard — Alignment check

## Are they the same thing?

**No.** They serve different purposes and can both be correct.

| | **MUW Dashboard** (`MUW Dashboard.sql`) | **TL Risk Factors** (`TL Dashboard.sql`) |
|--|----------------------------------------|------------------------------------------|
| **Primary output** | `MANUAL_UW_AGENT_V1`, weekly ops / yield / SLA | `MUW_RISK_FACTORS_DASHBOARD` |
| **Audience** | Ops / portfolio performance post-MUW | Team Lead calibration (risk factors + DQ) |
| **Grain** | Review + loan performance over time | One row per review |
| **Risk factors** | Not central | 6 RF fields + numeric 1–3 |
| **DQ** | Loan-level DPD roll rates | `UDR35` post-review on review date |
| **Week logic** | `week_end_date` (Tue-oriented ops metric in `auw_op_metrics_cte`) | `review_week_start` Wed-to-Wed |

## Date anchor — spec assumption

**Confirmed:** TL dashboard uses **`REVIEW_COMPLETE_DATE`**, with OB AUW fallback to `auw_pre_doc_review_complete_time__c` — same coalesce pattern as `review_complete_date_coalesce` in MUW agent views.

Monthly charts use `REVIEW_MONTH = DATE_TRUNC('month', review_complete_date)`.

Weekly aggregates use **`REVIEW_WEEK_START`** from the same date (Wed-to-Wed), not a different event timestamp.

## Program names — all six present

From latest export (~30k reviews):

| PROGRAM_NAME | Approx volume |
|--------------|---------------|
| Pre-Approval | ~15.5k |
| Second Look | ~5.9k |
| AUW Monitoring | ~4.3k |
| OB AUW | ~2.2k |
| HVC | ~1.1k |
| OG AUW | ~1.0k |

MUW ops SQL sometimes labels **Pre-Approvals** / **SL** — same `recordtypeid`, different display names. TL dashboard uses spec names above; filter mapping is in `UW_TYPE` (Onboarding / Ongoing / etc.).

## Is TL Dashboard “correct”?

**Yes for the Risk Factors TL spec**, with known caveats already documented:

1. **DQ right-censoring** — recent weeks need `is_seasoned_90d` or `is_mature_6mo` on DQ charts (`WEEKLY_DQ` enforces 90d).
2. **Vantage on Pre-Approval** — use `vantage_at_review` (SF fallback).
3. **Industry factor** — rubric may not be monotonic (spec calls this out).
4. **MUW Dashboard week ≠ TL week** — do not join ops `week_end_date` to `review_week_start` without conversion.

## New objects for Zac’s ask

| Object | Purpose |
|--------|---------|
| `ANALYTICS.CREDIT.MUW_RF_WEEKLY_BASE` | Eligible reviews (helper, optional to keep) |
| `ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_TREND` | B.1 / C.1 |
| `ANALYTICS.CREDIT.MUW_RISK_FACTORS_WEEKLY_DQ` | B.2 / B.3 / C.2 / C.3 |

**Refresh order (daily):**

1. `TL Dashboard.sql` → `MUW_RISK_FACTORS_DASHBOARD`
2. `TL_Dashboard_Weekly_Aggregates.sql` → weekly tables

## Four-row rollup pattern

| `rollup_type` | `uw_name` | `program_name` |
|---------------|-----------|----------------|
| `UW_PROGRAM` | actual UW | e.g. `Pre-Approval` |
| `UW_ALL` | actual UW | `ALL` |
| `TEAM_PROGRAM` | `''` (empty) | program |
| `TEAM_ALL` | `''` | `ALL` |

Dashboard **UW Type / program filter**: query `program_name = '<pick>'` OR `program_name = 'ALL'` without re-aggregating.
