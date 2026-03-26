# Collections Headcount Projection Model

## Overview
This model calculates required headcount for Collections (Pre-CO) and ILR (Post-CO Internal Recovery) teams based on workload demand and agent capacity.

## Files Included

### SQL Files
- **`Headcount_Projection_Framework.sql`** - Contains 4 views:
  1. `v_headcount_productivity_benchmarks` - Agent productivity by team/month
  2. `v_headcount_inventory_trends` - Account inventory with projections
  3. `v_headcount_calculator` - Main headcount calculation
  4. `v_headcount_by_bucket` - Detailed breakdown by DPD bucket

### Excel Template Files (CSV)
- **`Collections_Headcount_Model_Assumptions.csv`** - Input parameters
- **`Collections_Headcount_Model_Calculator.csv`** - Capacity calculations
- **`Collections_Headcount_Model_Scenarios.csv`** - Scenario analysis

---

## How to Build the Excel Model

### Step 1: Create New Excel Workbook
Create a new Excel file named `Collections_Headcount_Model.xlsx`

### Step 2: Create Tabs
1. **Assumptions** - User inputs and parameters
2. **Data_Import** - Paste SQL query results here
3. **Capacity_Calculator** - Main calculation engine
4. **Scenario_Analysis** - What-if modeling
5. **Dashboard** (optional) - Summary charts

### Step 3: Set Up Assumptions Tab
| Parameter | Pre-CO Collections | ILR | Notes |
|-----------|-------------------|-----|-------|
| Contact Attempts/Month | 5 | 5 | Adjust based on strategy |
| Target Connect Rate % | 25% | 20% | From benchmarks |
| Target AHT (mins) | 8 | 10 | From benchmarks |
| Target Occupancy % | 75% | 75% | Target utilization |
| Working Days/Month | 22 | 22 | Business days |
| Productive Hrs/Day | 5.5 | 5.5 | From benchmarks |
| Buffer % | 15% | 15% | PTO, attrition, ramp |

### Step 4: Set Up Data Import Tab
Run these SQL queries and paste results:

```sql
-- Productivity Benchmarks
SELECT * FROM analytics.credit.v_headcount_productivity_benchmarks 
ORDER BY team_type, call_month DESC;

-- Inventory Trends
SELECT * FROM analytics.credit.v_headcount_inventory_trends 
ORDER BY dpd_bucket, week_end_date DESC;

-- Headcount Calculator
SELECT * FROM analytics.credit.v_headcount_calculator;

-- Detailed by Bucket
SELECT * FROM analytics.credit.v_headcount_by_bucket;
```

### Step 5: Build Capacity Calculator Tab

**Section A: Demand (Workload)**
| DPD Bucket | Team | Current Accounts | Projected Accounts | Contact Attempts | Required Dials | Work Hours |
|------------|------|------------------|-------------------|------------------|----------------|------------|
| 1-2 | Collections | =VLOOKUP | =VLOOKUP | =Assumptions!B2 | =C×E | =(F×AHT)/(60×Connect%) |

**Section B: Supply (Capacity)**
| Team | Current HC | Prod Hrs/Day | Working Days | Occupancy | Capacity/Agent | Team Capacity |
|------|------------|--------------|--------------|-----------|----------------|---------------|
| Collections | =COUNT | =Assumptions!B6 | 22 | =Assumptions!B4 | =C×D×E | =B×F |

**Section C: Gap Analysis**
| Team | Work Hours Needed | Capacity/Agent | HC Needed | HC with Buffer | Current HC | Gap |
|------|-------------------|----------------|-----------|----------------|------------|-----|
| Collections | =SUM(Demand) | =Supply!F | =B/C | =D×(1+Buffer%) | =Supply!B | =F-D |

### Step 6: Build Scenario Analysis Tab

Create 3 scenarios with adjustable multipliers:
- **Optimistic**: Volume ×0.90, Productivity ×1.10, Buffer 10%
- **Base**: Volume ×1.00, Productivity ×1.00, Buffer 15%
- **Pessimistic**: Volume ×1.15, Productivity ×0.90, Buffer 20%

---

## Key Formulas

### Required Work Hours
```
Work Hours = (Accounts × Contact Attempts × AHT) / (60 × Connect Rate)
```

### Capacity per Agent (Monthly)
```
Capacity = Productive Hrs/Day × Working Days × Occupancy %
```

### Headcount Needed
```
Headcount = Required Work Hours / Capacity per Agent
```

### Headcount with Buffer
```
Headcount Buffered = Headcount Needed × (1 + Buffer %)
```

### Gap
```
Gap = Current Headcount - Headcount Needed
Negative Gap = Need to hire
Positive Gap = Overstaffed
```

---

## Refresh Process

1. Run SQL views to get latest data
2. Paste into Data_Import tab
3. Capacity_Calculator will auto-update
4. Review Scenario_Analysis for planning

---

## Contact Policy Guidelines

| DPD Bucket | Recommended Attempts/Month | Rationale |
|------------|---------------------------|-----------|
| 1-2 | 3-5 | Early delinquency, lighter touch |
| 3-8 | 5-7 | Moderate delinquency, regular contact |
| 9-13 | 7-10 | High risk, intensive contact |
| ILR | 4-6 | Post-CO, recovery focus |

Adjust these based on your collection strategy and historical effectiveness.

---

## Assumptions & Limitations

1. **Productivity benchmarks** use 3-month rolling averages for stability
2. **Inventory projections** use simple trend extrapolation (replace with formal forecast if available)
3. **Contact policy** is configurable in Assumptions tab
4. **Buffer** accounts for PTO, attrition, training, and ramp-up time
5. **ILR data** filtered by `recovery_suggested_state = 'ILR'`
