# Custody & Funds Governance Demo

This dataset simulates an institutional **Custody & Funds** environment with built-in **Data Quality (DQ) issues** to demonstrate:
- Power Query ETL (pre-checks)
- SQL post-ETL DQ rules & KPIs
- Power BI (RLS) modeling (use Accounts.Region for role filtering)
- Attribute-level lineage documentation (to be created in Visio/Lucid)

## Files
- `Accounts.csv` — Investor master (PII present for masking demos)
- `Securities.csv` — Securities reference
- `Transactions.csv` — Trade/settlement facts (5,000 rows, intentional errors included)
- `Corporate_Actions.csv` — Dividends, splits, mergers
- `custody_funds_demo.sql` — DDL + DQ checks + KPI SQL

## Intentional DQ Issues
- ~1.5% **invalid account lengths** (not 10 digits)
- ~1% **invalid currency codes** (`XXX`, `US`, `EURO`, empty)
- ~1% **future trade dates**
- ~0.5% **duplicate Transaction_IDs**
- ~1% **negative Trade_Amount**
- Some **missing Settlement_Date**
- A few **broken foreign keys** (Security_ID)

## Suggested Pipeline
1. Load CSVs into **staging** schema.
2. Run DQ queries (see `custody_funds_demo.sql`).
3. Fix or flag violations; then insert clean records into **prod** schema.
4. Connect **Power BI** to prod; implement **RLS** by `Region`.
5. Create a Visio/Lucidchart **attribute-level lineage**:
   `CSV -> Power Query -> staging -> prod -> Power BI`

## Notes
- Current date pinned to 2025-09-12 for reproducibility.
- Replace/extend DQ rules as needed for interviews.