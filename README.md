# Multi-Report ETL Pipeline

Automated SQL pipeline generating 3 operational reports from contact management data. Reduces manual reporting time from hours to minutes through staged transformation architecture.

## Business Impact

- ✅ **3 automated reports** - Mortgage portfolio, restructuring pipeline, commercial promises
- ✅ **100% reproducible** - No manual data manipulation required
- ✅ **Data quality guaranteed** - Deduplication and validation at each layer
- ✅ **Flexible output** - Easy CSV export for business users

## Technical Stack

- **Database**: SQLite / MySQL compatible
- **Key Techniques**:
  - Staged transformation (5 clean views from raw data)
  - CTEs for complex filtering logic
  - Window functions (ROW_NUMBER) for deduplication
  - CASE-based standardization
  - LEFT JOINs preserving all accounts

## Pipeline Architecture

### Staging Layer (`01_staging_layer.sql`)

Creates 5 clean views from raw data:
- `clean_accounts`: Standardized account assignments
- `clean_contacts_primary`: Latest relevant contact per account
- `clean_contacts_field`: Latest field visit per account
- `clean_contacts_promise`: Latest payment promise per account
- `clean_contacts_restructure`: Latest restructure request per account

### Reporting Layer (`02_reporting_layer.sql`)

Generates 3 business reports by combining staged views:
1. **Mortgage Portfolio Report**: Accounts requiring field visits
2. **Restructuring Pipeline**: Accounts requesting debt restructure
3. **Commercial Promises**: Loans with active payment commitments

## Key Features

**Data Transformation**
- Date standardization (DD/MM/YYYY → YYYY-MM-DD)
- Channel classification with priority ordering
- Contact type normalization
- NULL handling with COALESCE

**Deduplication Strategy**
- ROW_NUMBER() with business-driven sorting
- Handles multiple operations per account
- Prioritizes by risk segment and product type

**Quality Validation**
- Uniqueness checks per account_id
- Record count validation
- Missing data handling ("NO_CONTACT" indicators)

## Usage

```sql
-- 1. Execute staging layer (creates 5 views)
source 01_staging_layer.sql;

-- 2. Verify staging views
SELECT COUNT(*) FROM clean_accounts;
SELECT COUNT(*) FROM clean_contacts_primary;

-- 3. Generate reports
source 02_reporting_layer.sql;

-- 4. Export to CSV (database-specific command)
-- Example for SQLite:
.mode csv
.output mortgage_portfolio_report.csv
-- Run Report 1 query
```

## Input Tables

Pipeline expects 2 raw staging tables:
- `stg_accounts`: Account assignments and balances
- `stg_activities`: Contact history and outcomes

## Output Reports

### Report 1: Mortgage Portfolio
- **Purpose**: Track mortgage accounts and field visit completion
- **Key Fields**: account_id, risk_segment, field_visit_completed
- **Filters**: Product = MORTGAGE, Division = RETAIL

### Report 2: Restructuring Pipeline
- **Purpose**: Identify accounts requesting debt restructuring
- **Key Fields**: account_id, restructure_request_date
- **Filters**: Has restructure request, Division = RETAIL

### Report 3: Commercial Promises
- **Purpose**: Monitor commercial loans with payment promises
- **Key Fields**: account_id, promise_date, promise_status
- **Filters**: Product = COMMERCIAL_LOAN, Has active promise

## Data Model

```
stg_accounts (1) ←→ (many) stg_activities
      ↓                        ↓
clean_accounts          5 contact views
      ↓                        ↓
      └────── JOIN ────────────┘
                ↓
         3 final reports
```

## Performance Considerations

- **Indexes**: Created on account_id for join performance
- **Views**: Real-time data (no materialization needed for current scale)
- **Deduplication**: Single-pass ROW_NUMBER more efficient than subqueries
- **Date Parsing**: SUBSTR + concatenation handles DD/MM/YYYY format

## Data Quality Checks

```sql
-- Check for NULL account_ids
SELECT COUNT(*) FROM clean_accounts WHERE account_id IS NULL;

-- Verify deduplication worked
SELECT account_id, COUNT(*) 
FROM clean_contacts_primary 
GROUP BY account_id 
HAVING COUNT(*) > 1;

-- Check date transformation success
SELECT activity_date 
FROM clean_contacts_primary 
WHERE activity_date IS NULL 
LIMIT 10;
```

## Requirements

- MySQL 8.0+ or SQLite 3.8+
- Raw tables: `stg_accounts` and `stg_activities` must exist
- Sufficient permissions to create views and indexes

## Author

**Ivan Jarpa**  
Senior Data Analyst | SQL · ETL · Business Intelligence  
- LinkedIn: [linkedin.com/in/biexcel](https://linkedin.com/in/biexcel)
- GitHub: [github.com/jarpaivan-wq](https://github.com/jarpaivan-wq)
- Other ETL pipeline: [Collection ETL Pipeline](https://github.com/jarpaivan-wq/collection-etl-pipeline)

## License

This project demonstrates ETL design patterns for portfolio purposes. Code structure and techniques are open for educational use. Sample data structure shown; actual business data removed for confidentiality.

**Copyright © 2026 Ivan Jarpa. All rights reserved.**

---

**Keywords**: ETL Pipeline, SQL, Multi-Report, Contact Management, Data Transformation, Window Functions, CTEs, Deduplication, Business Intelligence
