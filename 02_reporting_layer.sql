-- ============================================================
-- MULTI-REPORT ETL PIPELINE - REPORTING LAYER
-- ============================================================
-- Author: Ivan Jarpa
-- Purpose: Generate 3 operational reports from staged data
-- Output: CSV exports for business users
-- ============================================================

-- ============================================================
-- DATA QUALITY CHECKS
-- ============================================================
-- Verify uniqueness per account_id in each view

SELECT COUNT(*) FROM clean_contacts_primary;
SELECT DISTINCT(COUNT(*)) FROM clean_contacts_primary;

SELECT COUNT(*) FROM clean_contacts_field;
SELECT DISTINCT(COUNT(*)) FROM clean_contacts_field;

SELECT COUNT(*) FROM clean_contacts_promise;
SELECT DISTINCT(COUNT(*)) FROM clean_contacts_promise;

SELECT COUNT(*) FROM clean_contacts_restructure;
SELECT DISTINCT(COUNT(*)) FROM clean_contacts_restructure;

SELECT COUNT(*) FROM clean_accounts;
SELECT DISTINCT(COUNT(*)) FROM clean_accounts;

-- ============================================================
-- REPORT 1: MORTGAGE PORTFOLIO TRACKING
-- ============================================================
-- Purpose: Monitor mortgage accounts with field visit status
-- Output File: mortgage_portfolio_report.csv
-- Business Users: Portfolio managers, field operations
-- ============================================================

WITH filtered_accounts AS (
    SELECT
        a.account_id,
        a.account_checkdigit,
        a.agent_type,
        a.customer_name,
        a.product_type,
        a.risk_segment,
        a.outstanding_balance,
        a.agent_name,
        a.operation_number,
        COALESCE(c.phone_number, 'NO_CONTACT') AS contact_phone,
        COALESCE(c.notes, 'NO_CONTACT') AS activity_notes,
        COALESCE(
            CASE 
                WHEN c.contact_type = '01.PRIMARY' THEN 'PRIMARY'
                WHEN c.contact_type = '02.THIRD_PARTY' THEN 'THIRD_PARTY'
                WHEN c.contact_type = '03.NO_CONTACT' THEN 'NO_CONTACT'
                WHEN c.contact_type = '04.AUTO_DIALER' THEN 'AUTO_DIALER'
                ELSE 'NO_CONTACT'
            END, 
            'NO_CONTACT'
        ) AS contact_type,
        COALESCE(STRFTIME('%Y-%m-%d', c.activity_date), 'NO_CONTACT') AS last_activity_date,
        CASE
            WHEN f.account_id IS NOT NULL THEN 'YES'
            ELSE 'NO'	
        END AS field_visit_completed,
        a.business_division,
        a.customer_city,
        a.coverage_area
    FROM clean_accounts a
    LEFT JOIN clean_contacts_primary c ON a.account_id = c.account_id 
    LEFT JOIN clean_contacts_field f ON a.account_id = f.account_id
    WHERE a.product_type = 'MORTGAGE'
        AND a.business_division = 'RETAIL'
        AND a.containment_percentage = 0
),
deduplicated_accounts AS (
    SELECT
        account_id,
        account_checkdigit,
        agent_type,
        customer_name,
        product_type,
        risk_segment,
        outstanding_balance,
        agent_name,
        operation_number,
        contact_phone,
        activity_notes,
        contact_type,
        last_activity_date,
        field_visit_completed,
        business_division,
        customer_city,
        coverage_area,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY risk_segment DESC
        ) AS row_num
    FROM filtered_accounts
)
SELECT
    'COMPANY_NAME' AS company,
    account_id,
    account_checkdigit,
    agent_type,
    customer_name,
    product_type,
    risk_segment,
    outstanding_balance,
    agent_name,
    operation_number,
    contact_phone,
    activity_notes,
    contact_type,
    last_activity_date,
    field_visit_completed,
    business_division,
    customer_city,
    coverage_area
FROM deduplicated_accounts
WHERE row_num = 1;

-- ============================================================
-- REPORT 2: RESTRUCTURING PIPELINE
-- ============================================================
-- Purpose: Track accounts requesting debt restructuring
-- Output File: restructuring_pipeline_report.csv
-- Business Users: Restructuring team, operations managers
-- ============================================================

WITH filtered_accounts AS (
    SELECT
        a.account_id,
        a.account_checkdigit,
        a.agent_type,
        a.customer_name,
        a.product_type,
        a.risk_segment,
        a.outstanding_balance,
        a.agent_name,
        a.operation_number,
        COALESCE(c.phone_number, 'NO_CONTACT') AS contact_phone,
        COALESCE(c.notes, 'NO_CONTACT') AS activity_notes,
        COALESCE(
            CASE 
                WHEN c.contact_type = '01.PRIMARY' THEN 'PRIMARY'
                WHEN c.contact_type = '02.THIRD_PARTY' THEN 'THIRD_PARTY'
                WHEN c.contact_type = '03.NO_CONTACT' THEN 'NO_CONTACT'
                WHEN c.contact_type = '04.AUTO_DIALER' THEN 'AUTO_DIALER'
                ELSE 'NO_CONTACT'
            END,
            'NO_CONTACT'
        ) AS contact_type,
        COALESCE(STRFTIME('%Y-%m-%d', c.activity_date), 'NO_CONTACT') AS last_activity_date,
        CASE
            WHEN f.account_id IS NOT NULL THEN 'YES'
            ELSE 'NO'	
        END AS field_visit_completed,
        a.business_division,
        a.customer_city,
        a.coverage_area
    FROM clean_accounts a
    LEFT JOIN clean_contacts_primary c ON a.account_id = c.account_id 
    LEFT JOIN clean_contacts_field f ON a.account_id = f.account_id
    LEFT JOIN clean_contacts_restructure r ON a.account_id = r.account_id
    WHERE a.business_division = 'RETAIL'
        AND a.containment_percentage = 0
        AND r.contact_type = 'RESTRUCTURE'
),
deduplicated_accounts AS (
    SELECT
        account_id,
        account_checkdigit,
        agent_type,
        customer_name,
        product_type,
        risk_segment,
        outstanding_balance,
        agent_name,
        operation_number,
        contact_phone,
        activity_notes,
        contact_type,
        last_activity_date,
        field_visit_completed,
        business_division,
        customer_city,
        coverage_area,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY product_type ASC, risk_segment DESC
        ) AS row_num
    FROM filtered_accounts
)
SELECT
    'COMPANY_NAME' AS company,
    account_id,
    account_checkdigit,
    agent_type,
    customer_name,
    product_type,
    risk_segment,
    outstanding_balance,
    agent_name,
    operation_number,
    contact_phone,
    activity_notes,
    contact_type,
    last_activity_date,
    field_visit_completed,
    business_division,
    customer_city,
    coverage_area
FROM deduplicated_accounts
WHERE row_num = 1;

-- ============================================================
-- REPORT 3: COMMERCIAL LOANS WITH PAYMENT PROMISES
-- ============================================================
-- Purpose: Monitor commercial loans with active payment promises
-- Output File: commercial_promises_report.csv
-- Business Users: Commercial team, collections managers
-- ============================================================

WITH filtered_accounts AS (
    SELECT
        a.account_id,
        a.account_checkdigit,
        a.customer_name,
        a.agent_type,
        a.risk_segment,
        a.outstanding_balance,
        COALESCE(c.collection_channel, 'NO_CONTACT') AS collection_channel,
        COALESCE(
            CASE 
                WHEN c.contact_type = '01.PRIMARY' THEN 'PRIMARY'
                WHEN c.contact_type = '02.THIRD_PARTY' THEN 'THIRD_PARTY'
                WHEN c.contact_type = '03.NO_CONTACT' THEN 'NO_CONTACT'
                WHEN c.contact_type = '04.AUTO_DIALER' THEN 'AUTO_DIALER'
                ELSE 'NO_CONTACT'
            END,
            'NO_CONTACT'
        ) AS contact_type,
        CASE
            WHEN p.account_id IS NOT NULL THEN 'YES'
            ELSE 'NO'
        END AS payment_promise_active,	
        COALESCE(STRFTIME('%Y-%m-%d', p.next_activity_date), 'NO_PROMISE_DATE') AS promise_date,
        COALESCE(c.phone_number, 'NO_CONTACT') AS contact_phone,
        COALESCE(c.notes, 'NO_CONTACT') AS activity_notes
    FROM clean_accounts a
    LEFT JOIN clean_contacts_primary c ON a.account_id = c.account_id 
    LEFT JOIN clean_contacts_promise p ON a.account_id = p.account_id
    WHERE a.business_division = 'RETAIL'
        AND a.containment_percentage = 0
        AND a.product_type = 'COMMERCIAL_LOAN'
),
deduplicated_accounts AS (
    SELECT
        account_id,
        account_checkdigit,
        customer_name,
        agent_type,
        risk_segment,
        outstanding_balance,
        collection_channel,
        contact_type,
        payment_promise_active,	
        promise_date,
        contact_phone,
        activity_notes,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY risk_segment DESC
        ) AS row_num
    FROM filtered_accounts
)
SELECT
    'COMPANY_NAME' AS company,
    account_id,
    account_checkdigit,
    customer_name,
    agent_type,
    risk_segment,
    outstanding_balance,
    collection_channel,
    contact_type,
    payment_promise_active,	
    promise_date,
    contact_phone,
    activity_notes
FROM deduplicated_accounts
WHERE row_num = 1;

-- ============================================================
-- END OF REPORTING LAYER
-- ============================================================
