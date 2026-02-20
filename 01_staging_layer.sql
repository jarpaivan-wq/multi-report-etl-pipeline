-- ============================================================
-- MULTI-REPORT ETL PIPELINE - STAGING LAYER
-- ============================================================
-- Author: Ivan Jarpa
-- Purpose: Preprocessing layer for multiple operational reports
-- Database: SQLite / MySQL compatible
-- ============================================================

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

CREATE INDEX idx_stg_accounts_id ON stg_accounts(account_id);
CREATE INDEX idx_stg_activities_id ON stg_activities(account_id);

-- ============================================================
-- VIEW 1: ACCOUNT ASSIGNMENTS
-- ============================================================
-- Purpose: Standardize account assignment data
-- Business Use: Portfolio tracking and agent assignment
-- ============================================================

DROP VIEW IF EXISTS clean_accounts;

CREATE VIEW clean_accounts AS
SELECT 
    collection_company,
    account_id,
    account_checkdigit,
    agent_type,
    customer_name,
    product_type,
    risk_segment,
    outstanding_balance,
    agent_name,
    operation_number,
    containment_percentage,
    business_division,
    customer_city,
    CASE 
        WHEN customer_city IN('METRO_AREA_1', 'METRO_AREA_2', 'METRO_AREA_3') THEN 'YES'
        ELSE 'NO'
    END AS coverage_area
FROM stg_accounts;

-- ============================================================
-- VIEW 2: PRIMARY CONTACT HISTORY
-- ============================================================
-- Purpose: Latest relevant contact per account (all channels)
-- Key Features:
--   - Date standardization (DD/MM/YYYY → YYYY-MM-DD)
--   - Channel classification with priority ordering
--   - Contact type standardization
--   - ROW_NUMBER to get latest relevant contact
-- ============================================================

DROP VIEW IF EXISTS clean_contacts_primary;

CREATE VIEW clean_contacts_primary AS
WITH contact_preprocessing AS (
    SELECT
        account_id,
        -- Date transformation: DD/MM/YYYY → YYYY-MM-DD
        DATE(
            SUBSTR(activity_date,7,4) || '-' ||
            SUBSTR(activity_date,4,2) || '-' ||
            SUBSTR(activity_date,1,2)
        ) AS activity_date,
        activity_time,
        DATE(
            SUBSTR(next_activity_date,7,4) || '-' ||
            SUBSTR(next_activity_date,4,2) || '-' ||
            SUBSTR(next_activity_date,1,2)
        ) AS next_activity_date,
        -- Channel classification (priority prefix for sorting)
        CASE
            WHEN collection_channel = 'PHONE' THEN '01.PHONE'
            WHEN collection_channel = 'FIELD' THEN '02.FIELD'
            WHEN collection_channel = 'MESSAGING' THEN '03.MESSAGING'
            WHEN collection_channel = 'EMAIL' THEN '04.EMAIL'
            WHEN collection_channel = 'AGENT_BANK' THEN '05.AGENT_BANK'
            ELSE 'UNCLASSIFIED_CHANNEL'
        END AS collection_channel,
        -- Contact type standardization
        CASE 
            WHEN contact_type = 'PRIMARY' THEN '01.PRIMARY'
            WHEN contact_type IN('THIRD_PARTY', 'RELATIVE') THEN '02.THIRD_PARTY'
            WHEN contact_type = 'NO_CONTACT' AND agent_name = 'AUTO_DIALER' THEN '04.AUTO_DIALER'
            WHEN contact_type = 'NO_CONTACT' THEN '03.NO_CONTACT'
            WHEN contact_type IN('GUARANTOR', 'GUARANTOR_NO_CONTACT') THEN 'GUARANTOR'
            WHEN collection_channel = 'FIELD' THEN 'FIELD'
            WHEN collection_channel = 'MESSAGING' AND contact_type = 'PRIMARY' THEN '01.PRIMARY'
            WHEN collection_channel = 'MESSAGING' AND contact_type IN('THIRD_PARTY','RELATIVE') THEN '02.THIRD_PARTY'
            WHEN collection_channel = 'MESSAGING' AND contact_type = 'NO_CONTACT' THEN '03.NO_CONTACT'
            WHEN collection_channel IN('EMAIL', 'AGENT_BANK') THEN 'EMAIL'
            ELSE 'UNCLASSIFIED_CONTACT'
        END AS contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name
    FROM stg_activities
),
ranked_contacts AS (
    SELECT
        account_id,
        activity_date,
        activity_time,
        next_activity_date,
        collection_channel,
        contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY collection_channel ASC, contact_type ASC, activity_date DESC
        ) AS row_num
    FROM contact_preprocessing
)
SELECT
    account_id,
    activity_date,
    activity_time,
    next_activity_date,
    collection_channel,
    contact_type,
    contact_outcome,
    non_payment_reason,
    contact_location,
    next_action,
    notes,
    phone_number,
    department,
    agent_name
FROM ranked_contacts
WHERE row_num = 1;

-- ============================================================
-- VIEW 3: FIELD VISIT HISTORY
-- ============================================================
-- Purpose: Latest field visit per account
-- Business Use: Field operations tracking and effectiveness
-- ============================================================

DROP VIEW IF EXISTS clean_contacts_field;

CREATE VIEW clean_contacts_field AS
WITH contact_preprocessing AS (
    SELECT
        account_id,
        DATE(
            SUBSTR(activity_date,7,4) || '-' ||
            SUBSTR(activity_date,4,2) || '-' ||
            SUBSTR(activity_date,1,2)
        ) AS activity_date,
        activity_time,
        DATE(
            SUBSTR(next_activity_date,7,4) || '-' ||
            SUBSTR(next_activity_date,4,2) || '-' ||
            SUBSTR(next_activity_date,1,2)
        ) AS next_activity_date,
        '02.FIELD' AS collection_channel,
        CASE 
            WHEN contact_type = 'PRIMARY' THEN '01.PRIMARY'
            WHEN contact_type IN('THIRD_PARTY', 'RELATIVE') THEN '02.THIRD_PARTY'
            WHEN contact_type = 'NO_CONTACT' AND agent_name = 'AUTO_DIALER' THEN '04.AUTO_DIALER'
            WHEN contact_type = 'NO_CONTACT' THEN '03.NO_CONTACT'
            WHEN contact_type IN('GUARANTOR', 'GUARANTOR_NO_CONTACT') THEN 'GUARANTOR'
            ELSE 'UNCLASSIFIED_CONTACT'
        END AS contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name
    FROM stg_activities
    WHERE collection_channel = 'FIELD'
),
ranked_contacts AS (
    SELECT
        account_id,
        activity_date,
        activity_time,
        next_activity_date,
        collection_channel,
        contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY contact_type ASC, activity_date DESC
        ) AS row_num
    FROM contact_preprocessing
)
SELECT
    account_id,
    activity_date,
    activity_time,
    next_activity_date,
    collection_channel,
    contact_type,
    contact_outcome,
    non_payment_reason,
    contact_location,
    next_action,
    notes,
    phone_number,
    department,
    agent_name
FROM ranked_contacts
WHERE row_num = 1;

-- ============================================================
-- VIEW 4: PAYMENT PROMISES
-- ============================================================
-- Purpose: Latest payment promise per account
-- Business Use: Promise tracking and follow-up
-- ============================================================

DROP VIEW IF EXISTS clean_contacts_promise;

CREATE VIEW clean_contacts_promise AS
WITH contact_preprocessing AS (
    SELECT
        account_id,
        DATE(
            SUBSTR(activity_date,7,4) || '-' ||
            SUBSTR(activity_date,4,2) || '-' ||
            SUBSTR(activity_date,1,2)
        ) AS activity_date,
        activity_time,
        DATE(
            SUBSTR(next_activity_date,7,4) || '-' ||
            SUBSTR(next_activity_date,4,2) || '-' ||
            SUBSTR(next_activity_date,1,2)
        ) AS next_activity_date,
        CASE
            WHEN collection_channel = 'PHONE' THEN '01.PHONE'
            WHEN collection_channel = 'FIELD' THEN '02.FIELD'
            WHEN collection_channel = 'MESSAGING' THEN '03.MESSAGING'
            WHEN collection_channel = 'EMAIL' THEN '04.EMAIL'
            WHEN collection_channel = 'AGENT_BANK' THEN '05.AGENT_BANK'
            ELSE 'UNCLASSIFIED_CHANNEL'
        END AS collection_channel,
        'PROMISE' AS contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name
    FROM stg_activities
    WHERE contact_outcome = 'PAYMENT_PROMISE'
),
ranked_contacts AS (
    SELECT
        account_id,
        activity_date,
        activity_time,
        next_activity_date,
        collection_channel,
        contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY activity_date DESC, activity_time DESC
        ) AS row_num
    FROM contact_preprocessing
)
SELECT
    account_id,
    activity_date,
    activity_time,
    next_activity_date,
    collection_channel,
    contact_type,
    contact_outcome,
    non_payment_reason,
    contact_location,
    next_action,
    notes,
    phone_number,
    department,
    agent_name
FROM ranked_contacts
WHERE row_num = 1;

-- ============================================================
-- VIEW 5: RESTRUCTURING REQUESTS
-- ============================================================
-- Purpose: Latest restructuring request per account
-- Business Use: Debt restructuring pipeline tracking
-- ============================================================

DROP VIEW IF EXISTS clean_contacts_restructure;

CREATE VIEW clean_contacts_restructure AS
WITH contact_preprocessing AS (
    SELECT
        account_id,
        DATE(
            SUBSTR(activity_date,7,4) || '-' ||
            SUBSTR(activity_date,4,2) || '-' ||
            SUBSTR(activity_date,1,2)
        ) AS activity_date,
        activity_time,
        DATE(
            SUBSTR(next_activity_date,7,4) || '-' ||
            SUBSTR(next_activity_date,4,2) || '-' ||
            SUBSTR(next_activity_date,1,2)
        ) AS next_activity_date,
        CASE
            WHEN collection_channel = 'PHONE' THEN '01.PHONE'
            WHEN collection_channel = 'FIELD' THEN '02.FIELD'
            WHEN collection_channel = 'MESSAGING' THEN '03.MESSAGING'
            WHEN collection_channel = 'EMAIL' THEN '04.EMAIL'
            WHEN collection_channel = 'AGENT_BANK' THEN '05.AGENT_BANK'
            ELSE 'UNCLASSIFIED_CHANNEL'
        END AS collection_channel,
        'RESTRUCTURE' AS contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name
    FROM stg_activities
    WHERE contact_outcome = 'RESTRUCTURE_REQUEST'
),
ranked_contacts AS (
    SELECT
        account_id,
        activity_date,
        activity_time,
        next_activity_date,
        collection_channel,
        contact_type,
        contact_outcome,
        non_payment_reason,
        contact_location,
        next_action,
        notes,
        phone_number,
        department,
        agent_name,
        ROW_NUMBER() OVER(
            PARTITION BY account_id 
            ORDER BY activity_date DESC, activity_time DESC
        ) AS row_num
    FROM contact_preprocessing
)
SELECT
    account_id,
    activity_date,
    activity_time,
    next_activity_date,
    collection_channel,
    contact_type,
    contact_outcome,
    non_payment_reason,
    contact_location,
    next_action,
    notes,
    phone_number,
    department,
    agent_name
FROM ranked_contacts
WHERE row_num = 1;

-- ============================================================
-- END OF STAGING LAYER
-- ============================================================
