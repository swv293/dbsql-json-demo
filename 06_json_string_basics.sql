-- ============================================================================
-- PART 2A: JSON STRING BASICS — Extraction Fundamentals
-- Tables used: claims_json.claims_submissions, ops_json.operational_events
-- ============================================================================
USE CATALOG serverless_stable_swv01_catalog;

-- ============================================================================
-- SCENARIO 1: Extract top-level claim attributes using colon (:) syntax
-- KEY POINT: The colon operator is the primary way to extract from JSON strings
-- in Databricks SQL. It returns STRING values by default.
-- ============================================================================

SELECT
  claim_id,
  claim_json:claim_number         AS claim_number,
  claim_json:filing_code          AS filing_code,
  claim_json:release_of_info      AS release_of_info,
  claim_json:coordination_of_benefits AS cob_flag
FROM claims_json.claims_submissions
LIMIT 10;

-- ============================================================================
-- SCENARIO 2: Extract nested fields using dot notation
-- KEY POINT: Use dots to navigate into nested objects. This is intuitive
-- for anyone who has worked with object-oriented languages.
-- ============================================================================

SELECT
  claim_id,
  claim_json:subscriber.member_id       AS member_id,
  claim_json:subscriber.group_number    AS group_number,
  claim_json:subscriber.relationship_code AS relationship,
  claim_json:billing_provider.npi       AS billing_npi,
  claim_json:billing_provider.name      AS billing_provider_name
FROM claims_json.claims_submissions
LIMIT 10;

-- ============================================================================
-- SCENARIO 3: Case sensitivity — colon vs brackets
-- KEY POINT: Colon paths are CASE-INSENSITIVE. Bracket paths ['field'] are
-- CASE-SENSITIVE. This matters when working with data from external systems
-- where field names may vary in casing.
-- ============================================================================

-- These all return the same value (case insensitive)
SELECT
  claim_id,
  claim_json:subscriber.member_id    AS lowercase_path,
  claim_json:SUBSCRIBER.MEMBER_ID   AS uppercase_path,
  claim_json:Subscriber.Member_Id   AS mixed_path
FROM claims_json.claims_submissions
LIMIT 3;

-- Brackets are case sensitive — notice the NULL for wrong case
SELECT
  claim_id,
  claim_json:['subscriber']          AS correct_case,
  claim_json:['SUBSCRIBER']          AS wrong_case_returns_null
FROM claims_json.claims_submissions
LIMIT 3;

-- ============================================================================
-- SCENARIO 4: Cast extracted values to proper data types
-- KEY POINT: By default, colon extraction returns STRING. Use ::type to cast
-- to the proper type for calculations and comparisons.
-- ============================================================================

-- Without casting — values come back as quoted strings
-- Notice the values have quotes around them — they're JSON string literals
SELECT
  claim_id,
  claim_json:adjudication.paid_amount       AS paid_raw_string,
  claim_json:adjudication.allowed_amount    AS allowed_raw_string,
  claim_json:coordination_of_benefits       AS cob_raw_string,
  typeof(claim_json:adjudication.paid_amount) AS what_type_is_this
FROM claims_json.claims_submissions
LIMIT 5;

-- With casting — now we can do math and proper filtering
SELECT
  claim_id,
  claim_json:adjudication.paid_amount::double       AS paid_amount,
  claim_json:adjudication.allowed_amount::double     AS allowed_amount,
  claim_json:coordination_of_benefits::boolean       AS has_cob,
  round(claim_json:adjudication.paid_amount::double /
    NULLIF(claim_json:adjudication.allowed_amount::double, 0) * 100, 1) AS pct_of_allowed
FROM claims_json.claims_submissions
WHERE claim_json:adjudication.status::string = 'paid'
  AND claim_json:adjudication.paid_amount::double > 500
ORDER BY paid_amount DESC
LIMIT 10;

-- ============================================================================
-- SCENARIO 4B: Using backticks for special characters in field names
-- KEY POINT: If your JSON keys contain spaces or special characters,
-- use backticks to escape them. Common with EDI/clearinghouse data.
-- ============================================================================

-- Demonstration with operational events
SELECT
  event_id,
  event_json:status                AS event_status,
  event_json:priority              AS priority,
  event_json:assigned_to           AS assigned_to,
  event_json:sla.target_hours::int AS sla_target_hrs,
  event_json:sla.actual_hours::double AS sla_actual_hrs,
  event_json:sla.met::boolean      AS sla_met
FROM ops_json.operational_events
LIMIT 10
