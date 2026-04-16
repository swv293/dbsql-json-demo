-- ============================================================================
-- PART 2C: JSON FUNCTIONS TOOLKIT — from_json, get_json_object, json_tuple,
--          schema_of_json, and NULL handling
-- ============================================================================
USE CATALOG serverless_stable_swv01_catalog;

-- ============================================================================
-- SCENARIO 9: from_json — Parse JSON into a proper Spark struct
-- KEY POINT: from_json converts a JSON string into a structured type.
-- Once parsed, you can use normal .field syntax (no more quoting issues).
-- Best for when you'll access many fields from the same JSON object.
-- ============================================================================

-- Parse adjudication into a struct
SELECT
  claim_id,
  adj.status          AS adj_status,
  adj.paid_amount     AS paid_amount,
  adj.allowed_amount  AS allowed_amount,
  adj.denial_codes    AS denial_codes,
  adj.remark_codes    AS remark_codes
FROM (
  SELECT
    claim_id,
    from_json(
      claim_json:adjudication,
      'STRUCT<status:STRING, paid_amount:DOUBLE, allowed_amount:DOUBLE, denial_codes:ARRAY<STRING>, remark_codes:ARRAY<STRING>>'
    ) AS adj
  FROM claims_json.claims_submissions
)
WHERE adj.status = 'denied'
LIMIT 10;

-- Parse the full subscriber object
SELECT
  claim_id,
  sub.member_id,
  sub.group_number,
  sub.relationship_code
FROM (
  SELECT
    claim_id,
    from_json(
      claim_json:subscriber,
      'STRUCT<member_id:STRING, group_number:STRING, relationship_code:STRING>'
    ) AS sub
  FROM claims_json.claims_submissions
)
LIMIT 10;

-- ============================================================================
-- SCENARIO 10: get_json_object — Legacy-style JSON path extraction
-- KEY POINT: If you're coming from Oracle or other databases, get_json_object
-- uses JSONPath-like syntax ($.field) that may feel familiar.
-- It's functionally similar to colon syntax but uses a string path.
-- ============================================================================

-- Compare colon syntax vs get_json_object — they do the same thing
SELECT
  claim_id,
  -- Colon syntax (Databricks native)
  claim_json:subscriber.member_id                             AS colon_member_id,
  -- get_json_object (compatible with other SQL engines)
  get_json_object(claim_json, '$.subscriber.member_id')       AS gjso_member_id,
  -- Both work for nested paths
  claim_json:billing_provider.npi                              AS colon_npi,
  get_json_object(claim_json, '$.billing_provider.npi')        AS gjso_npi,
  -- Array access
  claim_json:service_lines[0].procedure_code                   AS colon_first_cpt,
  get_json_object(claim_json, '$.service_lines[0].procedure_code') AS gjso_first_cpt
FROM claims_json.claims_submissions
LIMIT 5;

-- ============================================================================
-- SCENARIO 11: json_tuple — Extract multiple fields in one pass
-- KEY POINT: json_tuple is efficient when you need several top-level fields
-- at once. It avoids re-parsing the JSON for each field.
-- Works with LATERAL VIEW.
-- ============================================================================

-- Extract multiple fields from operational events in one shot
SELECT
  e.event_id,
  e.event_type,
  jt.status,
  jt.priority,
  jt.assigned_to
FROM ops_json.operational_events e
LATERAL VIEW json_tuple(
  e.event_json, 'status', 'priority', 'assigned_to'
) jt AS status, priority, assigned_to
LIMIT 10;

-- Compare with extracting each field separately (same result, more parsing)
SELECT
  event_id,
  event_type,
  event_json:status::string     AS status,
  event_json:priority::string   AS priority,
  event_json:assigned_to::string AS assigned_to
FROM ops_json.operational_events
LIMIT 10;

-- ============================================================================
-- SCENARIO 12: schema_of_json — Inspect unknown JSON structure
-- KEY POINT: When you receive a new data feed and don't know the JSON
-- structure, schema_of_json infers the schema for you. Great for
-- exploration and building your from_json schema strings.
-- ============================================================================

-- What's inside the claim JSON?
SELECT schema_of_json(claim_json) AS claim_schema
FROM claims_json.claims_submissions
LIMIT 1;

-- What's inside the member profile JSON?
SELECT schema_of_json(profile_json) AS profile_schema
FROM member_json.member_profiles
LIMIT 1;

-- What's inside the operational event JSON?
SELECT schema_of_json(event_json) AS event_schema
FROM ops_json.operational_events
WHERE event_type = 'prior_auth'
LIMIT 1;

-- ============================================================================
-- SCENARIO 13: NULL behavior in JSON
-- KEY POINT: There's an important difference between:
--   1) A key that doesn't exist in the JSON → SQL NULL
--   2) A key with an explicit JSON null value → SQL NULL (but different semantically)
--   3) A key with empty string → empty string, NOT NULL
-- Understanding this prevents subtle bugs in analytics.
-- ============================================================================

-- Demonstrate the difference using a temp view
-- (Inline string literals with : extraction need a FROM clause)
SELECT
  json_null,
  get_json_object(json_null, '$.key') IS NULL       AS json_null_is_sql_null,
  get_json_object(json_missing, '$.key') IS NULL     AS missing_key_is_sql_null,
  get_json_object(json_empty, '$.key')               AS empty_string_value,
  get_json_object(json_empty, '$.key') IS NULL       AS empty_string_is_not_null
FROM (
  SELECT
    '{"key": null}'  AS json_null,
    '{"other": 1}'   AS json_missing,
    '{"key": ""}'    AS json_empty
);

-- Practical example: resolution is null for unresolved events
SELECT
  event_id,
  event_json:status::string              AS status,
  event_json:resolution                  AS resolution_raw,
  event_json:resolution IS NULL          AS resolution_is_null,
  event_json:resolution.outcome::string  AS outcome,
  event_json:resolution.outcome IS NULL  AS outcome_is_null
FROM ops_json.operational_events
LIMIT 10;

-- Count resolved vs unresolved events
SELECT
  CASE
    WHEN event_json:resolution IS NULL OR event_json:resolution::string = 'null'
    THEN 'Unresolved'
    ELSE 'Resolved'
  END AS resolution_status,
  count(*) AS event_count
FROM ops_json.operational_events
GROUP BY 1
