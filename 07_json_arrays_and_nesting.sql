-- ============================================================================
-- PART 2B: ARRAYS & NESTING — Working with JSON Arrays
-- Tables used: claims_json_demo.claims_submissions, member_json_demo.member_profiles
-- ============================================================================
USE CATALOG serverless_stable_swv01_catalog;

-- ============================================================================
-- SCENARIO 5: Access specific array elements by index
-- KEY POINT: Arrays are 0-based. Use [0], [1], etc. to access specific
-- elements. Then use dot notation to get fields within that element.
-- ============================================================================

SELECT
  claim_id,
  claim_json:service_lines[0].procedure_code   AS first_line_cpt,
  claim_json:service_lines[0].charge_amount::double AS first_line_charge,
  claim_json:service_lines[0].place_of_service AS first_line_pos,
  claim_json:service_lines[1].procedure_code   AS second_line_cpt,
  claim_json:service_lines[1].charge_amount::double AS second_line_charge
FROM claims_json_demo.claims_submissions
LIMIT 10;

-- ============================================================================
-- SCENARIO 6: Wildcard array extraction with [*]
-- KEY POINT: Use [*] to extract a specific field from ALL elements in an
-- array. Returns a JSON array of those values. Great for quick lookups
-- without needing to explode.
-- ============================================================================

-- Get all procedure codes across all service lines
SELECT
  claim_id,
  claim_json:service_lines[*].procedure_code     AS all_procedure_codes,
  claim_json:service_lines[*].charge_amount       AS all_charges,
  claim_json:service_lines[*].diagnosis_pointers  AS all_dx_pointers
FROM claims_json_demo.claims_submissions
LIMIT 10;

-- Use wildcard to find claims containing a specific CPT code
SELECT
  claim_id,
  claim_type,
  total_charge_amount,
  claim_json:service_lines[*].procedure_code AS all_cpts
FROM claims_json_demo.claims_submissions
WHERE array_contains(
  from_json(claim_json:service_lines[*].procedure_code, 'ARRAY<STRING>'),
  '99214'
);

-- ============================================================================
-- SCENARIO 7: EXPLODE service lines — one row per line for analysis
-- KEY POINT: This is the most powerful technique. Use from_json + explode
-- to "un-nest" arrays into individual rows for aggregation and joins.
-- This is the equivalent of unnesting in Oracle 12c+.
-- ============================================================================

-- Step 1: Understand the schema first
SELECT schema_of_json(claim_json:service_lines) AS inferred_schema
FROM claims_json_demo.claims_submissions
LIMIT 1;

-- Step 2: Explode service lines into individual rows
SELECT
  c.claim_id,
  c.claim_type,
  c.submitted_date,
  sl.line_number,
  sl.procedure_code,
  sl.modifiers,
  sl.charge_amount,
  sl.place_of_service,
  sl.date_of_service,
  sl.rendering_provider_npi
FROM claims_json_demo.claims_submissions c
LATERAL VIEW OUTER explode(
  from_json(
    c.claim_json:service_lines,
    'ARRAY<STRUCT<line_number:INT, procedure_code:STRING, modifiers:ARRAY<STRING>, diagnosis_pointers:ARRAY<STRING>, units:INT, charge_amount:DOUBLE, place_of_service:STRING, date_of_service:STRING, rendering_provider_npi:STRING, revenue_code:STRING>>'
  )
) sl_tbl AS sl
LIMIT 20;

-- Step 3: Now we can aggregate at the line level — top CPT codes by volume
SELECT
  sl.procedure_code,
  count(*)                           AS line_count,
  round(sum(sl.charge_amount), 2)    AS total_charges,
  round(avg(sl.charge_amount), 2)    AS avg_charge
FROM claims_json_demo.claims_submissions c
LATERAL VIEW explode(
  from_json(
    c.claim_json:service_lines,
    'ARRAY<STRUCT<procedure_code:STRING, charge_amount:DOUBLE>>'
  )
) sl_tbl AS sl
GROUP BY sl.procedure_code
ORDER BY line_count DESC;

-- ============================================================================
-- SCENARIO 8: Explode nested member engagement history
-- KEY POINT: Same pattern works on deeply nested arrays. This is how you'd
-- build outreach effectiveness reports from member engagement data.
-- ============================================================================

-- Flatten engagement history for outreach analysis
SELECT
  m.member_id,
  m.line_of_business,
  m.profile_json:demographics.first_name::string AS first_name,
  m.profile_json:demographics.last_name::string  AS last_name,
  eng.event_type,
  eng.channel,
  eng.timestamp,
  eng.outcome,
  eng.agent_id
FROM member_json_demo.member_profiles m
LATERAL VIEW OUTER explode(
  from_json(
    m.profile_json:engagement_history,
    'ARRAY<STRUCT<event_type:STRING, channel:STRING, timestamp:STRING, outcome:STRING, agent_id:STRING>>'
  )
) eng_tbl AS eng
WHERE eng.event_type IS NOT NULL
LIMIT 20;

-- Outreach effectiveness by channel and outcome
SELECT
  eng.channel,
  eng.outcome,
  count(*) AS touch_count,
  count(DISTINCT m.member_id) AS unique_members
FROM member_json_demo.member_profiles m
LATERAL VIEW explode(
  from_json(
    m.profile_json:engagement_history,
    'ARRAY<STRUCT<event_type:STRING, channel:STRING, timestamp:STRING, outcome:STRING, agent_id:STRING>>'
  )
) eng_tbl AS eng
WHERE eng.event_type IS NOT NULL
GROUP BY eng.channel, eng.outcome
ORDER BY eng.channel, touch_count DESC;

-- ============================================================================
-- SCENARIO 8B: Working with member conditions array
-- KEY POINT: Arrays of objects are extremely common in healthcare data.
-- Same explode pattern lets you analyze conditions across your population.
-- ============================================================================

-- What are the most common conditions in our Medicare Advantage population?
SELECT
  cond.code AS dx_code,
  cond.description,
  count(DISTINCT m.member_id) AS member_count,
  round(count(DISTINCT m.member_id) * 100.0 /
    (SELECT count(*) FROM member_json_demo.member_profiles WHERE line_of_business = 'Medicare Advantage'), 1
  ) AS pct_of_ma_members
FROM member_json_demo.member_profiles m
LATERAL VIEW explode(
  from_json(
    m.profile_json:conditions,
    'ARRAY<STRUCT<code:STRING, description:STRING, onset_date:STRING, status:STRING>>'
  )
) cond_tbl AS cond
WHERE m.line_of_business = 'Medicare Advantage'
  AND cond.status = 'active'
GROUP BY cond.code, cond.description
ORDER BY member_count DESC
