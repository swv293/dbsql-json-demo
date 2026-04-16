-- ============================================================================
-- PART 3: THE VARIANT TYPE — Why It Matters and How to Use It
-- Table used: provider_json_demo.provider_network (VARIANT column)
-- ============================================================================
USE CATALOG serverless_stable_swv01_catalog;

-- ============================================================================
-- SCENARIO 14: Why VARIANT over STRING?
-- KEY POINT: VARIANT is a native binary semi-structured type. Unlike STRING:
--   - Parsed ONCE at write time, not re-parsed on every query
--   - Schema validation on ingest (malformed JSON caught early)
--   - Returns typed values natively (no need to cast from string)
--   - Better query performance on large datasets
--   - Supports indexing and predicate pushdown
-- ============================================================================

-- First, let's see what the VARIANT column looks like vs STRING
SELECT
  provider_id,
  provider_type,
  typeof(provider_data)   AS data_type,
  provider_data           AS raw_variant_view
FROM provider_json_demo.provider_network
LIMIT 3;

-- Side-by-side: same extraction syntax works on both STRING and VARIANT
-- But VARIANT doesn't need re-parsing — it's already structured
SELECT
  p.provider_id,
  -- These work the same on VARIANT as on STRING JSON
  p.provider_data:name                AS name_obj,
  p.provider_data:contract.contract_type AS contract_type,
  p.provider_data:contract.discount_pct  AS discount_pct
FROM provider_json_demo.provider_network p
LIMIT 5;

-- ============================================================================
-- SCENARIO 15: PARSE_JSON and TRY_PARSE_JSON — Getting data into VARIANT
-- KEY POINT: PARSE_JSON converts a JSON string to VARIANT at write time.
-- TRY_PARSE_JSON is the safe version — returns NULL instead of error
-- for malformed JSON. Always use TRY_ in production pipelines.
-- ============================================================================

-- PARSE_JSON: Converts string to VARIANT
SELECT PARSE_JSON('{"name": "Dr. Smith", "npi": "1234567890"}') AS parsed;

-- TRY_PARSE_JSON: Safe version — returns NULL for bad JSON
SELECT
  TRY_PARSE_JSON('{"valid": true}')     AS good_json,
  TRY_PARSE_JSON('{ bad json }')        AS bad_json_returns_null;

-- Show that our provider_data column was populated using PARSE_JSON
-- This is how the INSERT statement created the VARIANT values
DESCRIBE TABLE provider_json_demo.provider_network;

-- ============================================================================
-- SCENARIO 16: Querying VARIANT with colon syntax
-- KEY POINT: Same colon (:) and dot notation works on VARIANT.
-- The difference: VARIANT values are already typed — extracting a number
-- gives you a number, not a string representation.
-- ============================================================================

-- Extract individual provider fields
SELECT
  provider_id,
  provider_type,
  -- For individual providers
  provider_data:name.first::string        AS first_name,
  provider_data:name.last::string         AS last_name,
  provider_data:name.credentials::string  AS credentials,
  -- For organization providers
  provider_data:name.organization_name::string AS org_name
FROM provider_json_demo.provider_network
LIMIT 10;

-- VARIANT returns typed values — discount_pct is already numeric
SELECT
  provider_id,
  provider_data:contract.discount_pct       AS discount_raw,
  typeof(provider_data:contract.discount_pct) AS value_type,
  provider_data:contract.discount_pct::double AS discount_typed
FROM provider_json_demo.provider_network
LIMIT 5;

-- ============================================================================
-- SCENARIO 17: variant_get with explicit path and type
-- KEY POINT: variant_get gives you explicit control over the extraction
-- path and return type. Use JSONPath-style expressions.
-- try_variant_get is the safe version (returns NULL instead of error).
-- ============================================================================

-- variant_get with explicit typing
SELECT
  provider_id,
  variant_get(provider_data, '$.specialties[0].description', 'STRING') AS primary_specialty,
  variant_get(provider_data, '$.specialties[0].board_certified', 'BOOLEAN') AS is_board_certified,
  variant_get(provider_data, '$.contract.discount_pct', 'DOUBLE') AS discount_pct,
  variant_get(provider_data, '$.contract.fee_schedule_id', 'STRING') AS fee_schedule
FROM provider_json_demo.provider_network
LIMIT 10;

-- try_variant_get for safe extraction (no errors on missing paths)
SELECT
  provider_id,
  try_variant_get(provider_data, '$.specialties[1].description', 'STRING') AS second_specialty,
  try_variant_get(provider_data, '$.name.organization_name', 'STRING') AS org_name,
  try_variant_get(provider_data, '$.nonexistent.path', 'STRING') AS missing_returns_null
FROM provider_json_demo.provider_network
LIMIT 10;

-- ============================================================================
-- SCENARIO 18: variant_explode — Exploding VARIANT arrays
-- KEY POINT: variant_explode is the VARIANT-native way to flatten arrays.
-- It returns (pos, key, value) columns. For arrays, key is NULL.
-- Use LATERAL variant_explode for correlated queries.
-- ============================================================================

-- Explode the addresses array
SELECT
  p.provider_id,
  p.provider_type,
  addr.pos AS address_index,
  addr.key,
  addr.value:type::string    AS address_type,
  addr.value:city::string    AS city,
  addr.value:state::string   AS state,
  addr.value:zip::string     AS zip,
  addr.value:phone::string   AS phone
FROM provider_json_demo.provider_network p,
LATERAL variant_explode(p.provider_data:addresses) addr
LIMIT 15;

-- Explode network participation to see which plans each provider accepts
SELECT
  p.provider_id,
  p.provider_data:name.last::string AS provider_name,
  np.value:plan_code::string AS plan_code,
  np.value:network_tier::string AS network_tier,
  np.value:accepting_new_patients::boolean AS accepting_new
FROM provider_json_demo.provider_network p,
LATERAL variant_explode(p.provider_data:network_participation) np
WHERE p.provider_type = 'individual'
LIMIT 20;

-- ============================================================================
-- SCENARIO 19: schema_of_variant and schema_of_variant_agg
-- KEY POINT: Inspect the structure of VARIANT data:
--   - schema_of_variant: schema of a single value
--   - schema_of_variant_agg: aggregated schema across all rows
-- The aggregated version is especially useful — it shows the UNION of all
-- fields across all rows, catching variations in structure.
-- ============================================================================

-- Single row schema
SELECT
  provider_id,
  schema_of_variant(provider_data) AS row_schema
FROM provider_json_demo.provider_network
LIMIT 3;

-- Aggregated schema across all providers — shows the full possible structure
SELECT schema_of_variant_agg(provider_data) AS full_schema
FROM provider_json_demo.provider_network;

-- ============================================================================
-- SCENARIO 20: Complex VARIANT operations — chained extraction + filtering
-- KEY POINT: Combine variant_explode with filtering and further extraction
-- for real analytical queries. This is the kind of query an analyst
-- would write to answer "which in-network providers accept new MA patients?"
-- ============================================================================

-- Find all individual providers accepting new Medicare Advantage patients
SELECT
  p.provider_id,
  p.npi,
  p.provider_data:name.first::string AS first_name,
  p.provider_data:name.last::string AS last_name,
  p.provider_data:name.credentials::string AS credentials,
  variant_get(p.provider_data, '$.specialties[0].description', 'STRING') AS specialty,
  np.value:plan_code::string AS plan_code,
  np.value:network_tier::string AS tier,
  p.provider_data:addresses[0].city::string AS city,
  p.provider_data:contract.contract_type::string AS contract_type
FROM provider_json_demo.provider_network p,
LATERAL variant_explode(p.provider_data:network_participation) np
WHERE p.provider_type = 'individual'
  AND np.value:plan_code::string LIKE 'MA-%'
  AND np.value:accepting_new_patients::boolean = true
  AND np.value:network_tier::string = 'tier1'
ORDER BY p.provider_data:name.last::string;

-- Languages spoken by specialty — useful for network adequacy analysis
SELECT
  variant_get(p.provider_data, '$.specialties[0].description', 'STRING') AS specialty,
  lang.value::string AS language,
  count(*) AS provider_count
FROM provider_json_demo.provider_network p,
LATERAL variant_explode(p.provider_data:languages) lang
WHERE p.provider_type = 'individual'
GROUP BY 1, 2
ORDER BY specialty, provider_count DESC
