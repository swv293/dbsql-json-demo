-- ============================================================================
-- PART 4: CAPSTONE — Cross-Table Join Producing a Parsed Analytics Dataset
-- Joins all 4 tables, extracts JSON/VARIANT fields, produces a flattened
-- dataset ready for dashboards and BI tools.
-- ============================================================================
USE CATALOG serverless_stable_swv01_catalog;

-- ============================================================================
-- SCENARIO 21: Build an Analytics-Ready Flattened Dataset
--
-- This query demonstrates:
--   1. Colon extraction on STRING JSON columns (claims, members, ops)
--   2. Colon extraction on VARIANT columns (providers)
--   3. variant_get with type casting
--   4. LATERAL VIEW explode for service line arrays
--   5. LEFT JOINs using JSON-extracted keys
--   6. Aggregation across tables
--   7. CTE structure for readability
-- ============================================================================

WITH
-- CTE 1: Explode claim service lines into individual rows
claim_lines AS (
  SELECT
    c.claim_id,
    c.submitted_date,
    c.claim_type,
    c.total_charge_amount,
    c.claim_json:subscriber.member_id::string    AS member_id,
    c.claim_json:filing_code::string             AS filing_code,
    c.claim_json:adjudication.status::string     AS adj_status,
    c.claim_json:adjudication.paid_amount::double AS paid_amount,
    c.claim_json:adjudication.allowed_amount::double AS allowed_amount,
    c.claim_json:adjudication.denial_codes       AS denial_codes,
    c.claim_json:billing_provider.name::string   AS billing_provider_name,
    sl.line_number,
    sl.procedure_code,
    sl.charge_amount AS line_charge,
    sl.place_of_service,
    sl.rendering_provider_npi,
    sl.modifiers,
    sl.revenue_code
  FROM claims_json.claims_submissions c
  LATERAL VIEW explode(
    from_json(
      c.claim_json:service_lines,
      'ARRAY<STRUCT<line_number:INT, procedure_code:STRING, modifiers:ARRAY<STRING>, charge_amount:DOUBLE, place_of_service:STRING, rendering_provider_npi:STRING, revenue_code:STRING>>'
    )
  ) sl_tbl AS sl
),

-- CTE 2: Extract member profile details
member_details AS (
  SELECT
    m.member_id,
    m.line_of_business,
    m.plan_code,
    m.profile_json:demographics.first_name::string AS first_name,
    m.profile_json:demographics.last_name::string  AS last_name,
    m.profile_json:demographics.gender::string     AS gender,
    m.profile_json:demographics.address.city::string AS city,
    m.profile_json:demographics.address.state::string AS state,
    m.profile_json:risk_scores.hcc_score::double   AS hcc_score,
    m.profile_json:risk_scores.sdoh_risk_level::string AS sdoh_risk,
    m.profile_json:sdoh_flags.food_insecurity::boolean AS food_insecurity,
    m.profile_json:sdoh_flags.transportation_barrier::boolean AS transport_barrier,
    size(from_json(m.profile_json:conditions, 'ARRAY<STRUCT<code:STRING>>')) AS condition_count,
    size(from_json(m.profile_json:programs_enrolled, 'ARRAY<STRING>')) AS program_count
  FROM member_json.member_profiles m
),

-- CTE 3: Provider details from VARIANT — one row per NPI + plan combination
provider_details AS (
  SELECT
    p.npi,
    p.provider_type,
    COALESCE(
      p.provider_data:name.last::string,
      p.provider_data:name.organization_name::string
    ) AS provider_name,
    variant_get(p.provider_data, '$.specialties[0].description', 'STRING') AS specialty,
    p.provider_data:contract.contract_type::string AS contract_type,
    p.provider_data:contract.discount_pct::double AS discount_pct,
    np.value:plan_code::string AS plan_code,
    np.value:network_tier::string AS network_tier,
    np.value:accepting_new_patients::boolean AS accepting_new
  FROM provider_json.provider_network p,
  LATERAL variant_explode(p.provider_data:network_participation) np
),

-- CTE 4: Aggregate operational events per claim
ops_summary AS (
  SELECT
    related_claim_id AS claim_id,
    count(*) AS event_count,
    count_if(event_json:sla.met::boolean = false) AS sla_misses,
    max(event_json:status::string) AS latest_event_status,
    max(CASE WHEN event_type = 'prior_auth'
      THEN event_json:resolution.outcome::string END) AS pa_outcome
  FROM ops_json.operational_events
  WHERE related_claim_id IS NOT NULL
  GROUP BY related_claim_id
)

-- Final SELECT: join everything together
SELECT
  cl.claim_id,
  cl.submitted_date,
  cl.claim_type,
  cl.filing_code,
  cl.line_number,
  cl.procedure_code,
  cl.line_charge,
  cl.place_of_service,
  cl.modifiers,
  cl.revenue_code,
  cl.adj_status,
  cl.paid_amount,
  cl.allowed_amount,
  cl.billing_provider_name,

  -- Member fields (from STRING JSON)
  md.first_name,
  md.last_name,
  md.line_of_business,
  md.city AS member_city,
  md.state AS member_state,
  md.hcc_score,
  md.sdoh_risk,
  md.food_insecurity,
  md.transport_barrier,
  md.condition_count,
  md.program_count,

  -- Provider fields (from VARIANT)
  pd.provider_name AS rendering_provider,
  pd.specialty AS rendering_specialty,
  pd.network_tier,
  pd.accepting_new AS provider_accepting_new,
  pd.contract_type AS provider_contract_type,
  pd.discount_pct AS provider_discount_pct,

  -- Ops fields (from STRING JSON, aggregated)
  COALESCE(os.event_count, 0)       AS related_event_count,
  COALESCE(os.sla_misses, 0)        AS sla_miss_count,
  os.latest_event_status,
  os.pa_outcome

FROM claim_lines cl

-- Join to member profile using member_id extracted from claim JSON
LEFT JOIN member_details md
  ON cl.member_id = md.member_id

-- Join to provider using rendering NPI from claim JSON + member plan for tier
LEFT JOIN provider_details pd
  ON cl.rendering_provider_npi = pd.npi
  AND md.plan_code = pd.plan_code

-- Join to operational events summary
LEFT JOIN ops_summary os
  ON cl.claim_id = os.claim_id

ORDER BY cl.submitted_date DESC, cl.claim_id, cl.line_number
LIMIT 50;


-- ============================================================================
-- BONUS: Summary analytics from the joined dataset
-- KEY POINT: Once you've built the flattened dataset, standard SQL analytics
-- work as expected. This is the payoff of learning JSON extraction.
-- ============================================================================

-- Denial rate by member risk level and line of business
WITH claim_data AS (
  SELECT
    c.claim_id,
    c.claim_json:subscriber.member_id::string AS member_id,
    c.claim_json:adjudication.status::string AS adj_status
  FROM claims_json.claims_submissions c
)
SELECT
  m.line_of_business,
  m.profile_json:risk_scores.sdoh_risk_level::string AS sdoh_risk,
  count(DISTINCT cd.claim_id) AS total_claims,
  count(DISTINCT CASE WHEN cd.adj_status = 'denied' THEN cd.claim_id END) AS denied_claims,
  round(
    count(DISTINCT CASE WHEN cd.adj_status = 'denied' THEN cd.claim_id END) * 100.0 /
    NULLIF(count(DISTINCT cd.claim_id), 0), 1
  ) AS denial_rate_pct
FROM claim_data cd
JOIN member_json.member_profiles m ON cd.member_id = m.member_id
GROUP BY 1, 2
ORDER BY 1, 2;

-- SLA performance by event type — ops analytics
SELECT
  event_type,
  count(*) AS total_events,
  count_if(event_json:sla.met::boolean = true) AS sla_met,
  count_if(event_json:sla.met::boolean = false) AS sla_missed,
  round(count_if(event_json:sla.met::boolean = true) * 100.0 / count(*), 1) AS sla_met_pct,
  round(avg(event_json:sla.actual_hours::double), 1) AS avg_actual_hours,
  round(percentile_approx(event_json:sla.actual_hours::double, 0.95), 1) AS p95_hours
FROM ops_json.operational_events
GROUP BY event_type
ORDER BY event_type
