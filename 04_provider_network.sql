-- ============================================================================
-- TABLE: provider_json_demo.provider_network
-- Complexity: MODERATE-HIGH — uses VARIANT type instead of STRING
-- This is the VARIANT showcase table
-- ============================================================================

DROP TABLE IF EXISTS serverless_stable_swv01_catalog.provider_json_demo.provider_network;

CREATE TABLE serverless_stable_swv01_catalog.provider_json_demo.provider_network (
  provider_id    STRING    COMMENT 'Internal provider identifier (PRV-NNNNNN)',
  npi            STRING    COMMENT 'National Provider Identifier (10-digit)',
  provider_type  STRING    COMMENT 'individual or organization',
  effective_date DATE      COMMENT 'Network effective date',
  provider_data  VARIANT   COMMENT 'Provider directory payload as VARIANT: name, specialties array, addresses array, languages array, network_participation array, contract object'
) COMMENT 'Provider directory with VARIANT column — showcases native semi-structured data type for provider details, specialties, and network participation';

-- Generate 200 providers
INSERT INTO serverless_stable_swv01_catalog.provider_json_demo.provider_network
WITH provider_ids AS (
  SELECT explode(sequence(1, 200)) AS rn
),
base AS (
  SELECT
    rn,
    concat('PRV-', lpad(cast(rn AS STRING), 6, '0')) AS provider_id,
    concat('1', lpad(cast(abs(hash(rn, 'npi')) % 999999999 + 1000000000 AS STRING), 9, '0')) AS npi,
    CASE WHEN abs(hash(rn, 'pt')) % 10 < 7 THEN 'individual' ELSE 'organization' END AS provider_type,
    date_add('2020-01-01', abs(hash(rn, 'edt')) % 1460) AS effective_date,
    rn AS seed
  FROM provider_ids
)
SELECT
  provider_id,
  npi,
  provider_type,
  effective_date,
  PARSE_JSON(
    concat(
      CASE WHEN provider_type = 'individual' THEN
        concat(
          '{"name":{"first":"',
          CASE abs(hash(seed, 'dfn')) % 15
            WHEN 0 THEN 'Sarah' WHEN 1 THEN 'James' WHEN 2 THEN 'Maria' WHEN 3 THEN 'David'
            WHEN 4 THEN 'Emily' WHEN 5 THEN 'Robert' WHEN 6 THEN 'Lisa' WHEN 7 THEN 'Michael'
            WHEN 8 THEN 'Anna' WHEN 9 THEN 'William' WHEN 10 THEN 'Rachel' WHEN 11 THEN 'Thomas'
            WHEN 12 THEN 'Karen' WHEN 13 THEN 'Daniel' ELSE 'Jennifer'
          END,
          '","last":"',
          CASE abs(hash(seed, 'dln')) % 15
            WHEN 0 THEN 'Patel' WHEN 1 THEN 'Chen' WHEN 2 THEN 'Kim' WHEN 3 THEN 'Nguyen'
            WHEN 4 THEN 'Shah' WHEN 5 THEN 'Kumar' WHEN 6 THEN 'Park' WHEN 7 THEN 'Ahmed'
            WHEN 8 THEN 'Gupta' WHEN 9 THEN 'Lee' WHEN 10 THEN 'Wang' WHEN 11 THEN 'Singh'
            WHEN 12 THEN 'Thompson' WHEN 13 THEN 'White' ELSE 'Adams'
          END,
          '","credentials":"',
          CASE abs(hash(seed, 'cred')) % 4
            WHEN 0 THEN 'MD' WHEN 1 THEN 'DO' WHEN 2 THEN 'MD, FACP' ELSE 'DO, MBA'
          END,
          '"}')
      ELSE
        concat(
          '{"name":{"organization_name":"',
          CASE abs(hash(seed, 'org')) % 8
            WHEN 0 THEN 'Humana Health Centers'
            WHEN 1 THEN 'Kentucky Medical Associates'
            WHEN 2 THEN 'Bluegrass Community Health'
            WHEN 3 THEN 'Commonwealth Care Network'
            WHEN 4 THEN 'River Valley Medical Group'
            WHEN 5 THEN 'Appalachian Regional Healthcare'
            WHEN 6 THEN 'Derby City Medical Center'
            ELSE 'Southern Health Partners'
          END,
          '"}')
      END,
      ',"specialties":[{"code":"',
      CASE abs(hash(seed, 'sp1')) % 10
        WHEN 0 THEN '207R00000X","description":"Internal Medicine'
        WHEN 1 THEN '207RC0000X","description":"Cardiovascular Disease'
        WHEN 2 THEN '207RE0101X","description":"Endocrinology'
        WHEN 3 THEN '207RG0100X","description":"Gastroenterology'
        WHEN 4 THEN '207RP1001X","description":"Pulmonary Disease'
        WHEN 5 THEN '208D00000X","description":"General Practice'
        WHEN 6 THEN '207Q00000X","description":"Family Medicine'
        WHEN 7 THEN '207X00000X","description":"Orthopedic Surgery'
        WHEN 8 THEN '2084N0400X","description":"Neurology'
        ELSE '207Y00000X","description":"Ophthalmology'
      END,
      '","board_certified":', CASE WHEN abs(hash(seed, 'bc1')) % 100 < 70 THEN 'true' ELSE 'false' END,
      '}',
      CASE WHEN abs(hash(seed, 'sp2f')) % 100 < 35 THEN
        concat(',{"code":"',
          CASE abs(hash(seed, 'sp2')) % 5
            WHEN 0 THEN '261QM0801X","description":"Mental Health Facility'
            WHEN 1 THEN '261QR0400X","description":"Rehabilitation Facility'
            WHEN 2 THEN '282N00000X","description":"General Acute Care Hospital'
            WHEN 3 THEN '261QU0200X","description":"Urgent Care Facility'
            ELSE '261QP2300X","description":"Primary Care Clinic'
          END,
          '","board_certified":', CASE WHEN abs(hash(seed, 'bc2')) % 100 < 60 THEN 'true' ELSE 'false' END,
          '}')
      ELSE '' END,
      ']',
      ',"addresses":[{"type":"practice","street":"',
      cast(abs(hash(seed, 'stnum')) % 9899 + 100 AS STRING), ' ',
      CASE abs(hash(seed, 'stname')) % 6
        WHEN 0 THEN 'Medical Center Dr' WHEN 1 THEN 'Healthcare Blvd' WHEN 2 THEN 'Clinic Way'
        WHEN 3 THEN 'Wellness Pkwy' WHEN 4 THEN 'Hospital Dr' ELSE 'Medical Park Ln'
      END,
      '","city":"',
      CASE abs(hash(seed, 'pcity')) % 10
        WHEN 0 THEN 'Louisville' WHEN 1 THEN 'Lexington' WHEN 2 THEN 'Bowling Green'
        WHEN 3 THEN 'Covington' WHEN 4 THEN 'Frankfort' WHEN 5 THEN 'Richmond'
        WHEN 6 THEN 'Georgetown' WHEN 7 THEN 'Florence' WHEN 8 THEN 'Elizabethtown'
        ELSE 'Owensboro'
      END,
      '","state":"KY","zip":"',
      CASE abs(hash(seed, 'pcity')) % 10
        WHEN 0 THEN '40202' WHEN 1 THEN '40507' WHEN 2 THEN '42101'
        WHEN 3 THEN '41011' WHEN 4 THEN '40601' WHEN 5 THEN '40475'
        WHEN 6 THEN '40324' WHEN 7 THEN '41042' WHEN 8 THEN '42701'
        ELSE '42301'
      END,
      '","phone":"502-555-', lpad(cast(abs(hash(seed, 'ph1')) % 9000 + 1000 AS STRING), 4, '0'),
      '"}',
      CASE WHEN abs(hash(seed, 'ma')) % 2 = 0 THEN
        concat(',{"type":"mailing","street":"PO Box ',
          cast(abs(hash(seed, 'pob')) % 9000 + 1000 AS STRING),
          '","city":"Louisville","state":"KY","zip":"40201","phone":"502-555-',
          lpad(cast(abs(hash(seed, 'ph2')) % 9000 + 1000 AS STRING), 4, '0'), '"}')
      ELSE '' END,
      ']',
      ',"languages":["English"',
      CASE WHEN abs(hash(seed, 'l1')) % 100 < 25 THEN ',"Spanish"' ELSE '' END,
      CASE WHEN abs(hash(seed, 'l2')) % 100 < 10 THEN ',"Vietnamese"' ELSE '' END,
      CASE WHEN abs(hash(seed, 'l3')) % 100 < 8 THEN ',"Arabic"' ELSE '' END,
      ']',
      ',"network_participation":[',
      concat_ws(',',
        concat('{"plan_code":"MA-001","network_tier":"',
          CASE abs(hash(seed, 'nt1')) % 3 WHEN 0 THEN 'tier1' WHEN 1 THEN 'tier2' ELSE 'tier1' END,
          '","accepting_new_patients":', CASE WHEN abs(hash(seed, 'anp1')) % 100 < 85 THEN 'true' ELSE 'false' END,
          ',"effective_date":"', cast(effective_date AS STRING), '"}'),
        CASE WHEN abs(hash(seed, 'np2')) % 100 < 60 THEN
          concat('{"plan_code":"COM-005","network_tier":"',
            CASE abs(hash(seed, 'nt2')) % 3 WHEN 0 THEN 'tier1' WHEN 1 THEN 'tier2' ELSE 'out_of_network' END,
            '","accepting_new_patients":', CASE WHEN abs(hash(seed, 'anp2')) % 100 < 80 THEN 'true' ELSE 'false' END,
            ',"effective_date":"', cast(effective_date AS STRING), '"}')
        END,
        CASE WHEN abs(hash(seed, 'np3')) % 100 < 40 THEN
          concat('{"plan_code":"MCD-010","network_tier":"tier1","accepting_new_patients":',
            CASE WHEN abs(hash(seed, 'anp3')) % 100 < 90 THEN 'true' ELSE 'false' END,
            ',"effective_date":"', cast(effective_date AS STRING), '"}')
        END
      ),
      ']',
      ',"contract":{"fee_schedule_id":"FS-',
      lpad(cast(abs(hash(seed, 'fs')) % 100 + 1 AS STRING), 4, '0'),
      '","contract_type":"',
      CASE abs(hash(seed, 'ctype')) % 3
        WHEN 0 THEN 'fee_for_service' WHEN 1 THEN 'capitated' ELSE 'value_based'
      END,
      '","discount_pct":', cast(round((abs(hash(seed, 'disc')) % 300) / 10.0 + 10, 1) AS STRING),
      '}}'
    )
  ) AS provider_data
FROM base
