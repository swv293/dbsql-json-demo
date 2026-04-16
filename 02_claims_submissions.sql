-- ============================================================================
-- TABLE: claims_json.claims_submissions
-- Complexity: MODERATE — nested objects + arrays of service lines
-- ============================================================================

DROP TABLE IF EXISTS serverless_stable_swv01_catalog.claims_json.claims_submissions;

CREATE TABLE serverless_stable_swv01_catalog.claims_json.claims_submissions (
  claim_id        STRING    COMMENT 'Unique claim identifier (CLM-YYYY-NNNNN)',
  submitted_date  DATE      COMMENT 'Date the claim was submitted to the payer',
  payer_id        STRING    COMMENT 'Payer/plan identifier',
  claim_type      STRING    COMMENT 'professional or institutional',
  total_charge_amount DECIMAL(12,2) COMMENT 'Header-level total charge',
  claim_json      STRING    COMMENT 'Full claim payload as JSON string: subscriber, billing_provider, service_lines array, adjudication'
) COMMENT 'Claims submissions with JSON payloads containing service lines, adjudication, and provider details';

-- Generate 500 claims with 1-4 service lines each
INSERT INTO serverless_stable_swv01_catalog.claims_json.claims_submissions
WITH claim_ids AS (
  SELECT explode(sequence(1, 500)) AS rn
),
base AS (
  SELECT
    rn,
    concat('CLM-2024-', lpad(cast(rn AS STRING), 5, '0')) AS claim_id,
    date_add('2024-01-01', abs(hash(rn, 'dt')) % 365) AS submitted_date,
    concat('HUM-', lpad(cast(abs(hash(rn, 'pay')) % 20 + 1 AS STRING), 3, '0')) AS payer_id,
    CASE WHEN abs(hash(rn, 'ct')) % 10 < 6 THEN 'professional' ELSE 'institutional' END AS claim_type,
    concat('MBR-', lpad(cast(abs(hash(rn, 'mbr')) % 300 + 1 AS STRING), 6, '0')) AS member_id,
    concat('GRP-', lpad(cast(abs(hash(rn, 'grp')) % 50 + 1 AS STRING), 4, '0')) AS group_number,
    concat('1', lpad(cast(abs(hash(rn, 'bnpi')) % 999999999 + 1000000000 AS STRING), 9, '0')) AS billing_npi,
    CASE abs(hash(rn, 'fc')) % 5
      WHEN 0 THEN '09' WHEN 1 THEN '11' WHEN 2 THEN '12' WHEN 3 THEN '17' ELSE '22'
    END AS filing_code,
    abs(hash(rn, 'cob')) % 100 < 15 AS has_cob,
    CASE
      WHEN abs(hash(rn, 'adj')) % 100 < 75 THEN 'paid'
      WHEN abs(hash(rn, 'adj')) % 100 < 90 THEN 'denied'
      ELSE 'pending'
    END AS adj_status,
    abs(hash(rn, 'nl')) % 4 + 1 AS num_lines,
    rn AS seed
  FROM claim_ids
),
with_lines AS (
  SELECT b.*, le.line_num
  FROM base b
  LATERAL VIEW explode(sequence(1, b.num_lines)) le AS line_num
),
service_lines AS (
  SELECT
    seed,
    collect_list(
      concat(
        '{"line_number":', cast(line_num AS STRING),
        ',"procedure_code":"',
        CASE abs(hash(seed, 'cpt', line_num)) % 12
          WHEN 0 THEN '99213' WHEN 1 THEN '99214' WHEN 2 THEN '99215'
          WHEN 3 THEN '99283' WHEN 4 THEN '99284' WHEN 5 THEN '99285'
          WHEN 6 THEN '27447' WHEN 7 THEN '43239' WHEN 8 THEN '93000'
          WHEN 9 THEN '71046' WHEN 10 THEN '80053' ELSE '36415'
        END,
        '","modifiers":',
        CASE
          WHEN abs(hash(seed, 'mod', line_num)) % 10 < 4
          THEN concat('["',
            CASE abs(hash(seed, 'md1', line_num)) % 4 WHEN 0 THEN '25' WHEN 1 THEN '59' WHEN 2 THEN 'TC' ELSE '26' END,
            '"]')
          WHEN abs(hash(seed, 'mod', line_num)) % 10 < 6
          THEN concat('["',
            CASE abs(hash(seed, 'md2', line_num)) % 3 WHEN 0 THEN '25' WHEN 1 THEN '59' ELSE 'LT' END,
            '","',
            CASE abs(hash(seed, 'md3', line_num)) % 3 WHEN 0 THEN '76' WHEN 1 THEN 'XE' ELSE 'XS' END,
            '"]')
          ELSE '[]'
        END,
        ',"diagnosis_pointers":["',
        CASE abs(hash(seed, 'dx1', line_num)) % 6
          WHEN 0 THEN 'E11.9' WHEN 1 THEN 'I10' WHEN 2 THEN 'M17.11'
          WHEN 3 THEN 'J06.9' WHEN 4 THEN 'K21.0' ELSE 'Z00.00'
        END,
        CASE WHEN abs(hash(seed, 'dx2', line_num)) % 2 = 0
          THEN concat('","',
            CASE abs(hash(seed, 'dx2v', line_num)) % 5
              WHEN 0 THEN 'E78.5' WHEN 1 THEN 'I25.10' WHEN 2 THEN 'N18.3'
              WHEN 3 THEN 'G89.29' ELSE 'Z87.891'
            END)
          ELSE ''
        END,
        '"],"units":', cast(abs(hash(seed, 'u', line_num)) % 3 + 1 AS STRING),
        ',"charge_amount":', cast(round((abs(hash(seed, 'chg', line_num)) % 250000) / 100.0 + 50, 2) AS STRING),
        ',"place_of_service":"',
        CASE abs(hash(seed, 'pos', line_num)) % 5
          WHEN 0 THEN '11' WHEN 1 THEN '21' WHEN 2 THEN '22' WHEN 3 THEN '23' ELSE '81'
        END,
        '","date_of_service":"', cast(date_add(submitted_date, -(abs(hash(seed, 'dos', line_num)) % 5)) AS STRING),
        '","rendering_provider_npi":"', concat('1', lpad(cast(abs(hash(seed, 'rnpi', line_num)) % 999999999 + 1000000000 AS STRING), 9, '0')),
        '"',
        CASE WHEN claim_type = 'institutional'
          THEN concat(',"revenue_code":"',
            CASE abs(hash(seed, 'rev', line_num)) % 5
              WHEN 0 THEN '0120' WHEN 1 THEN '0250' WHEN 2 THEN '0320' WHEN 3 THEN '0450' ELSE '0510'
            END, '"')
          ELSE ''
        END,
        '}'
      )
    ) AS svc_lines_arr
  FROM with_lines
  GROUP BY seed, submitted_date, claim_type
)
SELECT
  b.claim_id,
  b.submitted_date,
  b.payer_id,
  b.claim_type,
  CAST(0 AS DECIMAL(12,2)) AS total_charge_amount,
  concat(
    '{"claim_number":"', b.claim_id,
    '","patient_control_number":"PCN-', lpad(cast(b.seed AS STRING), 8, '0'),
    '","filing_code":"', b.filing_code,
    '","release_of_info":"Y","coordination_of_benefits":', CASE WHEN b.has_cob THEN 'true' ELSE 'false' END,
    ',"subscriber":{"member_id":"', b.member_id,
    '","group_number":"', b.group_number,
    '","relationship_code":"', CASE abs(hash(b.seed, 'rel')) % 3 WHEN 0 THEN '18' WHEN 1 THEN '01' ELSE '19' END,
    '"},"billing_provider":{"npi":"', b.billing_npi,
    '","tax_id":"', concat(lpad(cast(abs(hash(b.seed, 'tax1')) % 90 + 10 AS STRING), 2, '0'), '-', lpad(cast(abs(hash(b.seed, 'tax2')) % 9000000 + 1000000 AS STRING), 7, '0')),
    '","name":"',
    CASE abs(hash(b.seed, 'bname')) % 8
      WHEN 0 THEN 'Sunrise Medical Group'
      WHEN 1 THEN 'Bluegrass Family Practice'
      WHEN 2 THEN 'Commonwealth Orthopedics'
      WHEN 3 THEN 'River City Cardiology'
      WHEN 4 THEN 'Heritage Health Partners'
      WHEN 5 THEN 'Appalachian Wellness Center'
      WHEN 6 THEN 'Derby City Internal Medicine'
      ELSE 'Southern Specialty Associates'
    END,
    '"},"service_lines":[',
    concat_ws(',', sl.svc_lines_arr),
    '],"adjudication":{"status":"', b.adj_status,
    '","paid_amount":',
    CASE WHEN b.adj_status = 'denied' THEN '0.00'
         WHEN b.adj_status = 'pending' THEN 'null'
         ELSE cast(round((abs(hash(b.seed, 'pa')) % 200000) / 100.0 + 100, 2) AS STRING)
    END,
    ',"allowed_amount":',
    CASE WHEN b.adj_status = 'pending' THEN 'null'
         ELSE cast(round((abs(hash(b.seed, 'aa')) % 250000) / 100.0 + 100, 2) AS STRING)
    END,
    ',"denial_codes":',
    CASE WHEN b.adj_status = 'denied'
      THEN concat('["',
        CASE abs(hash(b.seed, 'dc1')) % 5
          WHEN 0 THEN 'CO-4' WHEN 1 THEN 'CO-16' WHEN 2 THEN 'CO-29' WHEN 3 THEN 'PR-1' ELSE 'CO-197'
        END,
        CASE WHEN abs(hash(b.seed, 'dc2')) % 100 < 30
          THEN concat('","',
            CASE abs(hash(b.seed, 'dc3')) % 3 WHEN 0 THEN 'CO-50' WHEN 1 THEN 'OA-23' ELSE 'PR-2' END,
            '"]')
          ELSE '"]'
        END)
      ELSE '[]'
    END,
    ',"remark_codes":',
    CASE WHEN b.adj_status != 'pending' AND abs(hash(b.seed, 'rc')) % 100 < 30
      THEN concat('["',
        CASE abs(hash(b.seed, 'rcv')) % 4
          WHEN 0 THEN 'N362' WHEN 1 THEN 'N657' WHEN 2 THEN 'M15' ELSE 'N519'
        END, '"]')
      ELSE '[]'
    END,
    '}}'
  ) AS claim_json
FROM base b
JOIN service_lines sl ON sl.seed = b.seed;

-- Update total_charge_amount from the generated service line data
UPDATE serverless_stable_swv01_catalog.claims_json.claims_submissions
SET total_charge_amount = COALESCE(
  CAST(
    AGGREGATE(
      from_json(claim_json:service_lines, 'ARRAY<STRUCT<charge_amount: DOUBLE>>'),
      DOUBLE(0),
      (acc, x) -> acc + coalesce(x.charge_amount, 0)
    ) AS DECIMAL(12,2)
  ), 0)
