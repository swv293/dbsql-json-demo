-- ============================================================================
-- TABLE: member_json_demo.member_profiles
-- Complexity: HIGH — deep nesting, arrays of objects, SDoH flags, engagement
-- ============================================================================

DROP TABLE IF EXISTS serverless_stable_swv01_catalog.member_json_demo.member_profiles;

CREATE TABLE serverless_stable_swv01_catalog.member_json_demo.member_profiles (
  member_id            STRING  COMMENT 'Unique member identifier (MBR-NNNNNN)',
  enrollment_start_date DATE   COMMENT 'Coverage effective date',
  plan_code            STRING  COMMENT 'Plan/product code',
  line_of_business     STRING  COMMENT 'Medicare Advantage, Commercial, or Medicaid',
  profile_json         STRING  COMMENT 'Rich member profile JSON: demographics, risk_scores, sdoh_flags, conditions array, programs_enrolled array, engagement_history array, preferred_communication'
) COMMENT 'Member profiles with deeply nested JSON including demographics, risk, SDoH, conditions, and engagement history';

-- Generate 300 members
INSERT INTO serverless_stable_swv01_catalog.member_json_demo.member_profiles
WITH member_ids AS (
  SELECT explode(sequence(1, 300)) AS rn
),
base AS (
  SELECT
    rn,
    concat('MBR-', lpad(cast(rn AS STRING), 6, '0')) AS member_id,
    date_add('2020-01-01', abs(hash(rn, 'enrl')) % 1460) AS enrollment_start_date,
    CASE abs(hash(rn, 'lob')) % 3
      WHEN 0 THEN 'Medicare Advantage'
      WHEN 1 THEN 'Commercial'
      ELSE 'Medicaid'
    END AS line_of_business,
    rn AS seed
  FROM member_ids
),
names_data AS (
  SELECT
    b.*,
    CASE abs(hash(seed, 'fn')) % 20
      WHEN 0 THEN 'James' WHEN 1 THEN 'Mary' WHEN 2 THEN 'Robert' WHEN 3 THEN 'Patricia'
      WHEN 4 THEN 'John' WHEN 5 THEN 'Jennifer' WHEN 6 THEN 'Michael' WHEN 7 THEN 'Linda'
      WHEN 8 THEN 'David' WHEN 9 THEN 'Elizabeth' WHEN 10 THEN 'William' WHEN 11 THEN 'Barbara'
      WHEN 12 THEN 'Richard' WHEN 13 THEN 'Susan' WHEN 14 THEN 'Joseph' WHEN 15 THEN 'Jessica'
      WHEN 16 THEN 'Thomas' WHEN 17 THEN 'Sarah' WHEN 18 THEN 'Charles' ELSE 'Karen'
    END AS fname,
    CASE abs(hash(seed, 'ln')) % 20
      WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Brown'
      WHEN 4 THEN 'Jones' WHEN 5 THEN 'Garcia' WHEN 6 THEN 'Miller' WHEN 7 THEN 'Davis'
      WHEN 8 THEN 'Rodriguez' WHEN 9 THEN 'Martinez' WHEN 10 THEN 'Hernandez' WHEN 11 THEN 'Lopez'
      WHEN 12 THEN 'Wilson' WHEN 13 THEN 'Anderson' WHEN 14 THEN 'Taylor' WHEN 15 THEN 'Thomas'
      WHEN 16 THEN 'Moore' WHEN 17 THEN 'Jackson' WHEN 18 THEN 'Martin' ELSE 'Lee'
    END AS lname,
    CASE abs(hash(seed, 'city')) % 15
      WHEN 0 THEN 'Louisville' WHEN 1 THEN 'Lexington' WHEN 2 THEN 'Bowling Green'
      WHEN 3 THEN 'Covington' WHEN 4 THEN 'Frankfort' WHEN 5 THEN 'Richmond'
      WHEN 6 THEN 'Georgetown' WHEN 7 THEN 'Florence' WHEN 8 THEN 'Elizabethtown'
      WHEN 9 THEN 'Owensboro' WHEN 10 THEN 'Paducah' WHEN 11 THEN 'Ashland'
      WHEN 12 THEN 'Henderson' WHEN 13 THEN 'Radcliff' ELSE 'Hopkinsville'
    END AS city,
    CASE abs(hash(seed, 'city')) % 15
      WHEN 0 THEN '40202' WHEN 1 THEN '40507' WHEN 2 THEN '42101'
      WHEN 3 THEN '41011' WHEN 4 THEN '40601' WHEN 5 THEN '40475'
      WHEN 6 THEN '40324' WHEN 7 THEN '41042' WHEN 8 THEN '42701'
      WHEN 9 THEN '42301' WHEN 10 THEN '42001' WHEN 11 THEN '41101'
      WHEN 12 THEN '42420' WHEN 13 THEN '40160' ELSE '42240'
    END AS zip
  FROM base b
)
SELECT
  member_id,
  enrollment_start_date,
  concat(
    CASE line_of_business
      WHEN 'Medicare Advantage' THEN 'MA-'
      WHEN 'Commercial' THEN 'COM-'
      ELSE 'MCD-'
    END,
    lpad(cast(abs(hash(seed, 'pc')) % 20 + 1 AS STRING), 3, '0')
  ) AS plan_code,
  line_of_business,
  concat(
    '{"demographics":{"first_name":"', fname,
    '","last_name":"', lname,
    '","date_of_birth":"', cast(date_add('1940-01-01', abs(hash(seed, 'dob')) % 25000) AS STRING),
    '","gender":"', CASE WHEN abs(hash(seed, 'gen')) % 100 < 48 THEN 'M' ELSE 'F' END,
    '","language":"', CASE abs(hash(seed, 'lang')) % 5
      WHEN 0 THEN 'English' WHEN 1 THEN 'Spanish' WHEN 2 THEN 'English'
      WHEN 3 THEN 'English' ELSE 'Vietnamese' END,
    '","address":{"street":"', cast(abs(hash(seed, 'stnum')) % 9899 + 100 AS STRING), ' ',
    CASE abs(hash(seed, 'stname')) % 6
      WHEN 0 THEN 'Main St' WHEN 1 THEN 'Oak Ave' WHEN 2 THEN 'Elm Dr'
      WHEN 3 THEN 'Maple Ln' WHEN 4 THEN 'River Rd' ELSE 'Highland Blvd'
    END,
    '","city":"', city,
    '","state":"KY","zip":"', zip,
    '"}}',
    ',"risk_scores":{"hcc_score":', cast(round((abs(hash(seed, 'hcc')) % 3500) / 1000.0 + 0.5, 3) AS STRING),
    ',"rx_score":', cast(round((abs(hash(seed, 'rx')) % 2000) / 1000.0 + 0.2, 3) AS STRING),
    ',"sdoh_risk_level":"', CASE abs(hash(seed, 'sdoh')) % 4
      WHEN 0 THEN 'low' WHEN 1 THEN 'moderate' WHEN 2 THEN 'high' ELSE 'low' END,
    '"}',
    ',"sdoh_flags":{"food_insecurity":', CASE WHEN abs(hash(seed, 'fi')) % 100 < 12 THEN 'true' ELSE 'false' END,
    ',"transportation_barrier":', CASE WHEN abs(hash(seed, 'tb')) % 100 < 18 THEN 'true' ELSE 'false' END,
    ',"housing_instability":', CASE WHEN abs(hash(seed, 'hi')) % 100 < 8 THEN 'true' ELSE 'false' END,
    ',"social_isolation":', CASE WHEN abs(hash(seed, 'si')) % 100 < 15 THEN 'true' ELSE 'false' END,
    '}',
    ',"conditions":[',
    CASE
      WHEN abs(hash(seed, 'cond')) % 100 < 30 THEN
        concat(
          '{"code":"E11.9","description":"Type 2 diabetes mellitus","onset_date":"',
          cast(date_add('2018-01-01', abs(hash(seed, 'cdt1')) % 1500) AS STRING),
          '","status":"active"}',
          CASE WHEN abs(hash(seed, 'cond2')) % 2 = 0 THEN
            concat(',{"code":"I10","description":"Essential hypertension","onset_date":"',
              cast(date_add('2016-01-01', abs(hash(seed, 'cdt2')) % 2000) AS STRING),
              '","status":"active"}')
          ELSE '' END,
          CASE WHEN abs(hash(seed, 'cond3')) % 100 < 30 THEN
            concat(',{"code":"E78.5","description":"Hyperlipidemia","onset_date":"',
              cast(date_add('2019-01-01', abs(hash(seed, 'cdt3')) % 1200) AS STRING),
              '","status":"active"}')
          ELSE '' END
        )
      WHEN abs(hash(seed, 'cond')) % 100 < 50 THEN
        concat(
          '{"code":"I10","description":"Essential hypertension","onset_date":"',
          cast(date_add('2017-01-01', abs(hash(seed, 'cdt4')) % 1800) AS STRING),
          '","status":"active"}',
          CASE WHEN abs(hash(seed, 'cond4')) % 100 < 40 THEN
            concat(',{"code":"I25.10","description":"Coronary artery disease","onset_date":"',
              cast(date_add('2019-06-01', abs(hash(seed, 'cdt5')) % 1000) AS STRING),
              '","status":"active"}')
          ELSE '' END
        )
      WHEN abs(hash(seed, 'cond')) % 100 < 70 THEN
        concat(
          '{"code":"J44.1","description":"COPD with acute exacerbation","onset_date":"',
          cast(date_add('2020-01-01', abs(hash(seed, 'cdt6')) % 800) AS STRING),
          '","status":"active"}'
        )
      ELSE ''
    END,
    ']',
    ',"programs_enrolled":[',
    concat_ws(',',
      CASE WHEN abs(hash(seed, 'p1')) % 100 < 25 THEN '"CHF_Management"' END,
      CASE WHEN abs(hash(seed, 'p2')) % 100 < 30 THEN '"DM2_Care"' END,
      CASE WHEN abs(hash(seed, 'p3')) % 100 < 20 THEN '"Annual_Wellness"' END,
      CASE WHEN abs(hash(seed, 'p4')) % 100 < 15 THEN '"COPD_Support"' END,
      CASE WHEN abs(hash(seed, 'p5')) % 100 < 10 THEN '"Behavioral_Health"' END,
      CASE WHEN abs(hash(seed, 'p6')) % 100 < 12 THEN '"Fall_Prevention"' END
    ),
    ']',
    ',"engagement_history":[',
    CASE
      WHEN abs(hash(seed, 'eng')) % 100 < 80 THEN
        concat(
          '{"event_type":"outreach_call","channel":"phone","timestamp":"',
          cast(date_add('2024-01-01', abs(hash(seed, 'et1')) % 300) AS STRING),
          'T', lpad(cast(abs(hash(seed, 'eh1')) % 12 + 8 AS STRING), 2, '0'), ':',
          lpad(cast(abs(hash(seed, 'em1')) % 60 AS STRING), 2, '0'), ':00Z',
          '","outcome":"', CASE abs(hash(seed, 'eo1')) % 4
            WHEN 0 THEN 'completed' WHEN 1 THEN 'voicemail' WHEN 2 THEN 'no_answer' ELSE 'completed' END,
          '","agent_id":"AGT-', lpad(cast(abs(hash(seed, 'ea1')) % 50 + 1 AS STRING), 3, '0'), '"}',
          CASE WHEN abs(hash(seed, 'eng2')) % 100 < 60 THEN
            concat(',{"event_type":"', CASE WHEN abs(hash(seed, 'ety2')) % 2 = 0 THEN 'portal_message' ELSE 'sms_reminder' END,
              '","channel":"', CASE WHEN abs(hash(seed, 'ety2')) % 2 = 0 THEN 'portal' ELSE 'sms' END,
              '","timestamp":"',
              cast(date_add('2024-03-01', abs(hash(seed, 'et2')) % 200) AS STRING),
              'T14:30:00Z","outcome":"delivered","agent_id":null}')
          ELSE '' END,
          CASE WHEN abs(hash(seed, 'eng3')) % 100 < 30 THEN
            concat(',{"event_type":"care_mgmt_touch","channel":"phone","timestamp":"',
              cast(date_add('2024-06-01', abs(hash(seed, 'et3')) % 150) AS STRING),
              'T10:00:00Z","outcome":"completed","agent_id":"CM-',
              lpad(cast(abs(hash(seed, 'ea3')) % 20 + 1 AS STRING), 3, '0'), '"}')
          ELSE '' END
        )
      ELSE ''
    END,
    ']',
    ',"preferred_communication":{"channel":"',
    CASE abs(hash(seed, 'pcc')) % 4
      WHEN 0 THEN 'phone' WHEN 1 THEN 'email' WHEN 2 THEN 'portal' ELSE 'sms' END,
    '","time_of_day":"', CASE abs(hash(seed, 'tod')) % 3
      WHEN 0 THEN 'morning' WHEN 1 THEN 'afternoon' ELSE 'evening' END,
    '","opt_out_sms":', CASE WHEN abs(hash(seed, 'oos')) % 100 < 10 THEN 'true' ELSE 'false' END,
    '}}'
  ) AS profile_json
FROM names_data
