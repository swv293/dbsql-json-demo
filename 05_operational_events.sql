-- ============================================================================
-- TABLE: ops_json_demo.operational_events
-- Complexity: LOWER — good starter table, flat JSON with simple arrays
-- ============================================================================

DROP TABLE IF EXISTS serverless_stable_swv01_catalog.ops_json_demo.operational_events;

CREATE TABLE serverless_stable_swv01_catalog.ops_json_demo.operational_events (
  event_id          STRING      COMMENT 'Unique event identifier (EVT-NNNNNNNN)',
  event_type        STRING      COMMENT 'prior_auth, appeal, grievance, or call_center',
  source_system     STRING      COMMENT 'Originating system name',
  event_timestamp   TIMESTAMP   COMMENT 'When the event occurred',
  related_claim_id  STRING      COMMENT 'FK to claims_submissions.claim_id (nullable)',
  related_member_id STRING      COMMENT 'FK to member_profiles.member_id',
  event_json        STRING      COMMENT 'Event payload JSON: status, priority, assigned_to, requested_service object, status_history array, sla object, resolution object'
) COMMENT 'Operational events from UM, appeals, grievances, and call center with JSON status histories and SLA tracking';

-- Generate 400 events using hash() for deterministic pseudo-random values
INSERT INTO serverless_stable_swv01_catalog.ops_json_demo.operational_events
WITH event_ids AS (
  SELECT explode(sequence(1, 400)) AS rn
),
base AS (
  SELECT
    rn,
    concat('EVT-', lpad(cast(rn AS STRING), 8, '0')) AS event_id,
    CASE abs(hash(rn, 'type')) % 4
      WHEN 0 THEN 'prior_auth'
      WHEN 1 THEN 'appeal'
      WHEN 2 THEN 'grievance'
      ELSE 'call_center'
    END AS event_type,
    CASE abs(hash(rn, 'src')) % 4
      WHEN 0 THEN 'UM_Platform' WHEN 1 THEN 'CRM_System'
      WHEN 2 THEN 'Appeals_Tracker' ELSE 'Call_Center_IVR'
    END AS source_system,
    cast(
      concat(
        cast(date_add('2024-01-01', abs(hash(rn, 'dt')) % 365) AS STRING),
        ' ',
        lpad(cast(abs(hash(rn, 'hr')) % 12 + 7 AS STRING), 2, '0'), ':',
        lpad(cast(abs(hash(rn, 'mn')) % 60 AS STRING), 2, '0'), ':',
        lpad(cast(abs(hash(rn, 'sc')) % 60 AS STRING), 2, '0')
      ) AS TIMESTAMP
    ) AS event_timestamp,
    CASE WHEN abs(hash(rn, 'clm')) % 100 < 70
      THEN concat('CLM-2024-', lpad(cast(abs(hash(rn, 'clmid')) % 500 + 1 AS STRING), 5, '0'))
      ELSE NULL
    END AS related_claim_id,
    concat('MBR-', lpad(cast(abs(hash(rn, 'mbr')) % 300 + 1 AS STRING), 6, '0')) AS related_member_id,
    rn AS seed
  FROM event_ids
)
SELECT
  event_id,
  event_type,
  source_system,
  event_timestamp,
  related_claim_id,
  related_member_id,
  concat(
    '{"status":"',
    CASE abs(hash(seed, 'st')) % 4
      WHEN 0 THEN 'open' WHEN 1 THEN 'in_review' WHEN 2 THEN 'resolved' ELSE 'pending'
    END,
    '","priority":"',
    CASE abs(hash(seed, 'pri')) % 3
      WHEN 0 THEN 'high' WHEN 1 THEN 'medium' ELSE 'low'
    END,
    '","assigned_to":"',
    CASE abs(hash(seed, 'asgn')) % 6
      WHEN 0 THEN 'UM_Team_A' WHEN 1 THEN 'UM_Team_B' WHEN 2 THEN 'Appeals_Unit'
      WHEN 3 THEN 'Grievance_Unit' WHEN 4 THEN 'Call_Center_Tier1' ELSE 'Call_Center_Tier2'
    END, '"',
    CASE WHEN event_type = 'prior_auth' THEN
      concat(',"requested_service":{"procedure_code":"',
        CASE abs(hash(seed, 'proc')) % 6
          WHEN 0 THEN '27447' WHEN 1 THEN '43239' WHEN 2 THEN '93306'
          WHEN 3 THEN '70553' WHEN 4 THEN '27130' ELSE '33533'
        END,
        '","description":"',
        CASE abs(hash(seed, 'proc')) % 6
          WHEN 0 THEN 'Total knee replacement' WHEN 1 THEN 'Upper GI endoscopy'
          WHEN 2 THEN 'Echocardiogram complete' WHEN 3 THEN 'Brain MRI with contrast'
          WHEN 4 THEN 'Total hip replacement' ELSE 'CABG triple bypass'
        END,
        '","medical_necessity_code":"',
        CASE abs(hash(seed, 'mn')) % 3
          WHEN 0 THEN 'MN-001' WHEN 1 THEN 'MN-002' ELSE 'MN-003'
        END, '"}')
    ELSE '' END,
    ',"status_history":[',
    '{"status":"submitted","changed_by":"system","changed_at":"',
    cast(event_timestamp AS STRING), '","notes":"Initial submission"}',
    CASE WHEN abs(hash(seed, 'sh1')) % 100 < 70 THEN
      concat(',{"status":"in_review","changed_by":"',
        CASE abs(hash(seed, 'rev')) % 4
          WHEN 0 THEN 'reviewer_jones' WHEN 1 THEN 'reviewer_smith'
          WHEN 2 THEN 'reviewer_patel' ELSE 'reviewer_kim'
        END,
        '","changed_at":"',
        cast(event_timestamp + INTERVAL '2' HOUR AS STRING),
        '","notes":"Assigned for clinical review"}')
    ELSE '' END,
    CASE WHEN abs(hash(seed, 'sh2')) % 100 < 50 THEN
      concat(',{"status":"resolved","changed_by":"',
        CASE abs(hash(seed, 'rslv')) % 3
          WHEN 0 THEN 'mgr_williams' WHEN 1 THEN 'mgr_davis' ELSE 'mgr_garcia'
        END,
        '","changed_at":"',
        cast(event_timestamp + INTERVAL '48' HOUR AS STRING),
        '","notes":"',
        CASE abs(hash(seed, 'note')) % 3
          WHEN 0 THEN 'Approved after clinical review'
          WHEN 1 THEN 'Denied - does not meet criteria'
          ELSE 'Resolved per member request'
        END, '"}')
    ELSE '' END,
    ']',
    ',"sla":{"target_hours":',
    CASE event_type
      WHEN 'prior_auth' THEN '72'
      WHEN 'appeal' THEN '720'
      WHEN 'grievance' THEN '720'
      ELSE '24'
    END,
    ',"actual_hours":',
    cast(round((abs(hash(seed, 'slah')) % 12000) / 100.0 + 1, 1) AS STRING),
    ',"met":',
    CASE WHEN abs(hash(seed, 'slam')) % 100 < 78 THEN 'true' ELSE 'false' END,
    '}',
    CASE WHEN abs(hash(seed, 'res')) % 100 < 50 THEN
      concat(',"resolution":{"outcome":"',
        CASE abs(hash(seed, 'out')) % 4
          WHEN 0 THEN 'approved' WHEN 1 THEN 'denied' WHEN 2 THEN 'withdrawn' ELSE 'approved_modified'
        END,
        '","resolved_at":"',
        cast(event_timestamp + INTERVAL '72' HOUR AS STRING),
        '","resolved_by":"',
        CASE abs(hash(seed, 'rby')) % 3
          WHEN 0 THEN 'mgr_williams' WHEN 1 THEN 'mgr_davis' ELSE 'mgr_garcia'
        END, '"}')
    ELSE ',"resolution":null' END,
    '}'
  ) AS event_json
FROM base
