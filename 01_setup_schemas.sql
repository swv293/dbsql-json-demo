-- ============================================================================
-- STEP 1: CREATE SCHEMAS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.claims_json_demo
  COMMENT 'Claims submission and adjudication data with JSON payloads for DBSQL JSON demo';

CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.member_json_demo
  COMMENT 'Member profile and engagement data with JSON payloads for DBSQL JSON demo';

CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.provider_json_demo
  COMMENT 'Provider directory and network data with VARIANT payloads for DBSQL JSON demo';

CREATE SCHEMA IF NOT EXISTS serverless_stable_swv01_catalog.ops_json_demo
  COMMENT 'Operational events and workflow data with JSON payloads for DBSQL JSON demo'
