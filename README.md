# Working with JSON in Databricks SQL

A hands-on guide to querying, extracting, and transforming semi-structured JSON data using Databricks SQL. Built for analysts transitioning from traditional Oracle/SQL Server environments where data lives in flat, normalized tables.

## What You'll Learn

| Section | Concepts | Time |
|---------|----------|------|
| [1. Setup](#1-setup) | Create schemas, tables, and sample data | 5 min |
| [2. JSON Extraction Basics](#2-json-extraction-basics) | Colon syntax, dot notation, case sensitivity, type casting | 15 min |
| [3. Working with Arrays](#3-working-with-arrays) | Array indexing, wildcards, `explode`, `from_json` | 15 min |
| [4. JSON Functions Toolkit](#4-json-functions-toolkit) | `from_json`, `get_json_object`, `json_tuple`, `schema_of_json`, NULL handling | 10 min |
| [5. The VARIANT Type](#5-the-variant-type) | `PARSE_JSON`, `variant_get`, `variant_explode`, `schema_of_variant` | 15 min |
| [6. Putting It All Together](#6-putting-it-all-together) | Cross-table joins with JSON + VARIANT extraction | 10 min |

## The Data

Four tables simulate a healthcare payer's data landscape — claims, members, providers, and operational events. Each has a JSON column alongside traditional columns, just like real-world data that arrives from REST APIs, FHIR feeds, and operational systems.

| Schema | Table | Rows | JSON Column | Type | What's Inside |
|--------|-------|------|-------------|------|---------------|
| `claims_json_demo` | `claims_submissions` | 500 | `claim_json` | STRING | Subscriber, billing provider, service line arrays, adjudication |
| `member_json_demo` | `member_profiles` | 300 | `profile_json` | STRING | Demographics, risk scores, SDoH flags, conditions, engagement history |
| `provider_json_demo` | `provider_network` | 200 | `provider_data` | **VARIANT** | Name, specialties, addresses, languages, network participation, contract |
| `ops_json_demo` | `operational_events` | 400 | `event_json` | STRING | Status, priority, SLA tracking, status history, resolution |

---

## 1. Setup

Run the SQL files in order to create schemas and load data in your own workspace. Update the catalog name to match yours.

```sql
-- 01_setup_schemas.sql   → Creates 4 schemas
-- 02_claims_submissions.sql → 500 claims with service line arrays
-- 03_member_profiles.sql    → 300 members with nested demographics and engagement
-- 04_provider_network.sql   → 200 providers using the VARIANT type
-- 05_operational_events.sql → 400 operational events with SLA tracking
```

Verify everything loaded:

```sql
SELECT 'claims' AS table_name, count(*) AS rows FROM claims_json_demo.claims_submissions
UNION ALL SELECT 'members', count(*) FROM member_json_demo.member_profiles
UNION ALL SELECT 'providers', count(*) FROM provider_json_demo.provider_network
UNION ALL SELECT 'ops_events', count(*) FROM ops_json_demo.operational_events;
```

---

## 2. JSON Extraction Basics

> **File:** `06_json_string_basics.sql`

### The Colon Operator

The `:` operator is the primary way to extract fields from a JSON string column. Think of it as "inside this column, give me this field."

```sql
SELECT
  claim_id,
  claim_json:claim_number            AS claim_number,
  claim_json:filing_code             AS filing_code,
  claim_json:coordination_of_benefits AS cob_flag
FROM claims_json_demo.claims_submissions
LIMIT 10;
```

### Dot Notation for Nested Objects

Use dots to navigate into nested objects — subscriber info, billing provider details, etc.

```sql
SELECT
  claim_id,
  claim_json:subscriber.member_id       AS member_id,
  claim_json:subscriber.group_number    AS group_number,
  claim_json:billing_provider.npi       AS billing_npi,
  claim_json:billing_provider.name      AS billing_provider_name
FROM claims_json_demo.claims_submissions
LIMIT 10;
```

### Case Sensitivity

Colon paths are **case-insensitive**. Bracket paths `['field']` are **case-sensitive**. This matters when working with data from external systems where field names may vary.

```sql
-- All three return the same value
SELECT
  claim_json:subscriber.member_id    AS lowercase,
  claim_json:SUBSCRIBER.MEMBER_ID   AS uppercase,
  claim_json:Subscriber.Member_Id   AS mixed
FROM claims_json_demo.claims_submissions LIMIT 3;

-- Brackets are case-sensitive — wrong case returns NULL
SELECT
  claim_json:['subscriber']  AS correct_case,
  claim_json:['SUBSCRIBER']  AS wrong_case_returns_null
FROM claims_json_demo.claims_submissions LIMIT 3;
```

### Type Casting

By default, everything extracted from JSON comes back as a **string**. You must cast to the right type for math, filtering, and comparisons. This is the most common mistake new users make.

```sql
-- See the raw type
SELECT
  claim_json:adjudication.paid_amount,
  typeof(claim_json:adjudication.paid_amount) AS raw_type
FROM claims_json_demo.claims_submissions LIMIT 3;

-- Cast to proper types for real analytics
SELECT
  claim_id,
  claim_json:adjudication.paid_amount::double        AS paid_amount,
  claim_json:adjudication.allowed_amount::double      AS allowed_amount,
  claim_json:coordination_of_benefits::boolean        AS has_cob,
  round(claim_json:adjudication.paid_amount::double /
    NULLIF(claim_json:adjudication.allowed_amount::double, 0) * 100, 1
  ) AS pct_of_allowed
FROM claims_json_demo.claims_submissions
WHERE claim_json:adjudication.status::string = 'paid'
  AND claim_json:adjudication.paid_amount::double > 500
ORDER BY paid_amount DESC
LIMIT 10;
```

**Available casts:** `::string`, `::double`, `::int`, `::boolean`, `::date`, `::timestamp`

---

## 3. Working with Arrays

> **File:** `07_json_arrays_and_nesting.sql`

In traditional databases, a claim's service lines would be in a child table. In JSON, they're embedded as an array. Here's how to work with them.

### Array Indexing

Arrays are zero-based. Access specific elements with `[0]`, `[1]`, etc. If the index doesn't exist, you get NULL — no error.

```sql
SELECT
  claim_id,
  claim_json:service_lines[0].procedure_code       AS first_cpt,
  claim_json:service_lines[0].charge_amount::double AS first_charge,
  claim_json:service_lines[1].procedure_code       AS second_cpt
FROM claims_json_demo.claims_submissions
LIMIT 10;
```

### Wildcard Extraction

The `[*]` wildcard pulls a field from **every** element in the array and returns a JSON array of those values.

```sql
SELECT
  claim_id,
  claim_json:service_lines[*].procedure_code  AS all_cpts,
  claim_json:service_lines[*].charge_amount   AS all_charges
FROM claims_json_demo.claims_submissions
LIMIT 10;
```

Use it with `array_contains` to search inside arrays:

```sql
-- Find all claims that include CPT 99214
SELECT claim_id, claim_type, total_charge_amount
FROM claims_json_demo.claims_submissions
WHERE array_contains(
  from_json(claim_json:service_lines[*].procedure_code, 'ARRAY<STRING>'),
  '99214'
);
```

### Exploding Arrays into Rows

This is the most powerful technique. `explode` + `from_json` takes an array and creates **one row per element** — the equivalent of joining to a child table.

**Step 1 — Discover the schema:**

```sql
SELECT schema_of_json(claim_json:service_lines) AS inferred_schema
FROM claims_json_demo.claims_submissions LIMIT 1;
```

Copy the output — you'll use it as the schema string in Step 2.

**Step 2 — Explode into rows:**

```sql
SELECT
  c.claim_id, c.claim_type,
  sl.procedure_code, sl.charge_amount, sl.place_of_service, sl.modifiers
FROM claims_json_demo.claims_submissions c
LATERAL VIEW OUTER explode(
  from_json(
    c.claim_json:service_lines,
    'ARRAY<STRUCT<line_number:INT, procedure_code:STRING, modifiers:ARRAY<STRING>,
     diagnosis_pointers:ARRAY<STRING>, units:INT, charge_amount:DOUBLE,
     place_of_service:STRING, date_of_service:STRING,
     rendering_provider_npi:STRING, revenue_code:STRING>>'
  )
) sl_tbl AS sl
LIMIT 20;
```

> `LATERAL VIEW OUTER` is like a LEFT JOIN — claims with empty arrays still appear with NULLs.

**Step 3 — Aggregate:**

```sql
-- Top CPT codes by volume and charge
SELECT
  sl.procedure_code,
  count(*)                        AS line_count,
  round(sum(sl.charge_amount), 2) AS total_charges,
  round(avg(sl.charge_amount), 2) AS avg_charge
FROM claims_json_demo.claims_submissions c
LATERAL VIEW explode(
  from_json(c.claim_json:service_lines,
    'ARRAY<STRUCT<procedure_code:STRING, charge_amount:DOUBLE>>')
) sl_tbl AS sl
GROUP BY sl.procedure_code
ORDER BY line_count DESC;
```

> You only need to declare the fields you actually use in the schema string.

### Exploding Member Engagement Data

Same pattern works on any nested array. Here's outreach effectiveness from member engagement history:

```sql
SELECT
  eng.channel, eng.outcome,
  count(*) AS touch_count,
  count(DISTINCT m.member_id) AS unique_members
FROM member_json_demo.member_profiles m
LATERAL VIEW explode(
  from_json(m.profile_json:engagement_history,
    'ARRAY<STRUCT<event_type:STRING, channel:STRING, timestamp:STRING,
     outcome:STRING, agent_id:STRING>>')
) eng_tbl AS eng
WHERE eng.event_type IS NOT NULL
GROUP BY eng.channel, eng.outcome
ORDER BY eng.channel, touch_count DESC;
```

---

## 4. JSON Functions Toolkit

> **File:** `08_json_functions_toolkit.sql`

### `from_json` — Parse into a Struct

When you need multiple fields from the same nested object, parse it once into a struct. Then use normal `.field` syntax.

```sql
SELECT claim_id, adj.status, adj.paid_amount, adj.denial_codes
FROM (
  SELECT claim_id,
    from_json(claim_json:adjudication,
      'STRUCT<status:STRING, paid_amount:DOUBLE, allowed_amount:DOUBLE,
       denial_codes:ARRAY<STRING>, remark_codes:ARRAY<STRING>>') AS adj
  FROM claims_json_demo.claims_submissions
)
WHERE adj.status = 'denied'
LIMIT 10;
```

### `get_json_object` — JSONPath Syntax

If you're used to Oracle's JSON functions, `get_json_object` uses `$.field` path syntax that will feel familiar. It's functionally equivalent to colon syntax.

```sql
SELECT
  claim_id,
  claim_json:subscriber.member_id                          AS colon_syntax,
  get_json_object(claim_json, '$.subscriber.member_id')    AS jsonpath_syntax
FROM claims_json_demo.claims_submissions LIMIT 5;
```

> Colon syntax is recommended for Databricks — it's cleaner. But `get_json_object` is available for cross-engine compatibility.

### `json_tuple` — Extract Multiple Fields at Once

Avoids re-parsing the JSON for each field. Efficient when you need several top-level values.

```sql
SELECT e.event_id, e.event_type, jt.status, jt.priority, jt.assigned_to
FROM ops_json_demo.operational_events e
LATERAL VIEW json_tuple(
  e.event_json, 'status', 'priority', 'assigned_to'
) jt AS status, priority, assigned_to
LIMIT 10;
```

### `schema_of_json` — Discover Unknown Structures

When you receive a new data feed and don't know the JSON shape, this infers the schema for you. The output is copy-paste ready for `from_json`.

```sql
SELECT schema_of_json(claim_json) FROM claims_json_demo.claims_submissions LIMIT 1;
SELECT schema_of_json(profile_json) FROM member_json_demo.member_profiles LIMIT 1;
SELECT schema_of_json(event_json) FROM ops_json_demo.operational_events
  WHERE event_type = 'prior_auth' LIMIT 1;
```

### NULL Behavior

Important subtlety — both a **missing key** and an **explicit JSON null** become SQL NULL. An **empty string** is NOT null.

```sql
SELECT
  get_json_object('{"key": null}', '$.key') IS NULL    AS json_null_is_sql_null,
  get_json_object('{"other": 1}', '$.key') IS NULL     AS missing_key_is_sql_null,
  get_json_object('{"key": ""}', '$.key')              AS empty_string_value,
  get_json_object('{"key": ""}', '$.key') IS NULL      AS empty_string_is_not_null;
```

---

## 5. The VARIANT Type

> **File:** `09_variant_type.sql`

Everything above uses STRING columns with JSON text. The **VARIANT** type is purpose-built for semi-structured data and changes the game.

### Why VARIANT over STRING?

| | STRING JSON | VARIANT |
|---|---|---|
| Parsing | Re-parsed on **every query** | Parsed **once** at write time |
| Validation | Errors at query time | Bad JSON caught at ingest |
| Type handling | Everything returns as string | Returns native types |
| Performance | Re-parse overhead on large data | Optimized binary format |

```sql
-- See that provider_data is VARIANT, not STRING
SELECT provider_id, typeof(provider_data) AS data_type
FROM provider_json_demo.provider_network LIMIT 3;
```

### `PARSE_JSON` and `TRY_PARSE_JSON`

This is how data gets into a VARIANT column. `TRY_PARSE_JSON` returns NULL for malformed JSON instead of raising an error — use it in production pipelines.

```sql
SELECT PARSE_JSON('{"name": "Dr. Smith", "npi": "1234567890"}') AS parsed;

SELECT
  TRY_PARSE_JSON('{"valid": true}')  AS good_json,
  TRY_PARSE_JSON('{ bad json }')     AS returns_null;
```

### Querying VARIANT — Same Syntax

The colon operator works identically on VARIANT columns. The difference is under the hood — no re-parsing.

```sql
SELECT
  provider_id, provider_type,
  provider_data:name.first::string           AS first_name,
  provider_data:name.last::string            AS last_name,
  provider_data:name.credentials::string     AS credentials,
  provider_data:name.organization_name::string AS org_name
FROM provider_json_demo.provider_network
LIMIT 10;
```

### `variant_get` — Explicit Path and Type

`variant_get` gives you explicit control over the extraction path (JSONPath) and return type. `try_variant_get` is the safe version.

```sql
SELECT
  provider_id,
  variant_get(provider_data, '$.specialties[0].description', 'STRING') AS specialty,
  variant_get(provider_data, '$.specialties[0].board_certified', 'BOOLEAN') AS board_cert,
  variant_get(provider_data, '$.contract.discount_pct', 'DOUBLE') AS discount
FROM provider_json_demo.provider_network LIMIT 10;

-- Safe version: NULL for missing paths instead of error
SELECT
  provider_id,
  try_variant_get(provider_data, '$.specialties[1].description', 'STRING') AS second_specialty,
  try_variant_get(provider_data, '$.nonexistent.path', 'STRING') AS returns_null
FROM provider_json_demo.provider_network LIMIT 10;
```

### `variant_explode` — Flatten VARIANT Arrays

The VARIANT-native equivalent of `explode`. Returns `(pos, key, value)` columns — for arrays, `key` is NULL.

```sql
-- Explode network participation: one row per provider-plan combination
SELECT
  p.provider_id,
  p.provider_data:name.last::string           AS provider_name,
  np.value:plan_code::string                  AS plan_code,
  np.value:network_tier::string               AS network_tier,
  np.value:accepting_new_patients::boolean    AS accepting_new
FROM provider_json_demo.provider_network p,
LATERAL variant_explode(p.provider_data:network_participation) np
WHERE p.provider_type = 'individual'
LIMIT 20;
```

> No `from_json` schema string needed — VARIANT already knows the structure.

### `schema_of_variant` and `schema_of_variant_agg`

Inspect VARIANT structure. The `_agg` version unions all rows — essential when JSON shape varies (e.g., individual vs. organization providers).

```sql
-- Single row
SELECT schema_of_variant(provider_data) FROM provider_json_demo.provider_network LIMIT 3;

-- Union of all rows
SELECT schema_of_variant_agg(provider_data) FROM provider_json_demo.provider_network;
```

### Combining It All: Real Provider Query

Find all individual providers accepting new Medicare Advantage patients in Tier 1:

```sql
SELECT
  p.npi,
  p.provider_data:name.first::string AS first_name,
  p.provider_data:name.last::string AS last_name,
  variant_get(p.provider_data, '$.specialties[0].description', 'STRING') AS specialty,
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
```

---

## 6. Putting It All Together

> **File:** `10_capstone_join.sql`

This query joins all four tables — extracting from STRING JSON, extracting from VARIANT, exploding arrays, and aggregating — to produce one flat, analytics-ready dataset.

```sql
WITH
-- Explode claim service lines
claim_lines AS (
  SELECT
    c.claim_id, c.submitted_date, c.claim_type, c.total_charge_amount,
    c.claim_json:subscriber.member_id::string       AS member_id,
    c.claim_json:filing_code::string                AS filing_code,
    c.claim_json:adjudication.status::string        AS adj_status,
    c.claim_json:adjudication.paid_amount::double   AS paid_amount,
    c.claim_json:adjudication.allowed_amount::double AS allowed_amount,
    c.claim_json:billing_provider.name::string      AS billing_provider_name,
    sl.procedure_code, sl.charge_amount AS line_charge,
    sl.place_of_service, sl.rendering_provider_npi, sl.modifiers, sl.revenue_code
  FROM claims_json_demo.claims_submissions c
  LATERAL VIEW explode(
    from_json(c.claim_json:service_lines,
      'ARRAY<STRUCT<line_number:INT, procedure_code:STRING, modifiers:ARRAY<STRING>,
       charge_amount:DOUBLE, place_of_service:STRING,
       rendering_provider_npi:STRING, revenue_code:STRING>>')
  ) sl_tbl AS sl
),

-- Extract member profile details
member_details AS (
  SELECT
    member_id, line_of_business, plan_code,
    profile_json:demographics.first_name::string     AS first_name,
    profile_json:demographics.last_name::string      AS last_name,
    profile_json:demographics.address.city::string   AS city,
    profile_json:risk_scores.hcc_score::double       AS hcc_score,
    profile_json:risk_scores.sdoh_risk_level::string AS sdoh_risk,
    profile_json:sdoh_flags.food_insecurity::boolean AS food_insecurity,
    size(from_json(profile_json:conditions, 'ARRAY<STRUCT<code:STRING>>')) AS condition_count,
    size(from_json(profile_json:programs_enrolled, 'ARRAY<STRING>'))       AS program_count
  FROM member_json_demo.member_profiles
),

-- Explode provider VARIANT for network participation
provider_details AS (
  SELECT
    p.npi, p.provider_type,
    COALESCE(p.provider_data:name.last::string,
             p.provider_data:name.organization_name::string) AS provider_name,
    variant_get(p.provider_data, '$.specialties[0].description', 'STRING') AS specialty,
    p.provider_data:contract.contract_type::string  AS contract_type,
    np.value:plan_code::string                      AS plan_code,
    np.value:network_tier::string                   AS network_tier,
    np.value:accepting_new_patients::boolean        AS accepting_new
  FROM provider_json_demo.provider_network p,
  LATERAL variant_explode(p.provider_data:network_participation) np
),

-- Aggregate operational events per claim
ops_summary AS (
  SELECT
    related_claim_id AS claim_id,
    count(*) AS event_count,
    count_if(event_json:sla.met::boolean = false) AS sla_misses,
    max(event_json:status::string) AS latest_event_status
  FROM ops_json_demo.operational_events
  WHERE related_claim_id IS NOT NULL
  GROUP BY related_claim_id
)

-- Join everything
SELECT
  cl.claim_id, cl.submitted_date, cl.claim_type,
  cl.procedure_code, cl.line_charge, cl.adj_status, cl.paid_amount,
  md.first_name, md.last_name, md.line_of_business,
  md.hcc_score, md.sdoh_risk, md.condition_count,
  pd.provider_name AS rendering_provider, pd.specialty, pd.network_tier,
  COALESCE(os.event_count, 0) AS related_events,
  COALESCE(os.sla_misses, 0) AS sla_misses
FROM claim_lines cl
LEFT JOIN member_details md ON cl.member_id = md.member_id
LEFT JOIN provider_details pd ON cl.rendering_provider_npi = pd.npi AND md.plan_code = pd.plan_code
LEFT JOIN ops_summary os ON cl.claim_id = os.claim_id
ORDER BY cl.submitted_date DESC, cl.claim_id
LIMIT 50;
```

### Bonus: Analytics on the Extracted Data

Once you can extract and join JSON data, standard SQL analytics work normally:

```sql
-- Denial rate by SDoH risk level
WITH claim_data AS (
  SELECT c.claim_id,
    c.claim_json:subscriber.member_id::string AS member_id,
    c.claim_json:adjudication.status::string  AS adj_status
  FROM claims_json_demo.claims_submissions c
)
SELECT
  m.line_of_business,
  m.profile_json:risk_scores.sdoh_risk_level::string AS sdoh_risk,
  count(DISTINCT cd.claim_id) AS total_claims,
  count(DISTINCT CASE WHEN cd.adj_status = 'denied' THEN cd.claim_id END) AS denied,
  round(count(DISTINCT CASE WHEN cd.adj_status = 'denied' THEN cd.claim_id END)
    * 100.0 / NULLIF(count(DISTINCT cd.claim_id), 0), 1) AS denial_rate_pct
FROM claim_data cd
JOIN member_json_demo.member_profiles m ON cd.member_id = m.member_id
GROUP BY 1, 2 ORDER BY 1, 2;
```

---

## Quick Reference

| Syntax | What It Does | Example |
|--------|-------------|---------|
| `col:field` | Extract top-level field | `claim_json:filing_code` |
| `col:a.b.c` | Navigate nested objects | `claim_json:subscriber.member_id` |
| `col:['Field']` | Case-sensitive extraction | `claim_json:['subscriber']` |
| `col:arr[0]` | Access array element | `claim_json:service_lines[0]` |
| `col:arr[*].f` | Extract field from all elements | `claim_json:service_lines[*].procedure_code` |
| `::type` | Cast extracted value | `claim_json:adjudication.paid_amount::double` |
| `from_json(col, schema)` | Parse JSON into struct/array | `from_json(col:field, 'STRUCT<...>')` |
| `explode()` | Flatten array into rows | `LATERAL VIEW explode(from_json(...))` |
| `get_json_object(col, path)` | JSONPath-style extraction | `get_json_object(col, '$.field')` |
| `json_tuple(col, keys...)` | Extract multiple top-level fields | `LATERAL VIEW json_tuple(col, 'a', 'b')` |
| `schema_of_json(col)` | Infer JSON schema | Returns DDL-style schema string |
| `PARSE_JSON(str)` | Convert string to VARIANT | `PARSE_JSON('{"a": 1}')` |
| `TRY_PARSE_JSON(str)` | Safe VARIANT conversion | Returns NULL for bad JSON |
| `variant_get(v, path, type)` | Extract from VARIANT with type | `variant_get(col, '$.field', 'STRING')` |
| `try_variant_get(...)` | Safe VARIANT extraction | Returns NULL on missing/mismatch |
| `variant_explode(v)` | Flatten VARIANT array | `LATERAL variant_explode(col:arr)` |
| `schema_of_variant(v)` | Inspect single VARIANT value | Returns OBJECT/ARRAY schema |
| `schema_of_variant_agg(v)` | Aggregate schema across rows | Union of all row schemas |

## Files

| File | Description |
|------|-------------|
| `01_setup_schemas.sql` | Create the four `*_json_demo` schemas |
| `02_claims_submissions.sql` | Claims table DDL + 500 rows of synthetic data |
| `03_member_profiles.sql` | Member profiles DDL + 300 rows |
| `04_provider_network.sql` | Provider network DDL + 200 rows (VARIANT column) |
| `05_operational_events.sql` | Operational events DDL + 400 rows |
| `06_json_string_basics.sql` | Extraction fundamentals: colon, dots, casting |
| `07_json_arrays_and_nesting.sql` | Arrays: indexing, wildcards, explode |
| `08_json_functions_toolkit.sql` | from_json, get_json_object, json_tuple, schema_of_json |
| `09_variant_type.sql` | VARIANT deep-dive: parse, extract, explode, inspect |
| `10_capstone_join.sql` | Cross-table join producing a flat analytics dataset |

## Further Reading

- [Query JSON strings in Databricks SQL](https://learn.microsoft.com/en-us/azure/databricks/semi-structured/json)
- [VARIANT type documentation](https://docs.databricks.com/en/sql/language-manual/data-types/variant-type.html)
- [Semi-structured data functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html#json-functions)
