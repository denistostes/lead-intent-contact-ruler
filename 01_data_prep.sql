-- 01_data_prep.sql
-- Purpose: Prepare normalized dialer + DW base for Lead Intent & Contact Ruler analytics.
-- Privacy: all schemas and sensitive business labels are anonymized.

SET spark.sql.session.timeZone = America/Sao_Paulo;
SET spark.sql.legacy.timeParserPolicy = LEGACY;

CREATE OR REPLACE TEMP VIEW vw_campaign_map AS
SELECT * FROM VALUES
  (158,'WAVE3_CORE_PRODUCT_A_BE','Core_Product_A','primary',16),
  (157,'WAVE3_CORE_PRODUCT_A_ZD','Core_Product_A','primary',16),
  (161,'WAVE3_CORE_PRODUCT_A_SS','Core_Product_A','primary',16),
  (227,'WAVE3_CORE_PRODUCT_A_BH','Core_Product_A','primary',16),
  (162,'WAVE3_CORE_PRODUCT_C','Core_Product_C','primary',16),
  (176,'WAVE3_CORE_PRODUCT_B','Core_Product_B','primary',16),
  (238,'WAVE3_CORE_PRODUCT_B_BE','Core_Product_B','primary',16),
  (239,'WAVE3_CORE_PRODUCT_B_ZD','Core_Product_B','primary',16),
  (167,'WAVE3_REPROCESS','Core_Product_D','secondary',20),
  (231,'WAVE3_PRIME_REPROCESS','Core_Product_E','secondary',20),
  (235,'WAVE3_GROWTH','Core_Product_F','secondary',20),
  (64,'MANUAL_A','Manual', 'manual',20),
  (70,'MANUAL_B','Manual', 'manual',20)
AS t(id_campaign,campaign_name,macro_product,campaign_group,attempt_cap);

CREATE OR REPLACE TEMP VIEW cfg_capacity_params AS
SELECT
  DATE '2025-10-01' AS dt_inicio_attempts,
  DATE '2025-01-01' AS dt_inicio_obt;

CREATE OR REPLACE TEMP VIEW users_cte AS
WITH base AS (
  SELECT
    id_agent,
    user_name,
    login,
    email,
    lower(split(email, '@')[1]) AS email_domain,
    lower(email) AS email_norm
  FROM telephony_system.users
)
SELECT
  id_agent,
  MAX(user_name) AS user_name,
  MAX(login) AS login,
  MAX(email) AS email,
  MAX(email_norm) AS email_norm,
  MAX(
    CASE
      WHEN email IS NOT NULL THEN 'Partner_BPO_1'
      ELSE 'UNKNOWN'
    END
  ) AS sales_company
FROM base
GROUP BY id_agent;

CREATE OR REPLACE TEMP VIEW dim_user_email AS
SELECT
  lower(email) AS email_norm,
  MAX(sk_user) AS sk_user
FROM data_warehouse.dim_user
WHERE email IS NOT NULL
GROUP BY lower(email);

CREATE OR REPLACE TEMP VIEW cluster_metadata AS
SELECT id_lead, id_campaign, joker_14 AS cluster_original, ts_imported
FROM (
  SELECT
    id_lead,
    id_campaign,
    joker_14,
    ts_imported,
    ROW_NUMBER() OVER (PARTITION BY id_lead, id_campaign ORDER BY ts_imported DESC) AS rn
  FROM telephony_system.mailing
) t
WHERE rn = 1;

CREATE OR REPLACE TEMP VIEW base_inteligencia_dialer AS
WITH params AS (
  SELECT * FROM cfg_capacity_params
),
attempts AS (
  SELECT
    a.id_agent,
    a.id_attempts_raw_data,
    a.id_call,
    a.id_campaign,
    a.id_lead,
    a.id_disposition,
    a.route,
    a.table_name,
    a.ts_started,
    a.ts_call_ended,
    a.ts_wrap_ended,
    CAST(a.ts_started AS TIMESTAMP) AS call_datetime,
    CAST(a.ts_started AS DATE) AS call_date,
    HOUR(CAST(a.ts_started AS TIMESTAMP)) AS call_hour,
    GREATEST(
      CAST(
        unix_timestamp(CAST(COALESCE(a.ts_wrap_ended, a.ts_call_ended) AS TIMESTAMP))
        - unix_timestamp(CAST(a.ts_started AS TIMESTAMP))
        AS INT
      ),
      0
    ) AS handle_seconds
  FROM telephony_system.attempts_raw_data a
  CROSS JOIN params p
  WHERE a.id_campaign IN (64, 70, 157, 158, 161, 162, 167, 176, 227, 231, 235, 238, 239)
    AND CAST(a.ts_started AS DATE) >= p.dt_inicio_attempts
),
ids_attempts AS (
  SELECT DISTINCT id_lead FROM attempts
),
obt_filtered AS (
  SELECT
    obt.sk_lead,
    obt.nm_business_context,
    obt.company_report_origin,
    obt.planning_cluster AS planning_cluster_obt,
    obt.planning_operation,
    obt.planning_conversion_cluster,
    obt.cd_funnel_step,
    CAST(obt.date AS DATE) AS prospect_date,
    ROW_NUMBER() OVER (
      PARTITION BY obt.sk_lead
      ORDER BY
        CASE WHEN obt.cd_funnel_step = 'prospect' THEN 1 ELSE 2 END,
        CAST(obt.date AS DATE) ASC
    ) AS rn
  FROM data_warehouse.obt_supply obt
  CROSS JOIN params p
  JOIN ids_attempts ia ON ia.id_lead = obt.sk_lead
  WHERE obt.cd_funnel_step IN ('prospect', 'lead')
    AND CAST(obt.date AS DATE) >= p.dt_inicio_obt
),
obt_ctx AS (
  SELECT
    sk_lead,
    nm_business_context,
    company_report_origin,
    planning_cluster_obt,
    planning_operation,
    planning_conversion_cluster,
    prospect_date
  FROM obt_filtered
  WHERE rn = 1
),
obt_opp AS (
  SELECT
    o.sk_lead AS id_lead,
    o.sk_user_conversion,
    CAST(MIN(o.date) AS DATE) AS first_opp_date
  FROM data_warehouse.obt_supply o
  CROSS JOIN params p
  JOIN ids_attempts ia ON ia.id_lead = o.sk_lead
  WHERE o.cd_funnel_step LIKE 'opportunity%'
    AND CAST(o.date AS DATE) >= p.dt_inicio_obt
  GROUP BY o.sk_lead, o.sk_user_conversion
)
SELECT
  at.*,
  cm.macro_product AS planning_cluster,
  cm.campaign_name AS cluster_original,
  c.ts_imported,
  ROW_NUMBER() OVER (PARTITION BY at.id_lead, cm.macro_product ORDER BY at.ts_started) AS n_tentativa,
  o.nm_business_context,
  o.company_report_origin,
  o.planning_cluster_obt,
  o.planning_operation,
  o.planning_conversion_cluster,
  o.prospect_date,
  CASE WHEN at.id_disposition IN (137, 141, 142, 628, 627, 626, 144) THEN 1 ELSE 0 END AS is_alo,
  CASE WHEN oo.id_lead IS NOT NULL THEN 1 ELSE 0 END AS is_opp,
  CASE WHEN at.id_disposition = 2 THEN 1 ELSE 0 END AS is_connect,
  u.email_norm,
  u.sales_company
FROM attempts at
LEFT JOIN vw_campaign_map cm ON at.id_campaign = cm.id_campaign
LEFT JOIN cluster_metadata c ON at.id_lead = c.id_lead AND at.id_campaign = c.id_campaign
LEFT JOIN obt_ctx o ON at.id_lead = o.sk_lead
LEFT JOIN obt_opp oo ON at.id_lead = oo.id_lead AND oo.first_opp_date >= CAST(at.ts_started AS DATE)
LEFT JOIN users_cte u ON at.id_agent = u.id_agent;
