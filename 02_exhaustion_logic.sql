-- 02_exhaustion_logic.sql
-- Purpose: Exhaustion, reach, and elasticity signal logic.
-- NOTE: SQRT smoothing is intentionally preserved from notebook logic.

CREATE OR REPLACE TEMPORARY VIEW engine_decisao_prospect AS
WITH base_expandida AS (
    SELECT planning_cluster, id_lead, n_tentativa, is_connect, ts_started
    FROM base_inteligencia_dialer
    WHERE CAST(ts_started AS DATE) >= DATE '2025-10-01'
      AND planning_cluster != 'Manual'

    UNION ALL

    SELECT 'Consolidated_Core' AS planning_cluster, id_lead, n_tentativa, is_connect, ts_started
    FROM base_inteligencia_dialer
    WHERE planning_cluster IN ('Core_Product_A', 'Core_Product_B', 'Core_Product_C')
      AND CAST(ts_started AS DATE) >= DATE '2025-10-01'

    UNION ALL

    SELECT 'Consolidated_All' AS planning_cluster, id_lead, n_tentativa, is_connect, ts_started
    FROM base_inteligencia_dialer
    WHERE CAST(ts_started AS DATE) >= DATE '2025-10-01'
      AND planning_cluster != 'Manual'
),
universo_leads AS (
    SELECT planning_cluster, COUNT(DISTINCT id_lead) AS total_estoque_cluster
    FROM base_expandida
    GROUP BY 1
),
base_tentativa AS (
    SELECT planning_cluster, n_tentativa, id_lead, is_connect
    FROM base_expandida
),
primeiro_sucesso AS (
    SELECT planning_cluster, id_lead, MIN(n_tentativa) AS n_primeiro_sucesso
    FROM base_tentativa
    WHERE is_connect = 1
    GROUP BY 1, 2
),
agregado_tentativa AS (
    SELECT
      b.planning_cluster,
      b.n_tentativa,
      COUNT(DISTINCT b.id_lead) AS leads_tentados_n,
      COUNT(DISTINCT CASE WHEN b.is_connect = 1 THEN b.id_lead END) AS conexoes_n,
      COUNT(DISTINCT CASE WHEN p.n_primeiro_sucesso = b.n_tentativa THEN b.id_lead END) AS novas_conexoes_unicas
    FROM base_tentativa b
    LEFT JOIN primeiro_sucesso p
      ON b.planning_cluster = p.planning_cluster
     AND b.id_lead = p.id_lead
    GROUP BY 1,2
),
metricas_brutas AS (
    SELECT a.*, u.total_estoque_cluster,
           ROUND((conexoes_n * 100.0) / NULLIF(leads_tentados_n, 0), 2) AS P2C_marginal_pct,
           ROUND(SUM(novas_conexoes_unicas) OVER (PARTITION BY a.planning_cluster ORDER BY a.n_tentativa) * 100.0 / NULLIF(u.total_estoque_cluster, 0), 2) AS P2C_acumulado_pct,
           MAX(CASE WHEN a.leads_tentados_n >= 100 THEN (a.conexoes_n * 100.0 / NULLIF(a.leads_tentados_n, 0)) END) OVER (PARTITION BY a.planning_cluster) AS raw_max_p2c,
           MAX(a.leads_tentados_n) OVER (PARTITION BY a.planning_cluster) AS max_vol_cluster
    FROM agregado_tentativa a
    JOIN universo_leads u ON a.planning_cluster = u.planning_cluster
),
score_calculado AS (
    SELECT *,
           ROUND((leads_tentados_n * 100.0) / NULLIF(max_vol_cluster, 0), 2) AS pool_prospect_pct,
           ROUND((P2C_marginal_pct / NULLIF(raw_max_p2c, 0)) * 100, 2) AS eficiencia_relativa
    FROM metricas_brutas
),
score_ponderado AS (
    SELECT *,
           ROUND((eficiencia_relativa * SQRT(pool_prospect_pct / 100.0)), 2) AS score_p2c
    FROM score_calculado
),
inteligencia_final AS (
    SELECT *,
           P2C_acumulado_pct - LAG(P2C_acumulado_pct, 1, 0) OVER (PARTITION BY planning_cluster ORDER BY n_tentativa) AS ganho_reach,
           COALESCE(MIN(CASE WHEN score_p2c < 75 THEN n_tentativa END) OVER (PARTITION BY planning_cluster), 21) AS n_start_ss,
           COALESCE(MIN(CASE WHEN score_p2c < 50 THEN n_tentativa END) OVER (PARTITION BY planning_cluster), 21) AS n_end_ss,
           MAX(P2C_acumulado_pct) OVER (PARTITION BY planning_cluster) AS max_reach_total
    FROM score_ponderado
)
SELECT * FROM inteligencia_final
ORDER BY planning_cluster, n_tentativa;

CREATE OR REPLACE TEMPORARY VIEW engine_decisao_idade AS
WITH base_expandida AS (
    SELECT planning_cluster, id_lead, is_connect, ts_started, ts_imported
    FROM base_inteligencia_dialer
    WHERE CAST(ts_started AS DATE) >= DATE '2025-10-01'
      AND planning_cluster != 'Manual'

    UNION ALL

    SELECT 'Consolidated_Core' AS planning_cluster, id_lead, is_connect, ts_started, ts_imported
    FROM base_inteligencia_dialer
    WHERE planning_cluster IN ('Core_Product_A', 'Core_Product_B', 'Core_Product_C')
      AND CAST(ts_started AS DATE) >= DATE '2025-10-01'

    UNION ALL

    SELECT 'Consolidated_All' AS planning_cluster, id_lead, is_connect, ts_started, ts_imported
    FROM base_inteligencia_dialer
    WHERE CAST(ts_started AS DATE) >= DATE '2025-10-01'
      AND planning_cluster != 'Manual'
),
universo_leads AS (
    SELECT planning_cluster, COUNT(DISTINCT id_lead) as total_estoque_cluster
    FROM base_expandida
    GROUP BY 1
),
base_diaria AS (
    SELECT planning_cluster, id_lead, is_connect,
           DATEDIFF(CAST(ts_started AS DATE), CAST(ts_imported AS DATE)) AS dias_vida
    FROM base_expandida
),
primeiro_sucesso AS (
    SELECT planning_cluster, id_lead, MIN(dias_vida) AS dia_primeiro_sucesso
    FROM base_diaria
    WHERE is_connect = 1
    GROUP BY 1,2
),
agregado_dia AS (
    SELECT b.planning_cluster, b.dias_vida,
           COUNT(DISTINCT b.id_lead) AS leads_tentados_dia,
           COUNT(DISTINCT CASE WHEN b.is_connect = 1 THEN b.id_lead END) AS conexoes_dia,
           COUNT(DISTINCT CASE WHEN p.dia_primeiro_sucesso = b.dias_vida THEN b.id_lead END) AS novas_conexoes_unicas,
           ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT b.id_lead),0),2) AS avg_tentativas_por_idade
    FROM base_diaria b
    LEFT JOIN primeiro_sucesso p
      ON b.planning_cluster = p.planning_cluster AND b.id_lead = p.id_lead
    GROUP BY 1,2
),
metricas_brutas AS (
    SELECT a.*, u.total_estoque_cluster,
           ROUND((conexoes_dia * 100.0) / NULLIF(leads_tentados_dia, 0), 2) AS P2C_marginal_pct,
           ROUND(SUM(novas_conexoes_unicas) OVER (PARTITION BY a.planning_cluster ORDER BY a.dias_vida) * 100.0 / NULLIF(u.total_estoque_cluster, 0), 2) AS P2C_acumulado_pct,
           MAX(a.leads_tentados_dia) OVER (PARTITION BY a.planning_cluster) as max_vol_cluster
    FROM agregado_dia a
    JOIN universo_leads u ON a.planning_cluster = u.planning_cluster
),
prep_score AS (
    SELECT *, ROUND((leads_tentados_dia * 100.0) / NULLIF(max_vol_cluster, 0), 2) AS pool_prospect_pct
    FROM metricas_brutas
),
benchmark_valido AS (
    SELECT planning_cluster,
           MAX(CASE WHEN pool_prospect_pct >= 10 THEN P2C_marginal_pct END) OVER (PARTITION BY planning_cluster) AS raw_max_p2c_representativo
    FROM prep_score
),
score_calculado AS (
    SELECT p.*,
           ROUND((P2C_marginal_pct / NULLIF(b.raw_max_p2c_representativo, 0)) * 100, 2) AS eficiencia_relativa
    FROM prep_score p
    JOIN (SELECT DISTINCT planning_cluster, raw_max_p2c_representativo FROM benchmark_valido) b
      ON p.planning_cluster = b.planning_cluster
),
score_ponderado AS (
    SELECT *, ROUND((eficiencia_relativa * SQRT(pool_prospect_pct / 100.0)), 2) AS score_p2c
    FROM score_calculado
),
inteligencia_final AS (
    SELECT *,
           P2C_acumulado_pct - LAG(P2C_acumulado_pct, 1, 0) OVER (PARTITION BY planning_cluster ORDER BY dias_vida) AS ganho_reach,
           COALESCE(MIN(CASE WHEN score_p2c < 75 THEN dias_vida END) OVER (PARTITION BY planning_cluster), 16) AS d_start_ss,
           COALESCE(MIN(CASE WHEN score_p2c < 50 THEN dias_vida END) OVER (PARTITION BY planning_cluster), 16) AS d_end_ss,
           MAX(P2C_acumulado_pct) OVER (PARTITION BY planning_cluster) AS max_reach_total
    FROM score_ponderado
)
SELECT * FROM inteligencia_final
ORDER BY planning_cluster, dias_vida;
