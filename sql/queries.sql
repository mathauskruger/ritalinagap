-- ============================================================
-- queries.sql — Queries analíticas principais do RitalinaGap
-- Cole estas queries no Apache Superset para criar os charts
-- ============================================================


-- 1. OVERVIEW: Total anual por UF
-- Use: Bar chart no Superset — X: ano, Y: total, color: uf
SELECT
    ano,
    uf,
    SUM(unidades_dispensadas)   AS total_unidades,
    SUM(atendimentos_tdah)      AS total_atendimentos,
    ROUND(
        SUM(unidades_dispensadas)::NUMERIC /
        NULLIF(SUM(atendimentos_tdah), 0), 1
    ) AS ratio_medio
FROM vw_gap_mensal
GROUP BY ano, uf
ORDER BY ano, ratio_medio DESC;


-- 2. RANKING: UFs com maior descolamento no último ano
-- Use: Table chart no Superset
SELECT
    uf,
    SUM(unidades_dispensadas)   AS total_unidades,
    SUM(atendimentos_tdah)      AS total_atendimentos,
    ROUND(
        SUM(unidades_dispensadas)::NUMERIC /
        NULLIF(SUM(atendimentos_tdah), 0), 1
    ) AS ratio,
    CASE
        WHEN SUM(unidades_dispensadas)::NUMERIC /
             NULLIF(SUM(atendimentos_tdah), 0) > 60 THEN '🔴 ALTO'
        WHEN SUM(unidades_dispensadas)::NUMERIC /
             NULLIF(SUM(atendimentos_tdah), 0) > 30 THEN '🟡 MÉDIO'
        ELSE '🟢 NORMAL'
    END AS classificacao
FROM vw_gap_mensal
WHERE ano = 2023
GROUP BY uf
ORDER BY ratio DESC;


-- 3. SÉRIE TEMPORAL: Evolução mensal nacional
-- Use: Line chart no Superset — X: periodo, Y: valores
SELECT
    MAKE_DATE(ano, mes, 1)      AS periodo,
    SUM(unidades_dispensadas)   AS unidades_dispensadas,
    SUM(atendimentos_tdah)      AS atendimentos_tdah
FROM vw_gap_mensal
GROUP BY ano, mes
ORDER BY periodo;


-- 4. REGIONAL: Comparativo por região
-- Use: Pie ou bar chart por região
SELECT
    p.regiao,
    SUM(g.unidades_dispensadas)  AS total_unidades,
    SUM(g.atendimentos_tdah)     AS total_atendimentos,
    ROUND(
        SUM(g.unidades_dispensadas)::NUMERIC /
        NULLIF(SUM(g.atendimentos_tdah), 0), 1
    ) AS ratio_regional
FROM vw_gap_mensal g
JOIN populacao_uf p ON g.uf = p.uf
WHERE g.ano = 2023
GROUP BY p.regiao
ORDER BY ratio_regional DESC;


-- 5. PER CAPITA: Ajustado por população
-- Use: Mapa choropleth ou bar chart
SELECT
    g.uf,
    p.nome_estado,
    p.regiao,
    SUM(g.unidades_dispensadas)  AS total_unidades,
    p.populacao,
    ROUND(
        SUM(g.unidades_dispensadas)::NUMERIC /
        p.populacao * 10000, 2
    ) AS unidades_por_10k_hab,
    ROUND(
        SUM(g.atendimentos_tdah)::NUMERIC /
        p.populacao * 10000, 2
    ) AS atendimentos_por_10k_hab
FROM vw_gap_mensal g
JOIN populacao_uf p ON g.uf = p.uf
WHERE g.ano = 2023
GROUP BY g.uf, p.nome_estado, p.regiao, p.populacao
ORDER BY unidades_por_10k_hab DESC;


-- 6. ANOMALIAS: Meses com comportamento suspeito
-- Use: Table com conditional formatting no Superset
SELECT
    ano,
    mes,
    uf,
    unidades_dispensadas,
    atendimentos_tdah,
    unidades_por_atendimento,
    CASE
        WHEN unidades_por_atendimento > 100 THEN 'CRÍTICO'
        WHEN unidades_por_atendimento > 60  THEN 'ALTO'
        WHEN unidades_por_atendimento < 10  THEN 'BAIXO'
        ELSE 'NORMAL'
    END AS classificacao
FROM vw_gap_mensal
WHERE unidades_por_atendimento IS NOT NULL
ORDER BY unidades_por_atendimento DESC
LIMIT 50;
