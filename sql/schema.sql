-- ============================================================
-- RitalinaGap — Schema PostgreSQL
-- ============================================================

-- Dispensação de metilfenidato (fonte: ANVISA/SNGPC)
CREATE TABLE IF NOT EXISTS dispensacao_metilfenidato (
    id                  SERIAL PRIMARY KEY,
    ano                 INTEGER NOT NULL,
    mes                 INTEGER NOT NULL,
    uf                  CHAR(2) NOT NULL,
    municipio           VARCHAR(100),
    codigo_ibge         CHAR(7),
    principio_ativo     VARCHAR(100) DEFAULT 'METILFENIDATO',
    nome_comercial      VARCHAR(100),  -- Ritalina, Concerta, etc.
    concentracao        VARCHAR(20),   -- 10mg, 18mg, etc.
    forma_farmaceutica  VARCHAR(50),
    quantidade_unidades INTEGER,
    quantidade_caixas   INTEGER,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Atendimentos ambulatoriais com CID F90 (fonte: DataSUS/SIA)
CREATE TABLE IF NOT EXISTS atendimentos_tdah (
    id                  SERIAL PRIMARY KEY,
    ano                 INTEGER NOT NULL,
    mes                 INTEGER NOT NULL,
    uf                  CHAR(2) NOT NULL,
    municipio           VARCHAR(100),
    codigo_ibge         CHAR(7),
    cid_principal       CHAR(4) DEFAULT 'F900',  -- F900, F901, F908, F909
    procedimento        VARCHAR(100),
    quantidade_aprovada INTEGER,
    quantidade_apresentada INTEGER,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- View analítica principal: gap por UF e mês
CREATE OR REPLACE VIEW vw_gap_mensal AS
SELECT
    COALESCE(d.ano, a.ano)           AS ano,
    COALESCE(d.mes, a.mes)           AS mes,
    COALESCE(d.uf, a.uf)             AS uf,
    COALESCE(d.total_unidades, 0)    AS unidades_dispensadas,
    COALESCE(a.total_atendimentos, 0) AS atendimentos_tdah,
    -- Ratio: quanto de medicamento por atendimento registrado
    CASE
        WHEN COALESCE(a.total_atendimentos, 0) = 0 THEN NULL
        ELSE ROUND(
            COALESCE(d.total_unidades, 0)::NUMERIC /
            COALESCE(a.total_atendimentos, 0), 2
        )
    END AS unidades_por_atendimento
FROM (
    SELECT ano, mes, uf, SUM(quantidade_unidades) AS total_unidades
    FROM dispensacao_metilfenidato
    GROUP BY ano, mes, uf
) d
FULL OUTER JOIN (
    SELECT ano, mes, uf, SUM(quantidade_aprovada) AS total_atendimentos
    FROM atendimentos_tdah
    GROUP BY ano, mes, uf
) a ON d.ano = a.ano AND d.mes = a.mes AND d.uf = a.uf;

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_dispensacao_uf_ano ON dispensacao_metilfenidato(uf, ano);
CREATE INDEX IF NOT EXISTS idx_atendimentos_uf_ano ON atendimentos_tdah(uf, ano);

-- Tabela de referência: população por UF (para normalizar per capita)
CREATE TABLE IF NOT EXISTS populacao_uf (
    uf          CHAR(2) PRIMARY KEY,
    nome_estado VARCHAR(50),
    regiao      VARCHAR(10),
    populacao   INTEGER,
    ano_ref     INTEGER
);

-- Dados populacionais IBGE 2022 (Censo)
INSERT INTO populacao_uf (uf, nome_estado, regiao, populacao, ano_ref) VALUES
('AC', 'Acre', 'N', 906876, 2022),
('AL', 'Alagoas', 'NE', 3351543, 2022),
('AM', 'Amazonas', 'N', 4269995, 2022),
('AP', 'Amapá', 'N', 877613, 2022),
('BA', 'Bahia', 'NE', 14873064, 2022),
('CE', 'Ceará', 'NE', 9240580, 2022),
('DF', 'Distrito Federal', 'CO', 3094325, 2022),
('ES', 'Espírito Santo', 'SE', 4108508, 2022),
('GO', 'Goiás', 'CO', 7206589, 2022),
('MA', 'Maranhão', 'NE', 7153262, 2022),
('MG', 'Minas Gerais', 'SE', 21411923, 2022),
('MS', 'Mato Grosso do Sul', 'CO', 2839188, 2022),
('MT', 'Mato Grosso', 'CO', 3784239, 2022),
('PA', 'Pará', 'N', 8777124, 2022),
('PB', 'Paraíba', 'NE', 4059905, 2022),
('PE', 'Pernambuco', 'NE', 9674793, 2022),
('PI', 'Piauí', 'NE', 3289290, 2022),
('PR', 'Paraná', 'S', 11597484, 2022),
('RJ', 'Rio de Janeiro', 'SE', 17463349, 2022),
('RN', 'Rio Grande do Norte', 'NE', 3560903, 2022),
('RO', 'Rondônia', 'N', 1815278, 2022),
('RR', 'Roraima', 'N', 652713, 2022),
('RS', 'Rio Grande do Sul', 'S', 11466630, 2022),
('SC', 'Santa Catarina', 'S', 7786390, 2022),
('SE', 'Sergipe', 'NE', 2338474, 2022),
('SP', 'São Paulo', 'SE', 45919049, 2022),
('TO', 'Tocantins', 'N', 1607363, 2022)
ON CONFLICT (uf) DO NOTHING;

COMMENT ON TABLE dispensacao_metilfenidato IS 'Dispensação de metilfenidato em farmácias privadas — fonte ANVISA/SNGPC';
COMMENT ON TABLE atendimentos_tdah IS 'Atendimentos ambulatoriais com CID F90 (TDAH) — fonte DataSUS/SIA';
COMMENT ON VIEW vw_gap_mensal IS 'Gap entre dispensação e atendimentos por UF e mês';
