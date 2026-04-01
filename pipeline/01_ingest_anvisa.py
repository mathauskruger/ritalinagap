"""
01_ingest_anvisa.py
--------------------
Baixa dados de dispensação de metilfenidato do portal ANVISA (SNGPC).

COMO FUNCIONA:
- A ANVISA disponibiliza CSVs no portal dados.gov.br
- Os arquivos são separados por ano e contêm todas as substâncias controladas
- Filtramos apenas metilfenidato (cloridrato de metilfenidato)

PARA RODAR:
    python pipeline/01_ingest_anvisa.py

Os arquivos brutos ficam em data/raw/ e os filtrados em data/processed/
"""

import os
import requests
import pandas as pd
from pathlib import Path

# ── Configuração ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Substâncias que queremos (metilfenidato e nomes comerciais)
FILTROS_PRINCIPIO_ATIVO = [
    "METILFENIDATO",
    "CLORIDRATO DE METILFENIDATO",
]

# URLs do portal dados.gov.br — SNGPC industrializados
# Atualize os links conforme disponibilidade em:
# https://dados.gov.br/dados/conjuntos-dados/venda-de-medicamentos-controlados-e-antimicrobianos---medicamentos-industrializados
URLS_SNGPC = {
    2022: "https://dados.anvisa.gov.br/dados/SNGPC_Industrializados_2022.csv",
    2023: "https://dados.anvisa.gov.br/dados/SNGPC_Industrializados_2023.csv",
}


def baixar_arquivo(url: str, destino: Path) -> bool:
    """Baixa um arquivo se ainda não existir localmente."""
    if destino.exists():
        print(f"  [cache] {destino.name} já existe, pulando download.")
        return True

    print(f"  [download] {url}")
    print(f"  ⚠️  Arquivos SNGPC são grandes (~500MB). Pode demorar...")

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()

        total = int(response.headers.get("content-length", 0))
        baixado = 0

        with open(destino, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    pct = baixado / total * 100
                    print(f"\r  {pct:.1f}%", end="", flush=True)

        print(f"\n  ✓ Salvo em {destino}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"\n  ✗ Erro no download: {e}")
        print(f"\n  → Acesse manualmente: https://dados.gov.br/dados/conjuntos-dados/venda-de-medicamentos-controlados-e-antimicrobianos---medicamentos-industrializados")
        return False


def filtrar_metilfenidato(arquivo_csv: Path, ano: int) -> pd.DataFrame:
    """
    Lê o CSV do SNGPC e filtra apenas metilfenidato.
    
    O CSV do SNGPC tem ~20 colunas. As principais:
    - ANO_VENDA, MES_VENDA
    - UF_VENDA, MUNICIPIO_VENDA, CÓD_IBGE
    - PRINCIPIO_ATIVO, DESCRICAO_APRESENTACAO
    - QTD_UNIDADE_FARMACOTECNICA (unidades vendidas)
    """
    print(f"  Lendo {arquivo_csv.name}...")

    # Lê em chunks pois o arquivo é grande
    chunks = []
    for chunk in pd.read_csv(
        arquivo_csv,
        sep=";",
        encoding="latin-1",
        chunksize=100_000,
        dtype=str,
        low_memory=False,
    ):
        # Normaliza o nome do princípio ativo para comparação
        chunk["PRINCIPIO_ATIVO_NORM"] = (
            chunk["PRINCIPIO_ATIVO"]
            .str.upper()
            .str.strip()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("ascii")
        )

        # Filtra metilfenidato
        mask = chunk["PRINCIPIO_ATIVO_NORM"].str.contains(
            "METILFENIDATO", na=False
        )
        filtrado = chunk[mask].copy()

        if len(filtrado) > 0:
            chunks.append(filtrado)

    if not chunks:
        print(f"  ⚠️  Nenhum registro de metilfenidato encontrado em {ano}")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    print(f"  ✓ {len(df):,} registros de metilfenidato encontrados em {ano}")
    return df


def padronizar_colunas(df: pd.DataFrame, ano: int) -> pd.DataFrame:
    """Padroniza os nomes de colunas para o schema do banco."""
    
    # Mapa de colunas SNGPC → nosso schema
    # (os nomes podem variar entre anos, por isso o .get com fallback)
    colunas_map = {
        "ANO_VENDA": "ano",
        "MES_VENDA": "mes",
        "UF_VENDA": "uf",
        "MUNICIPIO_VENDA": "municipio",
        "CÓD_IBGE": "codigo_ibge",
        "PRINCIPIO_ATIVO": "principio_ativo",
        "DESCRICAO_APRESENTACAO": "nome_comercial",
        "QTD_UNIDADE_FARMACOTECNICA": "quantidade_unidades",
        "QTD_APRESENTACAO": "quantidade_caixas",
        "CONCENTRACAO": "concentracao",
        "FORMA_FARMACEUTICA": "forma_farmaceutica",
    }

    # Renomeia apenas as colunas que existem
    rename = {k: v for k, v in colunas_map.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Garante que ano/mes existam mesmo se não tiver a coluna
    if "ano" not in df.columns:
        df["ano"] = ano
    if "mes" not in df.columns:
        df["mes"] = None

    # Converte tipos
    for col in ["ano", "mes", "quantidade_unidades", "quantidade_caixas"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normaliza UF
    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    # Seleciona apenas colunas do nosso schema
    colunas_finais = [
        "ano", "mes", "uf", "municipio", "codigo_ibge",
        "principio_ativo", "nome_comercial", "concentracao",
        "forma_farmaceutica", "quantidade_unidades", "quantidade_caixas"
    ]
    colunas_presentes = [c for c in colunas_finais if c in df.columns]
    return df[colunas_presentes]


def gerar_csv_alternativo():
    """
    Gera um CSV de exemplo com dados realistas para desenvolvimento.
    Use enquanto o download do ANVISA não estiver disponível.
    
    Baseado em dados publicados em estudos e relatórios públicos.
    """
    import numpy as np
    np.random.seed(42)

    ufs = ["RS", "SP", "RJ", "MG", "PR", "SC", "BA", "CE", "GO", "DF"]
    anos = [2021, 2022, 2023]
    meses = list(range(1, 13))

    registros = []
    for ano in anos:
        for mes in meses:
            for uf in ufs:
                # Sul e SP têm consumo mais alto — baseado em literatura
                fator_regional = 1.5 if uf in ["RS", "SC", "PR", "SP"] else 1.0
                # Crescimento anual de ~15% (conservador vs. 775% em 10 anos)
                fator_temporal = 1.15 ** (ano - 2021)

                quantidade = int(
                    np.random.normal(5000, 1000) * fator_regional * fator_temporal
                )
                registros.append({
                    "ano": ano,
                    "mes": mes,
                    "uf": uf,
                    "municipio": f"Capital-{uf}",
                    "codigo_ibge": None,
                    "principio_ativo": "METILFENIDATO",
                    "nome_comercial": np.random.choice(["Ritalina", "Concerta", "Ritalina LA"]),
                    "concentracao": np.random.choice(["10mg", "18mg", "27mg", "36mg"]),
                    "forma_farmaceutica": "COMPRIMIDO",
                    "quantidade_unidades": max(0, quantidade),
                    "quantidade_caixas": max(0, quantidade // 30),
                })

    df = pd.DataFrame(registros)
    saida = PROCESSED_DIR / "metilfenidato_exemplo.csv"
    df.to_csv(saida, index=False)
    print(f"  ✓ CSV de exemplo gerado: {saida}")
    print(f"  ⚠️  ATENÇÃO: são dados simulados para desenvolvimento.")
    print(f"     Substitua pelos dados reais do ANVISA antes de publicar.")
    return df


def main():
    print("=" * 60)
    print("INGESTÃO ANVISA/SNGPC — Metilfenidato")
    print("=" * 60)

    todos = []

    for ano, url in URLS_SNGPC.items():
        print(f"\n→ Processando {ano}...")
        arquivo_raw = RAW_DIR / f"SNGPC_Industrializados_{ano}.csv"

        sucesso = baixar_arquivo(url, arquivo_raw)

        if sucesso and arquivo_raw.exists():
            df = filtrar_metilfenidato(arquivo_raw, ano)
            if not df.empty:
                df = padronizar_colunas(df, ano)
                todos.append(df)
        else:
            print(f"\n  → Gerando dados de exemplo para {ano}...")

    if todos:
        df_final = pd.concat(todos, ignore_index=True)
    else:
        print("\n⚠️  Downloads indisponíveis. Gerando dataset de exemplo para desenvolvimento...")
        df_final = gerar_csv_alternativo()

    # Salva processado
    saida = PROCESSED_DIR / "metilfenidato_processado.csv"
    df_final.to_csv(saida, index=False, encoding="utf-8")

    print(f"\n✓ Pipeline concluído!")
    print(f"  Registros totais: {len(df_final):,}")
    print(f"  UFs: {sorted(df_final['uf'].unique()) if 'uf' in df_final.columns else 'N/A'}")
    print(f"  Arquivo salvo: {saida}")


if __name__ == "__main__":
    main()
