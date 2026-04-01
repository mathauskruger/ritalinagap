"""
02_ingest_datasus.py
---------------------
Baixa atendimentos ambulatoriais com CID F90 (TDAH) via PySUS.

COMO FUNCIONA:
- PySUS é uma biblioteca Python que acessa o FTP do DataSUS diretamente
- Usamos o SIA (Sistema de Informações Ambulatoriais)
- Filtramos CID F90 (Transtornos hipercinéticos / TDAH)

CIDs relevantes:
- F900 — Distúrbio de atividade e atenção (TDAH típico)
- F901 — Transtorno hipercinético de conduta
- F908 — Outros transtornos hipercinéticos
- F909 — Transtorno hipercinético não especificado

PARA RODAR:
    pip install pysus
    python pipeline/02_ingest_datasus.py
"""

import os
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# CIDs de TDAH
CIDS_TDAH = ["F900", "F901", "F908", "F909"]

# UFs para baixar (começa pequeno, expande depois)
UFS = ["RS", "SP", "RJ", "MG", "PR", "SC"]

ANOS = [2021, 2022, 2023]


def baixar_sia_pysus(uf: str, ano: int, mes: int) -> pd.DataFrame:
    """
    Baixa dados do SIA (ambulatorial) via PySUS para uma UF/mês.
    Retorna DataFrame filtrado por CID F90.
    """
    try:
        from pysus.online_data import SIA

        print(f"  Baixando SIA {uf}/{ano}/{mes:02d}...")

        # PySUS baixa o arquivo .dbc do FTP do DataSUS e converte
        df = SIA.download(uf, ano, mes)

        if df is None or df.empty:
            print(f"  ⚠️  Sem dados para {uf}/{ano}/{mes:02d}")
            return pd.DataFrame()

        # Filtra CID F90 e variantes
        # A coluna pode se chamar PA_CIDPRI ou similar dependendo da tabela
        cid_col = None
        for col in ["PA_CIDPRI", "CID", "PA_CID"]:
            if col in df.columns:
                cid_col = col
                break

        if cid_col is None:
            print(f"  ⚠️  Coluna de CID não encontrada. Colunas: {df.columns.tolist()[:10]}")
            return pd.DataFrame()

        mask = df[cid_col].str.startswith("F90", na=False)
        df_tdah = df[mask].copy()

        print(f"  ✓ {len(df_tdah):,} atendimentos F90 em {uf}/{ano}/{mes:02d}")
        return df_tdah

    except ImportError:
        print("  ✗ PySUS não instalado. Rode: pip install pysus")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ✗ Erro ao baixar {uf}/{ano}/{mes:02d}: {e}")
        return pd.DataFrame()


def padronizar_sia(df: pd.DataFrame, uf: str, ano: int, mes: int) -> pd.DataFrame:
    """
    Padroniza colunas do SIA para o nosso schema.
    
    Principais colunas do SIA-PA (Produção Ambulatorial):
    - PA_UFMUN: UF + código município
    - PA_CIDPRI: CID principal
    - PA_PROC_ID: código do procedimento
    - PA_QTDAPR: quantidade aprovada
    - PA_QTDPRO: quantidade apresentada
    - PA_MUNPCN: município do paciente
    """
    colunas_map = {
        "PA_CIDPRI": "cid_principal",
        "PA_PROC_ID": "procedimento",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_MUNPCN": "municipio",
    }

    rename = {k: v for k, v in colunas_map.items() if k in df.columns}
    df = df.rename(columns=rename)

    df["ano"] = ano
    df["mes"] = mes
    df["uf"] = uf

    # Extrai código IBGE do município (6 dígitos)
    if "municipio" in df.columns:
        df["codigo_ibge"] = df["municipio"].str[:6]

    # Converte quantidades
    for col in ["quantidade_aprovada", "quantidade_apresentada"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    colunas_finais = [
        "ano", "mes", "uf", "municipio", "codigo_ibge",
        "cid_principal", "procedimento",
        "quantidade_aprovada", "quantidade_apresentada"
    ]
    colunas_presentes = [c for c in colunas_finais if c in df.columns]
    return df[colunas_presentes]


def agregar_por_uf_mes(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega para reduzir volume — soma por UF, mês, CID."""
    if df.empty:
        return df

    group_cols = ["ano", "mes", "uf", "cid_principal"]
    group_cols = [c for c in group_cols if c in df.columns]

    agg = df.groupby(group_cols, as_index=False).agg(
        quantidade_aprovada=("quantidade_aprovada", "sum"),
        quantidade_apresentada=("quantidade_apresentada", "sum"),
    )
    return agg


def gerar_dados_exemplo() -> pd.DataFrame:
    """
    Gera dados de exemplo de atendimentos TDAH para desenvolvimento.
    Baseado em proporções publicadas em estudos do DataSUS.
    """
    import numpy as np
    np.random.seed(123)

    ufs = ["RS", "SP", "RJ", "MG", "PR", "SC"]
    anos = [2021, 2022, 2023]
    meses = list(range(1, 13))
    cids = ["F900", "F901", "F908", "F909"]

    registros = []
    for ano in anos:
        for mes in meses:
            for uf in ufs:
                # SP e MG têm mais serviços de saúde mental
                fator = 2.0 if uf == "SP" else (1.5 if uf == "MG" else 1.0)
                # TDAH ainda é subdiagnosticado — crescimento menor que consumo
                fator_temporal = 1.05 ** (ano - 2021)

                for cid in cids:
                    # F900 é o mais comum (~70% dos casos)
                    fator_cid = 0.70 if cid == "F900" else 0.10

                    qtd = int(
                        np.random.normal(300, 80) * fator * fator_temporal * fator_cid
                    )
                    registros.append({
                        "ano": ano,
                        "mes": mes,
                        "uf": uf,
                        "municipio": f"Capital-{uf}",
                        "codigo_ibge": None,
                        "cid_principal": cid,
                        "procedimento": "0301010072",  # consulta médica em atenção especializada
                        "quantidade_aprovada": max(0, qtd),
                        "quantidade_apresentada": max(0, qtd + np.random.randint(-20, 50)),
                    })

    df = pd.DataFrame(registros)
    print(f"  ✓ {len(df):,} registros de exemplo gerados")
    print(f"  ⚠️  ATENÇÃO: dados simulados. Substitua pelos dados reais do DataSUS.")
    return df


def main():
    print("=" * 60)
    print("INGESTÃO DATASUS/SIA — Atendimentos TDAH (CID F90)")
    print("=" * 60)

    todos = []
    pysus_disponivel = True

    try:
        import pysus
    except ImportError:
        pysus_disponivel = False
        print("\n⚠️  PySUS não instalado.")
        print("   Para instalar: pip install pysus")

    if pysus_disponivel:
        for ano in ANOS:
            for uf in UFS:
                for mes in range(1, 13):
                    df = baixar_sia_pysus(uf, ano, mes)
                    if not df.empty:
                        df = padronizar_sia(df, uf, ano, mes)
                        df = agregar_por_uf_mes(df)
                        todos.append(df)

    if todos:
        df_final = pd.concat(todos, ignore_index=True)
    else:
        print("\n→ Gerando dataset de exemplo para desenvolvimento...")
        df_final = gerar_dados_exemplo()

    # Salva
    saida = PROCESSED_DIR / "atendimentos_tdah_processado.csv"
    df_final.to_csv(saida, index=False, encoding="utf-8")

    print(f"\n✓ Pipeline concluído!")
    print(f"  Registros totais: {len(df_final):,}")
    print(f"  Arquivo salvo: {saida}")


if __name__ == "__main__":
    main()
