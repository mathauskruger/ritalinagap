"""
03_clean_validate.py
---------------------
Limpa, valida e cruza os dados de dispensação e atendimentos.
Gera relatório de qualidade dos dados — essencial pra mostrar
governança de dados (exatamente o que a Find.AI valoriza).

PARA RODAR:
    python pipeline/03_clean_validate.py
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DOCS_DIR = BASE_DIR / "docs"
DOCS_DIR.mkdir(exist_ok=True)

UFS_VALIDAS = {
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO"
}


def carregar_dados():
    """Carrega os CSVs processados pelos scripts anteriores."""
    arq_anvisa = PROCESSED_DIR / "metilfenidato_processado.csv"
    arq_sus = PROCESSED_DIR / "atendimentos_tdah_processado.csv"

    if not arq_anvisa.exists():
        raise FileNotFoundError(f"Execute primeiro: python pipeline/01_ingest_anvisa.py")
    if not arq_sus.exists():
        raise FileNotFoundError(f"Execute primeiro: python pipeline/02_ingest_datasus.py")

    df_anvisa = pd.read_csv(arq_anvisa)
    df_sus = pd.read_csv(arq_sus)

    print(f"✓ Carregado ANVISA: {len(df_anvisa):,} registros")
    print(f"✓ Carregado SUS:    {len(df_sus):,} registros")
    return df_anvisa, df_sus


def validar_dataframe(df: pd.DataFrame, nome: str) -> dict:
    """
    Executa validações de qualidade e retorna relatório.
    Isso demonstra governança de dados — linguagem da Find.AI.
    """
    relatorio = {
        "fonte": nome,
        "timestamp": datetime.now().isoformat(),
        "total_registros": len(df),
        "problemas": [],
        "aprovado": True,
    }

    # 1. Valores nulos em colunas críticas
    criticas = ["ano", "mes", "uf"]
    for col in criticas:
        if col in df.columns:
            nulos = df[col].isna().sum()
            if nulos > 0:
                relatorio["problemas"].append({
                    "tipo": "NULOS_CRITICOS",
                    "coluna": col,
                    "quantidade": int(nulos),
                    "acao": "DROP"
                })

    # 2. UFs inválidas
    if "uf" in df.columns:
        ufs_invalidas = df[~df["uf"].isin(UFS_VALIDAS)]["uf"].unique().tolist()
        if ufs_invalidas:
            relatorio["problemas"].append({
                "tipo": "UF_INVALIDA",
                "valores": ufs_invalidas,
                "acao": "DROP"
            })

    # 3. Anos fora do range esperado
    if "ano" in df.columns:
        anos = df["ano"].dropna().unique()
        anos_invalidos = [int(a) for a in anos if int(a) < 2019 or int(a) > 2024]
        if anos_invalidos:
            relatorio["problemas"].append({
                "tipo": "ANO_FORA_RANGE",
                "valores": anos_invalidos,
                "acao": "WARN"
            })

    # 4. Meses inválidos
    if "mes" in df.columns:
        meses_invalidos = df[~df["mes"].between(1, 12)]["mes"].dropna().unique().tolist()
        if meses_invalidos:
            relatorio["problemas"].append({
                "tipo": "MES_INVALIDO",
                "valores": [int(m) for m in meses_invalidos],
                "acao": "DROP"
            })

    # 5. Quantidades negativas
    cols_qtd = [c for c in df.columns if "quantidade" in c]
    for col in cols_qtd:
        negativos = (df[col] < 0).sum()
        if negativos > 0:
            relatorio["problemas"].append({
                "tipo": "QUANTIDADE_NEGATIVA",
                "coluna": col,
                "quantidade": int(negativos),
                "acao": "ZERO"
            })

    if relatorio["problemas"]:
        drops = [p for p in relatorio["problemas"] if p["acao"] == "DROP"]
        if drops:
            relatorio["aprovado"] = False

    return relatorio


def limpar_dataframe(df: pd.DataFrame, relatorio: dict) -> pd.DataFrame:
    """Aplica as correções identificadas na validação."""
    df = df.copy()

    for problema in relatorio["problemas"]:
        if problema["tipo"] == "NULOS_CRITICOS" and problema["acao"] == "DROP":
            col = problema["coluna"]
            antes = len(df)
            df = df.dropna(subset=[col])
            print(f"  DROP: {antes - len(df)} linhas com {col} nulo")

        elif problema["tipo"] == "UF_INVALIDA":
            antes = len(df)
            df = df[df["uf"].isin(UFS_VALIDAS)]
            print(f"  DROP: {antes - len(df)} linhas com UF inválida")

        elif problema["tipo"] == "MES_INVALIDO":
            antes = len(df)
            df = df[df["mes"].between(1, 12)]
            print(f"  DROP: {antes - len(df)} linhas com mês inválido")

        elif problema["tipo"] == "QUANTIDADE_NEGATIVA" and problema["acao"] == "ZERO":
            col = problema["coluna"]
            df[col] = df[col].clip(lower=0)
            print(f"  ZERO: valores negativos em {col} zerados")

    return df


def calcular_gap(df_anvisa: pd.DataFrame, df_sus: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza os dois datasets e calcula o gap por UF e mês.
    Esta é a tabela analítica principal do projeto.
    """
    # Agrega ANVISA por UF/mês
    anvisa_agg = df_anvisa.groupby(["ano", "mes", "uf"], as_index=False).agg(
        unidades_dispensadas=("quantidade_unidades", "sum")
    )

    # Agrega SUS por UF/mês (soma todos os CIDs F90)
    sus_agg = df_sus.groupby(["ano", "mes", "uf"], as_index=False).agg(
        atendimentos_tdah=("quantidade_aprovada", "sum")
    )

    # Merge full outer join
    gap = pd.merge(
        anvisa_agg, sus_agg,
        on=["ano", "mes", "uf"],
        how="outer"
    ).fillna(0)

    # Calcula métricas de gap
    gap["unidades_por_atendimento"] = gap.apply(
        lambda r: round(r["unidades_dispensadas"] / r["atendimentos_tdah"], 2)
        if r["atendimentos_tdah"] > 0 else None,
        axis=1
    )

    # Flag de anomalia: ratio muito alto sugere consumo desproporcional
    # Referência: uma caixa de 30 comprimidos por mês por paciente = 30 unidades/atendimento
    gap["flag_anomalia"] = gap["unidades_por_atendimento"].apply(
        lambda x: "ALTO" if x and x > 60 else ("BAIXO" if x and x < 10 else "NORMAL")
    )

    gap = gap.sort_values(["ano", "mes", "uf"])
    return gap


def main():
    print("=" * 60)
    print("LIMPEZA E VALIDAÇÃO")
    print("=" * 60)

    df_anvisa, df_sus = carregar_dados()

    print("\n→ Validando ANVISA...")
    rel_anvisa = validar_dataframe(df_anvisa, "ANVISA/SNGPC")
    df_anvisa = limpar_dataframe(df_anvisa, rel_anvisa)

    print("\n→ Validando DataSUS...")
    rel_sus = validar_dataframe(df_sus, "DataSUS/SIA")
    df_sus = limpar_dataframe(df_sus, rel_sus)

    # Salva relatório de qualidade (governança de dados!)
    relatorio_final = {
        "anvisa": rel_anvisa,
        "datasus": rel_sus,
        "total_problemas": len(rel_anvisa["problemas"]) + len(rel_sus["problemas"])
    }
    with open(DOCS_DIR / "relatorio_qualidade.json", "w") as f:
        json.dump(relatorio_final, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Relatório de qualidade salvo em docs/relatorio_qualidade.json")

    # Salva dados limpos
    df_anvisa.to_csv(PROCESSED_DIR / "metilfenidato_limpo.csv", index=False)
    df_sus.to_csv(PROCESSED_DIR / "atendimentos_tdah_limpo.csv", index=False)

    # Calcula e salva gap analítico
    print("\n→ Calculando gap dispensação vs. diagnósticos...")
    df_gap = calcular_gap(df_anvisa, df_sus)
    df_gap.to_csv(PROCESSED_DIR / "gap_analitico.csv", index=False)

    print(f"\n✓ Gap calculado: {len(df_gap):,} combinações UF/mês")
    print(f"  Anomalias ALTO: {(df_gap['flag_anomalia'] == 'ALTO').sum()}")
    print(f"  Normais:        {(df_gap['flag_anomalia'] == 'NORMAL').sum()}")
    print(f"\n  Prévia do gap:")
    print(df_gap[df_gap["unidades_por_atendimento"].notna()].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
