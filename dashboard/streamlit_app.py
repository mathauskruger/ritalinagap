"""
streamlit_app.py
-----------------
Interface Streamlit com interpretação dos dados por IA (OpenRouter).
Complementa o dashboard principal do Superset com narrativa inteligente.

PARA RODAR:
    streamlit run dashboard/streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from openai import OpenAI
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
load_dotenv(BASE_DIR / ".env")

# ── Configuração ──────────────────────────────────────────────
st.set_page_config(
    page_title="RitalinaGap — Acesso ao TDAH no Brasil",
    page_icon="🧠",
    layout="wide"
)

# OpenRouter (mesmo cliente do 2Human)
@st.cache_resource
def get_client():
    api_key = os.getenv("OPENROUTER_API_KEY") or st.secrets.get("OPENROUTER_API_KEY")
    if not api_key:
        return None
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

client = get_client()

# ── Carrega dados ─────────────────────────────────────────────
@st.cache_data
def carregar_gap():
    arquivo = PROCESSED_DIR / "gap_analitico.csv"
    if not arquivo.exists():
        st.error("Execute o pipeline primeiro: python pipeline/03_clean_validate.py")
        st.stop()
    return pd.read_csv(arquivo)

@st.cache_data
def carregar_dispensacao():
    arquivo = PROCESSED_DIR / "metilfenidato_limpo.csv"
    if not arquivo.exists():
        return pd.DataFrame()
    return pd.read_csv(arquivo)

@st.cache_data
def carregar_atendimentos():
    arquivo = PROCESSED_DIR / "atendimentos_tdah_limpo.csv"
    if not arquivo.exists():
        return pd.DataFrame()
    return pd.read_csv(arquivo)

df_gap = carregar_gap()
df_anvisa = carregar_dispensacao()
df_sus = carregar_atendimentos()

# ── Header ────────────────────────────────────────────────────
st.title("🧠 RitalinaGap")
st.markdown("**Descolamento entre consumo de metilfenidato e diagnósticos de TDAH no Brasil**")
st.caption("Fontes: ANVISA/SNGPC (dispensação em farmácias privadas) · DataSUS/SIA (atendimentos ambulatoriais CID F90)")

# Aviso de dados simulados
if not (PROCESSED_DIR / "metilfenidato_processado.csv").exists() or \
   "exemplo" in str(list(PROCESSED_DIR.glob("*"))):
    st.warning("⚠️ Exibindo dados simulados para demonstração. Substitua pelos dados reais do ANVISA e DataSUS.")

st.markdown("---")

# ── Filtros na sidebar ────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔍 Filtros")

    anos_disp = sorted(df_gap["ano"].unique())
    ano_sel = st.selectbox("Ano", anos_disp, index=len(anos_disp)-1)

    ufs_disp = sorted(df_gap["uf"].unique())
    ufs_sel = st.multiselect("UFs", ufs_disp, default=ufs_disp[:6])

    st.markdown("---")
    st.markdown("### 📊 Sobre o projeto")
    st.markdown("""
    O consumo de metilfenidato cresceu 775% em dez anos no Brasil.
    Os diagnósticos de TDAH no SUS não acompanham esse ritmo.
    
    **Hipótese:** uso recreativo/performático está deslocando
    o acesso de quem realmente precisa.
    
    [GitHub](https://github.com/mathauskruger) · [LinkedIn](https://linkedin.com/in/mathauskruger)
    """)

# ── Filtra dados ──────────────────────────────────────────────
df_filtrado = df_gap[
    (df_gap["ano"] == ano_sel) &
    (df_gap["uf"].isin(ufs_sel))
]

# ── KPIs ──────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_unidades = df_filtrado["unidades_dispensadas"].sum()
total_atend = df_filtrado["atendimentos_tdah"].sum()
ratio_medio = (total_unidades / total_atend).round(1) if total_atend > 0 else 0
anomalias = (df_filtrado["flag_anomalia"] == "ALTO").sum()

col1.metric("Unidades dispensadas", f"{total_unidades:,.0f}", help="Total de unidades de metilfenidato dispensadas")
col2.metric("Atendimentos TDAH", f"{total_atend:,.0f}", help="Atendimentos ambulatoriais com CID F90")
col3.metric("Ratio médio", f"{ratio_medio:.1f}", help="Unidades por atendimento registrado")
col4.metric("Meses com anomalia", f"{anomalias}", help="UF/mês com ratio > 60 (alto consumo relativo)")

st.markdown("---")

# ── Gráfico 1: Evolução temporal ──────────────────────────────
st.subheader("📈 Evolução anual — dispensação vs. diagnósticos")

evolucao = df_gap[df_gap["uf"].isin(ufs_sel)].groupby("ano").agg(
    unidades=("unidades_dispensadas", "sum"),
    atendimentos=("atendimentos_tdah", "sum")
).reset_index()

fig1 = go.Figure()
fig1.add_trace(go.Bar(
    x=evolucao["ano"], y=evolucao["unidades"],
    name="Unidades dispensadas (ANVISA)",
    marker_color="#e63946"
))
fig1.add_trace(go.Bar(
    x=evolucao["ano"], y=evolucao["atendimentos"],
    name="Atendimentos TDAH (DataSUS)",
    marker_color="#457b9d"
))
fig1.update_layout(barmode="group", height=350, margin=dict(t=20))
st.plotly_chart(fig1, use_container_width=True)

# ── Gráfico 2: Gap por UF ─────────────────────────────────────
st.subheader(f"🗺️ Gap por UF — {ano_sel}")

gap_uf = df_filtrado.groupby("uf").agg(
    unidades=("unidades_dispensadas", "sum"),
    atendimentos=("atendimentos_tdah", "sum")
).reset_index()
gap_uf["ratio"] = (gap_uf["unidades"] / gap_uf["atendimentos"].replace(0, 1)).round(1)
gap_uf = gap_uf.sort_values("ratio", ascending=True)

fig2 = px.bar(
    gap_uf, x="ratio", y="uf", orientation="h",
    color="ratio",
    color_continuous_scale=["#457b9d", "#f4a261", "#e63946"],
    labels={"ratio": "Unidades por atendimento", "uf": "Estado"},
    height=400
)
fig2.update_layout(margin=dict(t=20))
st.plotly_chart(fig2, use_container_width=True)

st.caption("Ratio alto = muito medicamento dispensado relativamente ao número de diagnósticos registrados")

# ── Tabela detalhada ──────────────────────────────────────────
with st.expander("📋 Dados detalhados"):
    st.dataframe(
        df_filtrado[[
            "ano", "mes", "uf",
            "unidades_dispensadas", "atendimentos_tdah",
            "unidades_por_atendimento", "flag_anomalia"
        ]].sort_values(["uf", "mes"]),
        use_container_width=True
    )

# ── Interpretação por IA ──────────────────────────────────────
st.markdown("---")
st.subheader("🤖 Interpretação por IA")

if client is None:
    st.info("Configure OPENROUTER_API_KEY no arquivo .env para habilitar interpretação por IA.")
else:
    if st.button("Gerar análise dos dados filtrados", type="primary"):
        resumo = f"""
        Dados do Brasil — {ano_sel}, UFs: {', '.join(ufs_sel)}
        - Total unidades de metilfenidato dispensadas: {total_unidades:,.0f}
        - Total atendimentos com diagnóstico TDAH (CID F90): {total_atend:,.0f}
        - Ratio médio (unidades por atendimento): {ratio_medio:.1f}
        - Meses/UFs com anomalia (ratio > 60): {anomalias}
        - UF com maior ratio: {gap_uf.iloc[-1]['uf']} ({gap_uf.iloc[-1]['ratio']:.1f})
        - UF com menor ratio: {gap_uf.iloc[0]['uf']} ({gap_uf.iloc[0]['ratio']:.1f})
        """

        with st.spinner("Analisando dados..."):
            response = client.chat.completions.create(
                model="openai/gpt-4o-mini",
                messages=[{
                    "role": "user",
                    "content": f"""Você é um analista de dados de saúde pública especializado em acesso a medicamentos no Brasil.
                    
Analise os seguintes dados sobre o descolamento entre dispensação de metilfenidato (Ritalina) e diagnósticos de TDAH:

{resumo}

Forneça:
1. Interpretação do gap encontrado (2-3 frases)
2. Principal insight regional (1-2 frases)
3. Implicação para gestão de saúde (1-2 frases)
4. Limitação importante desses dados (1 frase)

Seja direto e técnico. Não use bullet points desnecessários."""
                }]
            )
            st.markdown(response.choices[0].message.content)
