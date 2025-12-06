# streamlit_app.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
from pathlib import Path

from processamento import processar_mira


st.set_page_config(
    page_title="Identificador de OCI",
    layout="wide",
)

st.title("Identificador de OCI")
st.write("Envie a tabela MIRA, identificamos as OCI, aplicamos filtros e geramos visualizações.")


# ---------------------------------------------------
# 1. Bases auxiliares (lidas dos .csv)
# ---------------------------------------------------
@st.cache_data
def carregar_bases_auxiliares():
    base_path = Path(__file__).parent / "bases_auxiliares"

    bases = {
        "df_pate": pd.read_csv(base_path / "df_pate.csv", dtype=str),
        "cbo": pd.read_csv(base_path / "cbo.csv", dtype=str),
        "pacotes": pd.read_csv(base_path / "pacotes.csv", dtype=str),
        "idade_sexo": pd.read_csv(base_path / "idade_sexo.csv", dtype=str),
        "cid": pd.read_csv(base_path / "cid.csv", dtype=str),
        "oci_nome": pd.read_csv(base_path / "oci_nome.csv", dtype=str),
    }
    return bases


# ---------------------------------------------------
# 2. Sidebar – upload e (futuros) filtros globais
# ---------------------------------------------------
st.sidebar.header("Entrada de dados")

arquivo_mira = st.sidebar.file_uploader(
    "Envie a tabela MIRA (CSV)",
    type=["csv"],
    help="Arquivo exportado do MIRA no formato CSV.",
)

st.sidebar.markdown("---")
st.sidebar.caption("Após o upload, a aplicação identifica automaticamente as OCI.")


# ---------------------------------------------------
# 3. Fluxo principal
# ---------------------------------------------------
if arquivo_mira is None:
    st.info("👈 Envie a tabela MIRA em CSV na barra lateral para começar.")
else:
    # 3.1. Ler MIRA
    df_mira = pd.read_csv(arquivo_mira, dtype=str)
    st.success(f"Arquivo carregado com sucesso! {df_mira.shape[0]} linhas.")

    # 3.2. Carregar bases auxiliares
    with st.spinner("Carregando bases auxiliares..."):
        bases_aux = carregar_bases_auxiliares()

    # 3.3. Rodar processamento de identificação de OCI
    with st.spinner("Processando dados e identificando OCI..."):
        df_final = processar_mira(df_mira, bases_aux)

    st.toast("Processamento concluído! 🎉")

    # ---------------------------------------------------
    # 4. Filtros dinâmicos (sidebar)
    # ---------------------------------------------------
    st.sidebar.header("Filtros da tabela final")

    filtro_conduta = None
    filtro_oci = None
    filtro_cid_compat = None

    if "conduta" in df_final.columns:
        opcoes_conduta = sorted(df_final["conduta"].dropna().unique())
        filtro_conduta = st.sidebar.multiselect(
            "Conduta",
            options=opcoes_conduta,
            default=opcoes_conduta,
        )

    if "no_oci" in df_final.columns:
        opcoes_oci = sorted(df_final["no_oci"].dropna().unique())
        filtro_oci = st.sidebar.multiselect(
            "OCI (nome)",
            options=opcoes_oci,
            default=opcoes_oci,
        )

    if "cid_compativel" in df_final.columns:
        opcoes_cid = [True, False]
        filtro_cid_compat = st.sidebar.multiselect(
            "CID compatível?",
            options=opcoes_cid,
            format_func=lambda x: "Sim" if x else "Não",
            default=opcoes_cid,
        )

    # Aplicar filtros
    df_filtrado = df_final.copy()

    if filtro_conduta:
        df_filtrado = df_filtrado[df_filtrado["conduta"].isin(filtro_conduta)]

    if filtro_oci:
        df_filtrado = df_filtrado[df_filtrado["no_oci"].isin(filtro_oci)]

    if filtro_cid_compat:
        df_filtrado = df_filtrado[df_filtrado["cid_compativel"].isin(filtro_cid_compat)]

    # ---------------------------------------------------
    # 5. Abas: tabela, filtros & download, gráficos
    # ---------------------------------------------------
    aba_tabela, aba_filtros, aba_graficos = st.tabs(
        ["📋 Tabela final", "🎯 Filtros & download", "📊 Gráficos"]
    )

    # Aba 1 – Tabela final (sem filtros)
    with aba_tabela:
        st.subheader("Tabela final (todas as OCI identificadas)")
        st.write(f"Total de linhas: {df_final.shape[0]}")
        st.dataframe(df_final, use_container_width=True, height=500)

        csv_completo = df_final.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar tabela completa (CSV)",
            data=csv_completo,
            file_name="oci_identificada_completa.csv",
            mime="text/csv",
        )

    # Aba 2 – Tabela filtrada + download
    with aba_filtros:
        st.subheader("Tabela filtrada")
        st.write(f"Total de linhas após filtros: {df_filtrado.shape[0]}")
        st.dataframe(df_filtrado, use_container_width=True, height=500)

        csv_filtrado = df_filtrado.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar tabela filtrada (CSV)",
            data=csv_filtrado,
            file_name="oci_identificada_filtrada.csv",
            mime="text/csv",
        )

    # Aba 3 – Gráficos e métricas
    with aba_graficos:
        st.subheader("Visão geral das OCI")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de registros (final)", df_final.shape[0])

        if "conduta" in df_final.columns:
            with col2:
                total_faturar = (df_final["conduta"] == "Faturar como OCI").sum()
                st.metric("Faturar como OCI", int(total_faturar))

        if "cid_compativel" in df_final.columns:
            with col3:
                total_incomp = (df_final["cid_compativel"] == False).sum()
                st.metric("CID incompatível", int(total_incomp))

        st.markdown("---")

        if "conduta" in df_filtrado.columns:
            st.subheader("Quantidade por conduta (dados filtrados)")
            contagem_conduta = (
                df_filtrado["conduta"].value_counts().rename_axis("conduta").reset_index(name="quantidade")
            )
            st.bar_chart(contagem_conduta.set_index("conduta")["quantidade"])

        if "no_oci" in df_filtrado.columns:
            st.subheader("Quantidade por OCI (dados filtrados)")
            contagem_oci = (
                df_filtrado["no_oci"].value_counts().rename_axis("no_oci").reset_index(name="quantidade")
            )
            st.bar_chart(contagem_oci.set_index("no_oci")["quantidade"])
