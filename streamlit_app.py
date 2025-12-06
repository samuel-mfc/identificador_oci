# streamlit_app.py

import os
import pandas as pd
import numpy as np
import streamlit as st

# =========================================================
# 1. Funções de processamento (adaptadas do seu script)
# =========================================================

def listar_procedimentos(df_procedimentos):
    procedimentos_por_paciente = {}
    for id_paciente in df_procedimentos['id_paciente'].unique():
        procedimentos_paciente = (
            df_procedimentos[df_procedimentos['id_paciente'] == id_paciente]['co_procedimento']
            .astype(str)
            .tolist()
        )
        procedimentos_por_paciente[id_paciente] = procedimentos_paciente
    return procedimentos_por_paciente


def preparar_regras(df_pacotes):
    pacotes_agrupados = {}

    df = df_pacotes.copy()
    df["CO_OCI"] = df["CO_OCI"].astype(str)
    df["CO_PROCEDIMENTO"] = df["CO_PROCEDIMENTO"].astype(str)

    for co_oci in df["CO_OCI"].unique():
        regras_pacote = df[df["CO_OCI"] == co_oci]

        grupo_e = []
        grupos_ou_dict = {}
        opcionais = []

        for _, row in regras_pacote.iterrows():
            proc = row["CO_PROCEDIMENTO"].strip()
            compat = row["TP_COMPATIBILIDADE"]
            grupo_alt = row.get("OBRIGATORIO_ALTERNATIVO", None)

            try:
                compat = int(compat)
            except Exception:
                pass

            if compat == 5 or compat == "5":
                if pd.isna(grupo_alt) or str(grupo_alt).strip() == "":
                    grupo_e.append(proc)
                else:
                    chave_grupo = str(grupo_alt).strip()
                    if chave_grupo not in grupos_ou_dict:
                        grupos_ou_dict[chave_grupo] = []
                    grupos_ou_dict[chave_grupo].append(proc)

            elif compat == 1 or compat == "1":
                opcionais.append(proc)

        grupo_ou = list(grupos_ou_dict.values())

        pacotes_agrupados[co_oci] = {
            "grupo_e": grupo_e,
            "grupo_ou": grupo_ou,
            "opcionais": opcionais
        }

    return pacotes_agrupados


def verificar_pacotes(procedimentos_por_paciente, regras_pacotes):
    resultados = {}

    for id_paciente, procedimentos_paciente in procedimentos_por_paciente.items():
        resultados_paciente = {}
        procedimentos_set = set(map(str, procedimentos_paciente))

        for id_pacote, grupos in regras_pacotes.items():
            procedimentos_relevantes = []

            grupo_e_completo = True
            for proc in grupos['grupo_e']:
                proc = str(proc).strip()
                if proc in procedimentos_set:
                    procedimentos_relevantes.append(proc)
                else:
                    grupo_e_completo = False

            grupo_ou_completo = True
            for lista_ou in grupos['grupo_ou']:
                encontrou = False
                for proc in lista_ou:
                    proc = str(proc).strip()
                    if proc in procedimentos_set:
                        if not encontrou:
                            procedimentos_relevantes.append(proc)
                            encontrou = True
                if not encontrou:
                    grupo_ou_completo = False

            pacote_completo = grupo_e_completo and grupo_ou_completo

            opcionais_relevantes = []
            if 'opcionais' in grupos and pacote_completo:
                for proc in grupos['opcionais']:
                    proc = str(proc).strip()
                    if proc in procedimentos_set:
                        opcionais_relevantes.append(proc)

            resultados_paciente[id_pacote] = {
                'status': pacote_completo,
                'procedimentos_relevantes': procedimentos_relevantes if pacote_completo else [],
                'procedimentos_opcionais': opcionais_relevantes if pacote_completo else []
            }

        resultados[id_paciente] = resultados_paciente

    return resultados


def marcar_solicitacoes_em_pacote(df_mira, resultados):
    registros = []

    for id_paciente, pacotes_dict in resultados.items():
        for id_pacote, dados in pacotes_dict.items():
            if not dados['status']:
                continue

            codigos_pacote = []
            codigos_pacote.extend(dados.get('procedimentos_relevantes', []))
            codigos_pacote.extend(dados.get('procedimentos_opcionais', []))

            for proc in codigos_pacote:
                registros.append({
                    'id_paciente': id_paciente,
                    'co_procedimento': str(proc),
                    'id_pacote': id_pacote
                })

    if not registros:
        df_out = df_mira.copy()
        df_out['em_pacote'] = False
        df_out['id_pacote'] = None
        return df_out

    df_map = pd.DataFrame(registros)

    df_map_agg = (
        df_map
        .groupby(['id_paciente', 'co_procedimento'])['id_pacote']
        .apply(lambda x: ','.join(sorted(map(str, set(x)))))
        .reset_index()
    )

    df_out = df_mira.copy()
    df_out['co_procedimento'] = df_out['co_procedimento'].astype(str)

    df_out = df_out.merge(
        df_map_agg,
        on=['id_paciente', 'co_procedimento'],
        how='left'
    )

    df_out['em_pacote'] = df_out['id_pacote'].notna()

    return df_out


def processar_mira(df_mira, df_pate, cid, oci_nome, pacotes, competencia_str=None):
    # Limpeza básica
    df_mira = df_mira.copy()
    df_mira.dropna(subset=['id_registro', 'id_paciente'], inplace=True)

    # Merge com df_pate para filtrar procedimentos de OCI
    df_mira = pd.merge(
        df_mira,
        df_pate,
        left_on='co_procedimento',
        right_on='codigo',
        how='left',
        indicator='merge'
    )
    df_mira.drop(columns=['codigo', 'merge'], inplace=True)

    # Datas
    df_mira['dt_solicitacao'] = pd.to_datetime(df_mira['dt_solicitacao'], errors='coerce')
    df_mira['dt_execucao'] = pd.to_datetime(df_mira['dt_execucao'], errors='coerce')

    # Concatena procedimento|CBO para grupo 03/04
    mask = (
        df_mira['co_procedimento'].astype(str).str.startswith(('03', '04')) &
        (df_mira['cbo_executante'].astype(str) != '')
    )
    df_mira.loc[mask, 'co_procedimento'] = (
        df_mira.loc[mask, 'co_procedimento'].astype(str) + '|' + df_mira.loc[mask, 'cbo_executante'].astype(str)
    )

    # Procedimentos executados
    procedimentos_produzidos = df_mira.query('dt_execucao.notna()')

    # Se quiser usar competência (mês atual + anterior)
    if not procedimentos_produzidos.empty and competencia_str is not None:
        mes_sel, ano_sel = competencia_str.split("/")
        mes_sel = int(mes_sel)
        ano_sel = int(ano_sel)

        if mes_sel == 1:
            mes_ant = 12
            ano_ant = ano_sel - 1
        else:
            mes_ant = mes_sel - 1
            ano_ant = ano_sel

        mascara = (
            ((procedimentos_produzidos['dt_execucao'].dt.month == mes_sel) &
             (procedimentos_produzidos['dt_execucao'].dt.year == ano_sel)) |
            ((procedimentos_produzidos['dt_execucao'].dt.month == mes_ant) &
             (procedimentos_produzidos['dt_execucao'].dt.year == ano_ant))
        )

        procedimentos_produzidos = procedimentos_produzidos[mascara]

    # Não realizados
    nao_realizados = df_mira.query('dt_execucao.isna()')

    # Junta de novo
    solicitacoes_oci = pd.concat([procedimentos_produzidos, nao_realizados], ignore_index=True)

    # 1) Listar procedimentos por paciente
    procedimentos_por_paciente = listar_procedimentos(solicitacoes_oci)

    # 2) Regras dos pacotes
    regras_pacotes = preparar_regras(pacotes)

    # 3) Verificar pacotes
    resultados = verificar_pacotes(procedimentos_por_paciente, regras_pacotes)

    # 4) DataFrame final com flag em_pacote
    solicitacoes_oci_marcadas = marcar_solicitacoes_em_pacote(solicitacoes_oci, resultados)

    # Filtra apenas solicitações que viraram OCI
    oci_identificada = solicitacoes_oci_marcadas.query('em_pacote == True').copy()

    # Explode id_pacote quando há múltiplas OCIs
    oci_identificada["id_pacote"] = oci_identificada["id_pacote"].astype(str).str.split(",")
    oci_identificada = oci_identificada.explode("id_pacote", ignore_index=True)
    oci_identificada["id_pacote"] = oci_identificada["id_pacote"].str.strip()
    oci_identificada.loc[oci_identificada["id_pacote"] == "", "id_pacote"] = pd.NA
    oci_identificada["em_pacote"] = oci_identificada["id_pacote"].notna()

    # Compatibilidade CID
    oci_identificada['cid_motivo'] = (
        oci_identificada['cid_motivo'].astype(str).str.upper().str.strip()
    )

    oci_identificada = oci_identificada.merge(
        cid.assign(cid_compativel=True),
        how='left',
        left_on=['id_pacote', 'cid_motivo'],
        right_on=['CO_OCI', 'CO_CID']
    )
    oci_identificada = oci_identificada.drop(columns=['CO_OCI', 'CO_CID'])
    oci_identificada['cid_compativel'] = oci_identificada['cid_compativel'].fillna(False)

    # Nome da OCI
    oci_identificada = pd.merge(
        oci_identificada,
        oci_nome,
        left_on='id_pacote',
        right_on='co_oci',
        how='left'
    )
    oci_identificada.drop(columns=['co_oci'], inplace=True)

    # ID composto
    oci_identificada['id_oci_paciente'] = (
        oci_identificada['id_paciente'].astype(str) + '|' + oci_identificada['id_pacote'].astype(str)
    )

    # Conduta
    oci_identificada['conduta'] = np.select(
        condlist=[
            oci_identificada['dt_execucao'].notna() & oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].notna() & ~oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].isna() & oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].isna() & ~oci_identificada['cid_compativel']
        ],
        choicelist=[
            'Faturar como OCI',
            'Revisar CID antes de faturar como OCI',
            'Executar como OCI',
            'Revisar CID antes de executar como OCI'
        ],
        default='indefinido'
    )

    # Ordena por paciente
    oci_identificada.sort_values(by='id_paciente', inplace=True)

    return oci_identificada


def calcular_competencias(df_mira):
    df = df_mira.copy()
    df['dt_execucao'] = pd.to_datetime(df['dt_execucao'], errors='coerce')
    procedimentos_produzidos = df.query('dt_execucao.notna()')

    if procedimentos_produzidos.empty:
        return []

    data_min = procedimentos_produzidos['dt_execucao'].min()
    data_max = procedimentos_produzidos['dt_execucao'].max()

    meses = pd.date_range(
        data_min.to_period('M').to_timestamp(),
        data_max.to_period('M').to_timestamp(),
        freq='MS'
    )

    competencias = meses.strftime('%m/%Y').tolist()
    return competencias


# =========================================================
# 2. Interface Streamlit
# =========================================================

st.set_page_config(page_title="Identificador de OCI", layout="wide")

st.title("🔍 Identificador de OCI a partir da MIRA")

st.sidebar.header("Configurações")

# 2.1 Upload da MIRA
uploaded_file = st.sidebar.file_uploader(
    "Carregue o arquivo MIRA (.csv)",
    type=["csv"]
)

# Carrega bases auxiliares fixas da pasta bases_auxiliares
@st.cache_data
def carregar_bases_auxiliares():
    base_path = "bases_auxiliares"
    df_pate = pd.read_csv(os.path.join(base_path, "df_pate.csv"), dtype=str)
    pacotes = pd.read_csv(os.path.join(base_path, "pacotes.csv"), dtype=str)
    cid = pd.read_csv(os.path.join(base_path, "cid.csv"), dtype=str)
    oci_nome = pd.read_csv(os.path.join(base_path, "oci_nome.csv"), dtype=str)
    # cbo, idade_sexo podem ser usados depois
    return df_pate, pacotes, cid, oci_nome

if uploaded_file is not None:
    # 1) Ler MIRA
    df_mira = pd.read_csv(uploaded_file, dtype=str)

    # 2) Bases auxiliares
    df_pate, pacotes, cid, oci_nome = carregar_bases_auxiliares()

    # 3) Competências disponíveis
    competencias = calcular_competencias(df_mira)

    competencia_str = None
    if len(competencias) > 0:
        competencia_str = st.sidebar.selectbox(
            "Competência (filtra mês escolhido + mês anterior)",
            options=competencias,
            index=len(competencias) - 1,  # por padrão, última competência
        )
    else:
        st.sidebar.warning("Não foi possível calcular competências (sem dt_execucao válida).")

    # 4) Processar MIRA -> OCI identificada
    with st.spinner("Processando solicitações e identificando OCIs..."):
        oci_identificada = processar_mira(
            df_mira,
            df_pate=df_pate,
            cid=cid,
            oci_nome=oci_nome,
            pacotes=pacotes,
            competencia_str=competencia_str
        )

    st.success(f"Processamento concluído! {len(oci_identificada)} registros de OCI identificados.")

    # =====================================================
    # Filtros principais
    # =====================================================
    st.sidebar.subheader("Filtros principais")

    condutas = sorted(oci_identificada['conduta'].dropna().unique().tolist())
    conduta_sel = st.sidebar.multiselect(
        "Conduta",
        options=condutas,
        default=condutas
    )

    oci_nomes = sorted(oci_identificada['no_oci'].dropna().unique().tolist())
    oci_sel = st.sidebar.multiselect(
        "Nome da OCI",
        options=oci_nomes,
        default=oci_nomes[:20] if len(oci_nomes) > 20 else oci_nomes
    )

    cid_options = ["Todos", "Compatível", "Incompatível"]
    cid_choice = st.sidebar.radio("Compatibilidade CID", cid_options, index=0)

    df_filtrado = oci_identificada.copy()

    if conduta_sel:
        df_filtrado = df_filtrado[df_filtrado['conduta'].isin(conduta_sel)]

    if oci_sel:
        df_filtrado = df_filtrado[df_filtrado['no_oci'].isin(oci_sel)]

    if cid_choice == "Compatível":
        df_filtrado = df_filtrado[df_filtrado['cid_compativel'] == True]
    elif cid_choice == "Incompatível":
        df_filtrado = df_filtrado[df_filtrado['cid_compativel'] == False]

    # =====================================================
    # Abas: Tabela / Gráficos
    # =====================================================
    tab1, tab2 = st.tabs(["📊 Tabela final", "📈 Gráficos"])

    with tab1:
        st.subheader("Tabela de OCIs identificadas (após filtros)")

        st.write(f"Total de registros filtrados: {len(df_filtrado)}")
        st.dataframe(df_filtrado, use_container_width=True)

        # Download do dataframe filtrado
        csv_filtrado = df_filtrado.to_csv(index=False, sep=";")
        st.download_button(
            label="⬇️ Baixar tabela filtrada (CSV)",
            data=csv_filtrado.encode("utf-8-sig"),
            file_name="oci_identificada_filtrada.csv",
            mime="text/csv"
        )

    with tab2:
        st.subheader("Distribuição por conduta")
    
        if not df_filtrado.empty:
            cont_conduta = df_filtrado['conduta'].value_counts().reset_index()
            cont_conduta.columns = ['conduta', 'quantidade']
            st.bar_chart(cont_conduta.set_index('conduta'))
    
            # ===== GRÁFICO DE OCI AJUSTADO =====
            st.subheader("Quantidade de OCI identificadas")
    
            import altair as alt
    
            # Contagem baseada em id_oci_paciente único
            cont_oci = (
                df_filtrado[['id_oci_paciente', 'no_oci']]
                .drop_duplicates(subset=['id_oci_paciente'])
                .groupby('no_oci')
                .size()
                .reset_index(name='quantidade')
                .sort_values('quantidade', ascending=True)
            )
    
            # --- função para quebrar texto em múltiplas linhas ---
            def wrap_text(text, width=30):
                if not isinstance(text, str):
                    text = str(text)
                return "\n".join(text[i:i+width] for i in range(0, len(text), width))
    
            # cria coluna com texto quebrado
            cont_oci["no_oci_wrapped"] = cont_oci["no_oci"].astype(str).apply(lambda x: wrap_text(x, 30))
    
            # --- padding automático com base no maior pedaço de texto ---
            max_line_len = (
                cont_oci["no_oci_wrapped"]
                .str.split("\n")
                .apply(lambda linhas: max(len(l) for l in linhas))
                .max()
            )
            left_padding = int(max_line_len * 7.5)  # ~7,5 px por caractere (ajustável)
    
            # configurações das barras
            bar_size = 12
            n_oci = len(cont_oci)
            chart_height = max(200, n_oci * (bar_size + 6))
    
            chart = (
                alt.Chart(cont_oci)
                .mark_bar(size=bar_size)
                .encode(
                    x=alt.X("quantidade:Q", title="Quantidade"),
                    y=alt.Y(
                        "no_oci_wrapped:N",
                        title="None",
                        sort="-x",
                        axis=alt.Axis(
                            labelLimit=10000,
                            labelAlign="right",
                            labelPadding=40,   # AQUI empurra o texto pra esquerda e as barras "ficam mais pra direita"
                        ),
                    ),
                    tooltip=["no_oci", "quantidade"],
                )
                .properties(
                    width="container",
                    height=chart_height,
                )
            )
    
            # usa toda a largura do container
            st.altair_chart(chart, use_container_width=True)
            # ==================================
    
        else:
            st.info("Nenhum dado após aplicar os filtros para gerar gráficos.")


else:
    st.info("👈 Carregue um arquivo MIRA em formato CSV na barra lateral para iniciar.")
