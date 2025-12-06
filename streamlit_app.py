import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# ============================================
# CONFIGURAÇÃO BÁSICA DA PÁGINA
# ============================================
st.set_page_config(
    page_title="Identificador de OCI",
    layout="wide",
)

st.title("Identificador de OCI")

st.write(
    """
    Faça o upload da base MIRA, e o app irá identificar as OCIs, 
    gerar a tabela final e permitir filtros + download.
    """
)

# ============================================
# 1. CARREGAR BASES AUXILIARES (CACHEADAS)
# ============================================
BASE_DIR = Path(__file__).parent
AUX_DIR = BASE_DIR / "bases_auxiliares"


@st.cache_data(show_spinner="Carregando bases auxiliares...")
def carregar_bases_auxiliares():
    """
    Ajuste aqui os nomes/extensões reais dos arquivos na pasta bases_auxiliares.
    Exemplo assumindo .parquet. Troque para .csv se necessário.
    """
    df_pate = pd.read_csv(AUX_DIR / "df_pate.csv")
    pacotes = pd.read_csv(AUX_DIR / "pacotes.csv")
    oci_nome = pd.read_csv(AUX_DIR / "oci_nome.csv")
    cbo = pd.read_csv(AUX_DIR / "cbo.csv")
    idade_sexo = pd.read_csv(AUX_DIR / "idade_sexo.csv")
    cid = pd.read_csv(AUX_DIR / "cid.csv")

    bases = {
        "df_pate": df_pate,
        "pacotes": pacotes,
        "oci_nome": oci_nome,
        "cbo": cbo,
        "idade_sexo": idade_sexo,
        "cid": cid,
    }
    return bases


bases_aux = carregar_bases_auxiliares()
df_pate = bases_aux["df_pate"]
pacotes = bases_aux["pacotes"]
oci_nome = bases_aux["oci_nome"]
cbo = bases_aux["cbo"]
idade_sexo = bases_aux["idade_sexo"]
cid = bases_aux["cid"]  se tiver


# ============================================
# 2. FUNÇÕES PRINCIPAIS (AQUI VAI SEU SCRIPT)
# ============================================

def listar_procedimentos(df_procedimentos):
    # Mesma lógica do seu script
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

    for id_paciente, pacotes_result in resultados.items():
        for id_pacote, dados in pacotes_result.items():
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


def processar_mira(df_mira, df_pate, pacotes, oci_nome, cid=None):
    """
    Reaproveite aqui o passo a passo do seu script:
    - merge com df_pate
    - tratamento de datas
    - filtro por competência
    - criação de solicitacoes_oci
    - aplicação das funções de pacote
    - merge com cid e oci_nome
    - criação da coluna 'conduta'

    No final, retorne o dataframe 'oci_identificada'.
    """

    # --- Exemplo mínimo só para estruturar (substitua pelo seu código completo) ---

    # Merge com df_pate
    df_mira = pd.merge(
        df_mira,
        df_pate,
        left_on='co_procedimento',
        right_on='codigo',
        how='left',
        indicator='merge'
    )
    df_mira.drop(columns=['codigo', 'merge'], inplace=True)

    # Converter datas
    df_mira['dt_execucao'] = pd.to_datetime(df_mira['dt_execucao'])
    df_mira['dt_solicitacao'] = pd.to_datetime(df_mira['dt_solicitacao'])

    # Aqui você pode trazer exatamente aquele trecho que calcula 'competencias'
    # e filtra mês selecionado + anterior.
    # Por enquanto, vamos manter sem filtro de competência.

    # Procedimentos produzidos / não produzidos
    procedimentos_produzidos = df_mira.query('dt_execucao.notna()')
    nao_realizados = df_mira.query('dt_execucao.isna()')

    solicitacoes_oci = pd.concat([procedimentos_produzidos, nao_realizados], ignore_index=True)

    # 1) Listar procedimentos por paciente
    procedimentos_por_paciente = listar_procedimentos(solicitacoes_oci)

    # 2) Preparar regras
    regras_pacotes = preparar_regras(pacotes)

    # 3) Verificar pacotes
    resultados = verificar_pacotes(procedimentos_por_paciente, regras_pacotes)

    # 4) Marcar no df
    solicitacoes_oci_marcadas = marcar_solicitacoes_em_pacote(solicitacoes_oci, resultados)

    # Filtra apenas solicitações que viraram OCI
    oci_identificada = solicitacoes_oci_marcadas.query('em_pacote == True').copy()
    oci_identificada.drop(columns=['em_pacote'], inplace=True)

    # Explode id_pacote se tiver múltiplos
    oci_identificada["id_pacote"] = oci_identificada["id_pacote"].str.split(",")
    oci_identificada = oci_identificada.explode("id_pacote", ignore_index=True)
    oci_identificada["id_pacote"] = oci_identificada["id_pacote"].str.strip()
    oci_identificada.loc[oci_identificada["id_pacote"] == "", "id_pacote"] = pd.NA
    oci_identificada["em_pacote"] = oci_identificada["id_pacote"].notna()

    # Se existir df cid, aplicar compatibilidade
    if cid is not None:
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
    else:
        # Se não tiver cid, marca tudo como compatível (ajuste se quiser outra lógica)
        oci_identificada['cid_compativel'] = True

    # Merge com oci_nome
    oci_identificada = pd.merge(
        oci_identificada,
        oci_nome,
        left_on='id_pacote',
        right_on='co_oci',
        how='left'
    )
    oci_identificada.drop(columns=['co_oci'], inplace=True)

    # id único por paciente + OCI
    oci_identificada['id_oci_paciente'] = (
        oci_identificada['id_paciente'].astype(str)
        + '|' +
        oci_identificada['id_pacote'].astype(str)
    )

    # Conduta
    oci_identificada['conduta'] = np.select(
        condlist=[
            oci_identificada['dt_execucao'].notna() & oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].notna() & ~oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].isna()  & oci_identificada['cid_compativel'],
            oci_identificada['dt_execucao'].isna()  & ~oci_identificada['cid_compativel']
        ],
        choicelist=[
            'Faturar como OCI',
            'Revisar CID antes de faturar como OCI',
            'Executar como OCI',
            'Revisar CID antes de executar como OCI'
        ],
        default='indefinido'
    )

    return oci_identificada


# ============================================
# 3. SIDEBAR – UPLOAD & FILTROS
# ============================================
st.sidebar.header("Configurações")

uploaded_file = st.sidebar.file_uploader(
    "Base MIRA (.csv)",
    type=["csv"]
)

# (Mais tarde podemos adicionar aqui um selectbox de competência)


# ============================================
# 4. PROCESSAMENTO E INTERFACE PRINCIPAL
# ============================================
if uploaded_file is None:
    st.info("⬅️ Faça o upload da base MIRA na barra lateral para começar.")
else:
    # Leitura do MIRA enviado
    # Ajuste o separador conforme sua base ("," ou ";")
    df_mira = pd.read_csv(uploaded_file, dtype=str, sep=";")

    st.subheader("Pré-visualização da base MIRA")
    st.dataframe(df_mira.head(50), use_container_width=True)

    with st.spinner("Processando e identificando OCIs..."):
        # Se você tiver a base cid carregada, passe como argumento
        # oci_identificada = processar_mira(df_mira, df_pate, pacotes, oci_nome, cid)
        oci_identificada = processar_mira(df_mira, df_pate, pacotes, oci_nome)

    st.success("Processamento concluído!")

    # ============================================
    # TABS: TABELA FINAL  |  RESUMO & GRÁFICOS
    # ============================================
    tab_tabela, tab_resumo = st.tabs(["📊 Tabela final", "📈 Resumo & gráficos"])

    # ------------------ TABELA FINAL ------------------
    with tab_tabela:
        st.subheader("Tabela final de OCI identificadas")

        # Filtros básicos
        col1, col2, col3 = st.columns(3)

        with col1:
            condutas_unicas = sorted(oci_identificada['conduta'].dropna().unique())
            condutas_sel = st.multiselect(
                "Filtrar por conduta",
                condutas_unicas,
                default=condutas_unicas
            )

        with col2:
            if "cid_compativel" in oci_identificada.columns:
                cid_opt = st.multiselect(
                    "CID compatível?",
                    options=[True, False],
                    default=[True, False]
                )
            else:
                cid_opt = None

        with col3:
            if "no_oci" in oci_identificada.columns:
                oci_unicas = sorted(
                    oci_identificada['no_oci'].dropna().unique().tolist()
                )
                oci_sel = st.multiselect(
                    "Filtrar por OCI",
                    oci_unicas,
                    default=oci_unicas[:10] if len(oci_unicas) > 10 else oci_unicas
                )
            else:
                oci_sel = None

        # Aplica filtros
        df_filtrado = oci_identificada.copy()

        if condutas_sel:
            df_filtrado = df_filtrado[df_filtrado['conduta'].isin(condutas_sel)]

        if cid_opt is not None:
            df_filtrado = df_filtrado[df_filtrado['cid_compativel'].isin(cid_opt)]

        if oci_sel:
            df_filtrado = df_filtrado[df_filtrado['no_oci'].isin(oci_sel)]

        st.write(f"Total de registros após filtros: **{len(df_filtrado)}**")
        st.dataframe(df_filtrado, use_container_width=True)

        # Download do dataframe filtrado
        csv_filtrado = df_filtrado.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Baixar tabela filtrada (CSV)",
            data=csv_filtrado,
            file_name="oci_identificada_filtrada.csv",
            mime="text/csv"
        )

    # ------------------ RESUMO & GRÁFICOS ------------------
    with tab_resumo:
        st.subheader("Resumo e gráficos")

        # Contagem por conduta
        contagem_conduta = (
            oci_identificada
            .groupby("conduta")['id_oci_paciente']
            .nunique()
            .reset_index(name="qtd_pacientes")
        )

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Pacientes por conduta**")
            st.dataframe(contagem_conduta, use_container_width=True)

        with col2:
            st.markdown("**Gráfico – pacientes por conduta**")
            st.bar_chart(
                contagem_conduta.set_index("conduta")["qtd_pacientes"]
            )

        # Sugestão: outro gráfico por OCI
        if "no_oci" in oci_identificada.columns:
            contagem_oci = (
                oci_identificada
                .drop_duplicates(subset="id_oci_paciente")
                .groupby("no_oci")['id_oci_paciente']
                .count()
                .sort_values(ascending=False)
                .reset_index(name="qtd_pacientes")
            )

            st.markdown("**Top OCIs por número de pacientes**")
            st.dataframe(contagem_oci.head(20), use_container_width=True)

            st.markdown("**Gráfico – Top OCIs**")
            st.bar_chart(
                contagem_oci.set_index("no_oci")["qtd_pacientes"].head(20)
            )
