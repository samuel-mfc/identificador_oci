# streamlit_app.py

import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import io
from datetime import datetime


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


def adicionar_cid_e_status_oci(oci_identificada: pd.DataFrame) -> pd.DataFrame:
    """
    Cria as colunas:
      - 'cid_oci'     -> OCI identificada / OCI potencial / OCI desqualificada
      - 'status_oci'  -> em fila / iniciada / finalizada
    Baseado no agrupamento por 'id_oci_paciente'.
    """

    df = oci_identificada.copy()

    # Garantir datetime para dt_execucao
    df["dt_execucao"] = pd.to_datetime(df["dt_execucao"], errors="coerce")

    def resumo_grupo(g: pd.DataFrame) -> pd.Series:

        # ==========================
        # 1) CID_OCI
        # ==========================
        cid_vals = g["cid_compativel"].fillna(False)

        if cid_vals.all():
            cid_oci = "OCI identificada"
        elif cid_vals.any():
            cid_oci = "OCI potencial"
        else:
            cid_oci = "OCI desqualificada"

        # ==========================
        # 2) STATUS_OCI
        # ==========================
        dt = g["dt_execucao"]

        # Caso 1 – todas dt_execucao nulas
        if dt.isna().all():
            status = "em fila"

        else:
            all_not_null = dt.notna().all()
            if all_not_null:
                # todas executadas -> pode ser finalizada se último proc é 0301010*
                idx_last = dt.idxmax()
                ultimo_proc = str(g.loc[idx_last, "co_procedimento"])
                if ultimo_proc.startswith("0301010"):
                    status = "finalizada"
                else:
                    # todas executadas, mas última não é 0301010 -> consideramos como "retorno"
                    status = "retorno"
            else:
                # pelo menos uma execução nula e outra não nula
                status = "iniciada"

        return pd.Series({
            "cid_oci": cid_oci,
            "status_oci": status
        })

    # Aplica por id_oci_paciente
    resumo = (
        df
        .groupby("id_oci_paciente")
        .apply(resumo_grupo)
        .reset_index()
    )

    # Junta nas linhas originais
    df = df.merge(resumo, on="id_oci_paciente", how="left")

    return df


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

def reset_filtros():
    st.session_state["status_oci_sel"] = status_oci_opcoes_raw.copy()
    st.session_state["status_oci_force"] = None
    # futuros filtros aqui

# =========================================================
# 2. Interface Streamlit
# =========================================================

st.set_page_config(page_title="Identificador de OCI", layout="wide")

st.title("🔍 Identificador de OCI a partir do Modelo de Informação de Regulação Assistencial (MIRA)")

st.sidebar.header("Configurações")

# 2.1 Upload da MIRA
uploaded_file = st.sidebar.file_uploader(
    "Carregue o arquivo MIRA (.csv ou .xls ou .xlsx)",
    type=["csv", "xlsx", "xls"]
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


# Variáveis padrão (para podermos usar nas abas mesmo sem upload)
df_filtrado = None
oci_identificada = None
# Controle de estado entre interações

if "reset_filtros" not in st.session_state:
    st.session_state["reset_filtros"] = False

if "status_oci_force" not in st.session_state:
    st.session_state["status_oci_force"] = None

if "oci_identificada" not in st.session_state:
    st.session_state["oci_identificada"] = None

if "competencia_str" not in st.session_state:
    st.session_state["competencia_str"] = None

if "status_oci_sel" not in st.session_state:
    st.session_state["status_oci_sel"] = None  # será preenchido com "todos" quando houver dados

if "uploaded_file_id" not in st.session_state:
    st.session_state["uploaded_file_id"] = None

# =========================================================
# Processamento só se houver arquivo
# =========================================================
if uploaded_file is not None:
    nome_arquivo = uploaded_file.name.lower()

    # Se trocar de arquivo, zera o resultado anterior
    if st.session_state["uploaded_file_id"] != uploaded_file.name:
        st.session_state["uploaded_file_id"] = uploaded_file.name
        st.session_state["oci_identificada"] = None

    # --- Leitura do arquivo MIRA ---
    if nome_arquivo.endswith(".csv"):
        try:
            df_mira = pd.read_csv(uploaded_file, dtype=str, encoding="utf-8", sep=";")
        except UnicodeDecodeError:
            uploaded_file.seek(0)
            df_mira = pd.read_csv(uploaded_file, dtype=str, encoding="latin1", sep=";")
        except Exception:
            st.error(
                "Arquivo CSV inválido. Este sistema aceita apenas CSV com separador ponto e vírgula (;).\n\n"
                "Abra o arquivo e salve novamente usando o separador ';'."
            )
            st.stop()

    elif nome_arquivo.endswith((".xlsx", ".xls")):
        try:
            df_mira = pd.read_excel(uploaded_file, dtype=str)
        except ImportError:
            st.error(
                "Este ambiente não está configurado para ler arquivos Excel.\n"
                "Por favor, envie o arquivo em formato CSV com separador ';'."
            )
            st.stop()
    else:
        st.error("Formato de arquivo não reconhecido. Envie CSV (com ';') ou XLSX.")
        st.stop()

    # 2) Bases auxiliares
    df_pate, pacotes, cid, oci_nome = carregar_bases_auxiliares()

    # 3) Formulário de parâmetros (competência ANTES de processar)
    ano_atual = datetime.now().year
    competencias = [f"{mes:02d}/{ano_atual}" for mes in range(1, 13)]

    # índice padrão: mês atual ou último selecionado
    if st.session_state["competencia_str"] in competencias:
        idx_default = competencias.index(st.session_state["competencia_str"])
    else:
        idx_default = datetime.now().month - 1  # mês atual (0–11)

    with st.sidebar.form("form_processo_oci"):
        st.subheader("Parâmetros de processamento")

        competencia_str = st.selectbox(
            "Competência (filtra mês escolhido + mês anterior)",
            options=competencias,
            index=idx_default
        )

        submitted = st.form_submit_button("🚀 Processar / atualizar OCIs")

    # 4) Só processa quando o formulário é enviado
    if submitted:
        st.session_state["competencia_str"] = competencia_str

        with st.spinner("Processando solicitações e identificando OCIs..."):
            oci_identificada_proc = processar_mira(
                df_mira,
                df_pate=df_pate,
                cid=cid,
                oci_nome=oci_nome,
                pacotes=pacotes,
                competencia_str=competencia_str
            )

            oci_identificada_proc = adicionar_cid_e_status_oci(oci_identificada_proc)

        st.session_state["oci_identificada"] = oci_identificada_proc

    # 5) Se já houver resultado processado em memória, aplica filtros
    if st.session_state["oci_identificada"] is not None:
        oci_identificada = st.session_state["oci_identificada"]

        st.success(
            f"Processamento concluído para a competência {st.session_state['competencia_str']} "
            "(mês selecionado + mês anterior)."
        )

        # =====================================================
        # Filtros principais
        # =====================================================
        st.sidebar.subheader("Filtros principais")

        # 1) Opções de Qualificação OCI (cid_oci)
        if "cid_oci" in oci_identificada.columns:
            qual_oci_opcoes = sorted(oci_identificada["cid_oci"].dropna().unique().tolist())
        else:
            qual_oci_opcoes = []

        qual_oci_sel = st.sidebar.multiselect(
            "Qualificação OCI",
            options=qual_oci_opcoes,
            default=qual_oci_opcoes
        )

        # 2) Filtro do nome da OCI
        oci_nomes = sorted(oci_identificada["no_oci"].dropna().unique().tolist()) \
            if "no_oci" in oci_identificada.columns else []

        oci_sel = st.sidebar.multiselect(
            "Nome da OCI",
            options=oci_nomes,
            default=oci_nomes[:20] if len(oci_nomes) > 20 else oci_nomes
        )

        # 3) Filtro de status da OCI
        if "status_oci" in oci_identificada.columns:
            status_oci_opcoes = sorted(
                oci_identificada["status_oci"].dropna().unique().tolist()
            )
        else:
            status_oci_opcoes = []
        
        status_oci_opcoes_raw = status_oci_opcoes.copy()
        
        
        def _norm_status(x: str) -> str:
            return (str(x) if x is not None else "").strip().lower()
        
        
        # -------------------------------------------------
        # RESET GLOBAL — SEMPRE ANTES DOS WIDGETS
        # -------------------------------------------------
        if st.session_state.get("reset_filtros"):
            st.session_state["status_oci_force"] = None
        
            if "status_oci_sel" in st.session_state:
                del st.session_state["status_oci_sel"]
        
            st.session_state["reset_filtros"] = False
        
        
        # -------------------------------------------------
        # APLICA CLIQUE DE KPI (force)
        # -------------------------------------------------
        if st.session_state.get("status_oci_force"):
            st.session_state["status_oci_sel"] = st.session_state["status_oci_force"]
            st.session_state["status_oci_force"] = None
        
        
        # -------------------------------------------------
        # INICIALIZA SE NÃO EXISTIR
        # -------------------------------------------------
        if "status_oci_sel" not in st.session_state:
            st.session_state["status_oci_sel"] = status_oci_opcoes_raw.copy()
        
        
        # -------------------------------------------------
        # SANEAMENTO
        # -------------------------------------------------
        current = st.session_state.get("status_oci_sel", status_oci_opcoes_raw)
        
        # garantir que current seja sempre uma lista (multiselect retorna lista)
        if current is None:
            current = []
        elif isinstance(current, str):
            current = [current]
        elif not isinstance(current, (list, tuple, set)):
            # fallback para qualquer outro tipo inesperado (ex.: NaN, número, etc.)
            current = [current]

        
        current_norm = {_norm_status(x) for x in current}
        opcoes_norm = [_norm_status(x) for x in status_oci_opcoes_raw]
        
        default_sane = [
            raw for raw, n in zip(status_oci_opcoes_raw, opcoes_norm)
            if n in current_norm
        ]
        
        if not default_sane:
            default_sane = status_oci_opcoes_raw.copy()
        
        st.session_state["status_oci_sel"] = default_sane
        
        
        # -------------------------------------------------
        # WIDGET
        # -------------------------------------------------
        st.sidebar.multiselect(
            "Status da OCI",
            options=status_oci_opcoes_raw,
            default=st.session_state["status_oci_sel"],
            key="status_oci_sel",
        )
        
        
        # -------------------------------------------------
        # BOTÃO LIMPAR FILTROS
        # -------------------------------------------------
        if st.sidebar.button("Limpar filtros", use_container_width=True):
            st.session_state["reset_filtros"] = True
            st.rerun()

        # 4) Aplicar filtros ao dataframe
        df_filtrado = oci_identificada.copy()

        # Filtrar por nome da OCI
        if oci_sel:
            df_filtrado = df_filtrado[df_filtrado["no_oci"].isin(oci_sel)]

        # Filtrar por qualificação OCI
        if qual_oci_sel:
            df_filtrado = df_filtrado[df_filtrado["cid_oci"].isin(qual_oci_sel)]

        status_oci_sel = st.session_state["status_oci_sel"]
        
        # Filtrar por status da OCI
        if status_oci_sel:
            df_filtrado = df_filtrado[df_filtrado["status_oci"].isin(status_oci_sel)]



else:
    # Sem arquivo, mantém df_filtrado = None e oci_identificada = None
    df_filtrado = None
    oci_identificada = None

# =====================================================
# Abas: Instruções / Painel / Tabela
# (sempre aparecem, mesmo sem upload)
# =====================================================
tab1, tab2, tab3 = st.tabs(["📘 Instruções", "📈 Painel", "📊 Tabela"])

with tab1:
    st.header("📘 Instruções para o arquivo MIRA")

    st.markdown("""
    Para que o processamento funcione corretamente, o arquivo MIRA enviado deve conter 
    **pelo menos as seguintes colunas**, com **esses nomes exatos**:

    ### 🔑 Colunas obrigatórias

    - `id_registro` – identificador único do registro/linha.
    - `id_paciente` – identificador único do paciente (CPF).
    - `co_procedimento` – código SIGTAP do procedimento.
    - `dt_solicitacao` – data da solicitação do procedimento.
    - `dt_execucao` – data de execução do procedimento (pode estar em branco quando não realizado).
    - `cbo_executante` – CBO do profissional executante (obrigatório para procedimentos do grupo 03 e 04).
    - `cid_motivo` – CID informado como motivo/diagnóstico para o procedimento (pode estar em branco quando não houver esse dado).

    ### 📌 Observações importantes

    - A coluna **dt_execucao** é usada para identificar competência e determinar se o procedimento
      foi realizado; ela deve estar em formato de data conhecido (`YYYY-MM-DD` ou `DD/MM/YYYY`).
    - O arquivo deve estar no formato **CSV**, **XLS** ou **XLSX**.
    - Caso use formato **CSV**, o separador utilizado deve ser o ponto e vírgula `;`
    - Colunas adicionais são aceitas e não atrapalham o processamento.

    ### 📁 Estrutura recomendada

    ```text
    id_registro | id_paciente | co_procedimento | dt_solicitacao | dt_execucao | cbo_executante | cid_motivo
    ```

    ### ℹ️ Dica
    Caso você tenha dúvidas sobre o conteúdo, abra seu arquivo antes de subir para verificar se
    os nomes das colunas estão corretos.
    """)

    # -------------------------------
    # Botão para baixar modelo MIRA
    # -------------------------------
    modelo_df = pd.DataFrame(columns=[
        "id_registro",
        "id_paciente",
        "co_procedimento",
        "dt_solicitacao",
        "dt_execucao",
        "cbo_executante",
        "cid_motivo"
    ])

    buffer = io.BytesIO()
    try:
        # tenta gerar XLSX
        modelo_df.to_excel(buffer, index=False, sheet_name="Modelo_MIRA")
        buffer.seek(0)
        data_bytes = buffer.getvalue()
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        file_name = "modelo_mira.xlsx"
    except Exception as e:
        # fallback para CSV se der erro (por exemplo, falta de engine Excel)
        st.warning(f"Não foi possível gerar o arquivo .xlsx (detalhes: {e}). Será disponibilizado um modelo em CSV.")
        data_bytes = modelo_df.to_csv(index=False).encode("utf-8-sig")
        mime_type = "text/csv"
        file_name = "modelo_mira.csv"

    st.download_button(
        label="📥 Baixar arquivo modelo (MIRA)",
        data=data_bytes,
        file_name=file_name,
        mime=mime_type
    )

with tab2:
    st.subheader("Painel")

    if df_filtrado is None:
        st.info("👈 Carregue um arquivo MIRA na barra lateral para gerar o painel.")
    else:
        if not df_filtrado.empty:
            # KPIs – OCI encontradas por status
            st.markdown("#### OCI encontradas")

            # Trabalhamos em nível de OCI (id_oci_paciente único)
            df_oci_unica_status = df_filtrado.drop_duplicates(subset=["id_oci_paciente"])

            qtd_em_fila = (
                df_oci_unica_status
                .loc[df_oci_unica_status["status_oci"] == "em fila", "id_oci_paciente"]
                .nunique()
            )

            qtd_iniciada = (
                df_oci_unica_status
                .loc[df_oci_unica_status["status_oci"] == "iniciada", "id_oci_paciente"]
                .nunique()
            )

            qtd_retorno = (
                df_oci_unica_status
                .loc[df_oci_unica_status["status_oci"] == "retorno", "id_oci_paciente"]
                .nunique()
            )

            qtd_finalizada = (
                df_oci_unica_status
                .loc[df_oci_unica_status["status_oci"] == "finalizada", "id_oci_paciente"]
                .nunique()
            )

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="Em fila",
                    value=f"{qtd_em_fila:,}".replace(",", ".")
                )
            
                if st.button("Filtrar", key="btn_filtrar_em_fila", use_container_width=True):
                    st.session_state["status_oci_force"] = ["em fila"]
                    st.rerun()
            

            with col2:
                st.metric(
                    label="Iniciadas",
                    value=f"{qtd_iniciada:,}".replace(",", ".")
                )

                if st.button("Filtrar", key="btn_filtrar_iniciada", use_container_width=True):
                    st.session_state["status_oci_force"] = ["iniciada"]
                    st.rerun()

            with col3:
                st.metric(
                    label="Realizar retorno",
                    value=f"{qtd_retorno:,}".replace(",", ".")
                )
                
                if st.button("Filtrar", key="btn_filtrar_retorno", use_container_width=True):
                    st.session_state["status_oci_force"] = ["retorno"]
                    st.rerun()
                    
            with col4:
                st.metric(
                    label="Finalizadas",
                    value=f"{qtd_finalizada:,}".replace(",", ".")
                )
                
                if st.button("Filtrar", key="btn_filtrar_finalizada", use_container_width=True):
                    st.session_state["status_oci_force"] = ["finalizada"]
                    st.rerun()
                    
            st.markdown("---")

            # ==========================================
            # Gráfico 2: Quantidade de OCI identificadas (horizontal)
            # ==========================================
            st.markdown("#### Quantidade de OCI identificadas")

            cont_oci = (
                df_filtrado.drop_duplicates(subset=["id_oci_paciente"])
                .groupby("no_oci")["id_oci_paciente"]
                .count()
                .reset_index()
                .sort_values(by="id_oci_paciente", ascending=True)
            )

            fig2 = px.bar(
                cont_oci,
                x="id_oci_paciente",
                y="no_oci",
                orientation="h"
            )

            fig2.update_traces(
                text=cont_oci["id_oci_paciente"],
                textposition="outside"
            )

            fig2.update_layout(
                height=600,
                margin=dict(l=200)
            )

            st.plotly_chart(fig2, use_container_width=True)

        else:
            st.info("Nenhum dado após aplicar os filtros para gerar o painel.")

        st.markdown("---")

        st.markdown("#### Extrair tabela")
        st.write(f"Total de registros filtrados: {len(df_filtrado)}")

        # Remove colunas internas antes de exibir
        colunas_remover = ['em_pacote', 'cid_compativel', 'id_oci_paciente']
        df_exibir = df_filtrado.drop(columns=[c for c in colunas_remover if c in df_filtrado.columns])

        st.dataframe(df_exibir, use_container_width=True)

        # Download do dataframe filtrado (também sem as colunas internas)
        csv_filtrado = df_exibir.to_csv(index=False, sep=";")
        st.download_button(
            label="⬇️ Baixar tabela filtrada (CSV)",
            data=csv_filtrado.encode("utf-8-sig"),
            file_name="oci_identificada_filtrada.csv",
            mime="text/csv"
        )

with tab3:
    st.subheader("Sobre o autor")
