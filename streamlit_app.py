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

# =========================================================
# Processamento só se houver arquivo
# =========================================================
if uploaded_file is not None:
    nome_arquivo = uploaded_file.name.lower()

    # --- Apenas CSV com separador ";" ---
    if nome_arquivo.endswith(".csv"):
        try:
            # Tentamos ler explicitamente com ";"
            df_mira = pd.read_csv(uploaded_file, dtype=str, encoding="utf-8", sep=";")
        except UnicodeDecodeError:
            # Caso o encoding não seja UTF-8, tentamos latin1
            uploaded_file.seek(0)
            df_mira = pd.read_csv(uploaded_file, dtype=str, encoding="latin1", sep=";")
        except Exception:
            # Se der erro de separador (ex.: o arquivo usa vírgula)
            st.error(
                "Arquivo CSV inválido. Este sistema aceita apenas CSV com separador ponto e vírgula (;).\n\n"
                "Abra o arquivo e salve novamente usando o separador ';'."
            )
            st.stop()

    # --- Excel permitido normalmente ---
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

    # 3) Competência definida automaticamente pelo mês atual
    hoje = datetime.today()
    competencia_str = hoje.strftime("%m/%Y")
    
    st.sidebar.info(f"Competência considerada: {competencia_str} (mês atual da aplicação)")

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

    st.success(f"Processamento concluído! Confira o painel e utilize os filtros para exportar a tabela com os dados que quiser.")

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
# Abas: Instruções / Tabela / Gráficos
# (sempre aparecem, mesmo sem upload)
# =====================================================
tab1, tab2, tab3 = st.tabs(["📘 Instruções", "📈 Painel", "📊 Tabela final"])

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
            import plotly.express as px

            # ==========================================
            # KPIs (substituem o gráfico 1)
            # ==========================================
            st.markdown("#### Indicadores gerais")

            total_registros = len(df_filtrado)
            total_oci_unicas = df_filtrado["id_oci_paciente"].nunique()
            total_pacientes = df_filtrado["id_paciente"].nunique() if "id_paciente" in df_filtrado.columns else None

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    label="Procedimentos filtrados",
                    value=f"{total_registros:,}".replace(",", ".")
                )

            with col2:
                st.metric(
                    label="OCI identificadas",
                    value=f"{total_oci_unicas:,}".replace(",", ".")
                )

            with col3:
                if total_pacientes is not None:
                    st.metric(
                        label="Usuários identificados",
                        value=f"{total_pacientes:,}".replace(",", ".")
                    )
                else:
                    st.metric(
                        label="Usuários identificados",
                        value="--"
                    )

            # KPIs por conduta
            st.markdown("#### Condutas a serem tomadas")

            cont_conduta = (
                df_filtrado
                .groupby("conduta")
                .size()
                .reset_index(name="quantidade")
                .sort_values("quantidade", ascending=False)
            )

            # Exibe as condutas em blocos de até 4 por linha
            for i in range(0, len(cont_conduta), 4):
                cols = st.columns(4)
                subset = cont_conduta.iloc[i:i+4]
                for col, (_, row) in zip(cols, subset.iterrows()):
                    with col:
                        st.metric(
                            label=row["conduta"],
                            value=f"{row['quantidade']:,}".replace(",", ".")
                        )

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
                orientation="h",
                labels={
                    "id_oci_paciente": "Quantidade de OCIs únicas",
                    "no_oci": "OCI"
                }
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

with tab3:
    st.subheader("Tabela de procedimentos de OCI identificados (após filtros)")

    if df_filtrado is None:
        st.info("👈 Carregue um arquivo MIRA na barra lateral para visualizar a tabela.")
    else:
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

