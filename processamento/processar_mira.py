# processamento/processar_mira.py
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


# ============================================================
# Funções auxiliares (iguais às do notebook, só organizadas)
# ============================================================

def listar_procedimentos(df_procedimentos: pd.DataFrame) -> dict:
    """
    Transforma o df de solicitações em um dicionário:
        { id_paciente: [lista de co_procedimento] }
    """
    procedimentos_por_paciente = {}

    for id_paciente in df_procedimentos["id_paciente"].unique():
        procedimentos_paciente = (
            df_procedimentos[df_procedimentos["id_paciente"] == id_paciente]["co_procedimento"]
            .astype(str)
            .tolist()
        )
        procedimentos_por_paciente[id_paciente] = procedimentos_paciente

    return procedimentos_por_paciente


def preparar_regras(df_pacotes: pd.DataFrame) -> dict:
    """
    Transforma o DataFrame 'df_pacotes' no dicionário de regras por pacote (CO_OCI),
    considerando:
      - TP_COMPATIBILIDADE == 5  -> obrigatório
         * sem OBRIGATORIO_ALTERNATIVO -> grupo_e (todos precisam estar presentes)
         * com OBRIGATORIO_ALTERNATIVO -> grupo_ou (grupos alternativos)
      - TP_COMPATIBILIDADE == 1  -> opcional (não fecha pacote, só registramos se estiver presente)
    """
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

            # OBRIGATÓRIOS (5)
            if compat == 5 or compat == "5":
                if pd.isna(grupo_alt) or str(grupo_alt).strip() == "":
                    grupo_e.append(proc)
                else:
                    chave = str(grupo_alt).strip()
                    if chave not in grupos_ou_dict:
                        grupos_ou_dict[chave] = []
                    grupos_ou_dict[chave].append(proc)

            # OPCIONAIS (1)
            elif compat == 1 or compat == "1":
                opcionais.append(proc)

        grupo_ou = list(grupos_ou_dict.values())

        pacotes_agrupados[co_oci] = {
            "grupo_e": grupo_e,
            "grupo_ou": grupo_ou,
            "opcionais": opcionais,
        }

    return pacotes_agrupados


def verificar_pacotes(procedimentos_por_paciente: dict, regras_pacotes: dict) -> dict:
    """
    Verifica, para cada paciente, quais pacotes (OCI) fecharam.
    Retorna:
        {
          id_paciente: {
              id_pacote (CO_OCI): {
                  'status': True/False,
                  'procedimentos_relevantes': [...],
                  'procedimentos_opcionais': [...]
              }
          }
        }
    """
    resultados = {}

    for id_paciente, procedimentos_paciente in procedimentos_por_paciente.items():
        resultados_paciente = {}

        procedimentos_set = set(map(str, procedimentos_paciente))

        for id_pacote, grupos in regras_pacotes.items():
            procedimentos_relevantes = []

            # Grupo E (todos precisam estar presentes)
            grupo_e = grupos.get("grupo_e", [])
            grupo_e_ok = True
            for proc in grupo_e:
                if proc in procedimentos_set:
                    procedimentos_relevantes.append(proc)
                else:
                    grupo_e_ok = False

            # Grupos de OU (pelo menos um de cada grupo)
            grupo_ou = grupos.get("grupo_ou", [])
            grupo_ou_ok = True
            for grupo in grupo_ou:
                presente_no_grupo = False
                for proc in grupo:
                    if proc in procedimentos_set:
                        procedimentos_relevantes.append(proc)
                        presente_no_grupo = True
                if not presente_no_grupo:
                    grupo_ou_ok = False

            # Pacote fecha se grupo_e_ok e grupo_ou_ok
            pacote_completo = grupo_e_ok and grupo_ou_ok

            opcionais_relevantes = []
            if pacote_completo:
                opcionais = grupos.get("opcionais", [])
                for proc in opcionais:
                    if proc in procedimentos_set:
                        opcionais_relevantes.append(proc)

            resultados_paciente[id_pacote] = {
                "status": pacote_completo,
                "procedimentos_relevantes": procedimentos_relevantes if pacote_completo else [],
                "procedimentos_opcionais": opcionais_relevantes if pacote_completo else [],
            }

        resultados[id_paciente] = resultados_paciente

    return resultados


def marcar_solicitacoes_em_pacote(df_mira: pd.DataFrame, resultados: dict) -> pd.DataFrame:
    """
    Retorna um df igual ao df_mira, com colunas extras:
      - 'em_pacote': True/False se aquela linha faz parte de algum pacote fechado
      - 'id_pacote': string com um ou mais CO_OCI (se quiser saber quais)
    """
    registros = []

    # 1) Montar tabela (id_paciente, co_procedimento, id_pacote)
    for id_paciente, pacotes in resultados.items():
        for id_pacote, dados in pacotes.items():
            if not dados["status"]:
                continue

            codigos_pacote = []
            codigos_pacote.extend(dados.get("procedimentos_relevantes", []))
            codigos_pacote.extend(dados.get("procedimentos_opcionais", []))

            for proc in codigos_pacote:
                registros.append(
                    {
                        "id_paciente": id_paciente,
                        "co_procedimento": str(proc),
                        "id_pacote": id_pacote,
                    }
                )

    if not registros:
        df_out = df_mira.copy()
        df_out["em_pacote"] = False
        df_out["id_pacote"] = None
        return df_out

    df_map = pd.DataFrame(registros)

    # 2) Agregar por (id_paciente, co_procedimento)
    df_map_agg = (
        df_map.groupby(["id_paciente", "co_procedimento"])["id_pacote"]
        .apply(lambda x: ",".join(sorted(map(str, set(x)))))
        .reset_index()
    )

    # 3) Merge com df_mira
    df_out = df_mira.copy()
    df_out["co_procedimento"] = df_out["co_procedimento"].astype(str)

    df_out = df_out.merge(
        df_map_agg,
        on=["id_paciente", "co_procedimento"],
        how="left",
    )

    df_out["em_pacote"] = df_out["id_pacote"].notna()

    return df_out


# ============================================================
# Função principal chamada pelo Streamlit
# ============================================================

def processar_mira(df_mira: pd.DataFrame, bases_auxiliares: dict) -> pd.DataFrame:
    """
    df_mira: DataFrame enviado pelo usuário (tabela MIRA).
    bases_auxiliares: dicionário com as bases já tratadas, lidas dos .csv:
        - df_pate
        - cbo
        - pacotes
        - idade_sexo
        - cid
        - oci_nome

    Retorna:
        oci_identificada: DataFrame final com colunas como:
          - id_paciente, id_registro, co_procedimento, dt_solicitacao, dt_execucao
          - id_pacote (CO_OCI), no_oci
          - cid_compativel (True/False)
          - conduta
          - id_oci_paciente
    """

    # -------------------------
    # 1) Preparar df_mira
    # -------------------------
    df = df_mira.copy()

    # Garante colunas mínimas
    cols_obrig = ["id_registro", "id_paciente", "co_procedimento"]
    for c in cols_obrig:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente em df_mira: {c}")

    df = df.dropna(subset=["id_registro", "id_paciente"])

    # Datas
    for col in ["dt_solicitacao", "dt_execucao"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Concatena CBO para procedimentos 03/04
    if "cbo_executante" in df.columns:
        df["co_procedimento"] = df["co_procedimento"].astype(str)
        df["cbo_executante"] = df["cbo_executante"].fillna("")

        mask = df["co_procedimento"].str.startswith(("03", "04")) & (df["cbo_executante"] != "")
        df.loc[mask, "co_procedimento"] = (
            df.loc[mask, "co_procedimento"] + "|" + df.loc[mask, "cbo_executante"]
        )

    # -------------------------
    # 2) Separar produzidos x não produzidos
    # -------------------------
    if "dt_execucao" not in df.columns:
        raise ValueError("df_mira precisa da coluna 'dt_execucao'.")

    procedimentos_produzidos = df.query("dt_execucao.notna()").copy()
    nao_realizados = df.query("dt_execucao.isna()").copy()

    # (Se quiser, no futuro, aplicar filtro por competência aqui)

    solicitacoes_oci = pd.concat(
        [procedimentos_produzidos, nao_realizados],
        ignore_index=True,
    )

    # -------------------------
    # 3) Preparar regras a partir dos pacotes
    # -------------------------
    pacotes = bases_auxiliares["pacotes"]
    cid = bases_auxiliares["cid"]
    oci_nome = bases_auxiliares["oci_nome"]

    regras_pacotes = preparar_regras(pacotes)

    procedimentos_por_paciente = listar_procedimentos(solicitacoes_oci)
    resultados = verificar_pacotes(procedimentos_por_paciente, regras_pacotes)

    # Marca quais solicitações fazem parte de algum pacote (OCI)
    solicitacoes_oci_marcadas = marcar_solicitacoes_em_pacote(
        solicitacoes_oci, resultados
    )

    # Filtra apenas solicitações que viraram OCI
    oci_identificada = solicitacoes_oci_marcadas.query("em_pacote == True").copy()
    oci_identificada.drop(columns=["em_pacote"], inplace=True)

    # Se vier string vazia em id_pacote, trata como NaN
    oci_identificada.loc[
        oci_identificada["id_pacote"] == "", "id_pacote"
    ] = pd.NA

    # -------------------------
    # 4) Compatibilidade CID
    # -------------------------
    if "cid_motivo" in oci_identificada.columns:
        oci_identificada["cid_motivo"] = (
            oci_identificada["cid_motivo"].astype(str).str.upper().str.strip()
        )

        cid_local = cid.copy()
        cid_local["CO_CID"] = (
            cid_local["CO_CID"].astype(str).str.upper().str.strip()
        )

        oci_identificada = oci_identificada.merge(
            cid_local.assign(cid_compativel=True),
            how="left",
            left_on=["id_pacote", "cid_motivo"],
            right_on=["CO_OCI", "CO_CID"],
        )

        oci_identificada = oci_identificada.drop(columns=["CO_OCI", "CO_CID"])
        oci_identificada["cid_compativel"] = oci_identificada[
            "cid_compativel"
        ].fillna(False)
    else:
        oci_identificada["cid_compativel"] = False

    # -------------------------
    # 5) Nome da OCI
    # -------------------------
    oci_identificada = oci_identificada.merge(
        oci_nome,
        left_on="id_pacote",
        right_on="co_oci",
        how="left",
    )
    oci_identificada.drop(columns=["co_oci"], inplace=True)

    # -------------------------
    # 6) ID único por OCI por paciente
    # -------------------------
    oci_identificada["id_oci_paciente"] = (
        oci_identificada["id_paciente"].astype(str)
        + "|"
        + oci_identificada["id_pacote"].astype(str)
    )

    # -------------------------
    # 7) Conduta
    # -------------------------
    condlist = [
        oci_identificada["dt_execucao"].notna()
        & oci_identificada["cid_compativel"],
        oci_identificada["dt_execucao"].notna()
        & ~oci_identificada["cid_compativel"],
        oci_identificada["dt_execucao"].isna()
        & oci_identificada["cid_compativel"],
        oci_identificada["dt_execucao"].isna()
        & ~oci_identificada["cid_compativel"],
    ]
    choicelist = [
        "Faturar como OCI",
        "Revisar CID antes de faturar como OCI",
        "Executar como OCI",
        "Revisar CID antes de executar como OCI",
    ]

    oci_identificada["conduta"] = np.select(
        condlist, choicelist, default="indefinido"
    )

    # Ordena um pouco
    oci_identificada = oci_identificada.sort_values(
        by=["id_paciente", "id_pacote"]
    ).reset_index(drop=True)

    return oci_identificada
