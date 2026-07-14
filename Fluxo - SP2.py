"""
Migracao PySpark do fluxo Alteryx "Fluxo - SP.yxmd".

Apuracao e reconciliacao do ISS (imposto municipal sobre servicos) do
Centro SAP 0100 (Sao Paulo), cruzando 5 fontes:
  1. ZFBL5N.xlsx        - linhas de faturamento SAP (contas a receber)
  2. Servicos SP.xlsx    - tabela de-para: codigo de servico -> aliquota de ISS
  3. Razao ISS B3.XLSX   - razao contabil (livro-razao) da conta de ISS
  4. NFS - Prefeitura.xlsx - notas fiscais de servico emitidas na prefeitura
  5. ZSD.XLSX            - controle interno SAP dos RPS gerados

Saidas (equivalentes aos DbFileOutput do Alteryx, todas em .../SP/Saida):
  - Relatorio Faturamento SP MES 2026 (Contas a Receber).xlsx
  - ZSD_Material Reg. Esp..xlsx
  - Cum. e Nao-Cum..xlsx
  - Razao ISS B3 SP.xlsx
  - Prefeitura x ZSD.xlsx / ZSD x Prefeitura.xlsx (reconciliacao de RPS)
  - Faturamento x SAP.xlsx / SAP x Faturamento.xlsx (excecoes estruturais,
    ficam sempre vazias em operacao normal - ver Pipeline 6)
  - Relatorio Final Alteryx.xlsx (reconciliacao Faturamento x SAP x Prefeitura)
  - Lancto de Pag ISS Faturamento - B3 SP.xlsx (lancamento contabil, partida dobrada)

Convencao de nomes de coluna
-----------------------------
Toda coluna usada dentro do Spark (filter/select/join/groupBy/withColumn)
segue snake_case sem acentos, espacos ou pontuacao (ex.: "Tp.Document" ->
tp_documento). Isso e proposital, nao cosmetico: um "." em nome de coluna
e interpretado pelo Spark como separador de campo aninhado dentro de
F.col()/select() (F.col("Tp.Document") tenta resolver o campo "Document"
dentro de uma coluna "Tp" e falha), e nomes com espaco/acento quebram em
escritas Delta e em varios conectores. Os nomes de negocio originais (em
portugues, com acentos) so sao recolocados no ultimo passo, na escrita do
Excel, para manter os relatorios legiveis para o time.

Requer pandas + openpyxl no cluster para leitura/escrita de .xlsx
(ja presentes no Databricks Runtime padrao; senao: %pip install openpyxl).
Para volumes muito maiores, substituir read_excel/write_excel pelo
conector com.crealytics:spark-excel.
"""

import datetime

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

spark = SparkSession.builder.appName("Fluxo_ISS_SP").getOrCreate()

# ---------------------------------------------------------------------------
# Caminhos de entrada / saida (ajustar para o mount/volume do Databricks)
# ---------------------------------------------------------------------------
BASE_IN = "/Volumes/iss/sp/entrada"
BASE_OUT = "/Volumes/iss/sp/saida"

PATH_ZFBL5N = f"{BASE_IN}/ZFBL5N.xlsx"
PATH_SERVICOS_SP = f"{BASE_IN}/Servicos SP.xlsx"
PATH_RAZAO_ISS_B3 = f"{BASE_IN}/Razao ISS B3.XLSX"
PATH_NFS_PREFEITURA = f"{BASE_IN}/NFS - Prefeitura.xlsx"
PATH_ZSD = f"{BASE_IN}/ZSD.XLSX"

PATH_OUT_RELATORIO_FATURAMENTO_SP = f"{BASE_OUT}/Relatorio Faturamento SP MES 2026 (Contas a Receber).xlsx"
PATH_OUT_ZSD_MATERIAL_REG_ESP = f"{BASE_OUT}/ZSD_Material Reg. Esp..xlsx"
PATH_OUT_CUM_NAO_CUM = f"{BASE_OUT}/Cum. e Nao-Cum..xlsx"
PATH_OUT_RAZAO_ISS_B3_SP = f"{BASE_OUT}/Razao ISS B3 SP.xlsx"
PATH_OUT_PREFEITURA_X_ZSD = f"{BASE_OUT}/Prefeitura x ZSD.xlsx"
PATH_OUT_ZSD_X_PREFEITURA = f"{BASE_OUT}/ZSD x Prefeitura.xlsx"
PATH_OUT_FATURAMENTO_X_SAP_EXC = f"{BASE_OUT}/Faturamento x SAP.xlsx"
PATH_OUT_SAP_X_FATURAMENTO_EXC = f"{BASE_OUT}/SAP x Faturamento.xlsx"
PATH_OUT_RELATORIO_FINAL = f"{BASE_OUT}/Relatorio Final Alteryx.xlsx"
PATH_OUT_LANCTO_PAGTO_ISS = f"{BASE_OUT}/Lancto de Pag ISS Faturamento - B3 SP.xlsx"


def read_excel(path: str, sheet: str, rename: dict | None = None):
    """Le a aba `sheet` de `path` e, se `rename` for informado, renomeia
    imediatamente as colunas originais (nomes de negocio) para os nomes
    internos limpos, antes de qualquer operacao Spark tocar nelas."""
    pdf = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    df = spark.createDataFrame(pdf)
    if rename:
        for old, new in rename.items():
            df = df.withColumnRenamed(old, new)
    return df


def write_excel(df, path: str, rename: dict | None = None, sheet: str = "Planilha1"):
    """Se `rename` for informado, troca os nomes internos limpos pelos
    nomes de negocio (em portugues) antes de gravar o Excel."""
    if rename:
        for old, new in rename.items():
            df = df.withColumnRenamed(old, new)
    pdf = df.toPandas()
    pdf.to_excel(path, sheet_name=sheet, index=False, engine="openpyxl")


def scalar_sum(df, col: str) -> float:
    value = df.agg(F.sum(F.col(col).cast(DoubleType())).alias("v")).collect()[0]["v"]
    return value if value is not None else 0.0


# ===========================================================================
# PIPELINE 1 - Faturamento x Tabela de servicos -> calculo do ISS por linha
# (ToolIDs 1, 68, 134, 2, 5, 6, 8, 10, 11, 74, 75, 76, 69)
# ===========================================================================

RENAME_FATURAMENTO = {
    "Emite Nota?": "emite_nota",
    "Empresa": "empresa",
    "Centro": "centro",
    "Tp.Document": "tp_documento",
    "Cod. Cliente": "cod_cliente",
    "Nome Cliente": "nome_cliente",
    "Nº doc.": "num_doc",
    "NF-e": "nf_e",
    "Data de faturamento": "data_faturamento",
    "Data de planejamento": "data_planejamento",
    "Montante MI Item": "montante_mi_item",
    "Material": "material",
    "Desc.Material": "desc_material",
    "IR": "ir",
    "PIS": "pis",
    "Cofins": "cofins",
    "Csll": "csll",
    # Demais colunas da planilha, nao usadas em nenhum filtro/join/formula/
    # output daqui pra frente, mas mantidas no dataframe interno para
    # espelhar o AlteryxSelect logo apos o DbFileInput (ToolID 1), que traz
    # todas as colunas da aba "Base de Faturamento $" (*Unknown/whitelist
    # amplo antes do Select restritivo que so acontece mais a frente, no
    # Node 2, logo antes do Join).
    "Id. Fiscal": "id_fiscal",
    "Tipo de venda": "tipo_venda",
    "Ordem de venda": "ordem_venda",
    "Item": "item",
    "Moeda": "moeda",
    "Montante": "montante",
    "MI": "mi",
    "Montante MI": "montante_mi",
    "Valor de descon.": "valor_descon",
    "Doc. Contábil": "doc_contabil",
    "Data de compensação": "data_compensacao",
    "Data base": "data_base",
    "Classif.Contábil": "classif_contabil",
    "Centro de lucro": "centro_lucro",
    "Clnt.Metranet": "clnt_metranet",
    "Boleto": "boleto",
    "Setor.Ativ": "setor_ativ",
    "CBS": "cbs",
    "IBS Municipal": "ibs_municipal",
    "IBS Estadual": "ibs_estadual",
    "Vlr.Liq": "vlr_liq",
    "Banco.Liqdnte": "banco_liqdnte",
    "Montante.MI": "montante_mi2",
    "Montante.ME": "montante_me",
    "Doc.Compens": "doc_compens",
    "Chave Referência3": "chave_referencia3",
    "Ano/Mês": "ano_mes",
    "Tipo de serviço": "tipo_servico",
    "Nº Pedido": "num_pedido",
    "Id Contrato": "id_contrato",
    "Sigla": "sigla",
    "Estado": "estado",
    "Cidade": "cidade",
    "Dom.Fiscal": "dom_fiscal",
    "Endereço": "endereco",
    "Bairro": "bairro",
    "Cod.Postal": "cod_postal",
}

df_fat_raw = read_excel(PATH_ZFBL5N, "Base de Faturamento $", rename=RENAME_FATURAMENTO)

# Node 68: so entram no calculo de ISS as linhas que efetivamente emitem NF
df_fat = df_fat_raw.filter(F.col("emite_nota") == "SIM")

# Node 134: normaliza o codigo do material para numero (chave de juncao com
# a tabela de servicos). O Node 2 do Alteryx, logo em seguida, restringe a
# um subconjunto de campos antes do Join - aqui mantemos todas as colunas
# (inclusive as nao usadas) no dataframe interno; a restricao real de
# colunas so acontece nos writes finais (Selects finais do Alteryx).
df_fat = df_fat.withColumn("material", F.col("material").cast(DecimalType(9, 0)))

RENAME_SERVICOS = {
    "Produto SAP": "produto_sap",
    "Descrição Serviços": "descricao_servicos",
    "Conta Contábil SAP": "conta_contabil_sap",
    "PIS/COFINS": "pis_cofins",
    "Código Prefeitura2": "codigo_prefeitura2",
    "Alíquota ISS": "aliquota_iss",
    # Demais colunas da aba "B3 - Códigos Novos$", nao usadas em nenhum
    # filtro/join/formula/output. Sao lidas e renomeadas aqui porque o
    # AlteryxSelect do Node 6 (logo apos o input) traz tudo por padrao
    # (*Unknown selected="True") - mas 4 delas sao descartadas ali mesmo,
    # de forma explicita (selected="False"), e nunca chegam a entrar no
    # Join do Node 8: CNAE, Alíquota IRRF, Alíquota Pis/Cofins/Csll e
    # Parecer Tributário (ver .drop(...) logo abaixo, que reproduz esse
    # descarte no mesmo ponto). As demais colunas "passageiras" de verdade
    # seguem no dataframe e so sao descartadas la na frente, no
    # AlteryxSelect do Node 122, bem antes do DbFileOutput "Relatorio
    # Faturamento SP" (Pipeline 2) - o .select() explicito de
    # df_relatorio_faturamento ja reproduz esse descarte final.
    "Conta Contábil RM": "conta_contabil_rm",
    "Item da Lei": "item_lei",
    "Item da Lei2": "item_lei2",
    "Item da Lei\nApós Revisão": "item_lei_apos_revisao",
    "Código Prefeitura": "codigo_prefeitura",
    "CNAE": "cnae",
    "F12": "f12",
    "Alíquota IRRF": "aliquota_irrf",
    "Alíquota Pis/Cofins/Csll": "aliquota_pis_cofins_csll",
    "Parecer Tributário": "parecer_tributario",
    "F17": "f17",
    "F18": "f18",
    "F19": "f19",
    "F20": "f20",
}

df_servicos_raw = read_excel(PATH_SERVICOS_SP, "B3 - Códigos Novos$", rename=RENAME_SERVICOS)

# Node 6: alem de *Unknown=True (traz tudo por padrao), o Select descarta
# explicitamente estas 4 colunas logo apos o input - elas nunca alcancam o
# Join do Node 8, entao o descarte precisa acontecer aqui (e nao mais a
# frente, junto com as demais colunas passageiras).
df_servicos_raw = df_servicos_raw.drop(
    "cnae", "aliquota_irrf", "aliquota_pis_cofins_csll", "parecer_tributario"
)

# Tabela de-para com a aliquota de ISS por codigo de servico SAP. So a
# chave de juncao (produto_sap) precisa de normalizacao de tipo; as demais
# colunas (usadas ou nao a jusante) seguem intactas no dataframe.
df_servicos = df_servicos_raw.withColumn(
    "produto_sap", F.col("produto_sap").cast(DecimalType(9, 0))
)

# Node 8: junta cada linha de faturamento com sua aliquota de ISS cadastrada
df_join = df_fat.join(df_servicos, df_fat["material"] == df_servicos["produto_sap"], "left")

# Ramo "Join" (matched) -> ISS calculado com a aliquota especifica do servico
df_com_aliquota = (
    df_join.filter(F.col("produto_sap").isNotNull())
    .withColumn("iss", F.round(F.col("montante_mi_item") * F.col("aliquota_iss"), 2))
)

# Ramo "Left" (unmatched) -> material sem aliquota cadastrada: "Regime
# Especial", aplica aliquota fixa de fallback de 2% (Nodes 74/75/76)
df_regime_especial = (
    df_join.filter(F.col("produto_sap").isNull())
    .select(df_fat.columns)
    .withColumn("iss", F.round(F.col("montante_mi_item") * F.lit(0.02), 2))
    .withColumn("aliquota_iss", F.lit("2%"))
)

RENAME_OUT_REGIME_ESPECIAL = {
    "empresa": "Empresa", "centro": "Centro", "tp_documento": "Tp.Document",
    "cod_cliente": "Cod. Cliente", "nome_cliente": "Nome Cliente",
    "num_doc": "Nº doc.", "nf_e": "NF-e",
    "data_faturamento": "Data de faturamento", "data_planejamento": "Data de planejamento",
    "montante_mi_item": "Montante MI Item", "material": "Material",
    "desc_material": "Desc.Material", "ir": "IR", "pis": "PIS",
    "cofins": "Cofins", "csll": "Csll", "iss": "ISS", "aliquota_iss": "Alíquota ISS",
    "tipo_servico": "Tipo de serviço",
}

# df_fat (e por tabela, df_regime_especial) agora carrega tambem as colunas
# "passageiras" nao usadas (ver RENAME_FATURAMENTO acima); restringimos aqui,
# na escrita, exatamente as colunas do Select final do Alteryx (Node 76,
# que mantem "Tipo de serviço" alem das demais - selected="True") -
# equivalente ao Select final do Alteryx logo antes do DbFileOutput.
write_excel(
    df_regime_especial.select(list(RENAME_OUT_REGIME_ESPECIAL.keys())),
    PATH_OUT_ZSD_MATERIAL_REG_ESP, rename=RENAME_OUT_REGIME_ESPECIAL,
)


# ===========================================================================
# PIPELINE 2 - Relatorio de Contas a Receber (saida de negocio principal)
# (ToolID 122 -> 52)
# ===========================================================================

df_relatorio_faturamento = df_com_aliquota.select(
    "empresa", "centro", "cod_cliente", "nome_cliente", "num_doc",
    "data_faturamento", "data_planejamento", "montante_mi_item", "iss",
    F.col("produto_sap").alias("cod_servico"),
    F.col("desc_material").alias("nome_servico"),
    F.col("codigo_prefeitura2").alias("cod_pmsp"),
    "ir", "pis", "cofins", "csll", "aliquota_iss", "conta_contabil_sap",
)

RENAME_OUT_RELATORIO_FATURAMENTO = {
    "empresa": "Empresa", "centro": "Centro", "cod_cliente": "Cod. Cliente",
    "nome_cliente": "Nome Cliente", "num_doc": "Nº doc.",
    "data_faturamento": "Data de faturamento", "data_planejamento": "Data de planejamento",
    "montante_mi_item": "Montante MI Item", "iss": "ISS",
    "cod_servico": "Cód.Serviço", "nome_servico": "Nome do Serviço",
    "cod_pmsp": "Cód. PMSP", "ir": "IR", "pis": "PIS", "cofins": "Cofins",
    "csll": "Csll", "aliquota_iss": "Alíquota ISS",
    "conta_contabil_sap": "Conta Contábil SAP",
}

write_excel(df_relatorio_faturamento, PATH_OUT_RELATORIO_FATURAMENTO_SP, rename=RENAME_OUT_RELATORIO_FATURAMENTO)


# ===========================================================================
# PIPELINE 3 - Base de ISS por servico e regime PIS/COFINS (Cum. e Nao-Cum.)
# (ToolIDs 117/118 no Alteryx sao um self-join redundante que so duplica
#  colunas ja presentes; aqui agrupamos direto a partir de df_com_aliquota)
# (ToolIDs 119, 120, 121)
# ===========================================================================

df_cum_nao_cum = (
    df_com_aliquota
    .groupBy("produto_sap", "pis_cofins", "descricao_servicos", "conta_contabil_sap")
    .agg(
        F.sum("montante_mi_item").alias("base_calculo"),
        F.sum("iss").alias("iss"),
    )
    .select(
        F.col("produto_sap").alias("cod_servico"),
        F.col("descricao_servicos").alias("nome_servico"),
        F.col("conta_contabil_sap").alias("conta_contabil"),
        F.col("pis_cofins").alias("regime_apuracao"),
        "base_calculo", "iss",
    )
    .orderBy("regime_apuracao")
)

RENAME_OUT_CUM_NAO_CUM = {
    "cod_servico": "Cód.Serviço", "nome_servico": "Nome do Serviço",
    "conta_contabil": "Conta Contábil", "regime_apuracao": "Regime de Apuração",
    "base_calculo": "Base de Cálculo", "iss": "ISS",
}

write_excel(df_cum_nao_cum, PATH_OUT_CUM_NAO_CUM, rename=RENAME_OUT_CUM_NAO_CUM)


# ===========================================================================
# PIPELINE 4 - Razao contabil do ISS (SAP GL), filtrado para SP
# (ToolIDs 13, 17, 59, 92, 53)
# ===========================================================================

RENAME_RAZAO = {
    "Conta do Razão": "conta_razao",
    "Empresa": "empresa",
    "Nº documento": "num_documento",
    "Data do documento": "data_documento",
    "Data de entrada": "data_entrada",
    "Data de lançamento": "data_lancamento_razao",
    "Conta lnçto.contrap.": "conta_lancto_contrapartida",
    "Chave de lançamento": "chave_lancamento",
    "Montante em moeda interna": "montante_moeda_interna",
    "Moeda interna": "moeda_interna",
    "Referência": "referencia",
    "Centro custo": "centro_custo",
    "Centro de lucro": "centro_lucro",
    "Ordem": "ordem",
    "Texto": "texto",
    "Material": "material",
    "Centro": "centro",
}

df_razao_raw = read_excel(PATH_RAZAO_ISS_B3, "Data$", rename=RENAME_RAZAO)

# Node 17: filtra para o Centro de Sao Paulo
df_razao_sp = df_razao_raw.filter(F.col("centro") == "0100")

RENAME_OUT_RAZAO_SP = {v: k for k, v in RENAME_RAZAO.items()}
write_excel(df_razao_sp, PATH_OUT_RAZAO_ISS_B3_SP, rename=RENAME_OUT_RAZAO_SP)

# Node 92 + 53: total de ISS contabilizado no razao SAP para SP.
# (somar direto e equivalente a agrupar por documento e depois somar)
sap_total = scalar_sum(df_razao_sp, "montante_moeda_interna")


# ===========================================================================
# PIPELINE 5 - NFS-e da prefeitura + reconciliacao de RPS x controle interno
# (ToolIDs 23, 137, 24, 25, 33, 36, 37, 38, 50, 51)
# ===========================================================================

RENAME_NFSE = {
    "Situação da Nota Fiscal": "situacao_nota_fiscal",
    "ISS devido": "iss_devido",
    "Número do RPS": "numero_rps",
    "Valor dos Serviços": "valor_servicos",
    # Demais colunas da aba "NFSe$", nao usadas em nenhum filtro/join/
    # formula/output, mantidas para espelhar o AlteryxSelect (ToolID 24)
    # que traz tudo por padrao (*Unknown selected="True").
    "Tipo de Registro": "tipo_registro",
    "Nº NFS-e": "numero_nfse",
    "Data Hora NFE": "data_hora_nfe",
    "Código de Verificação da NFS-e": "codigo_verificacao_nfse",
    "Tipo de RPS": "tipo_rps",
    "Série do RPS": "serie_rps",
    "Data do Fato Gerador": "data_fato_gerador",
    "Inscrição Municipal do Prestador": "inscricao_municipal_prestador",
    "Indicador de CPF/CNPJ do Prestador": "indicador_cpf_cnpj_prestador",
    "CPF/CNPJ do Prestador": "cpf_cnpj_prestador",
    "Razão Social do Prestador": "razao_social_prestador",
    "Tipo do Endereço do Prestador": "tipo_endereco_prestador",
    "Endereço do Prestador": "endereco_prestador",
    "Número do Endereço do Prestador": "numero_endereco_prestador",
    "Complemento do Endereço do Prestador": "complemento_endereco_prestador",
    "Bairro do Prestador": "bairro_prestador",
    "Cidade do Prestador": "cidade_prestador",
    "UF do Prestador": "uf_prestador",
    "CEP do Prestador": "cep_prestador",
    "Email do Prestador": "email_prestador",
    "Opção Pelo Simples": "opcao_pelo_simples",
    "Data de Cancelamento": "data_cancelamento",
    "Nº da Guia": "numero_guia",
    "Data de Quitação da Guia Vinculada a Nota Fiscal": "data_quitacao_guia",
    "Valor das Deduções": "valor_deducoes",
    "Código do Serviço Prestado na Nota Fiscal": "codigo_servico_prestado",
    "Alíquota": "aliquota",
    "Valor do Crédito": "valor_credito",
    "ISS Retido": "iss_retido",
    "Indicador de CPF/CNPJ do Tomador": "indicador_cpf_cnpj_tomador",
    "CPF/CNPJ do Tomador": "cpf_cnpj_tomador",
    "Inscrição Municipal do Tomador": "inscricao_municipal_tomador",
    "Inscrição Estadual do Tomador": "inscricao_estadual_tomador",
    "Razão Social do Tomador": "razao_social_tomador",
    "Tipo do Endereço do Tomador": "tipo_endereco_tomador",
    "Endereço do Tomador": "endereco_tomador",
    "Número do Endereço do Tomador": "numero_endereco_tomador",
    "Complemento do Endereço do Tomador": "complemento_endereco_tomador",
    "Bairro do Tomador": "bairro_tomador",
    "Cidade do Tomador": "cidade_tomador",
    "UF do Tomador": "uf_tomador",
    "CEP do Tomador": "cep_tomador",
    "Email do Tomador": "email_tomador",
    "Nº NFS-e Substituta": "numero_nfse_substituta",
    "ISS pago": "iss_pago",
    "ISS a pagar": "iss_a_pagar",
    "Indicador de CPF/CNPJ do Intermediário": "indicador_cpf_cnpj_intermediario",
    "CPF/CNPJ do Intermediário": "cpf_cnpj_intermediario",
    "Inscrição Municipal do Intermediário": "inscricao_municipal_intermediario",
    "Razão Social do Intermediário": "razao_social_intermediario",
    "Repasse do Plano de Saúde": "repasse_plano_saude",
    "PIS/PASEP": "pis_pasep",
    "COFINS": "cofins",
    "INSS": "inss",
    "IR": "ir",
    "CSLL": "csll",
    "Carga tributária: Valor": "carga_tributaria_valor",
    "Carga tributária: Porcentagem": "carga_tributaria_porcentagem",
    "Carga tributária: Fonte": "carga_tributaria_fonte",
    "CEI": "cei",
    "Matrícula da Obra": "matricula_obra",
    "Município Prestação - cód. IBGE": "municipio_prestacao_cod_ibge",
    "Situação do Aceite": "situacao_aceite",
    "Encapsulamento": "encapsulamento",
    "Valor Total Recebido": "valor_total_recebido",
    "Tipo de Consolidação": "tipo_consolidacao",
    "Nº NFS-e Consolidada": "numero_nfse_consolidada",
    "Campo Reservado": "campo_reservado",
    "Discriminação dos Serviços": "discriminacao_servicos",
}

df_nfse_raw = read_excel(PATH_NFS_PREFEITURA, "NFSe$", rename=RENAME_NFSE)

# Node 137: exclui notas canceladas ("C")
df_nfse_ok = df_nfse_raw.filter(F.col("situacao_nota_fiscal") != "C")

# Node 25: total de ISS declarado/devido segundo as notas emitidas na prefeitura
prefeitura_total = scalar_sum(df_nfse_ok, "iss_devido")

# Node 36: RPS das notas emitidas (para conciliar com o controle interno ZSD).
# So a chave de juncao (numero_rps) precisa de normalizacao de tipo; as
# demais colunas seguem intactas no dataframe.
df_nfse_rps = df_nfse_ok.withColumn(
    "numero_rps", F.col("numero_rps").cast(DecimalType(19, 0))
)

RENAME_ZSD = {
    "N.RPS": "n_rps",
    "Dt.Lançamento": "dt_lancamento",
    "Valor": "valor",
    # Demais colunas da aba "Data$" do ZSD, nao usadas em nenhum filtro/
    # join/formula/output, mantidas para espelhar o AlteryxSelect (ToolID
    # 36) que traz tudo por padrao (*Unknown selected="True").
    "Empresa": "empresa",
    "Centro": "centro",
    "Loc. Negocio": "loc_negocio",
    "Nº documento": "num_documento",
    "Item": "item",
    "N.Fatura": "n_fatura",
    "Usuário": "usuario",
    "Data RPS": "data_rps",
    "Cc.Contábil": "cc_contabil",
    "Status": "status",
}

df_zsd_raw = read_excel(PATH_ZSD, "Data$", rename=RENAME_ZSD)

# Node 37: RPS gerados internamente no SAP antes da conversao em NFS-e
# oficial. So a chave de juncao (n_rps) precisa de normalizacao de tipo; as
# demais colunas seguem intactas no dataframe.
df_zsd_rps = df_zsd_raw.withColumn("n_rps", F.col("n_rps").cast(DecimalType(19, 0)))

# Node 38: full outer join para identificar divergencias dos dois lados
df_join_rps = df_nfse_rps.join(
    df_zsd_rps, df_nfse_rps["numero_rps"] == df_zsd_rps["n_rps"], "outer"
)

# Notas emitidas na prefeitura sem registro correspondente no controle
# interno. df_nfse_rps/df_zsd_rps agora carregam tambem as colunas
# "passageiras" nao usadas (ver RENAME_NFSE/RENAME_ZSD acima); por isso
# selecionamos aqui, explicitamente, as mesmas 2 colunas que ja eram o
# resultado do antigo ".select(df_nfse_rps.columns)" (equivalente ao Select
# final do Alteryx antes deste DbFileOutput) - sem mudar o rename= abaixo.
df_prefeitura_sem_zsd = (
    df_join_rps.filter(F.col("n_rps").isNull()).select("numero_rps", "valor_servicos")
)
write_excel(
    df_prefeitura_sem_zsd, PATH_OUT_PREFEITURA_X_ZSD,
    rename={"numero_rps": "Número do RPS", "valor_servicos": "Valor dos Serviços"},
)

# RPS gerados internamente sem NFS-e correspondente encontrada na
# prefeitura. Mesma logica do bloco anterior: selecionamos explicitamente
# as 3 colunas que ja eram o resultado do antigo
# ".select(df_zsd_rps.columns)", sem mudar o rename= abaixo.
df_zsd_sem_prefeitura = (
    df_join_rps.filter(F.col("numero_rps").isNull())
    .select("n_rps", "dt_lancamento", "valor")
)
write_excel(
    df_zsd_sem_prefeitura, PATH_OUT_ZSD_X_PREFEITURA,
    rename={"n_rps": "N.RPS", "dt_lancamento": "Dt.Lançamento", "valor": "Valor"},
)


# ===========================================================================
# PIPELINE 6 - Reconciliacao final: Faturamento calculado x SAP x Prefeitura
# (ToolIDs 54, 138, 141/142/143, 145/146/149, 147/148; 48/49)
# No Alteryx original esses totais escalares sao concatenados via um
# "cross join" artificial (Formula Total=1 + Join Total=Total). Como cada
# soma sempre produz exatamente 1 linha, esse join sempre casa - por isso
# calculamos os escalares diretamente em Python, que e equivalente e mais
# simples/eficiente.
# ===========================================================================

faturamento_total = scalar_sum(df_com_aliquota, "iss")            # Node 54 -> "Faturamento"
iss_regime_especial = scalar_sum(df_regime_especial, "iss")        # Node 138 -> "ISS Reg. Especial"
fat_mais_reg_especial = round(faturamento_total + iss_regime_especial, 2)      # Node 142
faturamento_x_sap = round(fat_mais_reg_especial + sap_total, 2)                # Node 149
faturamento_x_prefeitura = round(fat_mais_reg_especial - prefeitura_total, 2)  # Node 147

df_relatorio_final = spark.createDataFrame(
    [(fat_mais_reg_especial, sap_total, faturamento_x_sap, prefeitura_total, faturamento_x_prefeitura)],
    ["fat_reg_especial", "sap", "faturamento_x_sap", "prefeitura", "faturamento_x_prefeitura"],
)

RENAME_OUT_RELATORIO_FINAL = {
    "fat_reg_especial": "Fat. + Reg. Especial",
    "sap": "SAP",
    "faturamento_x_sap": "Faturamento x SAP",
    "prefeitura": "PREFEITURA",
    "faturamento_x_prefeitura": "Faturamento x Prefeitura",
}
write_excel(df_relatorio_final, PATH_OUT_RELATORIO_FINAL, rename=RENAME_OUT_RELATORIO_FINAL)

# Nodes 48/49: saidas de excecao do join Total=Total entre os agregados
# Faturamento e SAP. Como ambos os lados sempre tem exatamente 1 linha, essas
# saidas ficam sempre vazias em operacao normal - mantidas so para paridade.
write_excel(
    spark.createDataFrame([], StructType([StructField("fat_reg_especial", DoubleType())])),
    PATH_OUT_FATURAMENTO_X_SAP_EXC, rename={"fat_reg_especial": "Fat. + Reg. Especial"},
)
write_excel(
    spark.createDataFrame([], StructType([StructField("sap", DoubleType())])),
    PATH_OUT_SAP_X_FATURAMENTO_EXC, rename={"sap": "SAP"},
)


# ===========================================================================
# PIPELINE 7 - Lancamento contabil (partida dobrada) do ISS a pagar
# (ToolIDs 123, 129, 126, 125, 124, 127, 128)
# ===========================================================================

hoje = datetime.date.today()
primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

data_lancamento = f"10.{hoje.month:02d}.{hoje.year}"
periodo = f"{hoje.month:02d}"
referencia_lancamento = ultimo_dia_mes_anterior.strftime("%d.%m.%Y")
texto = (
    "REF. REC. ISS FAT SP E REG. DE SEGUROS - "
    f"{ultimo_dia_mes_anterior.month:02d}/{ultimo_dia_mes_anterior.year}"
)
montante = round(prefeitura_total, 2) if prefeitura_total is not None else 0.0

colunas_lancamento_clean = [
    "data_documento", "tp_doc", "empresa", "data_lancamento_lc", "periodo",
    "moeda_taxa_cambio", "grp_ledger", "referencia", "txt_cab_doc", "chv_lancto",
    "conta", "cod_rze", "montante", "doc_compras", "item",
    "forma_pagamento", "bloqueio_pagamento", "condicao_pagamento",
    "data_base", "atribuicao", "cod_imposto", "domicilio_fiscal", "texto",
    "local_negocios", "centro_custo", "elemento_pep", "ordem",
    "numero_atividade", "diagrama_rede", "centro_lucro", "divisao",
    "tipo_movimento", "sociedade_parceira",
]

schema_lancamento = StructType([
    StructField("data_documento", StringType()),
    StructField("tp_doc", StringType()),
    StructField("empresa", IntegerType()),
    StructField("data_lancamento_lc", StringType()),
    StructField("periodo", StringType()),
    StructField("moeda_taxa_cambio", StringType()),
    StructField("grp_ledger", StringType()),
    StructField("referencia", StringType()),
    StructField("txt_cab_doc", StringType()),
    StructField("chv_lancto", IntegerType()),
    StructField("conta", LongType()),
    StructField("cod_rze", StringType()),
    StructField("montante", DecimalType(19, 2)),
    StructField("doc_compras", StringType()),
    StructField("item", StringType()),
    StructField("forma_pagamento", StringType()),
    StructField("bloqueio_pagamento", StringType()),
    StructField("condicao_pagamento", StringType()),
    StructField("data_base", StringType()),
    StructField("atribuicao", StringType()),
    StructField("cod_imposto", StringType()),
    StructField("domicilio_fiscal", StringType()),
    StructField("texto", StringType()),
    StructField("local_negocios", StringType()),
    StructField("centro_custo", StringType()),
    StructField("elemento_pep", StringType()),
    StructField("ordem", StringType()),
    StructField("numero_atividade", StringType()),
    StructField("diagrama_rede", StringType()),
    StructField("centro_lucro", StringType()),
    StructField("divisao", StringType()),
    StructField("tipo_movimento", StringType()),
    StructField("sociedade_parceira", StringType()),
])

# Linha 1 = debito (chave de lancamento 40, conta 21400122)
linha_debito = (
    data_lancamento, "FE", 1000, data_lancamento, periodo, "BRL", None,
    referencia_lancamento, "DIVERSOS", 40, 21400122, None, montante, None, None,
    "", None, "", "", None, None, None, texto, None, None, None, None,
    None, None, None, None, None, None,
)

# Linha 2 = credito (chave de lancamento 31, conta 400006)
linha_credito = (
    "", "", None, "", "", "", None, referencia_lancamento, None, 31, 400006, None,
    montante, None, None, "O", None, "Z001", data_lancamento, None, None,
    None, texto, None, None, None, None, None, None, None, None, None,
    None,
)

df_lancamento = (
    spark.createDataFrame([linha_debito, linha_credito], schema_lancamento)
    .select(colunas_lancamento_clean)
)

RENAME_OUT_LANCAMENTO = {
    "data_documento": "Data documento", "tp_doc": "Tp.doc.", "empresa": "Empresa",
    "data_lancamento_lc": "Data Lançamento", "periodo": "Período",
    "moeda_taxa_cambio": "Moeda/taxa câm.", "grp_ledger": "Grp. ledger",
    "referencia": "Referência", "txt_cab_doc": "Txt.cab.doc.", "chv_lancto": "ChvLnçt",
    "conta": "Conta", "cod_rze": "Cód.RzE", "montante": "Montante",
    "doc_compras": "Doc.compras", "item": "Item",
    "forma_pagamento": "Forma de Pagamento", "bloqueio_pagamento": "Bloqueio de Pagamento",
    "condicao_pagamento": "Condição de Pagamento", "data_base": "Data Base",
    "atribuicao": "Atribuição", "cod_imposto": "Cód.Imposto",
    "domicilio_fiscal": "DomicílioFiscal", "texto": "Texto",
    "local_negocios": "Local de Negócios", "centro_custo": "Centro de Custo",
    "elemento_pep": "Elemento PEP", "ordem": "Ordem",
    "numero_atividade": "Número de Atividade", "diagrama_rede": "Diagrama de Rede",
    "centro_lucro": "Centro de lucro", "divisao": "Divisão",
    "tipo_movimento": "Tipo de Movimento", "sociedade_parceira": "Sociedade Parceira",
}

write_excel(df_lancamento, PATH_OUT_LANCTO_PAGTO_ISS, rename=RENAME_OUT_LANCAMENTO)
