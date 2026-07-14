"""
Migracao PySpark do fluxo Alteryx "Fluxo - UIF.yxmd".

Apuracao e reconciliacao do ISS (imposto municipal sobre servicos) do
Centro SAP 0285 (UIF/UFIN), cruzando 5 fontes:
  1. ZFBL5N.xlsx          - linhas de faturamento SAP (contas a receber),
                             aba `com NF$` (ja pre-filtrada na origem para
                             linhas que emitem nota fiscal - por isso, ao
                             contrario do fluxo SP, nao ha filtro "Emite
                             Nota?" aqui)
  2. Servicos UIF.xlsx     - tabela de-para: codigo de servico -> aliquota
                             de ISS (aba `Servicos SP_UFIN_Faturamento$`)
  3. Razao ISS B3.XLSX     - razao contabil (livro-razao) da conta de ISS
  4. NFS - Prefeitura.xlsx - notas fiscais de servico emitidas na prefeitura
  5. ZSD.XLSX              - controle interno SAP dos RPS gerados

Saidas (equivalentes aos DbFileOutput do Alteryx, todas em .../UIF/Saida):
  - Relatório Faturamento UFIN MÊS 2026 (Contas a Receber).xlsx
  - Cum. e Não-Cum..xlsx
  - Razão ISS B3 UIF.xlsx
  - Prefeitura x ZSD.xlsx / ZSD x Prefeitura.xlsx (reconciliacao de RPS)
  - Faturamento x SAP.xlsx / SAP x Faturamento.xlsx (excecoes da
    reconciliacao Faturamento x Razao - ver Pipeline 6; ao contrario do
    fluxo SP, aqui essas duas saidas podem legitimamente conter linhas)
  - Relatório Final Alteryx.xlsx (reconciliacao Faturamento x SAP x Prefeitura)
  - Lançto de Pag ISS Faturamento - B3 UIF.xlsx (lancamento contabil,
    partida dobrada)

Convencao de nomes de coluna
-----------------------------
Toda coluna usada dentro do Spark (filter/select/join/groupBy/withColumn)
segue snake_case sem acentos, espacos ou pontuacao (ex.: "Mont.moeda doc."
-> mont_moeda_doc). Isso e proposital, nao cosmetico: um "." em nome de
coluna e interpretado pelo Spark como separador de campo aninhado dentro
de F.col()/select() (F.col("Mont.moeda doc.") tenta resolver o campo
"moeda doc." dentro de uma coluna "Mont" e falha), e nomes com
espaco/acento quebram em escritas Delta e em varios conectores. Os nomes
de negocio originais (em portugues, com acentos) so sao recolocados no
ultimo passo, na escrita do Excel, para manter os relatorios legiveis
para o time.

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

spark = SparkSession.builder.appName("Fluxo_ISS_UIF").getOrCreate()

# ---------------------------------------------------------------------------
# Caminhos de entrada / saida (ajustar para o mount/volume do Databricks)
# ---------------------------------------------------------------------------
BASE_IN = "/Volumes/iss/uif/entrada"
BASE_OUT = "/Volumes/iss/uif/saida"

PATH_ZFBL5N = f"{BASE_IN}/ZFBL5N.xlsx"
PATH_SERVICOS_UIF = f"{BASE_IN}/Serviços UIF.xlsx"
PATH_RAZAO_ISS_B3 = f"{BASE_IN}/Razão ISS B3.XLSX"
PATH_NFS_PREFEITURA = f"{BASE_IN}/NFS - Prefeitura.xlsx"
PATH_ZSD = f"{BASE_IN}/ZSD.XLSX"

PATH_OUT_RELATORIO_FATURAMENTO_UFIN = f"{BASE_OUT}/Relatório Faturamento UFIN MÊS 2026 (Contas a Receber).xlsx"
PATH_OUT_CUM_NAO_CUM = f"{BASE_OUT}/Cum. e Não-Cum..xlsx"
PATH_OUT_RAZAO_ISS_B3_UIF = f"{BASE_OUT}/Razão ISS B3 UIF.xlsx"
PATH_OUT_PREFEITURA_X_ZSD = f"{BASE_OUT}/Prefeitura x ZSD.xlsx"
PATH_OUT_ZSD_X_PREFEITURA = f"{BASE_OUT}/ZSD x Prefeitura.xlsx"
PATH_OUT_FATURAMENTO_X_SAP_EXC = f"{BASE_OUT}/Faturamento x SAP.xlsx"
PATH_OUT_SAP_X_FATURAMENTO_EXC = f"{BASE_OUT}/SAP x Faturamento.xlsx"
PATH_OUT_RELATORIO_FINAL = f"{BASE_OUT}/Relatório Final Alteryx.xlsx"
PATH_OUT_LANCTO_PAGTO_ISS = f"{BASE_OUT}/Lançto de Pag ISS Faturamento - B3 UIF.xlsx"


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
# (ToolIDs 1, 2, 5, 6, 8, 10, 11)
# Diferencas em relacao ao fluxo SP: (a) a aba `com NF$` do ZFBL5N ja vem
# pre-filtrada para linhas que emitem nota fiscal, entao nao ha filtro
# "Emite Nota?" aqui; (b) o Join (Nodes 8) so tem saida "Join" (matched)
# consumida a jusante - as saidas "Left"/"Right" (materiais sem aliquota
# cadastrada e vice-versa) nao alimentam nenhum output no Alteryx, ou
# seja, nao existe o conceito de "Regime Especial" (fallback de 2%) que
# existe no fluxo SP; linhas sem correspondencia sao simplesmente
# descartadas.
# ===========================================================================

RENAME_FATURAMENTO = {
    "Empresa": "empresa",
    "Centro": "centro",
    "Tipo de documento": "tipo_documento",
    "Conta": "conta",
    "Nome Cliente": "nome_cliente",
    "Nº documento": "num_documento",
    "Data do documento": "data_documento",
    "Vencimento líquido": "vencimento_liquido",
    "Mont.moeda doc.": "mont_moeda_doc",
    "Cód.Serviço": "cod_servico",
    "Nome do Serviço": "nome_servico",
    "IR": "ir",
    "PIS": "pis",
    "COFINS": "cofins",
    "CSLL": "csll",
}

df_fat_raw = read_excel(PATH_ZFBL5N, "com NF$", rename=RENAME_FATURAMENTO)

# Node 2: normaliza o codigo do servico para numero (chave de juncao com a
# tabela de servicos) e mantem apenas os campos usados a jusante
df_fat = (
    df_fat_raw
    .withColumn("cod_servico", F.col("cod_servico").cast(DecimalType(9, 0)))
    .select(
        "empresa", "centro", "tipo_documento", "conta", "nome_cliente",
        "num_documento", "data_documento", "vencimento_liquido",
        "mont_moeda_doc", "cod_servico", "nome_servico",
        "ir", "pis", "cofins", "csll",
    )
)

RENAME_SERVICOS = {
    "Material": "material",
    "Conta Contábil": "conta_contabil",
    "Código Serviço São Paulo": "codigo_servico_sp",
    "Alíquota São Paulo": "aliquota_sp",
    "PIS/COFINS - Regime de Apuração": "regime_apuracao",
}

df_servicos_raw = read_excel(
    PATH_SERVICOS_UIF, "Serviços SP_UFIN_Faturamento$", rename=RENAME_SERVICOS
)

# Node 6: tabela de-para com a aliquota de ISS por codigo de servico
df_servicos = df_servicos_raw.select(
    F.col("material").cast(DecimalType(9, 0)).alias("material"),
    "conta_contabil", "codigo_servico_sp", "aliquota_sp", "regime_apuracao",
)

# Node 8 (saida "Join"/matched apenas): junta cada linha de faturamento
# com sua aliquota de ISS cadastrada. Linhas sem correspondencia (saidas
# "Left"/"Right" do Alteryx) nao sao usadas em nenhum output - inner join.
df_com_iss = (
    df_fat.join(df_servicos, df_fat["cod_servico"] == df_servicos["material"], "inner")
    .withColumn("iss", F.round(F.col("mont_moeda_doc") * F.col("aliquota_sp"), 2))
    .select(
        "empresa", "centro", "tipo_documento", "conta", "nome_cliente",
        "num_documento", "data_documento", "vencimento_liquido",
        "mont_moeda_doc", "cod_servico", "nome_servico",
        "ir", "pis", "cofins", "csll",
        "conta_contabil", "codigo_servico_sp", "aliquota_sp", "regime_apuracao",
        "iss",
    )
)

RENAME_COM_ISS = {
    "empresa": "Empresa", "centro": "Centro", "tipo_documento": "Tipo de documento",
    "conta": "Conta", "nome_cliente": "Nome Cliente", "num_documento": "Nº documento",
    "data_documento": "Data do documento", "vencimento_liquido": "Vencimento líquido",
    "mont_moeda_doc": "Mont.moeda doc.", "cod_servico": "Cód.Serviço",
    "nome_servico": "Nome do Serviço", "ir": "IR", "pis": "PIS",
    "cofins": "COFINS", "csll": "CSLL", "conta_contabil": "Conta Contábil",
    "codigo_servico_sp": "Código Serviço São Paulo", "aliquota_sp": "Alíquota São Paulo",
    "regime_apuracao": "Regime de Apuração", "iss": "ISS",
}


# ===========================================================================
# PIPELINE 2 - Relatorio de Faturamento UFIN (saida de negocio principal)
# (ToolIDs 75, 76 -> 52)
# ===========================================================================

df_relatorio_faturamento = (
    df_com_iss
    .withColumn("data_documento", F.date_format(F.col("data_documento").cast("date"), "dd/MM/yy"))
    .withColumn("vencimento_liquido", F.date_format(F.col("vencimento_liquido").cast("date"), "dd/MM/yy"))
    .select(
        "empresa", "centro", "tipo_documento", "conta", "nome_cliente",
        "num_documento", "data_documento", "vencimento_liquido",
        "mont_moeda_doc", "iss", "cod_servico", "nome_servico",
        "ir", "pis", "cofins", "csll",
        "aliquota_sp", "codigo_servico_sp", "conta_contabil",
    )
)

write_excel(df_relatorio_faturamento, PATH_OUT_RELATORIO_FATURAMENTO_UFIN, rename=RENAME_COM_ISS)


# ===========================================================================
# PIPELINE 3 - Base de ISS por servico e regime PIS/COFINS (Cum. e Não-Cum.)
# (ToolIDs 56, 60, 61, 62, 63, 67 no Alteryx re-fazem um join redundante
#  entre a tabela de servicos e o dado ja calculado, apenas para recuperar
#  o campo "Regime de Apuração" - que ja esta disponivel em df_com_iss
#  porque, ao contrario do Node 10 do fluxo SP, o Node 10 deste fluxo
#  descarta esse campo apos o primeiro join do Pipeline 1. Como a chave de
#  re-juncao [Material]=[Cód.Serviço] e exatamente a mesma do Pipeline 1,
#  o resultado e identico a simplesmente manter "regime_apuracao" desde a
#  leitura da tabela de servicos - por isso agrupamos direto a partir de
#  df_com_iss, sem reproduzir o join redundante.)
# ===========================================================================

df_cum_nao_cum = (
    df_com_iss
    .groupBy("cod_servico", "regime_apuracao", "nome_servico", "conta_contabil")
    .agg(
        F.sum("mont_moeda_doc").alias("base_calculo"),
        F.sum("iss").alias("iss_sum"),
    )
    .select(
        "cod_servico", "nome_servico", "conta_contabil", "regime_apuracao",
        "base_calculo", "iss_sum",
    )
    .orderBy("regime_apuracao")
)

# Nomes com espaco inicial (" Base de Cálculo", " ISS") reproduzem
# literalmente os cabecalhos configurados no Select (Node 62) do Alteryx.
RENAME_OUT_CUM_NAO_CUM = {
    "cod_servico": "Cód.Serviço", "nome_servico": "Nome do Serviço",
    "conta_contabil": "Conta Contábil", "regime_apuracao": "Regime de Apuração",
    "base_calculo": " Base de Cálculo", "iss_sum": " ISS",
}

write_excel(df_cum_nao_cum, PATH_OUT_CUM_NAO_CUM, rename=RENAME_OUT_CUM_NAO_CUM)


# ===========================================================================
# PIPELINE 4 - Razao contabil do ISS (SAP GL), filtrado para o Centro UIF
# (ToolIDs 13, 17, 77, 53, 14, 55)
# ===========================================================================

RENAME_RAZAO = {
    "Conta do Razão": "conta_razao",
    "Empresa": "empresa_razao",
    "Nº documento": "num_documento",
    "Data do documento": "data_documento_razao",
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
    "Material": "material_razao",
    "Centro": "centro",
}

df_razao_raw = read_excel(PATH_RAZAO_ISS_B3, "Data$", rename=RENAME_RAZAO)

# Node 17: filtra para o Centro da UIF
df_razao_uif = df_razao_raw.filter(F.col("centro") == "0285")

# Node 77: reformata as 3 colunas de data para dd/mm/aaaa antes de gravar
df_razao_uif_fmt = (
    df_razao_uif
    .withColumn("data_documento_razao", F.date_format(F.col("data_documento_razao").cast("date"), "dd/MM/yyyy"))
    .withColumn("data_entrada", F.date_format(F.col("data_entrada").cast("date"), "dd/MM/yyyy"))
    .withColumn("data_lancamento_razao", F.date_format(F.col("data_lancamento_razao").cast("date"), "dd/MM/yyyy"))
)

RENAME_OUT_RAZAO_UIF = {v: k for k, v in RENAME_RAZAO.items()}
write_excel(df_razao_uif_fmt, PATH_OUT_RAZAO_ISS_B3_UIF, rename=RENAME_OUT_RAZAO_UIF)

# Node 14 + 55: razao agregado por documento (usado no Pipeline 6 para
# casar cada linha de faturamento com o total contabilizado no SAP para o
# mesmo Nº documento)
df_razao_por_doc = (
    df_razao_uif
    .groupBy("num_documento")
    .agg(F.sum("montante_moeda_interna").alias("montante_moeda_interna"))
)


# ===========================================================================
# PIPELINE 5 - NFS-e da prefeitura + reconciliacao de RPS x controle interno
# (ToolIDs 23, 54, 24, 25, 33, 36, 37, 38, 50, 51)
# Diferenca em relacao ao fluxo SP: o filtro de situacao da nota (Node 54,
# modo Simple, operador "=") mantem apenas notas com situacao "T", em vez
# de excluir apenas as canceladas ("!= C") como no fluxo SP.
# ===========================================================================

RENAME_NFSE = {
    "Situação da Nota Fiscal": "situacao_nota_fiscal",
    "ISS devido": "iss_devido",
    "Número do RPS": "numero_rps",
    "Data do Fato Gerador": "data_fato_gerador",
    "Valor dos Serviços": "valor_servicos",
}

df_nfse_raw = read_excel(PATH_NFS_PREFEITURA, "NFSe$", rename=RENAME_NFSE)

# Node 54: mantem apenas notas com situacao "T"
df_nfse_ok = df_nfse_raw.filter(F.col("situacao_nota_fiscal") == "T")

# Node 25: total de ISS declarado/devido segundo as notas emitidas na prefeitura
prefeitura_total = scalar_sum(df_nfse_ok, "iss_devido")

# Node 36: RPS das notas emitidas (para conciliar com o controle interno ZSD)
df_nfse_rps = df_nfse_ok.select(
    F.col("numero_rps").cast(DecimalType(19, 0)).alias("numero_rps"),
    "data_fato_gerador", "valor_servicos",
)

RENAME_ZSD = {
    "N.RPS": "n_rps",
    "Dt.Lançamento": "dt_lancamento",
    "Valor": "valor",
}

df_zsd_raw = read_excel(PATH_ZSD, "Data$", rename=RENAME_ZSD)

# Node 37: RPS gerados internamente no SAP antes da conversao em NFS-e oficial
df_zsd_rps = df_zsd_raw.select(
    F.col("n_rps").cast(DecimalType(19, 0)).alias("n_rps"),
    "dt_lancamento", "valor",
)

# Node 38: full outer join para identificar divergencias dos dois lados
df_join_rps = df_nfse_rps.join(
    df_zsd_rps, df_nfse_rps["numero_rps"] == df_zsd_rps["n_rps"], "outer"
)

# Notas emitidas na prefeitura sem registro correspondente no controle interno
df_prefeitura_sem_zsd = (
    df_join_rps.filter(F.col("n_rps").isNull()).select(df_nfse_rps.columns)
)
write_excel(
    df_prefeitura_sem_zsd, PATH_OUT_PREFEITURA_X_ZSD,
    rename={
        "numero_rps": "Número do RPS", "data_fato_gerador": "Data do Fato Gerador",
        "valor_servicos": "Valor dos Serviços",
    },
)

# RPS gerados internamente sem NFS-e correspondente encontrada na prefeitura
df_zsd_sem_prefeitura = (
    df_join_rps.filter(F.col("numero_rps").isNull()).select(df_zsd_rps.columns)
)
write_excel(
    df_zsd_sem_prefeitura, PATH_OUT_ZSD_X_PREFEITURA,
    rename={"n_rps": "N.RPS", "dt_lancamento": "Dt.Lançamento", "valor": "Valor"},
)


# ===========================================================================
# PIPELINE 6 - Reconciliacao final: Faturamento x Razao(SAP) x Prefeitura
# (ToolIDs 20, 46, 47, 27, 28, 29, 31, 48, 49)
#
# Este pipeline combina DOIS idiomas diferentes do Alteryx, tratados de
# forma diferente aqui:
#   - Node 20 e um JOIN DE VERDADE (nao um cross join artificial Total=1):
#     casa cada linha de faturamento com o total do razao SAP para o mesmo
#     "Nº documento". Por isso e reproduzido fielmente como um join Spark
#     (outer, para tambem gerar as excecoes dos Nodes 48/49), e nao
#     simplificado.
#   - Nodes 27/28 SAO o idioma de cross join artificial de agregados
#     escalares de 1 linha (Formula Total=1 + Join Total=Total) para
#     combinar o total Faturamento+SAP com o total da Prefeitura. Como
#     cada lado sempre tem exatamente 1 linha, esse join sempre casa - por
#     isso os escalares sao calculados diretamente em Python, equivalente
#     e mais simples, seguindo o mesmo padrao usado no fluxo SP.
# ===========================================================================

# Node 20 (outer join real por Nº documento)
df_fat_x_razao = df_com_iss.join(df_razao_por_doc, on="num_documento", how="outer")

# "Join" (matched): linhas de faturamento com Razao correspondente -> usadas
# para os totais "Faturamento" (Node 46) e "SAP" (Node 46)
df_fat_x_razao_matched = df_fat_x_razao.filter(
    F.col("iss").isNotNull() & F.col("montante_moeda_interna").isNotNull()
)

# "Left" (faturamento sem razao correspondente)
df_faturamento_sem_razao = (
    df_fat_x_razao
    .filter(F.col("iss").isNotNull() & F.col("montante_moeda_interna").isNull())
    .select(df_com_iss.columns)
)
write_excel(df_faturamento_sem_razao, PATH_OUT_FATURAMENTO_X_SAP_EXC, rename=RENAME_COM_ISS)

# "Right" (razao sem faturamento correspondente)
df_razao_sem_faturamento = (
    df_fat_x_razao
    .filter(F.col("iss").isNull() & F.col("montante_moeda_interna").isNotNull())
    .select(df_razao_por_doc.columns)
)
write_excel(
    df_razao_sem_faturamento, PATH_OUT_SAP_X_FATURAMENTO_EXC,
    rename={"num_documento": "Nº documento", "montante_moeda_interna": "Montante em moeda interna"},
)

faturamento_total = scalar_sum(df_fat_x_razao_matched, "iss")                       # Node 46 -> "Faturamento"
sap_total = scalar_sum(df_fat_x_razao_matched, "montante_moeda_interna")            # Node 46 -> "SAP"
faturamento_x_sap = round(faturamento_total + sap_total, 2)                         # Node 47
faturamento_x_prefeitura = round(faturamento_total - prefeitura_total, 2)           # Node 29

df_relatorio_final = spark.createDataFrame(
    [(faturamento_total, sap_total, faturamento_x_sap, prefeitura_total, faturamento_x_prefeitura)],
    ["faturamento", "sap", "faturamento_x_sap", "prefeitura", "faturamento_x_prefeitura"],
)

RENAME_OUT_RELATORIO_FINAL = {
    "faturamento": "Faturamento",
    "sap": "SAP",
    "faturamento_x_sap": "Faturamento x SAP",
    "prefeitura": "PREFEITURA",
    "faturamento_x_prefeitura": "Faturamento x Prefeitura",
}
write_excel(df_relatorio_final, PATH_OUT_RELATORIO_FINAL, rename=RENAME_OUT_RELATORIO_FINAL)


# ===========================================================================
# PIPELINE 7 - Lancamento contabil (partida dobrada) do ISS a pagar
# (ToolIDs 68, 69, 70, 71, 72, 73, 74)
# ===========================================================================

hoje = datetime.date.today()
primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

data_lancamento = f"10.{hoje.month:02d}.{hoje.year}"
periodo = f"{hoje.month:02d}"
referencia_lancamento = ultimo_dia_mes_anterior.strftime("%d.%m.%Y")
texto = (
    "REF.REC. ISS S/ FATURAMENTO UIF - "
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
