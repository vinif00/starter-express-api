"""
Migracao PySpark do fluxo Alteryx "UTVM - Regime Especial.yxmd".

Apuracao e reconciliacao do ISS (imposto municipal sobre servicos) do
Centro SAP 0290 (UTVM - Regime Especial), cruzando 4 fontes:
  1. ZFBL5N.xlsx             - linhas de faturamento SAP (contas a receber)
  2. Servicos UTVM.xlsx       - tabela de-para: material -> codigo de servico e aliquota
  3. Bal_Antes.xlsx           - balancete do mes anterior (dados a partir da linha 15)
  4. Bal_Depois.xlsx          - balancete do mes atual (dados a partir da linha 15)

Saidas (equivalentes aos DbFileOutput do Alteryx):
  - Dados NFSe emitidas em nome da B3.xlsx
  - Relatorio Regime Especial - UTVM - MES 2026 (Contas a Receber).xlsx
  - Check II_Contabil.xlsx
  - Lancto de Pag ISS Reg. Especial - B3 - UTVM.xlsx (lancamento contabil, 3 linhas)
  - Sobra - Relatorio Balcao.xlsx
  - Contas Novas.xlsx

Convencao de nomes de coluna
-----------------------------
Toda coluna usada dentro do Spark (filter/select/join/groupBy/withColumn)
segue snake_case sem acentos, espacos ou pontuacao. Os nomes de negocio
originais (em portugues, com acentos) so sao recolocados na escrita do
Excel, para manter os relatorios legiveis para o time.

Requer pandas + openpyxl no cluster para leitura/escrita de .xlsx
(ja presentes no Databricks Runtime padrao; senao: %pip install openpyxl).
"""

import datetime

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)

spark = SparkSession.builder.appName("Regime_Especial_UTVM").getOrCreate()

# ---------------------------------------------------------------------------
# Caminhos de entrada / saida (ajustar para o mount/volume do Databricks)
# ---------------------------------------------------------------------------
BASE_IN = "/Volumes/iss/utvm/entrada"
BASE_OUT = "/Volumes/iss/utvm/saida"

PATH_ZFBL5N = f"{BASE_IN}/ZFBL5N.xlsx"
PATH_SERVICOS_UTVM = f"{BASE_IN}/Servicos UTVM.xlsx"
PATH_BAL_ANTES = f"{BASE_IN}/Bal_Antes.xlsx"
PATH_BAL_DEPOIS = f"{BASE_IN}/Bal_Depois.xlsx"

PATH_OUT_NFSE_B3 = f"{BASE_OUT}/Dados NFSe emitidas em nome da B3.xlsx"
PATH_OUT_RELATORIO_CONTAS_RECEBER = f"{BASE_OUT}/Relatorio Regime Especial - UTVM - MES 2026 (Contas a Receber).xlsx"
PATH_OUT_CHECK_II_CONTABIL = f"{BASE_OUT}/Check II_Contabil.xlsx"
PATH_OUT_LANCTO_PAG_ISS = f"{BASE_OUT}/Lancto de Pag ISS Reg. Especial - B3 - UTVM.xlsx"
PATH_OUT_SOBRA_BALCAO = f"{BASE_OUT}/Sobra - Relatorio Balcao.xlsx"
PATH_OUT_CONTAS_NOVAS = f"{BASE_OUT}/Contas Novas.xlsx"


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
# PIPELINE 1 - Carregamento das 4 fontes de dados
# (ToolIDs 1, 23, 4, 22 - DbFileInputs; 5, 34 - Formulas de conversao)
# ===========================================================================

# --- 1a. Servicos UTVM (tabela mestre de servicos) ---
# (ToolIDs 1, 23)
RENAME_SERVICOS = {
    "Empresa": "empresa",
    "Cen.": "cen",
    "Segmento": "segmento",
    "Material": "material",
    "Nome Material": "nome_material",
    "Conta Contábil": "conta_contabil",
    "Código Serviço São Paulo": "codigo_servico_sp",
    "Lei Complementar 116/2203": "lei_complementar",
    "Alíquota São Paulo": "aliquota_sp",
    "CNAE": "cnae",
    "PIS/COFINS - Regime de Apuração": "pis_cofins_regime",
    "Alíquota IRRF": "aliquota_irrf",
    "Alíquota Pis/Cofins/Csll": "aliquota_pis_cofins_csll",
    "F14": "f14",
    "Parecer Tributário": "parecer_tributario",
    "OBS": "obs",
}

df_servicos_raw = read_excel(PATH_SERVICOS_UTVM, "Serviços SP_UTVM_Reg Especial$", rename=RENAME_SERVICOS)

# Normaliza Material para numero (chave de juncao) e aliquota para Double
# (ToolID 27: Select ja restringe para os campos usados no join do Stream A,
# mas mantemos todas as colunas no dataframe interno para reuso nos Streams A e B)
df_servicos = df_servicos_raw.withColumn(
    "material", F.col("material").cast(DecimalType(9, 0))
).withColumn(
    "aliquota_sp", F.col("aliquota_sp").cast(DoubleType())
)

# --- 1b. ZFBL5N (faturamento / contas a receber) ---
# (ToolIDs 4, 22)
RENAME_ZFBL5N = {
    "Nº doc.": "num_doc",
    "Empresa": "empresa",
    "Centro": "centro",
    "Cod. Cliente": "cod_cliente",
    "Nome Cliente": "nome_cliente",
    "Material": "material",
    "Desc.Material": "desc_material",
    "Montante": "montante",
    "Montante-Desconto": "montante_desconto",
    "IR": "ir",
    "PIS": "pis",
    "Cofins": "cofins",
    "Csll": "csll",
    "CBS": "cbs",
    "IBS": "ibs",
    "Vlr.Liq": "vlr_liq",
    "Emite Nota?": "emite_nota",
    "Tipo de venda": "tipo_venda",
    "Data de faturamento": "data_faturamento",
    "Data de planejamento": "data_planejamento",
    "Classif.Contábil": "classif_contabil",
    # Demais colunas da planilha nao usadas em filtro/join/formula/output,
    # mantidas para espelhar o AlteryxSelect que traz tudo por padrao.
    "Id. Fiscal": "id_fiscal",
    "NF-e": "nf_e",
    "Ordem de venda": "ordem_venda",
    "Item": "item",
    "Moeda": "moeda",
    "MI": "mi",
    "Montante MI": "montante_mi",
    "Montante MI Item": "montante_mi_item",
    "Valor de descon.": "valor_descon",
    "Doc. Contábil": "doc_contabil",
    "Data de compensação": "data_compensacao",
    "Data base": "data_base",
    "Centro de lucro": "centro_lucro",
    "Clnt.Metranet": "clnt_metranet",
    "Boleto": "boleto",
    "Setor.Ativ": "setor_ativ",
    "IBS Municipal": "ibs_municipal",
    "IBS Estadual": "ibs_estadual",
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

df_zfbl5n_raw = read_excel(PATH_ZFBL5N, "Base de Faturamento $", rename=RENAME_ZFBL5N)

# ToolIDs 5, 34: Converte Material e Montante-Desconto para numero
df_zfbl5n = (
    df_zfbl5n_raw
    .withColumn("material", F.col("material").cast(DecimalType(9, 0)))
    .withColumn("montante_desconto", F.col("montante_desconto").cast(DoubleType()))
)

# --- 1c. Bal_Antes (balancete mes anterior, dados a partir da linha 15) ---
# (ToolID 16)
# IMPORTANTE: O Excel tem 14 linhas de metadados. O cabecalho esta na linha 15.
# As colunas sao nomeadas F1, F2, ... F17 no proprio Excel.
df_bal_antes = read_excel(PATH_BAL_ANTES, "Antes$")

# --- 1d. Bal_Depois (balancete mes atual, dados a partir da linha 15) ---
# (ToolID 3)
df_bal_depois = read_excel(PATH_BAL_DEPOIS, "Depois$")


# ===========================================================================
# PIPELINE 2 - Balancete: Apuracao da Receita (Mes Anterior - Mes Atual)
# (ToolIDs 16, 17, 3, 11, 18, 20, 19)
#
# Este pipeline e intermediario: seu resultado alimenta o Pipeline 4
# (Conciliacao Contabil). Roda antes por dependencia.
# ===========================================================================

# ToolID 17: Bal_Antes - seleciona F6 (conta contabil), F8 (chave de join),
# F9 (descricao), F12 (valor mes anterior), todos convertidos para Double
df_antes = df_bal_antes.select(
    F.col("F6").cast(DoubleType()).alias("conta_contabil"),
    F.col("F8").cast(DoubleType()).alias("chave_join"),
    F.col("F9").alias("descricao_conta_contabil"),
    F.col("F12").cast(DoubleType()).alias("mes_anterior"),
)

# ToolID 11: Bal_Depois - seleciona F6 (chave de join), F10 (valor mes atual)
df_depois = df_bal_depois.select(
    F.col("F6").cast(DoubleType()).alias("chave_join_depois"),
    F.col("F10").cast(DoubleType()).alias("mes_atual"),
)

# ToolID 18: Join Antes.F8 (chave_join) = Depois.F6 (chave_join_depois)
df_balancete = df_antes.join(
    df_depois,
    F.col("chave_join") == F.col("chave_join_depois"),
    "inner",
).drop("chave_join", "chave_join_depois")

# ToolID 19: Formula - Receita = Mes Anterior - Mes Atual
# ToolID 20: Renomeia colunas para nomes de negocio
df_balancete = df_balancete.withColumn(
    "receita",
    F.col("mes_anterior") - F.col("mes_atual"),
).select(
    "conta_contabil",
    "descricao_conta_contabil",
    "mes_anterior",
    "mes_atual",
    "receita",
)


# ===========================================================================
# PIPELINE 3 - Relatorio NFSe emitidas em nome da B3
# (ToolIDs 22, 34, 25, 24, 23, 27, 26, 28, 29, 30, 32, 31, 35, 33)
#
# Fluxo:
#   1. Filtra ZFBL5N onde Emite Nota? = "NAO"
#   2. Join com Servicos UTVM em Material
#   3. Calcula ISS = Montante-Desconto * Aliquota Sao Paulo
#   4. Agrega por Codigo Servico para NFSe
#   5. Gera relatorio detalhado linha a linha (Contas a Receber)
# ===========================================================================

# ToolID 25: Filtra apenas linhas que NAO emitem nota
df_fat_sem_nota = df_zfbl5n.filter(F.col("emite_nota") == "NÃO")

# ToolID 24: Select do lado do faturamento (restringe colunas antes do Join)
# ToolID 27: Select do lado dos servicos (so colunas necessarias)
df_servicos_join_a = df_servicos.select(
    F.col("material"),
    F.col("nome_material"),
    F.col("conta_contabil"),
    F.col("codigo_servico_sp"),
    F.col("aliquota_sp"),
)

# ToolID 26: Join em Material (inner)
df_join_nfse = df_fat_sem_nota.join(df_servicos_join_a, on="material", how="inner")

# Registros sem match no mestre (left_anti) -> Sobra - Relatorio Balcao
# (ToolID 63: saida do ramo Left unmatched do Join 26)
df_sobra_balcao = df_fat_sem_nota.join(
    df_servicos_join_a.select("material").distinct(),
    on="material",
    how="left_anti",
)

# ToolID 28: Formula - ISS = Montante-Desconto * Aliquota Sao Paulo
df_join_nfse = df_join_nfse.withColumn(
    "iss", F.round(F.col("montante_desconto") * F.col("aliquota_sp"), 2)
)

# --- 3a. NFSe agregado por Codigo de Servico (Tools 29, 30, 32) ---

# ToolID 29: Summarize - GroupBy Codigo Servico SP, Sum ISS, Sum Montante-Desconto
df_nfse_agregado = df_join_nfse.groupBy("codigo_servico_sp").agg(
    F.sum("iss").cast(DoubleType()).alias("iss"),
    F.sum("montante_desconto").cast(DoubleType()).alias("receita"),
)

# ToolID 32: Formula - adiciona descricao historica com mes/ano anterior
hoje = datetime.date.today()
primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)
mes_ano_anterior = ultimo_dia_mes_anterior.strftime("%B/%Y").upper()

df_nfse_agregado = df_nfse_agregado.withColumn(
    "historico",
    F.concat_ws(
        "",
        F.lit("RECEITA DE PRESTACAO DE SERVICO REF. "),
        F.lit(mes_ano_anterior),
        F.lit(" DO CODIGO "),
        F.col("codigo_servico_sp").cast(StringType()),
        F.lit(" CONFORME RELATORIO ANALITICO - PROCESSO Nº 2008-0.365.344-8"),
        F.lit(" E REGIME ESPECIAL Nº 11.995."),
    ),
)

# ToolID 30: Select final com tipos, ordenando colunas
df_nfse_agregado = df_nfse_agregado.select(
    "codigo_servico_sp", "receita", "iss", "historico"
)

# --- 3b. Relatorio detalhado Contas a Receber (Tools 35, 33) ---

# ToolID 35: Select final com todas as colunas do relatorio detalhado
df_relatorio_contas_receber = df_join_nfse.select(
    "empresa",
    "centro",
    "tipo_venda",
    "cod_cliente",
    "nome_cliente",
    "num_doc",
    "data_faturamento",
    "data_planejamento",
    F.col("montante_desconto").alias("montante_desconto_out"),
    "iss",
    "material",
    "desc_material",
    "codigo_servico_sp",
    "aliquota_sp",
)

# --- Escrita das saidas do Pipeline 3 ---

RENAME_OUT_NFSE = {
    "codigo_servico_sp": "Código Serviço São Paulo",
    "receita": "Receita",
    "iss": "ISS",
    "historico": "HISTÓRICO",
}
write_excel(df_nfse_agregado, PATH_OUT_NFSE_B3, rename=RENAME_OUT_NFSE)

RENAME_OUT_RELATORIO = {
    "empresa": "Empresa",
    "centro": "Centro",
    "tipo_venda": "Tipo de venda",
    "cod_cliente": "Cod. Cliente",
    "nome_cliente": "Nome Cliente",
    "num_doc": "Nº doc.",
    "data_faturamento": "Data de faturamento",
    "data_planejamento": "Data de planejamento",
    "montante_desconto_out": "Montante - Desconto",
    "iss": "ISS",
    "material": "Material",
    "desc_material": "Desc.Material",
    "codigo_servico_sp": "Código Serviço São Paulo",
    "aliquota_sp": "Alíquota São Paulo",
}
write_excel(
    df_relatorio_contas_receber,
    PATH_OUT_RELATORIO_CONTAS_RECEBER,
    rename=RENAME_OUT_RELATORIO,
)


# ===========================================================================
# PIPELINE 4 - Conciliacao Contabil (Check II)
# (ToolIDs 4, 5, 2, 6, 7, 8, 9, 10, 12, 13, 15, 14, 21)
#
# Fluxo:
#   1. Join ZFBL5N com Servicos em Material
#   2. GroupBy Conta Contabil, Codigo Servico, CNAE (Sum Montante-Desconto)
#   3. Join com Balancete (Pipeline 2) em Conta Contabil
#   4. Calcula ISS = Receita * 2% e Dif = Receita - Relatorio ISS
# ===========================================================================

# ToolID 2: Select do lado Servicos para este fluxo
df_servicos_join_b = df_servicos.select(
    F.col("material"),
    F.col("conta_contabil"),
    F.col("codigo_servico_sp"),
    F.col("cnae"),
)

# ToolID 6: Join ZFBL5N com Servicos em Material
df_join_contabil = df_zfbl5n.join(df_servicos_join_b, on="material", how="inner")

# ToolID 7: Summarize - GroupBy Conta Contabil, Codigo Servico, CNAE
# Sum Montante-Desconto como "Relatorio ISS"
df_sumario_contabil = df_join_contabil.groupBy(
    "conta_contabil", "codigo_servico_sp", "cnae"
).agg(
    F.sum("montante_desconto").alias("relatorio_iss"),
)

# ToolID 9: Converte Conta Contabil para numero
df_sumario_contabil = df_sumario_contabil.withColumn(
    "conta_contabil", F.col("conta_contabil").cast(DoubleType())
)

# ToolID 10: Join com Balancete (Pipeline 2) em Conta Contabil
df_reconciliado = df_sumario_contabil.join(
    df_balancete.select("conta_contabil", "receita"),
    on="conta_contabil",
    how="inner",
)

# Registros sem match no balancete -> Contas Novas (ToolID 61)
df_contas_novas = df_sumario_contabil.join(
    df_balancete.select("conta_contabil").distinct(),
    on="conta_contabil",
    how="left_anti",
)

# ToolID 12: Formula - ISS = Receita * 0.02
# ToolID 14: Formula - Dif.. = Receita - Relatorio ISS
df_reconciliado = (
    df_reconciliado
    .withColumn("iss", F.round(F.col("receita") * F.lit(0.02), 2))
    .withColumn("dif", F.round(F.col("receita") - F.col("relatorio_iss"), 2))
)

# ToolID 15: Sort por Conta Contabil
df_reconciliado = df_reconciliado.orderBy("conta_contabil")

# ToolID 13: Select final com colunas na ordem do Alteryx
df_reconciliado = df_reconciliado.select(
    F.col("codigo_servico_sp").alias("codigo_servico_sao_paulo"),
    "cnae",
    F.col("conta_contabil").alias("contas_sap"),
    "receita",
    "relatorio_iss",
    "iss",
    "dif",
)

# --- Escrita da saida do Pipeline 4 ---

RENAME_OUT_CHECK = {
    "codigo_servico_sao_paulo": "Código Serviço São Paulo",
    "cnae": "CNAE",
    "contas_sap": "Contas SAP",
    "receita": "Receita",
    "relatorio_iss": "Relatório ISS",
    "iss": "ISS",
    "dif": "Dif..",
}
write_excel(df_reconciliado, PATH_OUT_CHECK_II_CONTABIL, rename=RENAME_OUT_CHECK)


# ===========================================================================
# PIPELINE 5 - Lancamento Contabil SAP (partida dobrada + cabecalho, 3 linhas)
# (ToolIDs 64, 36, 42, 39, 38, 37, 40, 41)
#
# Gera 3 linhas:
#   Linha 1 = cabecalho/documento + debito (chave 40, conta 21400122)
#   Linha 2 = credito (chave 31, conta 400006) + dados de pagamento
#   Linha 3 = linha zerada (contrapartida)
# O ISS total vem da soma do NFSe agregado (Pipeline 3a).
# ===========================================================================

# ToolID 64: Soma total do ISS (do NFSe agregado)
iss_total = round(scalar_sum(df_nfse_agregado, "iss"), 2)

# Datas do lancamento
data_lancamento = f"10.{hoje.month:02d}.{hoje.year}"
periodo = f"{hoje.month:02d}"
referencia = ultimo_dia_mes_anterior.strftime("%d.%m.%Y")
texto_lanc = (
    "REF. REC. ISS REGIME ESPECIAL UTVM - "
    f"{ultimo_dia_mes_anterior.month:02d}/{ultimo_dia_mes_anterior.year}"
)
data_base = data_lancamento  # dia 10 do mes corrente (para RowCount=2)

colunas_lancamento_clean = [
    "data_documento", "tp_doc", "empresa", "data_lancamento", "periodo",
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
    StructField("data_lancamento", StringType()),
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

# ToolIDs 37, 40: Formulas condicionais do Alteryx mapeadas como tuplas
# Linha 1 (RowCount=1): cabecalho + debito
linha_1 = (
    data_lancamento,          # data_documento
    "FE",                     # tp_doc
    1000,                     # empresa
    data_lancamento,          # data_lancamento
    periodo,                  # periodo
    "BRL",                    # moeda_taxa_cambio
    None,                     # grp_ledger
    referencia,               # referencia
    "05890",                  # txt_cab_doc
    40,                       # chv_lancto (debito)
    21400122,                 # conta (ISS a receber)
    None,                     # cod_rze
    iss_total,                # montante
    None,                     # doc_compras
    None,                     # item
    "",                       # forma_pagamento
    None,                     # bloqueio_pagamento
    "",                       # condicao_pagamento
    "",                       # data_base
    None,                     # atribuicao
    None,                     # cod_imposto
    None,                     # domicilio_fiscal
    texto_lanc,               # texto
    None,                     # local_negocios
    None,                     # centro_custo
    None,                     # elemento_pep
    None,                     # ordem
    None,                     # numero_atividade
    None,                     # diagrama_rede
    None,                     # centro_lucro
    None,                     # divisao
    None,                     # tipo_movimento
    None,                     # sociedade_parceira
)

# Linha 2 (RowCount=2): credito + dados de pagamento
linha_2 = (
    "",                       # data_documento
    "",                       # tp_doc
    None,                     # empresa
    "",                       # data_lancamento
    "",                       # periodo
    "",                       # moeda_taxa_cambio
    None,                     # grp_ledger
    referencia,               # referencia
    None,                     # txt_cab_doc
    31,                       # chv_lancto (credito)
    400006,                   # conta (despesa ISS)
    None,                     # cod_rze
    iss_total,                # montante
    None,                     # doc_compras
    None,                     # item
    "O",                      # forma_pagamento
    None,                     # bloqueio_pagamento
    "Z001",                   # condicao_pagamento
    data_base,                # data_base
    None,                     # atribuicao
    None,                     # cod_imposto
    None,                     # domicilio_fiscal
    texto_lanc,               # texto
    None,                     # local_negocios
    None,                     # centro_custo
    None,                     # elemento_pep
    None,                     # ordem
    None,                     # numero_atividade
    None,                     # diagrama_rede
    None,                     # centro_lucro
    None,                     # divisao
    None,                     # tipo_movimento
    None,                     # sociedade_parceira
)

# Linha 3 (RowCount=3): linha zerada
linha_3 = (
    "",                       # data_documento
    "",                       # tp_doc
    None,                     # empresa
    "",                       # data_lancamento
    "",                       # periodo
    "",                       # moeda_taxa_cambio
    None,                     # grp_ledger
    "",                       # referencia
    None,                     # txt_cab_doc
    None,                     # chv_lancto
    None,                     # conta
    None,                     # cod_rze
    0.0,                      # montante (zerado)
    None,                     # doc_compras
    None,                     # item
    "",                       # forma_pagamento
    None,                     # bloqueio_pagamento
    "",                       # condicao_pagamento
    "",                       # data_base
    None,                     # atribuicao
    None,                     # cod_imposto
    None,                     # domicilio_fiscal
    "",                       # texto
    None,                     # local_negocios
    None,                     # centro_custo
    None,                     # elemento_pep
    None,                     # ordem
    None,                     # numero_atividade
    None,                     # diagrama_rede
    None,                     # centro_lucro
    None,                     # divisao
    None,                     # tipo_movimento
    None,                     # sociedade_parceira
)

df_lancamento = spark.createDataFrame(
    [linha_1, linha_2, linha_3], schema_lancamento
).select(colunas_lancamento_clean)

RENAME_OUT_LANCAMENTO = {
    "data_documento": "Data documento",
    "tp_doc": "Tp.doc.",
    "empresa": "Empresa",
    "data_lancamento": "Data Lançamento",
    "periodo": "Período",
    "moeda_taxa_cambio": "Moeda/taxa câm.",
    "grp_ledger": "Grp. ledger",
    "referencia": "Referência",
    "txt_cab_doc": "Txt.cab.doc.",
    "chv_lancto": "ChvLnçt",
    "conta": "Conta",
    "cod_rze": "Cód.RzE",
    "montante": "Montante",
    "doc_compras": "Doc.compras",
    "item": "Item",
    "forma_pagamento": "Forma de Pagamento",
    "bloqueio_pagamento": "Bloqueio de Pagamento",
    "condicao_pagamento": "Condição de Pagamento",
    "data_base": "Data Base",
    "atribuicao": "Atribuição",
    "cod_imposto": "Cód.Imposto",
    "domicilio_fiscal": "DomicílioFiscal",
    "texto": "Texto",
    "local_negocios": "Local de Negócios",
    "centro_custo": "Centro de Custo",
    "elemento_pep": "Elemento PEP",
    "ordem": "Ordem",
    "numero_atividade": "Número de Atividade",
    "diagrama_rede": "Diagrama de Rede",
    "centro_lucro": "Centro de lucro",
    "divisao": "Divisão",
    "tipo_movimento": "Tipo de Movimento",
    "sociedade_parceira": "Sociedade Parceira",
}

write_excel(df_lancamento, PATH_OUT_LANCTO_PAG_ISS, rename=RENAME_OUT_LANCAMENTO)


# ===========================================================================
# PIPELINE 6 - Sobras: Registros nao encontrados nos joins
# (ToolIDs 63, 62, 61)
#
# Duas saidas:
#   - Sobra - Relatorio Balcao: linhas do ZFBL5N sem match no mestre
#     de servicos (capturado durante o Pipeline 3)
#   - Contas Novas: contas contabeis do faturamento sem match no
#     balancete (capturado durante o Pipeline 4)
# ===========================================================================

# ToolID 63 (Sobra): Select dos campos relevantes para o relatorio de sobra
# Mantem as colunas que fazem sentido para investigacao de materiais nao cadastrados
RENAME_OUT_SOBRA = {
    "empresa": "Empresa",
    "centro": "Centro",
    "num_doc": "Nº doc.",
    "cod_cliente": "Cod. Cliente",
    "nome_cliente": "Nome Cliente",
    "material": "Material",
    "desc_material": "Desc.Material",
    "montante_desconto": "Montante-Desconto",
    "emite_nota": "Emite Nota?",
}
write_excel(
    df_sobra_balcao.select(list(RENAME_OUT_SOBRA.keys())),
    PATH_OUT_SOBRA_BALCAO,
    rename=RENAME_OUT_SOBRA,
)

# ToolID 62, 61 (Contas Novas): contas sem match no balancete
RENAME_OUT_CONTAS_NOVAS = {
    "conta_contabil": "Conta Contábil",
    "codigo_servico_sp": "Código Serviço São Paulo",
    "cnae": "CNAE",
    "relatorio_iss": "Relatório ISS",
}
write_excel(
    df_contas_novas.select(list(RENAME_OUT_CONTAS_NOVAS.keys())),
    PATH_OUT_CONTAS_NOVAS,
    rename=RENAME_OUT_CONTAS_NOVAS,
)


# ===========================================================================
# Resumo da execucao
# ===========================================================================

print("=" * 60)
print("UTVM - Regime Especial: processamento concluido")
print(f"Data: {hoje.strftime('%d/%m/%Y')}")
print(f"ISS Total (NFSe): R$ {iss_total:,.2f}")
print(f"NFSe B3 (agregado):          {df_nfse_agregado.count()} codigos de servico")
print(f"Relatorio Contas a Receber:   {df_relatorio_contas_receber.count()} linhas")
print(f"Check II Contabil:            {df_reconciliado.count()} contas")
print(f"Lancamento SAP:               {df_lancamento.count()} linhas")
print(f"Sobra - Relatorio Balcao:     {df_sobra_balcao.count()} linhas")
print(f"Contas Novas:                 {df_contas_novas.count()} contas")
print("=" * 60)
