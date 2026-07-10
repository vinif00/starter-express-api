"""
Migracao PySpark do fluxo Alteryx "Fluxo - UTVM.yxmd".

Apuracao e reconciliacao do ISS (imposto municipal sobre servicos) do
Centro SAP 0290 (UTVM), cruzando 5 fontes (+ 1 fonte extra exclusiva do
UTVM para o regime especial):
  1. ZFBL5N.XLSX          - linhas de faturamento SAP (contas a receber),
                             aba `Base do Faturamento Geral$`
  2. Servicos UTVM.xlsx    - tabela de-para: material -> aliquota de ISS,
                             aba `Servicos SP_UTVM_Faturamento$` (regime normal)
                             e aba `Servicos SP_UTVM_Reg Especial$` (regime
                             especial - ramificacao extra, nao existe no SP)
  3. Razao ISS B3.xlsx     - razao contabil (livro-razao) da conta de ISS
  4. NFSe.xlsx             - notas fiscais de servico emitidas na prefeitura
  5. ZSD.XLSX              - controle interno SAP dos RPS gerados

Saidas (equivalentes aos DbFileOutput do Alteryx, todas em .../UTVM/Saida):
  - Relatorio Faturamento UTVM MES 2026 (Contas a Receber).xlsx
  - Reg Esp. NFSe Cliente.xlsx (ramificacao de Regime Especial - so existe no UTVM)
  - Cum. e Nao-Cum..xlsx
  - Razao ISS B3 UTVM.xlsx
  - Prefeitura x ZSD.xlsx / ZSD x Prefeitura.xlsx (reconciliacao de RPS)
  - Faturamento x SAP.xlsx / SAP x Faturamento.xlsx (excecoes estruturais,
    ficam sempre vazias em operacao normal - ver Pipeline 7)
  - Relatorio Final Alteryx.xlsx (reconciliacao Faturamento x SAP x Prefeitura)
  - Lancto de Pag ISS Faturamento - B3 UTVM.xlsx (lancamento contabil, partida dobrada)

Diferencas de negocio em relacao ao Fluxo - SP.py (modelo de referencia)
-------------------------------------------------------------------------
  - Fonte de faturamento: ZFBL5N.XLSX, aba `Base do Faturamento Geral$`.
  - Tabela de servicos (regime normal): chave de juncao e o proprio campo
    "Material" (nao "Produto SAP" como no SP), e o ISS e calculado como
    ISS = [Montante-Desconto] * [Aliquota Sao Paulo] (nao
    [Montante MI Item] * [Aliquota ISS] como no SP).
  - Filtro do Centro no razao contabil: [Centro] = "0290" (SP usa "0100").
  - Fonte de notas fiscais: NFSe.xlsx, aba `NFSe$`, com filtro
    [Situacao da Nota Fiscal] = "T" (SP usa != "C" - regra diferente).
    O Alteryx aplica esse MESMO filtro duas vezes em nos (ToolIDs 57 e 58)
    fisicamente distintos, um alimentando a reconciliacao de RPS e outro
    alimentando o total da PREFEITURA; como a expressao e identica nos dois,
    aqui filtramos uma unica vez e reaproveitamos o resultado (Pipeline 6).
  - Nao existe, no UTVM, o branch de fallback de aliquota fixa de 2% para
    materiais sem correspondencia na tabela de servicos (o "Left" nao-casado
    do Join principal - ToolID 8 - nao tem nenhuma conexao de saida no XML).
    Essas linhas de fatura simplesmente sao descartadas do calculo principal.
  - RAMIFICACAO EXTRA - REGIME ESPECIAL (Pipeline 3, exclusiva do UTVM):
    uma segunda leitura de "Servicos UTVM.xlsx", aba
    `Servicos SP_UTVM_Reg Especial$` (ToolID 71), e cruzada por "Material"
    (ToolID 72, Join) com a MESMA base geral de faturamento (a saida do
    Select/rename ToolID 2, ja filtrada por Emite Nota?="SIM" no ToolID 68 -
    portanto todas as linhas dessa ramificacao ja estao implicitamente
    filtradas por Emite Nota?="SIM", sem precisar de um filtro extra).
    Essa ramificacao tem sua PROPRIA formula de ISS (ToolID 75, identica em
    forma a formula do regime normal: ISS = [Montante-Desconto] *
    [Aliquota Sao Paulo], mas usando a aliquota cadastrada na aba de Regime
    Especial) e uma formula de HISTORICO contabil (ToolID 77) e escreve
    diretamente em "Reg Esp. NFSe Cliente.xlsx" (ToolID 69).
    IMPORTANTE (confirmado lendo a secao <Connections> do XML com cuidado):
    diferentemente do que a primeira leitura do fluxo sugeria, essa
    ramificacao NAO se rejunta ao pipeline principal em nenhum ponto - a
    saida do ToolID 77 vai exclusivamente para o DbFileOutput ToolID 69.
    Ela nao alimenta o "Cum. e Nao-Cum." (ToolIDs 117-121, que sao
    alimentados so pelos ToolIDs 6 e 11 da tabela de servicos normal) nem a
    reconciliacao final "Relatorio Final Alteryx" (ToolIDs 20/47/28/29/31,
    que somam apenas Faturamento + SAP - PREFEITURA, sem nenhum termo de
    Regime Especial). O ISS de Regime Especial fica inteiramente confinado
    ao relatorio "Reg Esp. NFSe Cliente.xlsx".
  - Nomes de arquivo de saida incluem "UTVM" e foram copiados literalmente
    do atributo <File> de cada DbFileOutput do XML.

Convencao de nomes de coluna
-----------------------------
Toda coluna usada dentro do Spark (filter/select/join/groupBy/withColumn)
segue snake_case sem acentos, espacos ou pontuacao pelos mesmos motivos
documentados no Fluxo - SP.py (nomes com "." quebram F.col()/select() por
serem interpretados como separador de campo aninhado; espacos/acentos
quebram escritas Delta e varios conectores). Os nomes de negocio originais
(em portugues, com acentos) so sao recolocados no ultimo passo, na escrita
do Excel, para manter os relatorios legiveis para o time.

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

spark = SparkSession.builder.appName("Fluxo_ISS_UTVM").getOrCreate()

# ---------------------------------------------------------------------------
# Caminhos de entrada / saida (ajustar para o mount/volume do Databricks)
# ---------------------------------------------------------------------------
BASE_IN = "/Volumes/iss/utvm/entrada"
BASE_OUT = "/Volumes/iss/utvm/saida"

PATH_ZFBL5N = f"{BASE_IN}/ZFBL5N.XLSX"
PATH_SERVICOS_UTVM = f"{BASE_IN}/Servicos UTVM.xlsx"
PATH_RAZAO_ISS_B3 = f"{BASE_IN}/Razao ISS B3.xlsx"
PATH_NFSE = f"{BASE_IN}/NFSe.xlsx"
PATH_ZSD = f"{BASE_IN}/ZSD.XLSX"

PATH_OUT_RELATORIO_FATURAMENTO_UTVM = f"{BASE_OUT}/Relatorio Faturamento UTVM MES 2026 (Contas a Receber).xlsx"
PATH_OUT_REG_ESP_NFSE_CLIENTE = f"{BASE_OUT}/Reg Esp. NFSe Cliente.xlsx"
PATH_OUT_CUM_NAO_CUM = f"{BASE_OUT}/Cum. e Nao-Cum..xlsx"
PATH_OUT_RAZAO_ISS_B3_UTVM = f"{BASE_OUT}/Razao ISS B3 UTVM.xlsx"
PATH_OUT_PREFEITURA_X_ZSD = f"{BASE_OUT}/Prefeitura x ZSD.xlsx"
PATH_OUT_ZSD_X_PREFEITURA = f"{BASE_OUT}/ZSD x Prefeitura.xlsx"
PATH_OUT_FATURAMENTO_X_SAP_EXC = f"{BASE_OUT}/Faturamento x SAP.xlsx"
PATH_OUT_SAP_X_FATURAMENTO_EXC = f"{BASE_OUT}/SAP x Faturamento.xlsx"
PATH_OUT_RELATORIO_FINAL = f"{BASE_OUT}/Relatorio Final Alteryx.xlsx"
PATH_OUT_LANCTO_PAGTO_ISS = f"{BASE_OUT}/Lancto de Pag ISS Faturamento - B3 UTVM.xlsx"


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


# Data de referencia do "hoje" do run, compartilhada pela formula de
# HISTORICO do Regime Especial (Pipeline 3) e pelo lancamento contabil
# (Pipeline 8), assim como DateTimeNow() no Alteryx e avaliado uma unica
# vez para todas as linhas de um mesmo run.
hoje = datetime.date.today()

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Marco", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


# ===========================================================================
# PIPELINE 1 - Faturamento x Tabela de servicos (regime normal) -> calculo
# do ISS por linha
# (ToolIDs 1, 68, 133, 2, 5, 6, 8, 10, 11)
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
    "Montante-Desconto": "montante_desconto",
    "Material": "material",
    "Desc.Material": "desc_material",
    "IR": "ir",
    "PIS": "pis",
    "Cofins": "cofins",
    "Csll": "csll",
}

df_fat_raw = read_excel(PATH_ZFBL5N, "Base do Faturamento Geral$", rename=RENAME_FATURAMENTO)

# Node 68: so entram no calculo de ISS as linhas que efetivamente emitem NF
df_fat = df_fat_raw.filter(F.col("emite_nota") == "SIM")

# Node 133 (ToNumber) + Node 2 (select/cast decimal 9,0): normaliza o
# codigo do material para numero (chave de juncao com a tabela de servicos)
# e mantem apenas os campos usados a jusante
df_fat = (
    df_fat
    .withColumn("material", F.col("material").cast(DecimalType(9, 0)))
    .select(
        "empresa", "centro", "tp_documento", "cod_cliente", "nome_cliente",
        "num_doc", "nf_e", "data_faturamento", "data_planejamento",
        "montante_desconto", "material", "desc_material",
        "ir", "pis", "cofins", "csll",
    )
)

RENAME_SERVICOS = {
    "Material": "material",
    "Nome Material": "nome_material",
    "Conta Contábil": "conta_contabil",
    "Código Serviço São Paulo": "codigo_servico_sp",
    "Alíquota São Paulo": "aliquota_sp",
    "PIS/COFINS - Regime de Apuração": "pis_cofins_regime",
}

df_servicos_raw = read_excel(PATH_SERVICOS_UTVM, "Serviços SP_UTVM_Faturamento$", rename=RENAME_SERVICOS)

# Node 6: tabela de-para com a aliquota de ISS por material (chave = "material",
# nao "produto_sap" como no SP)
df_servicos = df_servicos_raw.select(
    F.col("material").cast(DecimalType(9, 0)).alias("cod_servico"),
    "nome_material", "conta_contabil", "codigo_servico_sp", "aliquota_sp", "pis_cofins_regime",
)

# Node 8: junta cada linha de faturamento com sua aliquota de ISS cadastrada.
# ATENCAO: diferente do SP, o "Left" (materiais sem correspondencia) do Join
# do Alteryx nao tem NENHUMA conexao de saida no XML - nao ha branch de
# fallback com aliquota fixa aqui. Por isso usamos join "inner" diretamente
# e essas linhas sem match sao descartadas (fielmente ao Alteryx original).
df_join = df_fat.join(df_servicos, df_fat["material"] == df_servicos["cod_servico"], "inner")

# Node 10 (select) + Node 11 (Formula): ISS = [Montante-Desconto]*[Aliquota Sao Paulo]
# Mantemos aqui tambem nome_material/conta_contabil/pis_cofins_regime, que no
# Alteryx sao obtidos de novo via um self-join redundante (ToolIDs 117/118,
# ver Pipeline 4) contra a mesma tabela de servicos - trazer esses campos
# desde ja e equivalente e mais simples (mesma logica de simplificacao
# documentada no Pipeline 3 do Fluxo - SP.py).
df_com_aliquota = (
    df_join
    .withColumn("iss", F.round(F.col("montante_desconto") * F.col("aliquota_sp"), 2))
    .select(
        "empresa", "centro", "tp_documento", "cod_cliente", "nome_cliente", "num_doc",
        "nf_e", "data_faturamento", "data_planejamento", "montante_desconto",
        "desc_material", "ir", "pis", "cofins", "csll",
        "cod_servico", "nome_material", "conta_contabil", "codigo_servico_sp",
        "aliquota_sp", "pis_cofins_regime", "iss",
    )
)


# ===========================================================================
# PIPELINE 2 - Relatorio de Contas a Receber (saida de negocio principal)
# (ToolID 122 -> 52)
# ===========================================================================

df_relatorio_faturamento = df_com_aliquota.select(
    "empresa", "centro", "tp_documento", "cod_cliente", "nome_cliente", "num_doc",
    "data_faturamento", "data_planejamento", "montante_desconto", "iss",
    F.col("cod_servico").alias("cod_servico"),
    "desc_material", "ir", "pis", "cofins", "csll",
    "aliquota_sp", "codigo_servico_sp", "conta_contabil",
)

RENAME_OUT_RELATORIO_FATURAMENTO = {
    "empresa": "Empresa", "centro": "Centro", "tp_documento": "Tp.Document",
    "cod_cliente": "Cod. Cliente", "nome_cliente": "Nome Cliente", "num_doc": "Nº doc.",
    "data_faturamento": "Data de faturamento", "data_planejamento": "Data de planejamento",
    "montante_desconto": "Montante-Desconto", "iss": "ISS", "cod_servico": "Material",
    "desc_material": "Desc.Material", "ir": "IR", "pis": "PIS", "cofins": "COFINS", "csll": "CSLL",
    "aliquota_sp": "Alíquota São Paulo", "codigo_servico_sp": "Código Serviço São Paulo",
    "conta_contabil": "Conta Contábil",
}

write_excel(df_relatorio_faturamento, PATH_OUT_RELATORIO_FATURAMENTO_UTVM, rename=RENAME_OUT_RELATORIO_FATURAMENTO)


# ===========================================================================
# PIPELINE 3 - REGIME ESPECIAL (ramificacao exclusiva do UTVM, nao existe no SP)
# (ToolIDs 71, 73, 72, 74, 75, 76, 77, 69)
#
# Segunda leitura de "Servicos UTVM.xlsx", aba `Servicos SP_UTVM_Reg
# Especial$" (ToolID 71), cruzada por "Material" (ToolID 72 - Join) com a
# MESMA base geral de faturamento ja filtrada por Emite Nota?="SIM" (saida
# do Node 2, reaproveitada aqui como df_fat). Calcula seu proprio ISS
# (ToolID 75, mesma formula ISS = [Montante-Desconto]*[Aliquota Sao Paulo],
# porem usando a aliquota cadastrada na aba de Regime Especial) e monta um
# texto de HISTORICO contabil (ToolID 77) antes de escrever isoladamente em
# "Reg Esp. NFSe Cliente.xlsx" (ToolID 69).
#
# CONFIRMADO via <Connections> do XML: esta ramificacao NAO volta a se
# juntar ao pipeline principal em nenhum ponto - a saida do ToolID 77 so
# alimenta o DbFileOutput ToolID 69. Em particular, NAO alimenta o
# "Cum. e Nao-Cum." (Pipeline 4, cujo unico self-join, ToolID 117, e
# alimentado somente pelos ToolIDs 6 e 11 da tabela de servicos normal) nem
# a reconciliacao final "Relatorio Final Alteryx" (Pipeline 7, cujo total de
# "Faturamento" - ToolID 54 - soma apenas o ISS calculado no ToolID 11, sem
# nenhum termo do Regime Especial). O ISS de Regime Especial portanto fica
# inteiramente confinado a este relatorio proprio.
# ===========================================================================

RENAME_REGIME_ESPECIAL_SERVICOS = {
    "Material": "material",
    "Conta Contábil": "conta_contabil",
    "Código Serviço São Paulo": "codigo_servico_sp",
    "Alíquota São Paulo": "aliquota_sp",
}

df_reg_esp_servicos_raw = read_excel(
    PATH_SERVICOS_UTVM, "Serviços SP_UTVM_Reg Especial$", rename=RENAME_REGIME_ESPECIAL_SERVICOS
)

# Node 73: seleciona/normaliza a chave de juncao
df_reg_esp_servicos = df_reg_esp_servicos_raw.select(
    F.col("material").cast(DecimalType(9, 0)).alias("material_servico"),
    "conta_contabil", "codigo_servico_sp", "aliquota_sp",
)

# Campos da base geral de faturamento (Node 2 / df_fat) usados neste branch
df_reg_esp_fat = df_fat.select(
    "material", "cod_cliente", "nome_cliente", "num_doc", "nf_e", "montante_desconto"
)

# Node 72: Join (inner, "matched"/"Join" output) entre a tabela de servicos
# de Regime Especial e a base geral de faturamento, por "Material". Traz
# TODAS as linhas de fatura cujo material consta na tabela de Regime
# Especial, independente de tambem terem casado (ou nao) com a tabela de
# servicos "normal" no Pipeline 1.
df_reg_esp_join = df_reg_esp_fat.join(
    df_reg_esp_servicos,
    df_reg_esp_fat["material"] == df_reg_esp_servicos["material_servico"],
    "inner",
)

# Node 75: ISS = [Montante-Desconto]*[Aliquota Sao Paulo] (aliquota de Regime Especial)
# Node 77: HISTORICO = "RECEITA DE PRESTACAO DE SERVICO REF. " + <mes/ano anterior> +
#          " DO CODIGO " + ToString([Codigo Servico Sao Paulo]) + " CONFORME
#          RELATORIO ANALITICO - PROCESSO Nº 2008-0.365.344-8 E REGIME ESPECIAL Nº 11.995."
# ATENCAO: DateTimeFormat(..., '%B/%Y') no Alteryx depende do locale do
# engine; como e um texto de fundamentacao fiscal em portugues (igual ao
# resto do fluxo), assumimos nome do mes em portugues (nao em ingles).
# ATENCAO: "Codigo Servico Sao Paulo" vem como Double na origem; assumimos
# que e um codigo inteiro (cast para long antes de virar string), pois um
# ToString() de Alteryx sobre um Double inteiro nao imprime ".0".
mes_anterior_num = hoje.month - 1 if hoje.month > 1 else 12
ano_mes_anterior = hoje.year if hoje.month > 1 else hoje.year - 1
mes_anterior_nome = MESES_PT[mes_anterior_num]

df_reg_esp = (
    df_reg_esp_join
    .withColumn("iss", F.round(F.col("montante_desconto") * F.col("aliquota_sp"), 2))
    .withColumn(
        "historico",
        F.concat(
            F.lit("RECEITA DE PRESTAÇÃO DE SERVIÇO REF. "),
            F.lit(f"{mes_anterior_nome}/{ano_mes_anterior}"),
            F.lit(" DO CÓDIGO "),
            F.col("codigo_servico_sp").cast(LongType()).cast(StringType()),
            F.lit(" CONFORME RELATÓRIO ANALÍTICO - PROCESSO Nº 2008-0.365.344-8 E REGIME ESPECIAL Nº 11.995."),
        ),
    )
    .select(
        "codigo_servico_sp", "cod_cliente", "nome_cliente", "num_doc", "nf_e",
        "montante_desconto", "aliquota_sp", "iss", "conta_contabil", "historico",
    )
)

RENAME_OUT_REG_ESP = {
    "codigo_servico_sp": "Código Serviço São Paulo",
    "cod_cliente": "Cod. Cliente",
    "nome_cliente": "Nome Cliente",
    "num_doc": "Nº doc.",
    "nf_e": "NF-e",
    "montante_desconto": "Montante-Desconto",
    "aliquota_sp": "Alíquota São Paulo",
    "iss": "ISS",
    "conta_contabil": "Conta Contábil",
    "historico": "HISTÓRICO",
}

write_excel(df_reg_esp, PATH_OUT_REG_ESP_NFSE_CLIENTE, rename=RENAME_OUT_REG_ESP)


# ===========================================================================
# PIPELINE 4 - Base de ISS por servico e regime PIS/COFINS (Cum. e Nao-Cum.)
# (ToolIDs 117/118 no Alteryx sao um self-join redundante que so duplica
#  colunas ja presentes; aqui agrupamos direto a partir de df_com_aliquota,
#  mesma simplificacao aplicada no Fluxo - SP.py)
# (ToolIDs 119, 120, 121)
# ATENCAO: o Select do Node 119 no XML tenta renomear "Sum_Montante -
# Desconto" (com espacos) para "Base de Calculo", mas o campo que realmente
# chega do Summarize (Node 118) se chama "Sum_Montante-Desconto" (sem
# espacos) - um descasamento de nome no cache do Select do Alteryx. A
# intencao e clara (replicar a mesma coluna "Base de Calculo" do SP), entao
# implementamos o rename pretendido.
# ===========================================================================

df_cum_nao_cum = (
    df_com_aliquota
    .groupBy("cod_servico", "pis_cofins_regime", "nome_material", "conta_contabil")
    .agg(
        F.sum("montante_desconto").alias("base_calculo"),
        F.sum("iss").alias("iss"),
    )
    .select(
        F.col("cod_servico").alias("material"),
        "nome_material", "conta_contabil",
        F.col("pis_cofins_regime").alias("regime_apuracao"),
        "base_calculo", "iss",
    )
    .orderBy("regime_apuracao")
)

RENAME_OUT_CUM_NAO_CUM = {
    "material": "Material", "nome_material": "Nome Material",
    "conta_contabil": "Conta Contábil", "regime_apuracao": "PIS/COFINS - Regime de Apuração",
    "base_calculo": "Base de Cálculo", "iss": "ISS",
}

write_excel(df_cum_nao_cum, PATH_OUT_CUM_NAO_CUM, rename=RENAME_OUT_CUM_NAO_CUM)


# ===========================================================================
# PIPELINE 5 - Razao contabil do ISS (SAP GL), filtrado para o Centro UTVM
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

# Node 17: filtra para o Centro do UTVM
df_razao_utvm = df_razao_raw.filter(F.col("centro") == "0290")

RENAME_OUT_RAZAO_UTVM = {v: k for k, v in RENAME_RAZAO.items()}
write_excel(df_razao_utvm, PATH_OUT_RAZAO_ISS_B3_UTVM, rename=RENAME_OUT_RAZAO_UTVM)

# Node 92 + 53: total de ISS contabilizado no razao SAP para o UTVM.
# (somar direto e equivalente a agrupar por documento e depois somar)
sap_total = scalar_sum(df_razao_utvm, "montante_moeda_interna")


# ===========================================================================
# PIPELINE 6 - NFSe da prefeitura + reconciliacao de RPS x controle interno
# (ToolIDs 23, 57, 58, 24, 25, 27, 36, 37, 38, 50, 51)
# O Alteryx aplica o filtro [Situação da Nota Fiscal] = "T" em dois nos
# fisicamente distintos (57 e 58) com a MESMA expressao - um alimentando a
# reconciliacao de RPS, outro alimentando o total da PREFEITURA. Como a
# logica e identica, filtramos uma unica vez e reaproveitamos o resultado.
# ===========================================================================

RENAME_NFSE = {
    "Situação da Nota Fiscal": "situacao_nota_fiscal",
    "ISS devido": "iss_devido",
    "Número do RPS": "numero_rps",
    "Valor dos Serviços": "valor_servicos",
}

df_nfse_raw = read_excel(PATH_NFSE, "NFSe$", rename=RENAME_NFSE)

# Nodes 57/58: mantem somente notas com Situacao = "T"
df_nfse_ok = df_nfse_raw.filter(F.col("situacao_nota_fiscal") == "T")

# Node 25: total de ISS declarado/devido segundo as notas emitidas na prefeitura
prefeitura_total = scalar_sum(df_nfse_ok, "iss_devido")

# Node 36: RPS das notas emitidas (para conciliar com o controle interno ZSD)
df_nfse_rps = df_nfse_ok.select(
    F.col("numero_rps").cast(DecimalType(19, 0)).alias("numero_rps"),
    "valor_servicos",
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
    rename={"numero_rps": "Número do RPS", "valor_servicos": "Valor dos Serviços"},
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
# PIPELINE 7 - Reconciliacao final: Faturamento calculado x SAP x Prefeitura
# (ToolIDs 54, 55, 56, 20, 47, 28, 29, 31; 48/49)
# Assim como no Fluxo - SP.py, os totais escalares sao concatenados no
# Alteryx original via um "cross join" artificial (Formula Total=1 + Join
# Total=Total, ToolID 20 e depois 28). Como cada soma sempre produz
# exatamente 1 linha, esse join sempre casa - por isso calculamos os
# escalares diretamente em Python. Diferente do SP, aqui NAO ha termo de
# "ISS Reg. Especial" somado ao Faturamento (ver nota do Pipeline 3).
# ===========================================================================

faturamento_total = scalar_sum(df_com_aliquota, "iss")              # Node 54 -> "Faturamento"
faturamento_x_sap = round(faturamento_total + sap_total, 2)         # Node 47
faturamento_x_prefeitura = round(faturamento_total - prefeitura_total, 2)  # Node 29

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

# Node 20 Left/Right: saidas de excecao do join Total=Total entre os
# agregados Faturamento e SAP. Como ambos os lados sempre tem exatamente
# 1 linha, essas saidas ficam sempre vazias em operacao normal - mantidas
# so para paridade.
write_excel(
    spark.createDataFrame([], StructType([StructField("faturamento", DoubleType())])),
    PATH_OUT_FATURAMENTO_X_SAP_EXC, rename={"faturamento": "Faturamento"},
)
write_excel(
    spark.createDataFrame([], StructType([StructField("sap", DoubleType())])),
    PATH_OUT_SAP_X_FATURAMENTO_EXC, rename={"sap": "SAP"},
)


# ===========================================================================
# PIPELINE 8 - Lancamento contabil (partida dobrada) do ISS a pagar
# (ToolIDs 123, 129, 126, 125, 124, 127, 128)
# Estrutura identica ao Pipeline 7 do Fluxo - SP.py (mesmas contas, mesma
# chave de lancamento e mesma empresa - confirmado lendo a Configuration do
# Node 124 do XML do UTVM), mudando apenas o texto do historico contabil.
# ===========================================================================

primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

data_lancamento = f"10.{hoje.month:02d}.{hoje.year}"
periodo = f"{hoje.month:02d}"
referencia_lancamento = ultimo_dia_mes_anterior.strftime("%d.%m.%Y")
texto = (
    "REF. REC. ISS FAT. E REGIME ESP. UTVM - "
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
