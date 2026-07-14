"""
Migracao PySpark do fluxo Alteryx "Fluxo - SPA.yxmd".

Apuracao e reconciliacao do ISS (imposto municipal sobre servicos) do
Centro SAP 0280 (SPA), cruzando 5 fontes:
  1. ZFBL5N.xlsx (aba "Com NF$")          - linhas de faturamento SAP, ja
     filtradas na origem para as que emitem nota fiscal
  2. Serviços SPA.xlsx (aba "Serviços SP_SPA$") - tabela de-para: codigo de
     servico -> aliquota de ISS
  3. Razão ISS B3.XLSX (aba "Data$")       - razao contabil (livro-razao) da
     conta de ISS
  4. nfs_emitidas.xlsx (aba "nfs_emitidas$") - notas fiscais de servico
     emitidas
  5. ZSD.XLSX (aba "Data$")                - controle interno SAP dos RPS
     gerados

Saidas (equivalentes aos DbFileOutput do Alteryx, todas em .../spa/saida):
  - Relatório Faturamento SPA MÊS 2025 (Contas a Receber).xlsx
  - Cum. e Não-Cum..xlsx
  - Razão ISS B3 SPA.xlsx
  - Prefeitura x ZSD.xlsx / ZSD x Prefeitura.xlsx (reconciliacao de RPS)
  - Faturamento x SAP.xlsx / SAP x Faturamento.xlsx (excecoes estruturais,
    ficam sempre vazias em operacao normal - ver Pipeline 6)
  - Relatório Final Alteryx.xlsx (reconciliacao Faturamento x SAP x
    Prefeitura)
  - Lançto de Pag ISS Faturamento - B3 SPA.xlsx (lancamento contabil,
    partida dobrada)

Diferencas estruturais em relacao ao "Fluxo - SP.yxmd" (confirmadas lendo a
Configuration/Connections do XML da SPA, nao so as Annotations):
  - A aba de faturamento usada e "Com NF$", que ja vem pre-filtrada (na
    origem) para linhas que emitem NF. Por isso nao ha, na SPA, um filtro
    "Emite Nota? = SIM" equivalente ao do SP.
  - No Join faturamento x tabela de servicos (ToolID 8), o fluxo Alteryx da
    SPA so usa a saida "Join" (linhas casadas): as saidas "Left"/"Right"
    (nao casadas) nao estao conectadas a nenhum outro node no XML. Ou seja,
    material sem servico cadastrado e simplesmente descartado - a SPA NAO
    tem um branch de "Regime Especial" (aliquota fixa de 2%) nem a saida
    "ZSD_Material Reg. Esp..xlsx" que existem no fluxo do SP.
  - A fonte de notas fiscais e "nfs_emitidas.xlsx" (nao "NFS - Prefeitura.
    xlsx"), com campos proprios (Tipo_Op, Referência, Valor_Total,
    ISS_Incluso etc., diferentes dos campos "NFSe$" do SP). O filtro
    equivalente ao "[Situação da Nota Fiscal] != C" do SP e, na SPA,
    "IsNull([Tipo_Op])" (ToolID 74): mantem so as notas sem tipo de
    operacao especial marcado.
  - Antes de gravar a razao contabil (Razão ISS B3 SPA.xlsx), 3 campos de
    data sao reformatados para texto "dd/mm/aaaa" (ToolID 72); o relatorio
    de faturamento tambem tem 2 datas reformatadas do mesmo jeito
    (ToolID 73). O fluxo do SP nao tinha esse passo de formatacao.
  - No lancamento contabil (Pipeline 7), a conta de credito, a forma de
    pagamento e o texto do lancamento sao especificos da SPA (ver Pipeline
    7 abaixo) e o campo "Txt.cab.doc." nunca e preenchido (no SP a linha de
    debito recebia "DIVERSOS").

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

spark = SparkSession.builder.appName("Fluxo_ISS_SPA").getOrCreate()

# ---------------------------------------------------------------------------
# Caminhos de entrada / saida (ajustar para o mount/volume do Databricks)
# ---------------------------------------------------------------------------
BASE_IN = "/Volumes/iss/spa/entrada"
BASE_OUT = "/Volumes/iss/spa/saida"

PATH_ZFBL5N = f"{BASE_IN}/ZFBL5N.xlsx"
PATH_SERVICOS_SPA = f"{BASE_IN}/Serviços SPA.xlsx"
PATH_RAZAO_ISS_B3 = f"{BASE_IN}/Razão ISS B3.XLSX"
PATH_NFS_EMITIDAS = f"{BASE_IN}/nfs_emitidas.xlsx"
PATH_ZSD = f"{BASE_IN}/ZSD.XLSX"

PATH_OUT_RELATORIO_FATURAMENTO_SPA = f"{BASE_OUT}/Relatório Faturamento SPA MÊS 2025 (Contas a Receber).xlsx"
PATH_OUT_CUM_NAO_CUM = f"{BASE_OUT}/Cum. e Não-Cum..xlsx"
PATH_OUT_RAZAO_ISS_B3_SPA = f"{BASE_OUT}/Razão ISS B3 SPA.xlsx"
PATH_OUT_PREFEITURA_X_ZSD = f"{BASE_OUT}/Prefeitura x ZSD.xlsx"
PATH_OUT_ZSD_X_PREFEITURA = f"{BASE_OUT}/ZSD x Prefeitura.xlsx"
PATH_OUT_FATURAMENTO_X_SAP_EXC = f"{BASE_OUT}/Faturamento x SAP.xlsx"
PATH_OUT_SAP_X_FATURAMENTO_EXC = f"{BASE_OUT}/SAP x Faturamento.xlsx"
PATH_OUT_RELATORIO_FINAL = f"{BASE_OUT}/Relatório Final Alteryx.xlsx"
PATH_OUT_LANCTO_PAGTO_ISS = f"{BASE_OUT}/Lançto de Pag ISS Faturamento - B3 SPA.xlsx"


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
# (ToolIDs 1, 61, 2, 5, 6, 8, 10, 11)
# ===========================================================================

RENAME_FATURAMENTO = {
    "Empresa": "empresa",
    "Centro": "centro",
    "Tp.Document": "tp_documento",
    "Cod. Cliente": "cod_cliente",
    "Nome Cliente": "nome_cliente",
    "Nº doc.": "num_doc",
    "Data de faturamento": "data_faturamento",
    "Data de planejamento": "data_planejamento",
    "Montante MI Item": "montante_mi_item",
    "Material": "material",
    "Desc.Material": "desc_material",
    "IR": "ir",
    "PIS": "pis",
    "Cofins": "cofins",
    "Csll": "csll",
    # Colunas abaixo nao sao usadas em nenhum filtro/join/formula/output,
    # mas existem na aba "Com NF$" (DbFileInput ToolID 1) e, no Alteryx
    # original, continuam disponiveis va rios tools adiante - so sao
    # descartadas no Select final (ToolID 69/73) antes do DbFileOutput.
    # Mantidas aqui so por fidelidade estrutural ao pipeline original.
    "Id. Fiscal": "id_fiscal",
    "Tipo de venda": "tipo_venda",
    "Ordem de venda": "ordem_venda",
    "Item": "item",
    "Moeda": "moeda",
    "Montante": "montante",
    "MI": "mi",
    "Valor de descon.": "valor_descon",
    "Doc. Contábil": "doc_contabil",
    "Data de compensação": "data_compensacao",
    "Data base": "data_base",
    "Classif.Contábil": "classif_contabil",
    "Centro de lucro": "centro_lucro",
    "Emite Nota?": "emite_nota",
    "Clnt.Metranet": "clnt_metranet",
    "NF-e": "nf_e",
    "Boleto": "boleto",
    "Setor.Ativ": "setor_ativ",
    "Vlr.Liq": "vlr_liq",
    "Banco.Liqdnte": "banco_liqdnte",
    "Montante.MI": "montante_mi",
    "Montante.ME": "montante_me",
    "Doc.Compens": "doc_compens",
    "Chave Referência 3": "chave_referencia_3",
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

df_fat_raw = read_excel(PATH_ZFBL5N, "Com NF$", rename=RENAME_FATURAMENTO)

# Node 61: normaliza o codigo do material para numero (chave de juncao com
# a tabela de servicos). Ao contrario do SP, nao existe aqui um filtro
# "Emite Nota? = SIM": a aba "Com NF$" ja vem pre-filtrada na origem. As
# demais colunas da planilha (nao usadas a jusante) sao mantidas na
# dataframe por fidelidade ao Alteryx original - so sao descartadas no
# Select final antes do DbFileOutput (ver RENAME_OUT_RELATORIO_FATURAMENTO).
df_fat = df_fat_raw.withColumn("material", F.col("material").cast(DecimalType(9, 0)))

RENAME_SERVICOS = {
    "Produto SAP": "produto_sap",
    "Descrição Serviços": "descricao_servicos",
    "Conta Contábil SAP": "conta_contabil_sap",
    "PIS COFINS": "pis_cofins",
    "Código Prefeitura": "codigo_prefeitura",
    "Alíquota ISS": "aliquota_iss",
}

df_servicos_raw = read_excel(PATH_SERVICOS_SPA, "Serviços SP_SPA$", rename=RENAME_SERVICOS)

# Node 6: tabela de-para com a aliquota de ISS por codigo de servico SAP
df_servicos = df_servicos_raw.select(
    F.col("produto_sap").cast(DecimalType(9, 0)).alias("produto_sap"),
    "descricao_servicos", "conta_contabil_sap", "pis_cofins",
    "codigo_prefeitura", "aliquota_iss",
)

# Node 8: junta cada linha de faturamento com sua aliquota de ISS cadastrada.
# Ao contrario do SP, o Alteryx da SPA so usa a saida "Join" (casados) deste
# node - as saidas "Left"/"Right" (nao casados) nao estao conectadas a nada
# no XML original. Ou seja, material sem servico cadastrado e simplesmente
# descartado: nao ha branch de "Regime Especial" (aliquota fixa 2%) na SPA,
# entao o Join equivale a um inner join direto.
df_com_aliquota = (
    df_fat.join(df_servicos, df_fat["material"] == df_servicos["produto_sap"], "inner")
    .withColumn("iss", F.round(F.col("montante_mi_item") * F.col("aliquota_iss"), 2))
)


# ===========================================================================
# PIPELINE 2 - Relatorio de Contas a Receber (saida de negocio principal)
# (ToolIDs 69, 73 -> 52)
# ===========================================================================

df_relatorio_faturamento = (
    df_com_aliquota
    .select(
        "empresa", "centro", "tp_documento", "cod_cliente", "nome_cliente",
        "num_doc", "data_faturamento", "data_planejamento",
        "montante_mi_item", "iss",
        F.col("produto_sap").alias("cod_servico"),
        F.col("desc_material").alias("nome_servico"),
        "ir", "pis", "cofins", "csll", "aliquota_iss",
        "codigo_prefeitura", "conta_contabil_sap",
    )
    # Node 73: formata as datas como texto "dd/mm/aaaa" no relatorio final.
    # Assume-se que a data de origem esta em um formato que o Spark
    # reconhece automaticamente (to_date sem "format" explicito); ajustar
    # se o formato real da planilha de origem for diferente.
    .withColumn("data_faturamento", F.date_format(F.to_date("data_faturamento"), "dd/MM/yyyy"))
    .withColumn("data_planejamento", F.date_format(F.to_date("data_planejamento"), "dd/MM/yyyy"))
)

RENAME_OUT_RELATORIO_FATURAMENTO = {
    "empresa": "Empresa", "centro": "Centro", "tp_documento": "Tp.Document",
    "cod_cliente": "Cod. Cliente", "nome_cliente": "Nome Cliente",
    "num_doc": "Nº doc.", "data_faturamento": "Data do documento",
    "data_planejamento": "Vencimento líquido",
    "montante_mi_item": " Montante em moeda interna ", "iss": "ISS",
    "cod_servico": "Cód.Serviço", "nome_servico": "Nome do Serviço",
    "ir": "IR", "pis": "PIS", "cofins": "Cofins", "csll": "Csll",
    "aliquota_iss": "Alíquota ISS", "codigo_prefeitura": "Código Prefeitura",
    "conta_contabil_sap": "Conta Contábil SAP",
}

write_excel(df_relatorio_faturamento, PATH_OUT_RELATORIO_FATURAMENTO_SPA, rename=RENAME_OUT_RELATORIO_FATURAMENTO)


# ===========================================================================
# PIPELINE 3 - Base de ISS por servico e regime PIS/COFINS (Cum. e Nao-Cum.)
# (ToolID 75 no Alteryx e um self-join redundante que so duplica colunas ja
#  presentes em df_com_aliquota; aqui agrupamos direto, mesma simplificacao
#  usada no Fluxo - SP.py para os ToolIDs 117/118)
# (ToolIDs 76, 77, 78, 79)
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

# Os nomes de negocio abaixo, com espaco antes/depois, reproduzem
# literalmente o rename configurado no Select (ToolID 77) do Alteryx.
RENAME_OUT_CUM_NAO_CUM = {
    "cod_servico": "Cód.Serviço", "nome_servico": "Nome do Serviço",
    "conta_contabil": "Conta Contábil", "regime_apuracao": "Regime de Apuração",
    "base_calculo": " Base de Cálculo ", "iss": " ISS",
}

write_excel(df_cum_nao_cum, PATH_OUT_CUM_NAO_CUM, rename=RENAME_OUT_CUM_NAO_CUM)


# ===========================================================================
# PIPELINE 4 - Razao contabil do ISS (SAP GL), filtrado para o Centro SPA
# (ToolIDs 13, 17, 14, 60, 53, 72, 59)
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

# Node 17: filtra para o Centro da SPA
df_razao_spa = df_razao_raw.filter(F.col("centro") == "0280")

# Node 14 + 60 + 53: total de ISS contabilizado no razao SAP para a SPA.
# (somar direto e equivalente a agrupar por documento e depois somar)
sap_total = scalar_sum(df_razao_spa, "montante_moeda_interna")

# Node 72: formata as 3 datas como texto "dd/mm/aaaa" antes de gravar o
# relatorio (o Fluxo - SP.py nao tem esse passo - a razao do SP e gravada
# sem reformatar as datas). Mesma ressalva do Pipeline 2 sobre o formato de
# origem assumido pelo to_date.
df_razao_spa_fmt = (
    df_razao_spa
    .withColumn("data_documento", F.date_format(F.to_date("data_documento"), "dd/MM/yyyy"))
    .withColumn("data_entrada", F.date_format(F.to_date("data_entrada"), "dd/MM/yyyy"))
    .withColumn("data_lancamento_razao", F.date_format(F.to_date("data_lancamento_razao"), "dd/MM/yyyy"))
)

RENAME_OUT_RAZAO_SPA = {v: k for k, v in RENAME_RAZAO.items()}
write_excel(df_razao_spa_fmt, PATH_OUT_RAZAO_ISS_B3_SPA, rename=RENAME_OUT_RAZAO_SPA)


# ===========================================================================
# PIPELINE 5 - NFS emitidas + reconciliacao de RPS x controle interno ZSD
# (ToolIDs 23, 74, 24, 25, 70, 36, 33, 37, 38, 50, 51)
# ===========================================================================

RENAME_NFS = {
    "Tipo_Op": "tipo_op",
    "Referência": "referencia",
    "Valor_Total": "valor_total",
    "ISS_Incluso": "iss_incluso",
}

df_nfse_raw = read_excel(PATH_NFS_EMITIDAS, "nfs_emitidas$", rename=RENAME_NFS)

# Node 74 ("IsNull([Tipo_Op])"): mantem so as notas sem tipo de operacao
# especial marcado. E o equivalente, nesta fonte, ao filtro
# "[Situação da Nota Fiscal] != 'C'" usado no fluxo do SP.
df_nfse_ok = df_nfse_raw.filter(F.col("tipo_op").isNull())

# Node 24 + 25: total de ISS incluso nas notas emitidas ("PREFEITURA")
prefeitura_total = scalar_sum(df_nfse_ok, "iss_incluso")

# Node 70 + 36: referencia (identificador da nota/RPS) das notas emitidas,
# para conciliar com o controle interno ZSD
df_nfse_rps = df_nfse_ok.select(
    F.col("referencia").cast(DecimalType(19, 0)).alias("referencia"),
    "valor_total",
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
    df_zsd_rps, df_nfse_rps["referencia"] == df_zsd_rps["n_rps"], "outer"
)

# Notas emitidas sem registro correspondente no controle interno
df_prefeitura_sem_zsd = (
    df_join_rps.filter(F.col("n_rps").isNull()).select(df_nfse_rps.columns)
)
write_excel(
    df_prefeitura_sem_zsd, PATH_OUT_PREFEITURA_X_ZSD,
    rename={"referencia": "Referência", "valor_total": "Valor_Total"},
)

# RPS gerados internamente sem nota fiscal correspondente encontrada
df_zsd_sem_prefeitura = (
    df_join_rps.filter(F.col("referencia").isNull()).select(df_zsd_rps.columns)
)
write_excel(
    df_zsd_sem_prefeitura, PATH_OUT_ZSD_X_PREFEITURA,
    rename={"n_rps": "N.RPS", "dt_lancamento": "Dt.Lançamento", "valor": "Valor"},
)


# ===========================================================================
# PIPELINE 6 - Reconciliacao final: Faturamento calculado x SAP x Prefeitura
# (ToolIDs 54, 56, 55, 20, 47, 27, 28, 29 -> 31; 48/49)
# Assim como no Fluxo - SP.py, os cross-joins artificiais de escalares 1x1
# do Alteryx (Formula Total=1 + Join Total=Total, nos ToolIDs 55/56/20 e
# 27/28) sao calculados direto em Python, pois cada lado sempre produz
# exatamente 1 linha. Diferente do SP, a SPA nao tem "Regime Especial"
# (nao existe essa saida no fluxo), entao "Faturamento x SAP" soma apenas
# o ISS de faturamento calculado com o total do razao SAP.
# ===========================================================================

faturamento_total = scalar_sum(df_com_aliquota, "iss")                     # Node 54 -> "Faturamento"
faturamento_x_sap = round(faturamento_total + sap_total, 2)                # Node 47
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

# Nodes 48/49: saidas de excecao do join Total=Total entre os agregados
# Faturamento e SAP. Como ambos os lados sempre tem exatamente 1 linha, essas
# saidas ficam sempre vazias em operacao normal - mantidas so para paridade.
write_excel(
    spark.createDataFrame([], StructType([StructField("faturamento", DoubleType())])),
    PATH_OUT_FATURAMENTO_X_SAP_EXC, rename={"faturamento": "Faturamento"},
)
write_excel(
    spark.createDataFrame([], StructType([StructField("sap", DoubleType())])),
    PATH_OUT_SAP_X_FATURAMENTO_EXC, rename={"sap": "SAP"},
)


# ===========================================================================
# PIPELINE 7 - Lancamento contabil (partida dobrada) do ISS a pagar
# (ToolIDs 62, 68, 65, 64, 63, 66, 67)
# ===========================================================================

hoje = datetime.date.today()
primeiro_dia_mes_atual = hoje.replace(day=1)
ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

data_lancamento = f"10.{hoje.month:02d}.{hoje.year}"
periodo = f"{hoje.month:02d}"
referencia_lancamento = ultimo_dia_mes_anterior.strftime("%d.%m.%Y")
texto = (
    "REF.REC. ISS S/ FATURAMENTO SPA - "
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

# Linha 1 = debito (chave de lancamento 40, conta 21400122). Ao contrario do
# SP, o fluxo da SPA nunca preenche "Txt.cab.doc." (fica em branco nas duas
# linhas - nao ha formula para esse campo no Alteryx da SPA).
linha_debito = (
    data_lancamento, "FE", 1000, data_lancamento, periodo, "BRL", None,
    referencia_lancamento, None, 40, 21400122, None, montante, None, None,
    "", None, "", "", None, None, None, texto, None, None, None, None,
    None, None, None, None, None, None,
)

# Linha 2 = credito (chave de lancamento 31, conta 400003 - diferente da
# conta 400006 usada no SP; forma de pagamento "B", diferente do "O" do SP)
linha_credito = (
    "", "", None, "", "", "", None, referencia_lancamento, None, 31, 400003, None,
    montante, None, None, "B", None, "Z001", data_lancamento, None, None,
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
