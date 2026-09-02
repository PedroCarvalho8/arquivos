"""
Glue Job de ingestao: snapshot DynamoDB (ja exportado para S3) -> tabela Iceberg.

Um unico job serve todas as tabelas - nada aqui conhece nome de coluna. O schema
autoritativo e o da tabela Iceberg de destino: o job le esse schema, projeta o
export nele e sobrescreve a tabela inteira. Sem inferencia, sem evolucao
automatica, sem MERGE. Reexecutar com o mesmo manifesto produz exatamente o
mesmo resultado.

Argumentos: --tabela_origem --tabela_destino --bucket_export --manifesto
            --execution_id

A configuracao do catalogo Iceberg (spark.sql.catalog.*, spark.sql.extensions)
vem dos parametros do Job, nao daqui.
"""

import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import AccumulatorParam
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

ARGUMENTOS = [
    "JOB_NAME",
    "tabela_origem",
    "tabela_destino",
    "bucket_export",
    "manifesto",
    "execution_id",
]

# Um atributo N chega como string JSON e o parser do from_json so aceita token
# numerico nestes tipos: coluna declarada assim e gravada 100% NULL. O
# diagnostico denuncia; o contrato correto e decimal(p,s) ou string.
TIPOS_INTEGRAIS = (ByteType, ShortType, IntegerType, LongType)


class ConjuntoAccumulator(AccumulatorParam):
    """Uniao de conjuntos entre tasks: reexecucao de task e inofensiva."""

    def zero(self, valor):
        return set(valor)

    def addInPlace(self, a, b):
        a.update(b)
        return a


def desembrulhar(valor):
    """
    Converte um valor DynamoDB JSON no valor JSON simples correspondente.

    Recursivo e generico: nao consulta o schema de destino, so os descritores.
    """
    descritor, conteudo = next(iter(valor.items()))

    if descritor in ("S", "N", "B", "BOOL", "SS", "NS", "BS"):
        # N e NS ficam string de proposito. DynamoDB aceita 38 digitos de
        # precisao; from_json le string em decimal(p,s) sem perda e recusa
        # int/long, que e exatamente o que se quer - nada de coercao silenciosa.
        # B e BS ficam em base64, que from_json decodifica sozinho em binary.
        return conteudo
    if descritor == "NULL":
        return None
    if descritor == "M":
        return {chave: desembrulhar(item) for chave, item in conteudo.items()}
    if descritor == "L":
        return [desembrulhar(item) for item in conteudo]

    raise ValueError(f"Descritor DynamoDB desconhecido: {descritor}")


def construir_udf(atributos_vistos):
    """DynamoDB JSON -> JSON simples, anotando os atributos de primeiro nivel."""

    def converter(linha):
        if not linha or not linha.strip():
            return None
        item = json.loads(linha)["Item"]
        atributos_vistos.add(set(item))
        return json.dumps(
            {chave: desembrulhar(valor) for chave, valor in item.items()}
        )

    return F.udf(converter, StringType())


def arquivos_do_export(spark, bucket, chave_manifesto):
    """
    Lista exata dos arquivos de dados, lida do manifest-files.json.

    Glob no prefixo nao serve: varios exports coexistem no bucket e so o
    manifesto diz quais arquivos pertencem a este.
    """
    linhas = spark.read.text(f"s3://{bucket}/{chave_manifesto}").collect()
    caminhos, itens = [], 0
    for linha in linhas:
        if not linha["value"].strip():
            continue
        registro = json.loads(linha["value"])
        caminhos.append(f"s3://{bucket}/{registro['dataFileS3Key']}")
        itens += registro.get("itemCount", 0)
    return caminhos, itens


def aplicar_sentinela(df, schema):
    """
    Campo `required` do Iceberg recusa NULL.

    Uma coluna de chave ausente do lote - origem sem sort key, por exemplo -
    abortaria a escrita inteira; string vazia e a sentinela. Required nao-string
    continua falhando: nao ha sentinela honesta para um numero.
    """
    for campo in schema:
        if not campo.nullable and isinstance(campo.dataType, StringType):
            df = df.withColumn(
                campo.name, F.coalesce(F.col(campo.name), F.lit(""))
            )
    return df


def registrar_diagnostico(args, schema, atributos_export, itens_no_manifesto):
    """Divergencia nao falha o job - so precisa ficar visivel no CloudWatch."""
    declarados = {campo.name for campo in schema}
    print(
        json.dumps(
            {
                "evento": "ingestao.diagnostico",
                "execution_id": args["execution_id"],
                "tabela_origem": args["tabela_origem"],
                "tabela_destino": args["tabela_destino"],
                "itens_no_manifesto": itens_no_manifesto,
                "atributos_fora_do_schema": sorted(
                    atributos_export - declarados
                ),
                "colunas_ausentes_no_lote": sorted(
                    declarados - atributos_export
                ),
                "colunas_integrais_invalidas": sorted(
                    campo.name
                    for campo in schema
                    if isinstance(campo.dataType, TIPOS_INTEGRAIS)
                ),
            },
            ensure_ascii=False,
        )
    )


def main():
    args = getResolvedOptions(sys.argv, ARGUMENTOS)
    contexto = GlueContext(SparkContext.getOrCreate())
    spark = contexto.spark_session
    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    # STATIC e o que faz o INSERT OVERWRITE trocar a tabela inteira. Em DYNAMIC
    # ele substituiria so as particoes presentes no lote, preservando as que
    # sumiram da origem - o oposto de um full export.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")

    destino = args["tabela_destino"]
    # Schema lido da tabela, e nao do GetTable do Glue: em Iceberg o Catalog
    # guarda o ponteiro de metadados, o schema autoritativo esta no metadata.
    schema = spark.table(destino).schema

    caminhos, itens_no_manifesto = arquivos_do_export(
        spark, args["bucket_export"], args["manifesto"]
    )
    atributos_vistos = spark.sparkContext.accumulator(
        set(), ConjuntoAccumulator()
    )

    if caminhos:
        df = (
            spark.read.text(caminhos)
            .select(construir_udf(atributos_vistos)(F.col("value")).alias("json"))
            .where(F.col("json").isNotNull())
            .select(F.from_json(F.col("json"), schema).alias("item"))
            .select("item.*")
        )
    else:
        # Export sem arquivos de dados = origem vazia. O snapshot fiel dessa
        # origem e a tabela vazia, nao a tabela de ontem.
        df = spark.createDataFrame([], schema)

    df = aplicar_sentinela(df, schema)

    # INSERT OVERWRITE casa colunas por posicao, nao por nome.
    visao = "lote_export"
    df.select(*[campo.name for campo in schema]).createOrReplaceTempView(visao)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {destino} SELECT * FROM {visao}")
    finally:
        # O accumulator so tem valor depois da acao; o finally garante o
        # diagnostico tambem quando a escrita falha por divergencia de schema.
        registrar_diagnostico(
            args, schema, atributos_vistos.value, itens_no_manifesto
        )

    job.commit()


if __name__ == "__main__":
    main()
