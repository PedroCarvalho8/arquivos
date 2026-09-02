"""
Ingestão de export FULL do DynamoDB para tabela Iceberg (AWS Glue / PySpark).

Um único job serve todas as tabelas: nada aqui conhece nome de coluna, tipo ou
domínio. O contrato é o schema da tabela Iceberg de destino, declarado em
Terraform; este job apenas projeta o export nesse contrato.

Fluxo:
  1. lê o schema do destino (fonte de verdade dos tipos);
  2. lê manifest-files.json e monta a lista exata de arquivos do export;
  3. desembrulha o DynamoDB JSON de forma recursiva e genérica;
  4. projeta no schema do destino via from_json + cast explícito;
  5. loga os atributos presentes no export e ausentes do contrato;
  6. INSERT OVERWRITE.

Idempotência: reexecutar sobre o mesmo export produz exatamente as mesmas
linhas. O overwrite é total e não há estado acumulado entre execuções.

Sobre tipos numéricos: o DynamoDB aceita 38 dígitos significativos em N, e um
decimal(38, s) só comporta 38 - s dígitos inteiros. Para identificadores
numéricos e monetários de faixa larga, declare a coluna como string no
Terraform. Se o valor não couber no tipo declarado o job falha, não arredonda.

Argumentos:
  --tabela_origem    nome da tabela DynamoDB (correlação de log)
  --tabela_destino   catalogo.database.tabela da tabela Iceberg
  --bucket_export    bucket onde o export foi escrito
  --manifesto        chave do manifesto devolvida por DescribeExport
  --execution_id     identificador da execução da state machine
  --chave_particao   opcional; nome da partition key da origem
  --chave_ordenacao  opcional; nome da sort key da origem. A state machine
                     omite o argumento quando a tabela não tem sort key.

O catálogo Iceberg (spark.sql.catalog.<nome>) e --datalake-formats iceberg
são configuração do Glue Job, não deste arquivo.
"""

import json
import re
import sys
from typing import Any, Dict, Iterator, List, Set, Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.accumulators import AccumulatorParam
from pyspark.context import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

ARGS_OBRIGATORIOS = [
    "tabela_origem",
    "tabela_destino",
    "bucket_export",
    "manifesto",
    "execution_id",
]
# JOB_NAME é sempre injetado pelo Glue, mas não quando o script roda fora dele.
ARGS_OPCIONAIS = ["chave_particao", "chave_ordenacao", "JOB_NAME"]

DESCRITORES_ESCALARES = ("S", "N", "B")
DESCRITORES_CONJUNTO = ("SS", "NS", "BS")

# manifest-files.json é JSON Lines com uma entrada por arquivo de dados.
# Schema declarado em vez de inferido para não varrer o arquivo duas vezes.
SCHEMA_MANIFESTO = StructType(
    [
        StructField("dataFileS3Key", StringType()),
        StructField("itemCount", LongType()),
    ]
)

_CONTEXTO: Dict[str, str] = {}


def _log(evento: str, **campos: Any) -> None:
    """Uma linha JSON por evento: o CloudWatch Insights consulta por campo."""
    print(
        json.dumps({"evento": evento, **_CONTEXTO, **campos}, ensure_ascii=False, default=str),
        flush=True,
    )


# ---------------------------------------------------------------------------
# Desembrulho do DynamoDB JSON
# ---------------------------------------------------------------------------


def _desembrulhar(valor: Dict[str, Any]) -> Any:
    """
    Remove os descritores de tipo do DynamoDB JSON, recursivamente.

    Todo escalar sai como TEXTO, inclusive N. O motivo não é estilo: o parser
    JSON do Spark não converte string para tipo numérico — um "123.45"
    projetado direto em decimal viraria NULL. E emitir o número cru no JSON é
    pior, porque o Jackson resolve tokens float como double e trunca os 38
    dígitos que o DynamoDB aceita. Texto + cast explícito preserva o valor.
    """
    descritor, conteudo = next(iter(valor.items()))

    if descritor == "NULL":
        return None
    if descritor == "M":
        return {chave: _desembrulhar(v) for chave, v in conteudo.items()}
    if descritor == "L":
        return [_desembrulhar(v) for v in conteudo]
    if descritor in DESCRITORES_CONJUNTO:
        # Conjuntos do DynamoDB não têm ordem. Ordenar aqui é o que torna dois
        # exports do mesmo dado byte-idênticos depois da projeção.
        return sorted(conteudo)
    if descritor == "BOOL":
        return "true" if conteudo else "false"
    if descritor in DESCRITORES_ESCALARES:
        return conteudo

    raise ValueError(f"Descritor DynamoDB desconhecido: {descritor!r}")


class ConjuntoAcumulador(AccumulatorParam):
    """União de conjuntos: idempotente sob reexecução especulativa de task."""

    def zero(self, valor: Set[str]) -> Set[str]:
        return set()

    def addInPlace(self, a: Set[str], b: Set[str]) -> Set[str]:
        return a | b


def _achatar_particao(linhas: Iterator[Any], acumulador: Any) -> Iterator[Tuple[str]]:
    """
    Converte cada linha do export em JSON plano e coleta os nomes de atributo.

    A coleta viaja de carona na mesma passada da escrita: descobrir divergência
    de schema com um segundo scan custaria uma leitura inteira do export.
    """
    vistos: Set[str] = set()
    for linha in linhas:
        texto = linha.value
        if not texto or not texto.strip():
            continue
        item = json.loads(texto)["Item"]
        vistos.update(item.keys())
        plano = {chave: _desembrulhar(valor) for chave, valor in item.items()}
        yield (json.dumps(plano, ensure_ascii=False),)
    acumulador.add(vistos)


# ---------------------------------------------------------------------------
# Projeção no schema declarado
# ---------------------------------------------------------------------------


def schema_textual(tipo: DataType) -> DataType:
    """Mesma forma do schema declarado, com todo escalar trocado por string."""
    if isinstance(tipo, StructType):
        return StructType(
            [StructField(c.name, schema_textual(c.dataType), True) for c in tipo.fields]
        )
    if isinstance(tipo, ArrayType):
        return ArrayType(schema_textual(tipo.elementType), True)
    if isinstance(tipo, MapType):
        # Chave de mapa no DynamoDB JSON é sempre string; o cast para o tipo
        # declarado acontece na projeção.
        return MapType(StringType(), schema_textual(tipo.valueType), True)
    return StringType()


def projetar(coluna: Column, tipo: DataType) -> Column:
    """Converte a árvore textual para os tipos declarados, nó a nó."""
    if isinstance(tipo, StructType):
        montado = F.struct(
            *[projetar(coluna[c.name], c.dataType).alias(c.name) for c in tipo.fields]
        )
        # struct() sobre coluna nula devolve um struct de nulos, não NULL: sem
        # esta guarda, um atributo M ausente viraria um registro vazio existente.
        return F.when(coluna.isNull(), F.lit(None).cast(tipo)).otherwise(montado)
    if isinstance(tipo, ArrayType):
        return F.transform(coluna, lambda item: projetar(item, tipo.elementType))
    if isinstance(tipo, MapType):
        alvo = F.transform_values(coluna, lambda _, valor: projetar(valor, tipo.valueType))
        if not isinstance(tipo.keyType, StringType):
            alvo = F.transform_keys(alvo, lambda chave, _: chave.cast(tipo.keyType))
        return alvo
    if isinstance(tipo, BinaryType):
        # O descritor B vem em base64; cast para binary daria os bytes do texto.
        return F.unbase64(coluna)
    return coluna.cast(tipo)


# ---------------------------------------------------------------------------
# Leitura do export
# ---------------------------------------------------------------------------


def chaves_dos_dados(spark: SparkSession, bucket: str, chave_manifesto: str) -> Tuple[List[str], int]:
    """
    Lista os arquivos de dados a partir do manifesto do export.

    DescribeExport devolve o caminho de manifest-summary.json; a lista de
    arquivos vive em manifest-files.json, no mesmo prefixo. Glob do prefixo
    não serve: vários exports coexistem no bucket e o dia seguinte leria o
    lote do dia anterior junto.
    """
    chave = re.sub(r"manifest-summary\.json$", "manifest-files.json", chave_manifesto)
    linhas = (
        spark.read.schema(SCHEMA_MANIFESTO)
        .json(f"s3://{bucket}/{chave}")
        .select("dataFileS3Key", "itemCount")
        .collect()
    )
    caminhos = [f"s3://{bucket}/{linha['dataFileS3Key']}" for linha in linhas if linha["dataFileS3Key"]]
    itens = sum(linha["itemCount"] or 0 for linha in linhas)
    return caminhos, itens


def ler_export(spark: SparkSession, caminhos: List[str]) -> DataFrame:
    """Uma linha de texto por item exportado."""
    if not caminhos:
        return spark.createDataFrame([], StructType([StructField("value", StringType())]))
    # Os arquivos vêm em gzip, que não é splittable: o paralelismo máximo é o
    # número de arquivos do manifesto, não o número de workers.
    return spark.read.text(caminhos)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


def resolver_argumentos(argv: List[str]) -> Dict[str, str]:
    """getResolvedOptions falha em argumento ausente; os opcionais entram só se vierem."""
    presentes = [nome for nome in ARGS_OPCIONAIS if f"--{nome}" in argv]
    return getResolvedOptions(argv, ARGS_OBRIGATORIOS + presentes)


def colunas_sentinela(schema: StructType, chaves: Set[str]) -> List[str]:
    """
    Colunas string que recebem '' no lugar de NULL.

    Cobre os dois casos de sort key ausente: o item que não traz o atributo e a
    tabela de origem que não tem sort key, mas cuja coluna existe no contrato
    Iceberg. Coluna de chave marcada como required também entra: NULL ali
    aborta a escrita inteira em vez de sinalizar o registro.
    """
    return [
        campo.name
        for campo in schema.fields
        if isinstance(campo.dataType, StringType)
        and (campo.name in chaves or not campo.nullable)
    ]


def main() -> None:
    args = resolver_argumentos(sys.argv)
    _CONTEXTO.update(
        {
            "execution_id": args["execution_id"],
            "tabela_origem": args["tabela_origem"],
            "tabela_destino": args["tabela_destino"],
        }
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args.get("JOB_NAME", "ingestao-dynamodb-iceberg"), args)

    # Export full substitui a tabela inteira. Em modo dynamic, partições que
    # sumiram da origem sobreviveriam ao overwrite com dados velhos.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")

    # Sem ANSI, cast fora do intervalo devolve NULL sem avisar: um valor
    # monetário que não cabe no decimal declarado sumiria calado. Com ANSI o
    # cast levanta erro e o lote inteiro para, que é o comportamento correto
    # para violação de contrato.
    spark.conf.set("spark.sql.ansi.enabled", "true")

    destino = args["tabela_destino"]

    # O Catalog do Glue guarda o ponteiro de metadados, não as colunas: o
    # schema autoritativo sai do arquivo de metadados, que é o que spark.table lê.
    schema: StructType = spark.table(destino).schema

    caminhos, itens_manifesto = chaves_dos_dados(spark, args["bucket_export"], args["manifesto"])
    _log(
        "export_localizado",
        arquivos=len(caminhos),
        itens_no_manifesto=itens_manifesto,
        colunas_declaradas=len(schema.fields),
    )
    if not caminhos:
        # Export de tabela vazia é legítimo e o overwrite vai zerar o destino.
        _log("export_sem_arquivos", nivel="ALERTA")

    acumulador = sc.accumulator(set(), ConjuntoAcumulador())
    rdd_plano = ler_export(spark, caminhos).rdd.mapPartitions(
        lambda linhas: _achatar_particao(linhas, acumulador)
    )
    df_plano = spark.createDataFrame(
        rdd_plano, StructType([StructField("json_plano", StringType())])
    )

    df_lido = df_plano.select(F.from_json("json_plano", schema_textual(schema)).alias("dados"))
    df = df_lido.select(
        *[projetar(F.col("dados")[c.name], c.dataType).alias(c.name) for c in schema.fields]
    )

    chaves = {args[k] for k in ("chave_particao", "chave_ordenacao") if args.get(k)}
    for nome in colunas_sentinela(schema, chaves):
        df = df.withColumn(nome, F.coalesce(F.col(nome), F.lit("")))

    view = "stg_" + re.sub(r"\W", "_", destino)
    df.createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE {destino} SELECT * FROM {view}")

    # O acumulador só está populado depois da ação acima.
    presentes: Set[str] = acumulador.value
    # from_json casa campo sem diferenciar caixa (spark.sql.caseSensitive=false),
    # então "Id" contra "id" não é divergência.
    declaradas = {campo.name.lower() for campo in schema.fields}
    fora_do_contrato = sorted(a for a in presentes if a.lower() not in declaradas)
    sem_dado = sorted(
        campo.name for campo in schema.fields if campo.name.lower() not in {a.lower() for a in presentes}
    )
    _log(
        "divergencia_de_schema",
        atributos_fora_do_contrato=fora_do_contrato,
        colunas_sem_dado_no_lote=sem_dado,
        atributos_no_export=len(presentes),
    )
    _log("ingestao_concluida", itens_no_manifesto=itens_manifesto)

    job.commit()


if __name__ == "__main__":
    main()
