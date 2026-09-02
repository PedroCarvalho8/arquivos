"""
Glue Job de manutencao das tabelas Iceberg. Uma execucao por tabela.

Cada INSERT OVERWRITE diario cria um snapshot que referencia uma copia integral
dos dados. Sem expiracao o storage cresce linearmente - 9 tabelas, uma copia por
dia, para sempre. Este job e o contrapeso do full export.

Argumentos: --tabela_destino --dias_retencao --snapshots_retidos --execution_id
"""

import json
import sys
from datetime import datetime, timedelta, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ARGUMENTOS = [
    "JOB_NAME",
    "tabela_destino",
    "dias_retencao",
    "snapshots_retidos",
    "execution_id",
]

FORMATO_TIMESTAMP = "%Y-%m-%d %H:%M:%S"

# Arquivo orfao recente pode ser de uma escrita concorrente ainda nao commitada:
# apagar seria corromper o commit em voo. Tres dias e a folga padrao do Iceberg.
DIAS_ORFAOS = 3


def executar(spark, sql):
    """Roda a procedure e devolve as linhas de resultado ja em dict."""
    return [linha.asDict() for linha in spark.sql(sql).collect()]


def main():
    args = getResolvedOptions(sys.argv, ARGUMENTOS)
    contexto = GlueContext(SparkContext.getOrCreate())
    spark = contexto.spark_session
    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    # As procedures vivem em <catalogo>.system e recebem o identificador
    # relativo ao catalogo, sem o prefixo dele.
    catalogo, identificador = args["tabela_destino"].split(".", 1)
    agora = datetime.now(timezone.utc)
    corte_snapshots = (
        agora - timedelta(days=int(args["dias_retencao"]))
    ).strftime(FORMATO_TIMESTAMP)
    corte_orfaos = (agora - timedelta(days=DIAS_ORFAOS)).strftime(
        FORMATO_TIMESTAMP
    )

    resultados = {}

    # Compactar antes de expirar: rewrite_data_files cria um snapshot novo, e os
    # arquivos que ele substitui so somem num expire posterior. Na ordem inversa,
    # cada compactacao deixaria uma geracao inteira de arquivos presa por mais
    # uma semana.
    resultados["rewrite_data_files"] = executar(
        spark,
        f"CALL {catalogo}.system.rewrite_data_files("
        f"table => '{identificador}', "
        f"options => map('min-input-files', '5'))",
    )
    resultados["expire_snapshots"] = executar(
        spark,
        f"CALL {catalogo}.system.expire_snapshots("
        f"table => '{identificador}', "
        f"older_than => TIMESTAMP '{corte_snapshots}', "
        f"retain_last => {int(args['snapshots_retidos'])})",
    )
    resultados["remove_orphan_files"] = executar(
        spark,
        f"CALL {catalogo}.system.remove_orphan_files("
        f"table => '{identificador}', "
        f"older_than => TIMESTAMP '{corte_orfaos}')",
    )

    print(
        json.dumps(
            {
                "evento": "manutencao.iceberg",
                "execution_id": args["execution_id"],
                "tabela": args["tabela_destino"],
                "corte_snapshots": corte_snapshots,
                "corte_orfaos": corte_orfaos,
                "resultados": resultados,
            },
            ensure_ascii=False,
            default=str,
        )
    )

    job.commit()


if __name__ == "__main__":
    main()
