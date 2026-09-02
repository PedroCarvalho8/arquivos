"""
Manutenção semanal de uma tabela Iceberg (AWS Glue / PySpark).

Cada ingestão diária é um INSERT OVERWRITE, e cada overwrite grava uma cópia
integral dos dados num snapshot novo. Sem expiração, o storage cresce
linearmente com os dias, em todas as tabelas ao mesmo tempo.

A ordem importa: rewrite_data_files cria arquivos novos, expire_snapshots
descarta os snapshots antigos e os arquivos que só eles referenciavam, e
remove_orphan_files varre o que sobrou sem referência nenhuma.

Argumentos:
  --tabela_destino   catalogo.database.tabela
  --retencao_horas   idade máxima dos snapshots mantidos
  --execution_id     identificador da execução da state machine
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Snapshots mantidos mesmo que mais velhos que a retenção: um rollback precisa
# de pelo menos um estado anterior ao atual.
SNAPSHOTS_RETIDOS = 2

# Arquivos órfãos usam uma janela própria e larga de propósito: um job em curso
# escreve arquivos que ainda não foram commitados e pareceriam órfãos.
HORAS_ORFAOS = 72

# Abaixo disso a compactação não paga o próprio custo.
MIN_ARQUIVOS_PARA_COMPACTAR = "5"


def _log(evento: str, **campos: Any) -> None:
    print(json.dumps({"evento": evento, **campos}, ensure_ascii=False, default=str), flush=True)


def _instante(horas: int) -> str:
    corte = datetime.now(timezone.utc) - timedelta(hours=horas)
    return corte.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    args = getResolvedOptions(sys.argv, ["tabela_destino", "retencao_horas", "execution_id"])
    retencao = int(args["retencao_horas"])

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args.get("JOB_NAME", "manutencao-iceberg"), args)

    # As procedures vivem no namespace system do catálogo, e recebem a tabela
    # já sem o prefixo do catálogo.
    catalogo, tabela = args["tabela_destino"].split(".", 1)
    contexto: Dict[str, str] = {
        "execution_id": args["execution_id"],
        "tabela": args["tabela_destino"],
    }

    comandos = [
        (
            "rewrite_data_files",
            f"CALL {catalogo}.system.rewrite_data_files("
            f"table => '{tabela}', "
            f"options => map('min-input-files', '{MIN_ARQUIVOS_PARA_COMPACTAR}'))",
        ),
        (
            "expire_snapshots",
            f"CALL {catalogo}.system.expire_snapshots("
            f"table => '{tabela}', "
            f"older_than => TIMESTAMP '{_instante(retencao)}', "
            f"retain_last => {SNAPSHOTS_RETIDOS})",
        ),
        (
            "remove_orphan_files",
            f"CALL {catalogo}.system.remove_orphan_files("
            f"table => '{tabela}', "
            f"older_than => TIMESTAMP '{_instante(HORAS_ORFAOS)}')",
        ),
    ]

    for nome, sql in comandos:
        resultado = spark.sql(sql).collect()
        _log(nome, **contexto, resultado=[linha.asDict() for linha in resultado])

    job.commit()


if __name__ == "__main__":
    main()
