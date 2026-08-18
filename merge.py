"""
MERGE dinâmico para tabelas Iceberg no AWS Glue (PySpark).

Gera e executa um MERGE INTO que:
  - suporta duas operações por coluna de valor: "substituir" ou "somar";
  - a operação pode ser fixa por coluna OU decidida por linha, via uma
    coluna de modo na staging (ex: s.modo = 'somar');
  - atualiza a coluna de data correspondente APENAS quando o valor muda
    (substituição diferente do atual, ou soma com incremento != 0);
  - não reescreve linhas idênticas (reduz write amplification em COW);
  - insere linhas novas com todas as datas preenchidas.

ATENÇÃO — idempotência: "substituir" é idempotente (reprocessar a mesma
carga não altera o resultado); "somar" NÃO é (reprocessar duplica o
acumulado). Para cargas com soma, garanta processamento exactly-once
(ex: controle de batch_id já processado) antes de reexecutar.

Uso típico:

    from iceberg_merge_upsert import RegraColuna, executar_merge_upsert

    executar_merge_upsert(
        spark=spark,
        tabela_destino="glue_catalog.db.minha_tabela",
        df_staging=df,
        colunas_chave=["chave"],
        regras=[
            RegraColuna("status", "dt_status"),                      # substitui
            RegraColuna("saldo", "dt_saldo", operacao="somar"),      # acumula
            RegraColuna("pontos", "dt_pontos", coluna_modo="modo"),  # por linha
        ],
    )
"""

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Union
import uuid

from pyspark.sql import DataFrame, SparkSession

OPERACOES_VALIDAS = {"substituir", "somar"}

# Valor esperado na coluna de modo (staging) para acionar a soma;
# qualquer outro valor (ou NULL) substitui.
VALOR_MODO_SOMAR = "somar"


@dataclass(frozen=True)
class RegraColuna:
    """
    Regra de atualização de uma coluna de valor.

    Attributes:
        valor: nome da coluna de valor.
        data: nome da coluna de data associada (marca de atualização).
        operacao: "substituir" (padrão) ou "somar". Ignorada se
            coluna_modo for informada.
        coluna_modo: nome de uma coluna na STAGING que decide a operação
            por linha. Se s.<coluna_modo> = 'somar', soma; caso contrário,
            substitui.
    """

    valor: str
    data: str
    operacao: str = "substituir"
    coluna_modo: Optional[str] = None

    def __post_init__(self) -> None:
        if self.operacao not in OPERACOES_VALIDAS:
            raise ValueError(
                f"Operação inválida '{self.operacao}' para a coluna "
                f"'{self.valor}'. Use uma de: {sorted(OPERACOES_VALIDAS)}"
            )


# Aceita também tuplas, para retrocompatibilidade:
#   (valor, data)            -> substituir
#   (valor, data, operacao)  -> operação fixa
RegraOuTupla = Union[RegraColuna, Tuple[str, str], Tuple[str, str, str]]


def _q(col: str) -> str:
    """Escapa identificador com backticks."""
    return f"`{col.strip('`')}`"


def _normalizar_regras(regras: Sequence[RegraOuTupla]) -> List[RegraColuna]:
    normalizadas = []
    for r in regras:
        if isinstance(r, RegraColuna):
            normalizadas.append(r)
        elif isinstance(r, tuple) and len(r) in (2, 3):
            normalizadas.append(RegraColuna(*r))
        else:
            raise ValueError(f"Regra inválida: {r!r}")
    return normalizadas


def _exprs_regra(regra: RegraColuna) -> Tuple[str, str]:
    """
    Retorna (condicao_mudou, expressao_novo_valor) para a regra.

    - substituir: mudou se NOT (t.v <=> s.v); novo valor = s.v
    - somar:      mudou se coalesce(s.v, 0) <> 0;
                  novo valor = coalesce(t.v, 0) + coalesce(s.v, 0)
    - coluna_modo: CASE por linha entre os dois comportamentos.
    """
    v = _q(regra.valor)
    subst_valor = f"s.{v}"
    subst_mudou = f"NOT (t.{v} <=> s.{v})"
    soma_valor = f"coalesce(t.{v}, 0) + coalesce(s.{v}, 0)"
    soma_mudou = f"coalesce(s.{v}, 0) <> 0"

    if regra.coluna_modo:
        m = f"s.{_q(regra.coluna_modo)}"
        cond_soma = f"{m} <=> '{VALOR_MODO_SOMAR}'"
        mudou = (
            f"CASE WHEN {cond_soma} THEN {soma_mudou} "
            f"ELSE {subst_mudou} END"
        )
        novo_valor = (
            f"CASE WHEN {cond_soma} THEN {soma_valor} "
            f"ELSE {subst_valor} END"
        )
        return mudou, novo_valor

    if regra.operacao == "somar":
        return soma_mudou, soma_valor

    return subst_mudou, subst_valor


def gerar_merge_sql(
    tabela_destino: str,
    view_staging: str,
    colunas_chave: Sequence[str],
    regras: Sequence[RegraOuTupla],
    colunas_extras: Optional[Sequence[str]] = None,
    predicado_extra_on: Optional[str] = None,
    expressao_timestamp: str = "current_timestamp()",
) -> str:
    """
    Gera o SQL do MERGE INTO.

    Args:
        tabela_destino: tabela Iceberg (ex: "glue_catalog.db.tabela").
        view_staging: temp view com os dados de entrada.
        colunas_chave: colunas de join do MERGE.
        regras: lista de RegraColuna (ou tuplas retrocompatíveis).
        colunas_extras: colunas atualizadas/inseridas sempre, sem data.
        predicado_extra_on: predicado extra no ON (partition pruning).
        expressao_timestamp: expressão SQL da data de atualização.
    """
    if not colunas_chave:
        raise ValueError("Informe ao menos uma coluna de chave.")
    regras_n = _normalizar_regras(regras)
    if not regras_n:
        raise ValueError("Informe ao menos uma regra de coluna.")

    colunas_extras = list(colunas_extras or [])

    # ---- ON ----
    on_parts = [f"t.{_q(k)} = s.{_q(k)}" for k in colunas_chave]
    if predicado_extra_on:
        on_parts.append(f"({predicado_extra_on})")
    on_clause = " AND ".join(on_parts)

    # ---- Condições de mudança e SET por regra ----
    condicoes_mudou = []
    set_parts = []
    for regra in regras_n:
        v, d = _q(regra.valor), _q(regra.data)
        mudou, novo_valor = _exprs_regra(regra)
        condicoes_mudou.append(f"({mudou})")
        # Valor e data protegidos pela mesma condição de mudança:
        # se nada mudou, ambos permanecem como estão (evita, p.ex.,
        # NULL virar 0 numa soma sem incremento).
        set_parts.append(
            f"t.{v} = CASE WHEN {mudou} THEN {novo_valor} ELSE t.{v} END"
        )
        set_parts.append(
            f"t.{d} = CASE WHEN {mudou} THEN {expressao_timestamp} "
            f"ELSE t.{d} END"
        )
    for c in colunas_extras:
        condicoes_mudou.append(f"(NOT (t.{_q(c)} <=> s.{_q(c)}))")
        set_parts.append(f"t.{_q(c)} = s.{_q(c)}")

    condicao_mudou = " OR ".join(condicoes_mudou)
    set_clause = ",\n    ".join(set_parts)

    # ---- INSERT (linha nova: valor da staging + timestamp) ----
    cols_insert = (
        [_q(k) for k in colunas_chave]
        + [x for r in regras_n for x in (_q(r.valor), _q(r.data))]
        + [_q(c) for c in colunas_extras]
    )
    vals_insert = (
        [f"s.{_q(k)}" for k in colunas_chave]
        + [
            x
            for r in regras_n
            for x in (f"s.{_q(r.valor)}", expressao_timestamp)
        ]
        + [f"s.{_q(c)}" for c in colunas_extras]
    )

    return f"""
MERGE INTO {tabela_destino} t
USING {view_staging} s
ON {on_clause}
WHEN MATCHED AND ({condicao_mudou}) THEN UPDATE SET
    {set_clause}
WHEN NOT MATCHED THEN INSERT ({", ".join(cols_insert)})
VALUES ({", ".join(vals_insert)})
""".strip()


def _validar_colunas(
    df: DataFrame,
    colunas_chave: Sequence[str],
    regras: Sequence[RegraColuna],
    colunas_extras: Iterable[str],
) -> None:
    """Garante que as colunas necessárias existem na staging."""
    disponiveis = {c.lower() for c in df.columns}
    necessarias = (
        list(colunas_chave)
        + [r.valor for r in regras]
        + [r.coluna_modo for r in regras if r.coluna_modo]
        + list(colunas_extras)
    )
    faltando = [c for c in necessarias if c.lower() not in disponiveis]
    if faltando:
        raise ValueError(
            f"Colunas ausentes no DataFrame de staging: {faltando}. "
            f"Colunas disponíveis: {sorted(df.columns)}"
        )


def executar_merge_upsert(
    spark: SparkSession,
    tabela_destino: str,
    df_staging: DataFrame,
    colunas_chave: Sequence[str],
    regras: Sequence[RegraOuTupla],
    colunas_extras: Optional[Sequence[str]] = None,
    predicado_extra_on: Optional[str] = None,
    expressao_timestamp: str = "current_timestamp()",
    deduplicar_staging: bool = True,
    exibir_sql: bool = False,
) -> None:
    """
    Registra a staging como temp view, gera o MERGE e executa.

    Nota sobre deduplicar_staging: o MERGE do Iceberg falha se uma linha
    do destino casar com mais de uma linha da staging. Para "substituir",
    dropDuplicates resolve; para "somar", se a staging pode ter VÁRIOS
    incrementos da mesma chave, agregue antes (groupBy(chaves).sum(...))
    em vez de deduplicar, senão incrementos serão descartados.
    """
    regras_n = _normalizar_regras(regras)
    colunas_extras = list(colunas_extras or [])
    _validar_colunas(df_staging, colunas_chave, regras_n, colunas_extras)

    if deduplicar_staging:
        df_staging = df_staging.dropDuplicates(list(colunas_chave))

    view = f"stg_merge_{uuid.uuid4().hex[:8]}"
    df_staging.createOrReplaceTempView(view)

    sql = gerar_merge_sql(
        tabela_destino=tabela_destino,
        view_staging=view,
        colunas_chave=colunas_chave,
        regras=regras_n,
        colunas_extras=colunas_extras,
        predicado_extra_on=predicado_extra_on,
        expressao_timestamp=expressao_timestamp,
    )

    if exibir_sql:
        print(sql)

    try:
        spark.sql(sql)
    finally:
        spark.catalog.dropTempView(view)


# ----------------------------------------------------------------------------
# Exemplo de uso em um Glue Job
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    df = spark.read.parquet("s3://meu-bucket/entrada/")

    executar_merge_upsert(
        spark=spark,
        tabela_destino="glue_catalog.db.minha_tabela",
        df_staging=df,
        colunas_chave=["chave"],
        regras=[
            RegraColuna("status", "dt_status"),                      # substitui
            RegraColuna("saldo", "dt_saldo", operacao="somar"),      # acumula
            RegraColuna("pontos", "dt_pontos", coluna_modo="modo"),  # por linha
        ],
        # predicado_extra_on="t.dt_particao = s.dt_particao",
        exibir_sql=True,
    )
