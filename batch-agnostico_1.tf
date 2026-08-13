# Os 6 leitores vivem no proprio .asl.json: um estado Map cada, roteados por
# um Choice sobre $.leitor. O Terraform so injeta valores de ambiente via
# replace() de placeholders - sem jsondecode/merge, sem for_each.
locals {
  asl_texto = replace(replace(replace(replace(
    file("${path.module}/batch-agnostico.asl.json"),
    "bucket-resultados-placeholder", aws_s3_bucket.resultados.id),
    "arn:aws:sns:us-east-1:000000000000:alertas-placeholder", aws_sns_topic.alertas.arn),
    "workgroup-placeholder", aws_athena_workgroup.lotes.name),
    "tabela-lotes-placeholder", aws_dynamodb_table.lotes.name
  )

}

resource "aws_sfn_state_machine" "batch_agnostico" {
  name       = "batch-agnostico"
  role_arn   = aws_iam_role.sfn.arn
  type       = "STANDARD"
  definition = local.asl_texto

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = false # dados de informe nao vao para log
    level                  = "ERROR"
  }

  tracing_configuration { enabled = true }
}

# ---------------------------------------------------------------------------
# IAM: a convencao de nome E a fronteira de permissao.
# Sem ela, seria necessario wildcard sobre toda a conta.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn" {
  statement {
    sid     = "InvocarExecutorasETratadores"
    actions = ["lambda:InvokeFunction", "lambda:GetFunction"]
    resources = [
      "arn:aws:lambda:${var.region}:${var.account_id}:function:executora-lote-*",
      "arn:aws:lambda:${var.region}:${var.account_id}:function:tratador-lote-*",
    ]
  }

  statement {
    sid       = "RodarGlueTratadores"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:BatchStopJobRun", "glue:GetJob"]
    resources = ["arn:aws:glue:${var.region}:${var.account_id}:job/tratador-lote-*"]
  }

  # Validacao previa do prepared statement (ValidarTratadorAthena).
  statement {
    sid       = "ValidarSqlDoDominio"
    actions   = ["athena:GetPreparedStatement"]
    resources = ["arn:aws:athena:${var.region}:${var.account_id}:workgroup/${aws_athena_workgroup.lotes.name}"]
  }

  statement {
    sid = "ExecucaoAninhadaDistributedMap"
    actions = [
      "states:StartExecution",
      "states:DescribeExecution",
      "states:StopExecution",
      "states:ListMapRuns",
      "states:DescribeMapRun",
    ]
    resources = ["arn:aws:states:${var.region}:${var.account_id}:stateMachine:batch-agnostico"]
  }

  statement {
    sid       = "LerEntrada"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.manifestos.arn, "${aws_s3_bucket.manifestos.arn}/*"]
  }

  statement {
    sid       = "EscreverResultWriter"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.resultados.arn}/*"]
  }

  statement {
    sid       = "MetadadosDoLote"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.lotes.arn]
  }

  statement {
    sid       = "Alertas"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alertas.arn]
  }
}

# Inventario de LOTES (nao de itens). Volume baixo: um item por execucao.
# Distinta da tabela de idempotencia uuid5, que guarda um item por linha.
resource "aws_dynamodb_table" "lotes" {
  name         = "controle-lotes"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery { enabled = true }
}

# ---------------------------------------------------------------------------
# Reconciliacao: o SQL vive aqui, versionado, e nao no input da execucao.
# ---------------------------------------------------------------------------

resource "aws_athena_prepared_statement" "tratador_informes" {
  name          = "tratador_lote_informes"
  workgroup     = aws_athena_workgroup.lotes.name
  query_statement = <<-SQL
    MERGE INTO informes_json t
    USING (
      SELECT doc_id, ano_calendario, versao
      FROM resultados_lote
      WHERE execution_id = ?
    ) s
    ON  t.doc_id         = s.doc_id
    AND t.ano_calendario = s.ano_calendario
    AND t.versao         = s.versao
    WHEN MATCHED THEN UPDATE SET
      status_publicacao = 'PUBLICADO',
      publicado_em      = current_timestamp
  SQL
}

# Retencao dos manifestos: prefixo imutavel, limpeza por idade.
# Substitui o move entre em_processamento/sucesso/falha.
resource "aws_s3_bucket_lifecycle_configuration" "resultados" {
  bucket = aws_s3_bucket.resultados.id

  rule {
    id     = "expirar-resultados"
    status = "Enabled"
    filter {}
    expiration { days = 400 } # uma temporada de IR inteira
  }
}
