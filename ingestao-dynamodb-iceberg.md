# Ingestão DynamoDB → Iceberg — o que uma tabela precisa

Referência para habilitar uma tabela nova no fluxo diário. O job de ingestão é
genérico: nada nele conhece nome de coluna, e nenhuma tabela nova exige mexer no
`ingestao_dynamodb_iceberg.py` nem no `ingestao-dynamodb-iceberg.asl.json`.

Em compensação, tudo que a tabela precisa está declarado nela mesma. Este
documento é essa lista.

---

## 1. Checklist para habilitar uma tabela

1. **Tabela DynamoDB de origem existe** na mesma conta e região da state machine.
   O ARN de origem é *derivado por convenção*, não configurado.

2. **PITR habilitado na tabela DynamoDB.** Sem isso o export falha com
   `PointInTimeRecoveryUnavailableException`. É o item mais esquecido, porque
   não tem nada a ver com Iceberg nem com Glue.

3. **Tabela Iceberg criada com o nome `<prefixo><nome_exato_no_dynamo>`**, no
   banco Glue configurado na state machine. O nome é a única ligação entre as
   duas pontas.

4. **Parâmetro `ingestao.habilitado = "true"` nos `Parameters` da tabela Glue.**
   Sem ele a tabela é ignorada em silêncio — sem erro e sem log.

   > **Criando via DDL do Iceberg, este passo é separado e manual.**
   > `ALTER TABLE ... SET TBLPROPERTIES ('ingestao.habilitado'='true')`
   > **não funciona**: propriedade de tabela Iceberg vai para o `.metadata.json`,
   > e o `glue:getTables` não enxerga aquilo. Tem que ser nos `Parameters` da
   > tabela Glue — via console (Table properties), `aws glue update-table` ou
   > Terraform.
   >
   > Commits do Iceberg **preservam** esse parâmetro (o `GlueTableOperations`
   > copia os parameters existentes e só sobrescreve `table_type`,
   > `metadata_location` e `previous_metadata_location`), então basta setar uma
   > vez.

5. **Schema declarado seguindo o contrato de tipos** da seção 2.

6. **Otimizadores do Glue habilitados** na tabela (seção 4).

Conferir tudo de uma vez, com a mesma consulta que a state machine faz:

```bash
aws glue get-tables --database-name <banco> --expression '<prefixo>*' \
  --query 'TableList[].{tabela:Name, habilitado:Parameters."ingestao.habilitado"}'
```

`habilitado: null` = a tabela não vai entrar no lote de amanhã.

---

## 2. Contrato de tipos

O job lê o schema da tabela de destino e projeta o export nele com `from_json`.
O schema é contrato: não há inferência nem evolução automática.

Atributos `N` chegam como **string JSON** — DynamoDB aceita 38 dígitos de
precisão e coagir isso silenciosamente seria perda de dado. O parser do Spark lê
string em `decimal(p,s)` sem perda, mas **só aceita token numérico em tipos
inteiros**.

| DynamoDB | Declare como | Nunca declare como |
|---|---|---|
| `S` | `string` | |
| `N` | `decimal(38,s)` ou `string` | `tinyint` `smallint` `int` `bigint` — coluna vira **100% NULL** |
| `B` | `binary` (decodifica o base64) ou `string` (mantém base64) | |
| `BOOL` | `boolean` | |
| `NULL` | qualquer tipo nullable | |
| `SS` | `array<string>` | |
| `NS` | `array<decimal(38,s)>` ou `array<string>` | `array<bigint>` |
| `BS` | `array<binary>` ou `array<string>` | |
| `M` | `struct<...>`, `map<string,string>` ou `string` — ver abaixo | |
| `L` | `array<T>` se homogênea; `array<string>` ou `string` se heterogênea | |

**A regra do `N` vale em qualquer profundidade.** Campo numérico dentro de um
`struct` aninhado segue a mesma restrição.

**Precisão:** valor que não cabe no `decimal` declarado vira NULL **naquela
célula, silenciosamente, linha a linha** — sem sinal no log. Para monetário e
identificador numérico, sobre precisão: `decimal(38,x)` custa o mesmo que
`decimal(10,2)` em Parquet, que é codificado por valor.

### Como modelar um `M`

| Declaração | Comportamento | Quando |
|---|---|---|
| `struct<...>` | Campo a campo. Campos no dado que não estão no struct são **descartados em silêncio** | Map é um *registro*: campos conhecidos e estáveis |
| `map<string,string>` | Todas as chaves preservadas, valores viram texto | Map é um *dicionário*: chaves dinâmicas |
| `string` | JSON íntegro como texto | Estrutura livre ou que muda rápido |

`struct` é o padrão preferível quando dá: os campos viram colunas separadas no
Parquet, com column pruning e predicate pushdown. `string` custa reparse em toda
consulta.

> **Ponto cego conhecido:** o log de diagnóstico só compara atributos de
> **primeiro nível**. Campo novo dentro de um `M` com destino `struct` é
> descartado sem aparecer em `atributos_fora_do_schema`.

---

## 3. Colunas obrigatórias e partições

- **Marque como `required` (NOT NULL) apenas as colunas de chave**, e apenas se
  forem `string`.
- `required` de string ausente do lote → o job grava **string vazia** como
  sentinela. É assim que origem sem sort key não derruba a carga.
- `required` **não-string** ausente → **a escrita falha**. Não existe sentinela
  honesta para um número.
- Coluna de partição nunca pode ser nula.

Sobre particionamento: o `INSERT OVERWRITE` roda em modo `STATIC` e substitui a
tabela inteira, então a escolha de partição é **puramente otimização de leitura**
— não muda nada na carga.

A ordem das colunas não importa: o job alinha a projeção pela ordem do schema de
destino. E adicionar uma coluna entre execuções não exige mexer em nada — a
próxima carga lê o schema novo e preenche com NULL até o atributo aparecer na
origem.

---

## 4. Propriedades da tabela e manutenção

### Propriedades ao criar

| Propriedade | Sugestão | Por quê |
|---|---|---|
| `format-version` | `2` | |
| `write.target-file-size-bytes` | 128–256 MB | Com overwrite integral diário, o tamanho de arquivo se controla **na escrita**. É o que torna a compaction quase dispensável aqui |
| `write.metadata.delete-after-commit.enabled` | `true` | Limpa `.metadata.json` antigos automaticamente no commit |
| `write.metadata.previous-versions-max` | `10` | Quantos manter |

> **`history.expire.max-snapshot-age-ms` e `history.expire.min-snapshots-to-keep`
> não expiram nada sozinhas.** Iceberg é formato, não serviço — não há daemon.
> São apenas os defaults que o `expire_snapshots` lê quando alguém o executa.
> A única limpeza automática no commit é a de `.metadata.json` acima, que são
> arquivos de alguns KB e não resolvem o problema de storage.

### Expurgo de snapshots: otimizadores do Glue Data Catalog

Cada carga diária é um `INSERT OVERWRITE`, e em Iceberg isso **não apaga nada**:
escreve arquivos novos, cria snapshot novo, e o snapshot anterior continua
referenciando os arquivos antigos. Cada dia deixa para trás **uma cópia integral
da tabela**. Sem expurgo, o storage cresce linearmente para sempre.

Habilite os três otimizadores gerenciados, um por tabela
(`aws_glue_catalog_table_optimizer`):

| Tipo | Configuração | Papel |
|---|---|---|
| `retention` | 3 dias, mínimo 2 snapshots, deletar arquivos expirados | **É o que libera storage.** Remove o snapshot do metadado E apaga os arquivos que só ele referenciava |
| `orphan_file_deletion` | 3 dias de retenção | Pega o que o retention não alcança: arquivos que nunca entraram em snapshot — resto de job que morreu depois de escrever e antes de commitar |
| `compaction` | binpack | Menos necessário aqui, se `write.target-file-size-bytes` estiver ajustado |

Retenção de 3 dias limita o time travel a ~3 dias. Dá para voltar à carga de
ontem se uma ingestão entrar torta, que é o caso real. Requisito de auditoria
mais longo exige subir o número — e cada dia extra custa uma cópia integral de
cada tabela.

---

## 5. Infraestrutura ao redor

Itens que não estão na tabela, mas quebram ou custam caro se faltarem.

- **Lifecycle de expiração no bucket de export.** Cada execução escreve uma cópia
  integral de cada tabela no S3 e **nada apaga isso**. É o mesmo problema dos
  snapshots, do outro lado do pipeline, e não tem otimizador do Glue para
  resolver. Sem uma regra de expiração (7 dias basta — o job só lê o export do
  dia), esse bucket cresce para sempre.

- **Prefixo S3 exclusivo por tabela Iceberg.** Se duas tabelas do Catalog
  compartilharem location, o `retention` ou o `orphan_file_deletion` de uma
  apaga arquivo vivo da outra. Uma tabela por origem já satisfaz isso, desde que
  o layout do warehouse não aninhe prefixos.

- **Nenhuma regra de lifecycle do S3 sobre o caminho do warehouse Iceberg.** O S3
  apagaria manifest e dados ainda referenciados por snapshot ativo. Vale para o
  warehouse; o bucket de export é o caso oposto, acima.

- **`MaxConcurrentRuns` do Glue Job ≥ 4**, que é o `MaxConcurrency` do
  Distributed Map. Abaixo disso a state machine passa o tempo em retry de
  `ConcurrentRunsExceededException`.

- **Parâmetros do Glue Job:** `--datalake-formats iceberg` e a configuração do
  catálogo (`spark.sql.catalog.*`, `spark.sql.extensions`). O script não
  configura o catálogo.

- **IAM.** State machine: `dynamodb:DescribeTable`, `ExportTableToPointInTime` e
  `DescribeExport` nas origens; `glue:GetTables`; `glue:StartJobRun`;
  `dynamodb:PutItem` na tabela de observabilidade; `s3:PutObject` no bucket de
  resultados. Job: leitura no bucket de export, leitura/escrita no warehouse,
  catálogo Glue. Se algum bucket usa SSE-KMS, as duas pontas precisam da chave.

- **Alarme na auto-suspensão da compaction.** O otimizador se desabilita sozinho
  após 4 falhas consecutivas e degrada em silêncio.

- **Custo do export.** É cobrado por GB de tabela, todo dia, independente de
  quanto mudou. É esse número que a série de `BilledSizeBytes` na tabela de
  observabilidade existe para acompanhar — é o dado que decide, mais adiante, se
  alguma tabela justifica migrar para incremental.

---

## 6. Quando o schema diverge

O job quase nunca falha por divergência: ela fica visível, não derruba a carga.

| Situação | Comportamento | Onde aparece no log |
|---|---|---|
| Atributo no Dynamo, sem coluna no Iceberg | Descartado, job passa | `atributos_fora_do_schema` |
| Coluna no Iceberg, atributo ausente no lote | NULL | `colunas_ausentes_no_lote` |
| Idem, coluna `required` de string | String vazia (sentinela) | — |
| Idem, coluna `required` não-string | **Escrita falha** | Erro do job |
| `N` → `decimal(p,s)` ou `string` | Exato, 38 dígitos preservados | — |
| `N` → `int`/`bigint` | Coluna inteira NULL | `colunas_integrais_invalidas` |
| Valor não cabe na precisão do `decimal` | Célula vira NULL, linha sobrevive | — |

`colunas_integrais_invalidas` é reportada **proativamente**: lista toda coluna
integral do schema, tenha ou não chegado dado nela. O erro de modelagem aparece
na primeira execução, não seis meses depois.

A linha sem log é a da precisão. É o único caso de perda silenciosa no fluxo, e
o motivo da recomendação de sobrar precisão na seção 2.

---

## 7. Diagnóstico

Cada execução do job emite uma linha estruturada no CloudWatch:

```json
{
  "evento": "ingestao.diagnostico",
  "execution_id": "...",
  "tabela_origem": "pedidos",
  "tabela_destino": "glue_catalog.raw.dynamodb_pedidos",
  "itens_no_manifesto": 1234,
  "atributos_fora_do_schema": ["campo_novo"],
  "colunas_ausentes_no_lote": ["coluna_descontinuada"],
  "colunas_integrais_invalidas": ["quantidade"]
}
```

O desfecho por tabela também vai para a tabela de observabilidade, com
`pk = INGESTAO#<tabela>` e `sk = <instante do corte>`, incluindo
`billed_size_bytes`, `itens_exportados`, `duracao_ms` e `status_ingestao`.
