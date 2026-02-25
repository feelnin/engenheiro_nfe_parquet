# Documentação Técnica — NF-e Parquet ETL

Guia de referência para desenvolvedores. Cobre arquitetura, responsabilidades de cada camada, fluxo de dados, decisões de design e guias práticos para novas features e correções.

---

## Índice

1. [Visão da Arquitetura](#1-visão-da-arquitetura)
2. [Fluxo de dados completo](#2-fluxo-de-dados-completo)
3. [Camadas e responsabilidades](#3-camadas-e-responsabilidades)
4. [Referência por arquivo](#4-referência-por-arquivo)
5. [Schema Parquet](#5-schema-parquet)
6. [Sistema de Checkpoint](#6-sistema-de-checkpoint)
7. [Sistema de Logging](#7-sistema-de-logging)
8. [Guias práticos](#8-guias-práticos)

---

## 1. Visão da Arquitetura

O projeto segue uma arquitetura em **camadas horizontais**. Cada camada tem uma única responsabilidade e só depende das camadas abaixo dela. Nenhuma camada acessa diretamente a camada acima.

```
┌─────────────────────────────────────────────────────┐
│                      cli.py                         │  Ponto de entrada
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│              orchestrator / pipeline_mt             │  Coordenação geral
└──┬──────────┬──────────┬──────────┬─────────────────┘
   │          │          │          │
┌──▼──┐  ┌───▼──┐  ┌────▼───┐  ┌──▼──────┐
│ io  │  │parse │  │transform│  │  write  │  Operações atômicas
└──┬──┘  └───┬──┘  └────┬───┘  └──┬──────┘
   │          │          │          │
┌──▼──────────▼──────────▼──────────▼──────┐
│         checkpoint  /  schema             │  Infraestrutura transversal
└───────────────────────────────────────────┘
┌───────────────────────────────────────────┐
│              observability                │  Logging (injetado em todos)
└───────────────────────────────────────────┘
┌───────────────────────────────────────────┐
│               config                      │  Configuração (lida uma vez)
└───────────────────────────────────────────┘
```

---

## 2. Fluxo de dados completo

```
config.yaml
    │
    ▼
cli.py ──► setup_logging()
    │
    ▼
pipeline_mt.run_once_mt()
    │
    ├─► SQLiteCheckpointStore.load_cache()        # carrega histórico para RAM
    │
    ├─► last_n_months_yyyymm()                    # calcula janela móvel
    │
    └─► _run_source_mt()  [para cada origem]
            │
            ├─► scan_source()                     # descobre *.xml e *.zip
            │
            ├─► ThreadPoolExecutor                # processamento paralelo
            │       │
            │       ├─► _process_xml()
            │       │       ├─► fingerprint_size_mtime()
            │       │       ├─► CheckpointStore.was_processed()  → pula se já visto
            │       │       └─► parse_nfe_xml()   → dict canônico
            │       │
            │       └─► _process_zip()
            │               ├─► fingerprint_size_mtime()
            │               ├─► CheckpointStore.was_processed()  → pula se já visto
            │               ├─► extract_zip_to_temp()            → tmp_dir (auto-cleanup)
            │               └─► parse_nfe_xml()  para cada XML   → list[dict]
            │
            ├─► is_year_allowed()                 # filtro por ano de emissão
            ├─► janela móvel                      # filtro por mês (ref_aaaamm)
            │
            ├─► _flush_month_buffer()             # parte → disco quando buffer cheio
            │       └─► pq.write_table()          # part-000000.parquet em staging/parts/
            │
            ├─► _compact_month()                  # une todas as parts em 1 arquivo final
            │       ├─► ParquetWriter (streaming) # não carrega tudo em RAM
            │       └─► atomic_replace()          # os.replace → sem arquivo corrompido
            │
            └─► mark_processed_batch()            # commit do checkpoint (só se errors == 0)
```

---

## 3. Camadas e responsabilidades

### `config`
Lê o `config.yaml` uma única vez na inicialização e expõe dataclasses imutáveis (`frozen=True`) para o resto do sistema. **Nenhuma outra camada lê YAML diretamente.**

### `io`
Tudo que envolve **acesso ao sistema de arquivos de origem**:
- Descoberta de arquivos (`scanner.py`)
- Extração de ZIPs para diretório temporário com cleanup garantido (`zip_extract.py`)

Esta camada **nunca escreve** na origem. Não conhece parsing nem negócio.

### `parse`
Converte bytes de XML em um `dict` canônico (`ParseResult.record`). Toda a lógica de extração de campos XML está aqui. Retorna warnings não-fatais em vez de lançar exceções para campos ausentes ou malformados.

### `transform`
Funções puras de transformação e filtragem sobre os dados já parseados:
- `filters.py` — decide se uma NF-e entra ou não com base no ano de emissão
- `window.py` — calcula o conjunto de meses da janela móvel

Não acessa disco, não conhece XML, não conhece Parquet.

### `schema`
Define o **schema Arrow** canônico em um único lugar. Qualquer alteração de tipo ou adição de coluna é feita exclusivamente aqui, garantindo consistência entre escrita e leitura.

### `checkpoint`
Garante **idempotência**: cada arquivo processado com sucesso é registrado no SQLite pelo seu fingerprint. Na próxima execução, arquivos não modificados são ignorados sem precisar ser lidos. Carrega todo o histórico para RAM no início da execução para evitar I/O durante o processamento paralelo.

### `write`
Escreve os arquivos Parquet finais de forma segura:
- Sempre escreve em arquivo `.tmp` primeiro
- Usa `os.replace` (atômico no mesmo filesystem) para promover ao path final
- Nunca deixa um arquivo Parquet parcial/corrompido visível

### `orchestrator`
Coordena todas as camadas acima. É o único lugar onde as dependências se cruzam. Gerencia o pool de threads, os buffers por mês, o ciclo de vida do checkpoint e os logs de progresso.

### `observability`
Infraestrutura de logging injetada em todos os módulos via `get_logger(__name__)`. Configurada uma única vez no `cli.py` antes de qualquer outra coisa.

---

## 4. Referência por arquivo

### `src/nfe_parquet/cli.py`
**Ponto de entrada da aplicação.**

Responsabilidades:
- Parseia o argumento `config` da linha de comando
- Chama `load_config()` e trata erros de configuração antes de qualquer outra coisa
- Chama `setup_logging()` — **única chamada em todo o projeto**
- Chama `run_once_mt()` e trata `KeyboardInterrupt` e exceções fatais
- Registra tempo total de execução

> Regra: nenhuma lógica de negócio aqui. Se precisar adicionar um novo subcomando, use `argparse` com subparsers.

---

### `src/nfe_parquet/config/models.py`
**Dataclasses imutáveis que representam o config.**

| Dataclass | Campos principais |
|---|---|
| `PathsConfig` | todos os diretórios de entrada, saída, tmp e staging |
| `RulesConfig` | `min_year`, `moving_window_months` |
| `PerformanceConfig` | `max_workers`, `file_chunk_size`, `record_chunk_size` |
| `CheckpointConfig` | `sqlite_path` |
| `LoggingConfig` | `level`, `json`, `file_path` |
| `AppConfig` | agrupa todos os anteriores |

> Regra: todos os campos são `frozen=True`. Para adicionar uma nova configuração, adicione o campo aqui e leia-o em `loader.py`.

---

### `src/nfe_parquet/config/loader.py`
**Lê e converte o YAML para `AppConfig`.**

Função principal: `load_config(config_path: Path) -> AppConfig`

Não faz validação de negócio (ex: se o diretório existe). Apenas converte tipos (string → Path, string → int). Validações de existência de diretório ficam no orchestrator, se necessário.

---

### `src/nfe_parquet/io/scanner.py`
**Descobre todos os arquivos de trabalho em uma origem.**

```python
@dataclass(frozen=True)
class WorkItem:
    source: str        # "importados" | "processados"
    source_root: Path
    file_path: Path
    file_type: str     # "xml" | "zip"

def scan_source(root: Path, source_name: str) -> Iterable[WorkItem]
```

Usa `rglob` — varre recursivamente **incluindo a raiz**. Retorna um iterável lazy (não carrega tudo em memória). A ordem de varredura é `*.xml` antes de `*.zip`.

---

### `src/nfe_parquet/io/zip_extract.py`
**Extrai ZIP para diretório temporário com cleanup garantido.**

```python
@contextmanager
def extract_zip_to_temp(zip_path: Path, tmp_root: Path) -> Iterator[Path]
```

Cria `tmp_root/_zip_{stem}/`, extrai tudo, yield do diretório. O bloco `finally` garante `shutil.rmtree` mesmo se o código do caller lançar exceção. O arquivo ZIP de origem **nunca é modificado**.

---

### `src/nfe_parquet/parse/nfe_parser.py`
**Converte bytes XML em `ParseResult` (dict canônico + warnings).**

Função principal:
```python
def parse_nfe_xml(xml_bytes: bytes, meta: SourceMeta, ingested_at: datetime) -> ParseResult
```

Estratégia de parsing:
- Suporta XML raiz como `nfeProc` (com `protNFe`) ou `NFe` diretamente
- `chNFe` é extraído preferencialmente do `protNFe/infProt/chNFe`; fallback para o atributo `Id` do `infNFe`
- `dhEmi` prefere `ide/dhEmi` (ISO 8601 com timezone); fallback para `ide/dEmi` (data simples)
- Campos ausentes viram `None` — nunca lançam exceção
- Warnings não-fatais são acumulados em `ParseResult.warnings` e gravados no campo `parser_warnings` do registro

Campos de lista (itens, duplicatas, pagamentos) são listas alinhadas por posição. Um `det` sem `<prod>` insere `None` em todas as listas para manter o alinhamento.

---

### `src/nfe_parquet/parse/xml_utils.py`
**Helpers lxml para navegação sem XPath e sem namespace.**

| Função | O que faz |
|---|---|
| `parse_xml_bytes(bytes)` | parseia XML tolerando BOM |
| `find_first_by_localpath(root, "ide/dhEmi")` | navega por localnames ignorando namespace |
| `find_text(root, "emit/CNPJ")` | retorna texto do primeiro nó encontrado ou `None` |
| `find_nodes(root, "det")` | retorna lista de todos os nós que casam com o path |
| `find_all_texts(root, "cobr/dup/nDup")` | retorna textos de todos os filhos do último segmento |

> Regra: nunca use XPath no projeto. As NF-e têm namespace variável entre versões; navegar por localname é mais robusto.

---

### `src/nfe_parquet/transform/filters.py`
**Filtro de ano de emissão.**

```python
def is_year_allowed(dh_emi: datetime | None, min_year: int) -> bool
```

Retorna `False` se `dh_emi` for `None` (NF-e sem data parseável é descartada).

---

### `src/nfe_parquet/transform/window.py`
**Calcula a janela móvel de meses.**

```python
def last_n_months_yyyymm(now: datetime, n: int) -> set[str]
```

Exemplo: se `now = 2026-02-24` e `n = 2`, retorna `{"202602", "202601"}`. NF-e com `ref_aaaamm` fora desse conjunto são descartadas no pipeline (campo `filtered` no resumo).

---

### `src/nfe_parquet/schema/parquet_schema.py`
**Schema Arrow canônico — única fonte de verdade de tipos.**

```python
def get_arrow_schema() -> pa.Schema
```

Todos os decimais usam `pa.decimal128(38, 12)` para preservar precisão fiscal. Timestamps usam `pa.timestamp("ms")`. Listas de itens usam `pa.list_(tipo)`.

> Regra: qualquer nova coluna deve ser adicionada **aqui primeiro**, antes de ser populada no parser.

---

### `src/nfe_parquet/checkpoint/fingerprint.py`
**Gera fingerprint de um arquivo pelo tamanho e mtime.**

```python
def fingerprint_size_mtime(path: Path) -> str
# retorna: "102400|1708789200000000000"
```

Usa `st_mtime_ns` (nanosegundos) em vez de `st_mtime` (float) para evitar colisões por arredondamento.

---

### `src/nfe_parquet/checkpoint/store_sqlite.py`
**Store SQLite para idempotência.**

Fluxo recomendado:
1. `store.load_cache()` — carrega todos os fingerprints processados para um `set` em RAM
2. `store.was_processed(key)` — O(1) via lookup no set (sem I/O de disco)
3. `store.mark_processed_batch(keys, ...)` — commit em lote ao final, em transação única

A tabela `checkpoint` tem como PK composta: `(source, source_file_path, source_entry_path, fingerprint)`.

O ZIP inteiro é a **unidade de idempotência** — se qualquer XML interno falhar, o ZIP não é marcado como processado e será reprocessado integralmente na próxima execução.

---

### `src/nfe_parquet/write/atomic_commit.py`
**Substituição atômica de arquivo.**

```python
def atomic_replace(src: Path, dst: Path) -> None
```

Usa `os.replace()` que é atômico quando `src` e `dst` estão no mesmo filesystem. Se falhar (filesystems distintos, ex: C: → L:), faz `copy2` + `replace` com arquivo `.copytmp` intermediário.

---

### `src/nfe_parquet/orchestrator/chunking.py`
**Iterador que agrupa um iterável em batches de tamanho fixo.**

```python
def chunked(it: Iterable[T], size: int) -> Iterator[list[T]]
```

Usado no pipeline para submeter `file_chunk_size` arquivos por vez ao `ThreadPoolExecutor`, evitando que todos os futures sejam criados de uma vez para conjuntos muito grandes.

---

### `src/nfe_parquet/orchestrator/pipeline_mt.py`
**Orquestrador principal — coordena todas as camadas.**

Funções e responsabilidades:

| Função | Responsabilidade |
|---|---|
| `run_once_mt(cfg)` | inicializa checkpoint e chama `_run_source_mt` para cada origem |
| `_run_source_mt(...)` | scan → pool → filtros → flush → compact → checkpoint commit |
| `_process_work_item(...)` | dispatcher: decide entre `_process_xml` e `_process_zip` |
| `_process_xml(...)` | verifica checkpoint, lê e parseia 1 XML |
| `_process_zip(...)` | verifica checkpoint, extrai ZIP, parseia todos os XMLs internos |
| `_flush_month_buffer(...)` | escreve `part-XXXXXX.parquet` em `staging/parts/{source}/{month}/` |
| `_compact_month(...)` | une todas as parts em `AAAAMM.parquet` final via streaming |

**Garantia Exactly-Once:** o commit dos checkpoints só acontece se `errors == 0` ao final do processamento de uma origem.

---

### `src/nfe_parquet/observability/json_formatter.py`
**Formatter JSON estruturado.**

Campos fixos em toda linha: `ts` (ISO-8601 UTC), `level`, `logger`, `message`. Campos passados via `extra={}` no call site viram chaves adicionais no JSON. Valores não-serializáveis passam por `repr()` em vez de quebrar o log.

---

### `src/nfe_parquet/observability/setup.py`
**Inicialização do sistema de logging.**

```python
def setup_logging(log_cfg: LoggingConfig, log_file: Path | None = None) -> None
def get_logger(name: str) -> logging.Logger
```

`setup_logging()` deve ser chamado **uma única vez**, no `cli.py`. Configura o logger raiz `nfe_parquet`. Todos os sub-loggers herdam a configuração automaticamente via hierarquia do `logging` do Python.

`get_logger(__name__)` é o padrão de uso em todos os módulos.

---

## 5. Schema Parquet

### Campos de negócio

| Campo | Tipo Arrow | Fonte XML |
|---|---|---|
| `dhEmi` | `timestamp(ms)` | `ide/dhEmi` ou `ide/dEmi` |
| `nNF` | `string` | `ide/nNF` |
| `chNFe` | `string` | `protNFe/infProt/chNFe` (ou fallback `infNFe/@Id`) |
| `natOp` | `string` | `ide/natOp` |
| `cMunFG` | `string` | `ide/cMunFG` |
| `emit_CNPJ` | `string` | `emit/CNPJ` |
| `emit_xNome` | `string` | `emit/xNome` |
| `emit_UF` | `string` | `emit/enderEmit/UF` |
| `emit_xMun` | `string` | `emit/enderEmit/xMun` |
| `dest_CNPJ` | `string` | `dest/CNPJ` |
| `dest_xNome` | `string` | `dest/xNome` |
| `dest_UF` | `string` | `dest/enderDest/UF` |
| `dest_xMun` | `string` | `dest/enderDest/xMun` |
| `vBCST` | `decimal128(38,12)` | `total/ICMSTot/vBCST` |
| `qBCMonoRet` | `decimal128(38,12)` | `imposto/ICMS/ICMS61/qBCMonoRet` |
| `modFrete` | `string` | `transp/modFrete` |
| `infCpl` | `string` | `infAdic/infCpl` |
| `transp_transporta_xNome` | `string` | `transp/transporta/xNome` |
| `transp_transporta_CNPJ` | `string` | `transp/transporta/CNPJ` |
| `transp_veic_placa` | `string` | `transp/veicTransp/placa` |

### Arrays de itens (alinhados por posição)

| Campo | Tipo Arrow | Fonte XML |
|---|---|---|
| `itens_cProd` | `list(string)` | `det/prod/cProd` |
| `itens_xProd` | `list(string)` | `det/prod/xProd` |
| `itens_CFOP` | `list(string)` | `det/prod/CFOP` |
| `itens_qCom` | `list(decimal128(38,12))` | `det/prod/qCom` |
| `itens_uCom` | `list(string)` | `det/prod/uCom` |
| `itens_vUnCom` | `list(decimal128(38,12))` | `det/prod/vUnCom` |
| `itens_vProd` | `list(decimal128(38,12))` | `det/prod/vProd` |

### Arrays de duplicatas e pagamentos

| Campo | Tipo Arrow | Fonte XML |
|---|---|---|
| `dup_nDup` | `list(string)` | `cobr/dup/nDup` |
| `dup_dVenc` | `list(date32)` | `cobr/dup/dVenc` |
| `pag_tPag` | `list(string)` | `pag/detPag/tPag` |

### Metadados de auditoria

| Campo | Tipo Arrow | Descrição |
|---|---|---|
| `ref_aaaamm` | `string` | Mês de referência derivado de `dhEmi` (ex: `"202601"`) |
| `source` | `string` | `"importados"` ou `"processados"` |
| `source_root` | `string` | Diretório raiz da origem |
| `source_file_path` | `string` | Caminho do arquivo XML ou ZIP |
| `source_file_type` | `string` | `"xml"` ou `"zip"` |
| `source_entry_path` | `string` | Caminho interno do XML dentro do ZIP (null se XML direto) |
| `source_file_mtime` | `timestamp(ms)` | Data de modificação do arquivo de origem |
| `ingested_at` | `timestamp(ms)` | Momento em que o pipeline rodou |
| `parser_warnings` | `list(string)` | Warnings não-fatais do parser (null se nenhum) |

---

## 6. Sistema de Checkpoint

### Tabela SQLite

```sql
CREATE TABLE checkpoint (
    source              TEXT NOT NULL,
    source_file_path    TEXT NOT NULL,
    source_entry_path   TEXT NOT NULL DEFAULT '',
    fingerprint         TEXT NOT NULL,
    processed_at        TEXT NOT NULL,
    ref_aaaamm          TEXT,
    status              TEXT NOT NULL,
    notes               TEXT,
    PRIMARY KEY (source, source_file_path, source_entry_path, fingerprint)
)
```

### Ciclo de vida por execução

```
Início da execução
    └─► load_cache()          # set completo em RAM — zero I/O durante processamento

Para cada arquivo
    └─► was_processed(key)    # O(1) — lookup no set em RAM
            ├─► True  → skipped (ignora)
            └─► False → processa normalmente → acumula em pending_checkpoints

Fim da execução (por origem)
    ├─► errors == 0 → mark_processed_batch()   # commit em lote, 1 transação
    └─► errors > 0  → nenhum commit            # reprocessa tudo na próxima execução
```

### Forçar reprocessamento

```bash
# Reprocessa tudo (apaga o histórico inteiro)
del C:\nfe_etl\checkpoint.sqlite

# Reprocessa um arquivo específico (remove apenas sua linha)
sqlite3 checkpoint.sqlite "DELETE FROM checkpoint WHERE source_file_path LIKE '%nome_do_arquivo%';"
```

---

## 7. Sistema de Logging

### Como adicionar log em um novo módulo

```python
from ..observability.setup import get_logger

log = get_logger(__name__)  # nome automático: "nfe_parquet.meu_modulo"

# Simples
log.info("operacao_concluida")

# Com contexto estruturado (campos extras viram chaves no JSON)
log.info("arquivo_processado", extra={"source": "importados", "file": str(path), "rows": 1240})

# Com traceback completo
try:
    ...
except Exception:
    log.error("falha_no_parse", extra={"file": str(path)}, exc_info=True)
```

### Níveis recomendados por situação

| Nível | Quando usar |
|---|---|
| `DEBUG` | Detalhes por arquivo (skips de checkpoint, warnings de parse, flush de parts) |
| `INFO` | Marcos de execução (scan concluído, compact concluído, resumo da origem) |
| `WARNING` | Situações anômalas não-fatais (scan vazio, compact sem parts, checkpoint pulado por erros) |
| `ERROR` | Falhas em arquivos individuais ou fases do pipeline (sempre com `exc_info=True`) |
| `CRITICAL` | Reservado para falhas que impedem qualquer execução |

---

## 8. Guias práticos

### Adicionar um novo campo ao Parquet

1. **`schema/parquet_schema.py`** — adicione a coluna com o tipo Arrow correto
2. **`parse/nfe_parser.py`** — popule o campo no `record` dentro de `parse_nfe_xml()`
3. Rode os testes: `pytest`
4. Apague o checkpoint se quiser repopular arquivos já processados

### Adicionar uma nova origem (nova pasta de entrada)

1. **`config/models.py`** — adicione os campos `input_nova` e `output_nova` em `PathsConfig`
2. **`config/loader.py`** — leia os novos campos do YAML
3. **`config/config.yaml`** — adicione as novas entradas em `paths.inputs` e `paths.outputs`
4. **`orchestrator/pipeline_mt.py`** — no `run_once_mt()`, adicione a nova origem no loop (ou na sequência de chamadas de `_run_source_mt`)

### Adicionar um novo filtro de NF-e

1. Crie uma função pura em **`transform/filters.py`**:
    ```python
    def is_uf_allowed(uf: str | None, allowed: set[str]) -> bool:
        return uf in allowed if uf else False
    ```
2. Aplique o filtro em **`pipeline_mt.py`**, no loop de registros após `is_year_allowed()`:
    ```python
    if not is_uf_allowed(rec.get("emit_UF"), cfg.rules.allowed_ufs):
        filtered += 1
        continue
    ```
3. Se o filtro for configurável, adicione o parâmetro em `RulesConfig` e `loader.py`

### Corrigir extração de um campo XML

Todo o parsing está em **`parse/nfe_parser.py`**. O caminho mais comum:

```python
# Antes
"campo_x": _none_or_str(find_text(infnfe, "caminho/errado")),

# Depois
"campo_x": _none_or_str(find_text(infnfe, "caminho/correto")),
```

Para campos que aparecem em múltiplos nós (como os itens `det`), o loop está logo abaixo dos escalares. Para campos decimais, use sempre `_to_decimal_or_none()` — nunca `float()`.

### Alterar a janela móvel

Apenas altere `moving_window_months` no `config.yaml`. Nenhuma mudança de código necessária.

### Depurar um arquivo específico com log detalhado

Mude temporariamente `level: "DEBUG"` no `config.yaml`. Os logs de nível DEBUG mostram:
- Qual arquivo foi pulado por checkpoint e por quê
- Warnings de parse campo a campo
- Qual part foi gerada, com quantas linhas e em quanto tempo
- Cada XML processado dentro de ZIPs

### Entender por que uma NF-e não aparece no Parquet

Verifique na ordem:

1. **`skipped`** no `source_run_summary` — o arquivo foi pulado por checkpoint (já processado antes com mesmo fingerprint)
2. **`filtered`** no `source_run_summary` — a NF-e foi processada mas descartada; causas:
    - `dhEmi` ausente ou não parseável (`is_year_allowed` retorna `False` para `None`)
    - Ano de emissão menor que `min_year`
    - `ref_aaaamm` fora da janela móvel de meses
3. **`errors`** no `source_run_summary` — falha durante o processamento; verifique os logs de nível `ERROR` com `exc_info`
4. **`parser_warnings`** no próprio Parquet — a NF-e entrou mas com campos faltando; filtre pelo campo `parser_warnings IS NOT NULL`