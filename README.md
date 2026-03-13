# nfe-parquet

Pipeline ETL que lê documentos fiscais brasileiros (**NF-e** e **CT-e**) em formato XML — tanto soltos quanto dentro de ZIPs — e os converte em arquivos **Parquet mensais**, particionados por período de emissão (AAAAMM).

Os arquivos Parquet gerados são consumidos diretamente pelo **Qlik Sense SaaS** por meio de versões "flat" (achatadas), onde cada item de linha fiscal ocupa uma linha própria na tabela.

---

## Índice

1. [Pré-requisitos](#pré-requisitos)
2. [Instalação](#instalação)
3. [Configuração](#configuração)
4. [Como executar](#como-executar)
5. [Estrutura do projeto](#estrutura-do-projeto)
6. [Como o pipeline funciona](#como-o-pipeline-funciona)
7. [Saídas geradas](#saídas-geradas)
8. [Testes](#testes)
9. [Garantias de produção](#garantias-de-produção)
10. [Códigos de saída](#códigos-de-saída)
11. [Guia de extensão para desenvolvedores](#guia-de-extensão-para-desenvolvedores)

---

## Pré-requisitos

- **Python 3.11 ou superior**
- Acesso de leitura às pastas de entrada (XMLs / ZIPs dos documentos fiscais)
- Acesso de escrita às pastas de saída (Parquets), staging e tmp
- (Produção) Unidade de rede `L:\` mapeada, onde os Parquets finais são gravados

---

## Instalação

```bash
# 1. Clone o repositório e entre na pasta
git clone https://larcopetroleo.visualstudio.com/BI/_git/EXTRATOR_XML_SICOF
cd EXTRATOR_XML_SICOF

# 2. Crie e ative o ambiente virtual
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux / macOS

# 3. Instale o pacote em modo editável (inclui dependências de dev)
pip install -e ".[dev]"
```

**Dependências principais:** `lxml`, `pyarrow`, `pyyaml`, `python-dateutil`
**Dependências de desenvolvimento:** `pytest`, `pytest-cov`

---

## Configuração

O pipeline é totalmente controlado pelo arquivo `config/config.yaml`. Copie-o e ajuste os caminhos para o seu ambiente antes de executar.

```yaml
paths:
  inputs:
    importados:  "C:\\dados\\importados"    # XMLs/ZIPs de NF-e importadas
    processados: "C:\\dados\\processados"   # XMLs/ZIPs de NF-e e CT-e processadas
  outputs:
    importados:  "L:\\saida\\importados"    # Parquets NF-e importadas
    processados: "L:\\saida\\processados"   # Parquets NF-e processadas
    cte:         "L:\\saida\\cte"           # Parquets CT-e processadas
  tmp_extract_dir: "C:\\nfe_etl\\tmp"       # Pasta temporária para extrair ZIPs
  staging_dir:     "C:\\nfe_etl\\staging"   # Pasta de trabalho intermediária

rules:
  min_year: 2026                  # Ignora documentos emitidos antes deste ano
  moving_window_months: 2         # Reprocessa apenas os últimos N meses

performance:
  max_workers: 8                  # Número de threads paralelas
  file_chunk_size: 2000           # Arquivos enviados por lote ao executor
  record_chunk_size: 50000        # Registros acumulados antes de descarregar para staging

checkpoint:
  sqlite_path: "C:\\nfe_etl\\checkpoint.sqlite"  # Banco SQLite de controle

logging:
  level: "INFO"       # DEBUG | INFO | WARNING | ERROR | CRITICAL
  json: true          # true = JSON estruturado (produção) | false = texto legível (dev)
  file_path: "C:\\nfe_etl\\logs\\nfe_parquet.log"  # Omitir para logar só no console
```

### Entendendo os parâmetros mais importantes

| Parâmetro | O que faz |
|-----------|-----------|
| `min_year` | Documentos com data de emissão anterior a este ano são descartados silenciosamente. Útil para evitar reprocessar dados históricos desnecessários. |
| `moving_window_months` | Define quantos meses retroativos o pipeline considera em cada execução. Com `2`, o pipeline processa o mês atual e o anterior. Documentos fora desta janela são ignorados. |
| `max_workers` | Controla o paralelismo. Ajuste conforme o número de núcleos da máquina. |
| `file_chunk_size` | Quantos arquivos são enviados de uma vez ao pool de threads. Reduzir ajuda em máquinas com pouca RAM. |
| `record_chunk_size` | Quando o buffer de registros em memória atinge este limite, os dados são despejados em um arquivo temporário no staging para liberar RAM. |
| `checkpoint` | O SQLite registra quais arquivos já foram processados (usando tamanho + data de modificação como "impressão digital"). Na próxima execução, esses arquivos são pulados. |

---

## Como executar

```bash
# Sintaxe geral
python -m nfe_parquet.cli <caminho-para-config.yaml>

# Exemplo com o config padrão
python -m nfe_parquet.cli config/config.yaml
```

O pipeline processa NF-e e CT-e de forma sequencial por origem. Ao final, gera automaticamente as versões flat para o Qlik Sense.

---

## Estrutura do projeto

```
EXTRATOR_XML_SICOF/
├── config/
│   ├── config.yaml             # Configuração principal (ajuste antes de usar)
│   └── logging.yaml            # Configuração secundária de logging
│
├── src/nfe_parquet/
│   ├── cli.py                  # Ponto de entrada: carrega config, inicia logging, chama pipeline
│   │
│   ├── config/
│   │   ├── models.py           # Dataclasses tipadas (AppConfig, PathsConfig, etc.)
│   │   └── loader.py           # Lê o config.yaml e instancia as dataclasses
│   │
│   ├── domain/
│   │   └── models.py           # SourceMeta (metadados do arquivo) e ParseResult (1 registro + warnings)
│   │
│   ├── io/
│   │   ├── scanner.py          # Varre diretórios e retorna WorkItems (xml ou zip)
│   │   └── zip_extract.py      # Extrai ZIPs com segurança para pasta temporária
│   │
│   ├── parse/
│   │   ├── xml_utils.py        # Parsing defensivo de XML (rejeita arquivos vazios)
│   │   ├── nfe_parser.py       # Extrai campos de NF-e: suporta nfeProc, NFe direto, fallback dEmi
│   │   └── cte_parser.py       # Extrai campos de CT-e; is_cte_xml() detecta o tipo pela tag raiz
│   │
│   ├── transform/
│   │   ├── filters.py          # is_year_allowed(): filtra por min_year
│   │   └── window.py           # last_n_months_yyyymm(): calcula a janela móvel de meses
│   │
│   ├── schema/
│   │   ├── parquet_schema.py   # Schema Arrow da NF-e (colunas escalares + listas)
│   │   └── cte_schema.py       # Schema Arrow do CT-e
│   │
│   ├── checkpoint/
│   │   ├── fingerprint.py      # fingerprint_size_mtime(): gera hash (tamanho + mtime)
│   │   └── store_sqlite.py     # SQLiteCheckpointStore: grava e consulta arquivos já processados
│   │
│   ├── write/
│   │   ├── parquet_writer.py   # write_monthly_parquet(): escreve tabela Arrow em disco
│   │   └── atomic_commit.py    # atomic_replace(): substitui arquivo via .tmp + os.replace
│   │
│   ├── orchestrator/
│   │   ├── pipeline_mt.py      # run_once_mt(): coordena todo o pipeline NF-e + CT-e + flatten
│   │   ├── pipeline_cte_mt.py  # run_cte_mt(): mesmo fluxo do NF-e, adaptado para CT-e
│   │   └── chunking.py         # chunked(): divide iteráveis em lotes
│   │
│   ├── flatten/
│   │   ├── runner.py           # run_flatten(): orquestra geração dos Parquets flat
│   │   ├── flatten_nfe.py      # Explode listas NF-e em linhas (itens, dup, pag, warnings)
│   │   └── flatten_cte.py      # Explode listas CT-e em linhas (infQ, infNFe, transp)
│   │
│   └── observability/
│       ├── setup.py            # setup_logging() e get_logger(): logging JSON estruturado
│       └── json_formatter.py   # Formatter que serializa logs em JSON
│
└── tests/
    ├── conftest.py             # Fixtures compartilhadas
    ├── fixtures/               # XMLs e ZIPs de exemplo para testes
    │   ├── nfe_nfeproc.xml
    │   ├── nfe_direta_demi.xml
    │   ├── nfe_campos_opcionais_ausentes.xml
    │   ├── cte_cteproc.xml
    │   ├── zip_com_xmls.zip    # Gerado por make_zip.py
    │   └── make_zip.py
    ├── test_parser.py
    ├── test_transform.py
    ├── test_checkpoint.py
    ├── test_writer.py
    └── test_flatten.py
```

---

## Como o pipeline funciona

### Visão geral do fluxo

```
Arquivos de entrada
(XML soltos e ZIPs)
        │
        ▼
┌───────────────┐
│    Scanner    │  Varre os diretórios de entrada e lista arquivos .xml e .zip
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Checkpoint   │  Consulta o SQLite: arquivo já foi processado? Se sim, pula.
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Parser     │  Lê o XML (ou extrai do ZIP) e extrai os campos fiscais.
│  (paralelo)   │  Usa ThreadPoolExecutor com max_workers threads.
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Filtros    │  Descarta registros fora do min_year ou da janela móvel de meses.
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Buffer     │  Acumula registros por mês (AAAAMM) em memória.
│   mensal      │  Quando atinge record_chunk_size, descarrega para staging.
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Compactação  │  Junta todos os parts do mês em um único Parquet.
│   + Dedupe    │  Remove duplicatas por chNFe / chCTe (mantém o mais recente).
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Escrita      │  Grava o Parquet final via .tmp + os.replace (atômico).
│  atômica      │  Ex: processados/202602.parquet
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Checkpoint   │  Marca todos os arquivos processados no SQLite.
│  commit       │  SOMENTE se errors == 0. Se houve erro, tudo será reprocessado.
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   Flatten     │  Gera versões "explodidas" dos Parquets para o Qlik Sense.
│  (pós-etl)    │  Ex: processados_flat/202602.parquet
└───────────────┘
```

### Passo a passo detalhado

1. **`cli.py`** lê o `config.yaml`, inicializa o logging e chama `run_once_mt()`.

2. **`run_once_mt()`** (em `orchestrator/pipeline_mt.py`):
   - Abre o banco de checkpoint SQLite e carrega o cache em RAM.
   - Calcula quais meses estão na janela móvel (ex: `{"202602", "202601"}`).
   - Processa as duas origens NF-e (`importados` e `processados`) chamando `_run_source_mt()` para cada uma.
   - Chama `run_cte_mt()` para processar os CT-es (mesma pasta `processados`, mesmo checkpoint).
   - Chama `run_flatten()` para gerar os Parquets flat.

3. **`_run_source_mt()`** — para cada origem NF-e:
   - Varre o diretório de entrada com `scan_source()`, gerando uma lista de `WorkItem`.
   - Distribui os arquivos em lotes (`chunked`) para um `ThreadPoolExecutor`.
   - Cada worker executa `_process_work_item()`:
     - Verifica o fingerprint no checkpoint (pula se já processado).
     - Para XMLs: lê os bytes e chama `parse_nfe_xml()`.
     - Para ZIPs: extrai para tmp, itera os XMLs internos e parseia cada um.
   - Os registros retornados são filtrados por `min_year` e `moving_window_months`.
   - Registros válidos são acumulados em buffers mensais (`dict[str, list[dict]]`).
   - Quando o buffer de um mês atinge `record_chunk_size`, `_flush_month_buffer()` o grava como um arquivo `part-XXXXXX.parquet` no staging.
   - Ao final de todos os lotes, os parts de cada mês são compactados com deduplicação por `chNFe` em `_compact_month()`.
   - O checkpoint é commitado **somente se não houve erros**.

4. **Flatten** (`flatten/runner.py`):
   - Varre os diretórios de saída em busca de Parquets dos meses da janela móvel.
   - Para cada arquivo, verifica se o flat já está atualizado (mtime flat ≥ mtime original). Se sim, pula.
   - Chama `flatten_nfe_parquet()` ou `flatten_cte_parquet()` para gerar a versão explodida.
   - O diretório flat é sempre o diretório de saída com o sufixo `_flat`:
     - `processados/202602.parquet` → `processados_flat/202602.parquet`
     - `importados/202602.parquet` → `importados_flat/202602.parquet`
     - `cte/202602.parquet` → `cte_flat/202602.parquet`

### O que é o "flatten" (achatamento)?

Os Parquets normais armazenam os itens de cada NF-e como **listas** dentro de uma única linha. Por exemplo, uma NF-e com 3 produtos tem uma lista `itens_xProd` com 3 elementos.

O Qlik Sense não consegue trabalhar diretamente com colunas de lista. Por isso, o flatten **explode** essas listas: cada elemento vira uma linha separada, com todos os dados escalares da NF-e repetidos.

```
Parquet normal (1 linha por NF-e):
┌─────────────┬───────────────────────────────────┐
│   chNFe     │  itens_xProd                      │
├─────────────┼───────────────────────────────────┤
│ 5226023...  │  ["PRODUTO A", "PRODUTO B"]        │
└─────────────┴───────────────────────────────────┘

Parquet flat (1 linha por item):
┌─────────────┬───────────────┐
│   chNFe     │  itens_xProd  │
├─────────────┼───────────────┤
│ 5226023...  │  PRODUTO A    │
│ 5226023...  │  PRODUTO B    │
└─────────────┴───────────────┘
```

**Grupos de explode NF-e:** itens (produtos), dup (duplicatas de cobrança), pag (formas de pagamento) e parser_warnings. Grupos independentes entre si geram produto cartesiano.

**Grupos de explode CT-e:** infQ (quantidades de carga), infNFe (NF-es vinculadas) e transp (volumes). Todos geram produto cartesiano.

---

## Saídas geradas

Após uma execução bem-sucedida, os seguintes arquivos são gerados ou atualizados:

| Caminho | Conteúdo |
|---------|----------|
| `outputs.importados/AAAAMM.parquet` | NF-e importadas do mês |
| `outputs.processados/AAAAMM.parquet` | NF-e processadas do mês |
| `outputs.cte/AAAAMM.parquet` | CT-e processadas do mês |
| `outputs.importados_flat/AAAAMM.parquet` | Versão flat para Qlik Sense |
| `outputs.processados_flat/AAAAMM.parquet` | Versão flat para Qlik Sense |
| `outputs.cte_flat/AAAAMM.parquet` | Versão flat CT-e para Qlik Sense |
| `checkpoint.sqlite` | Registro dos arquivos já processados |

> **Nota:** Os diretórios `_flat` são criados automaticamente ao lado dos diretórios de saída originais.

---

## Testes

### Configuração inicial (apenas uma vez)

Antes de rodar os testes pela primeira vez, gere o fixture de ZIP:

```bash
python tests/fixtures/make_zip.py
```

### Executando os testes

```bash
# Todos os testes
pytest tests/ -v

# Um arquivo de testes específico
pytest tests/test_flatten.py -v

# Uma classe de testes específica
pytest tests/test_flatten.py::TestExplodeNfe -v

# Um teste específico
pytest tests/test_flatten.py::TestExplodeNfe::test_nfe_2itens_2dup_1pag_sem_warn -v

# Com relatório de cobertura
pytest tests/ -v --cov=src/nfe_parquet --cov-report=term-missing
```

### O que cada arquivo de testes cobre

| Arquivo | O que testa |
|---------|-------------|
| `test_parser.py` | Parsing de NF-e e CT-e: campos extraídos, fallbacks, XMLs incompletos |
| `test_transform.py` | Filtros de ano e cálculo da janela móvel de meses |
| `test_checkpoint.py` | Fingerprint, inserção e consulta no SQLiteCheckpointStore |
| `test_writer.py` | Escrita atômica de Parquet (.tmp + replace) |
| `test_flatten.py` | Schemas flat, explode de listas NF-e e CT-e, I/O do flatten, runner |

---

## Garantias de produção

### Idempotência
Cada arquivo é identificado por seu **fingerprint** (tamanho em bytes + data de modificação). Se o fingerprint não mudou desde a última execução, o arquivo é pulado. Isso permite executar o pipeline múltiplas vezes sem duplicar dados.

O checkpoint **só é salvo se a execução terminou sem erros**. Se algum arquivo falhou, toda a origem será reprocessada na próxima execução.

### Atomicidade
O Parquet final é gravado em um arquivo temporário `.tmp` e só substituí o arquivo anterior via `os.replace()` após a escrita completa. Isso garante que nunca haverá um arquivo corrompido ou parcialmente escrito em disco, mesmo se a execução for interrompida no meio.

### Resiliência a arquivos problemáticos
- Arquivos vazios (0 bytes ou só espaços em branco) são descartados silenciosamente como `skipped`, sem bloquear o restante do pipeline.
- Erros de parse em um arquivo geram um log de erro e incrementam o contador de falhas, mas não interrompem o processamento dos demais.

### Deduplicação
Na etapa de compactação, registros com a mesma chave de acesso (`chNFe` ou `chCTe`) são deduplicados. O critério de desempate prioriza o arquivo com `mtime` mais recente, garantindo que a versão mais atual do documento prevaleça.

---

## Códigos de saída

O pipeline retorna códigos de saída padrão POSIX, compatíveis com Task Scheduler, Airflow e cron:

| Código | Significado |
|--------|-------------|
| `0` | Pipeline concluído sem nenhum erro |
| `1` | Pelo menos uma origem teve erros durante o processamento |
| `1` | Erro fatal: exceção não tratada ou arquivo de configuração inválido |
| `130` | Execução interrompida pelo usuário (Ctrl+C) |

---

## Guia de extensão para desenvolvedores

Esta seção explica como estender o pipeline sem quebrar o que já existe.

---

### 1. Adicionar um novo campo escalar à NF-e

Um campo escalar é um valor único por documento (ex: CNPJ do emitente, valor total). Dois arquivos precisam ser editados.

#### Passo 1 — Declare o campo no schema Arrow

Arquivo: `src/nfe_parquet/schema/parquet_schema.py`

Adicione a coluna na lista do `get_arrow_schema()`, no grupo temático correto. Exemplo: adicionar o campo `vNF` (valor total da NF-e):

```python
# antes de "metadados"
("vNF", DECIMAL),
```

Tipos disponíveis: `pa.string()`, `pa.timestamp("ms")`, `pa.date32()`, `DECIMAL` (para valores monetários/quantidades).

#### Passo 2 — Extraia o valor no parser

Arquivo: `src/nfe_parquet/parse/nfe_parser.py`

No dicionário `record` dentro de `parse_nfe_xml()`, adicione a extração usando `find_text()` com o caminho XPath relativo ao nó `infnfe`:

```python
"vNF": _to_decimal_or_none(find_text(infnfe, "total/ICMSTot/vNF"), warnings, "vNF"),
```

Funções auxiliares disponíveis:
- `find_text(node, "caminho/xpath")` → retorna `str | None`
- `_none_or_str(texto)` → limpa espaços, retorna `None` se vazio
- `_to_decimal_or_none(texto, warnings, "nome_campo")` → converte para `Decimal` sem perda de precisão
- `_parse_datetime_or_none(texto)` → converte ISO 8601 para `datetime`
- `_parse_date_or_none(texto)` → converte para `date`

#### Resultado

O campo aparecerá automaticamente no Parquet final e também no Parquet flat (o flatten copia todos os campos escalares para cada linha explodida).

---

### 2. Adicionar um novo campo escalar ao CT-e

O processo é idêntico ao da NF-e, mas em arquivos diferentes:

1. **Schema:** `src/nfe_parquet/schema/cte_schema.py` → função `get_cte_arrow_schema()`
2. **Parser:** `src/nfe_parquet/parse/cte_parser.py` → dicionário `record` dentro de `parse_cte_xml()`

O nó raiz no parser CT-e é `infcte` (equivalente ao `infnfe` da NF-e). Os caminhos XPath seguem a estrutura do leiaute CT-e.

---

### 3. Adicionar um novo campo de lista à NF-e

Campos de lista representam conjuntos de valores por documento — por exemplo, os itens de linha (`det`), as duplicatas de cobrança (`cobr/dup`) ou as formas de pagamento (`pag/detPag`). São mais complexos porque afetam o schema, o parser e o flatten.

#### Passo 1 — Schema

Arquivo: `src/nfe_parquet/schema/parquet_schema.py`

Declare a coluna como `pa.list_(tipo)`. Exemplo: adicionar o NCM de cada item:

```python
("itens_NCM", pa.list_(pa.string())),
```

Mantenha-a próxima às outras colunas do mesmo grupo (ex: junto das outras `itens_*`).

#### Passo 2 — Parser: inicialize a lista e popule no loop

Arquivo: `src/nfe_parquet/parse/nfe_parser.py`

**Inicialize a lista no dicionário `record`** (junto das outras do mesmo grupo):

```python
"itens_NCM": [],
```

**Popule dentro do loop `for det in det_nodes`**, mantendo o alinhamento com as outras colunas:

```python
# Dentro do bloco "if prod is None": append(None) para manter alinhamento
record["itens_NCM"].append(None)

# No bloco normal (prod não é None):
record["itens_NCM"].append(_none_or_str(find_text(prod, "NCM")))
```

> **Importante:** O alinhamento é obrigatório para o grupo `itens_*`. Cada lista deve ter exatamente o mesmo número de elementos, com `None` onde o valor estiver ausente. O flatten usa zip-alignment para combinar os campos desse grupo.

#### Passo 3 — Flatten: adicione ao grupo de explode

Arquivo: `src/nfe_parquet/flatten/flatten_nfe.py`

Localize a constante `_NFE_EXPLODE_GROUPS` (ou equivalente) e adicione o novo campo ao grupo correto:

```python
# Grupo "itens" — usa zip-alignment (todos os campos têm o mesmo comprimento)
_ITENS_COLS = [
    "itens_cProd",
    "itens_xProd",
    "itens_CFOP",
    "itens_qCom",
    "itens_uCom",
    "itens_vUnCom",
    "itens_vProd",
    "itens_NCM",   # ← adicione aqui
]
```

Após essa adição, o Parquet flat passará a ter uma coluna `itens_NCM` com o valor de cada item na linha correspondente.

---

### 4. Regenerar os Parquets após uma mudança de schema

Quando você adicionar, remover ou alterar o tipo de uma coluna, os Parquets existentes no disco **não são atualizados automaticamente** — eles foram gerados com o schema antigo. Para forçar o reprocessamento:

#### Opção A — Reprocessar tudo (mais simples)

Exclua o banco de checkpoint. Na próxima execução, todos os arquivos de entrada serão processados do zero:

```bash
# Ajuste o caminho conforme seu config.yaml
del C:\nfe_etl\checkpoint.sqlite
```

#### Opção B — Reprocessar apenas um mês específico

1. Exclua o Parquet do mês desejado em todas as origens:

```bash
del "L:\saida\processados\202602.parquet"
del "L:\saida\importados\202602.parquet"
del "L:\saida\cte\202602.parquet"
```

2. No `config.yaml`, garanta que o mês está dentro da `moving_window_months`. Se o mês for antigo, aumente temporariamente o valor.

3. Execute o pipeline normalmente. O checkpoint ainda impedirá o reprocessamento dos arquivos cujos Parquets de outros meses já estão corretos.

> **Nota:** Após regenerar os Parquets, os Parquets flat (`_flat`) também precisam ser regenerados. Exclua os arquivos `_flat` correspondentes ou simplesmente deixe o pipeline rodar — ele detecta automaticamente que o flat está desatualizado em relação ao Parquet original (por mtime) e o regenera.

---

### 5. Adicionar uma nova origem de documentos NF-e

Para processar documentos de um novo diretório de entrada (ex: `terceiros`):

#### Passo 1 — Config

Arquivo: `config/config.yaml`

Adicione a nova origem em `paths.inputs` e a saída correspondente em `paths.outputs`:

```yaml
paths:
  inputs:
    terceiros: "C:\\dados\\terceiros"   # novo diretório de entrada
  outputs:
    terceiros: "L:\\saida\\terceiros"  # novo diretório de saída
```

#### Passo 2 — Orquestrador

Arquivo: `src/nfe_parquet/orchestrator/pipeline_mt.py`

Na função `run_once_mt()`, adicione uma chamada a `_run_source_mt()` para a nova origem, seguindo o mesmo padrão das origens existentes:

```python
_run_source_mt(
    source="terceiros",
    input_dir=Path(cfg.paths.inputs["terceiros"]),
    output_dir=Path(cfg.paths.outputs["terceiros"]),
    ...  # mesmos parâmetros das outras origens
)
```

#### Passo 3 — Flatten

Arquivo: `src/nfe_parquet/flatten/runner.py`

Adicione o novo par `(diretório_parquet, diretório_flat)` à lista de origens que o `run_flatten()` processa.

---

### Referência rápida: qual arquivo editar para cada tipo de mudança

| O que você quer fazer | Arquivos a editar |
|-----------------------|-------------------|
| Novo campo escalar NF-e | `schema/parquet_schema.py` + `parse/nfe_parser.py` |
| Novo campo escalar CT-e | `schema/cte_schema.py` + `parse/cte_parser.py` |
| Novo campo de lista NF-e | `schema/parquet_schema.py` + `parse/nfe_parser.py` + `flatten/flatten_nfe.py` |
| Novo campo de lista CT-e | `schema/cte_schema.py` + `parse/cte_parser.py` + `flatten/flatten_cte.py` |
| Reprocessar tudo | Deletar `checkpoint.sqlite` |
| Reprocessar um mês | Deletar `AAAAMM.parquet` da(s) saída(s) afetada(s) |
| Nova origem NF-e | `config/config.yaml` + `orchestrator/pipeline_mt.py` + `flatten/runner.py` |
