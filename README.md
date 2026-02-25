# NF-e Parquet ETL

Pipeline Python para processar NF-e em XML (incluindo XMLs dentro de ZIPs) e gerar **1 arquivo Parquet por mês (`AAAAMM.parquet`)**, por origem, com idempotência via checkpoint SQLite e logs JSON estruturados.

---

## Visão geral

```
XML_SICOF/
├── processados_importados/   ←─┐
│   ├── *.xml                   │  scan recursivo
│   └── *.zip (com *.xml)       │
└── processados_XML/          ←─┘

              ↓  parse + filtro + janela móvel

PARQUET_NFE/
├── importados/
│   ├── 202601.parquet
│   └── 202602.parquet
└── processados/
    ├── 202601.parquet
    └── 202602.parquet
```

**Regras principais:**
- Nunca altera ou exclui arquivos de origem.
- Filtra NF-e por ano de emissão (`ide/dhEmi` ou `ide/dEmi`) com `min_year` configurável.
- Reprocessa sempre uma **janela móvel** dos últimos N meses (reconciliação automática).
- ZIPs são extraídos em diretório temporário e limpos ao final, mesmo em caso de erro.
- Idempotência garantida por fingerprint `(tamanho + mtime)` gravado em SQLite.
- 1 linha por NF-e; itens, duplicatas e pagamentos como arrays alinhados.

---

## Estrutura do projeto

```
nfe-parquet/
├── config/
│   └── config.yaml                        # configuração da execução (não versionado)
├── src/
│   └── nfe_parquet/
│       ├── checkpoint/
│       │   ├── fingerprint.py             # hash size+mtime do arquivo
│       │   └── store_sqlite.py            # store SQLite com cache em RAM
│       ├── config/
│       │   ├── loader.py                  # lê e valida o config.yaml
│       │   └── models.py                  # dataclasses de configuração
│       ├── domain/
│       │   └── models.py                  # SourceMeta, ParseResult
│       ├── io/
│       │   ├── scanner.py                 # scan recursivo de XML e ZIP
│       │   └── zip_extract.py             # extração temporária de ZIP
│       ├── observability/
│       │   ├── json_formatter.py          # formatter JSON estruturado
│       │   └── setup.py                   # setup_logging() + get_logger()
│       ├── orchestrator/
│       │   ├── chunking.py                # iterador em batches
│       │   └── pipeline_mt.py             # pipeline principal (multi-thread)
│       ├── parse/
│       │   ├── nfe_parser.py              # parser XML → dict canônico
│       │   └── xml_utils.py               # helpers lxml sem XPath
│       ├── schema/
│       │   └── parquet_schema.py          # schema Arrow tipado
│       ├── transform/
│       │   ├── filters.py                 # filtro por ano de emissão
│       │   └── window.py                  # cálculo da janela móvel de meses
│       ├── write/
│       │   └── atomic_commit.py           # os.replace atômico
│       └── cli.py                         # ponto de entrada da aplicação
├── tests/
│   ├── fixtures/                          # XMLs e ZIPs de teste
│   └── test_placeholder.py
├── pyproject.toml
└── README.md
```

---

## Pré-requisitos

- Python **3.11** ou superior
- pip atualizado

---

## Instalação

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd nfe-parquet
```

### 2. Crie o ambiente virtual

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux / macOS
python -m venv .venv
source .venv/bin/activate
```

### 3. Instale o pacote e suas dependências

```bash
pip install -e .
```

O modo `-e` (editable) permite editar o código sem reinstalar.

---

## Configuração

Crie o arquivo `config/config.yaml` baseado no exemplo abaixo. Este arquivo **não é versionado** (está no `.gitignore`).

```yaml
paths:
  inputs:
    importados:  "C:\\XML_SICOF\\processados_importados"
    processados: "C:\\XML_SICOF\\processados_XML"
  outputs:
    importados:  "L:\\Arquivos carga BI\\PY\\PARQUET_NFE\\importados"
    processados: "L:\\Arquivos carga BI\\PY\\PARQUET_NFE\\processados"
  tmp_extract_dir: "C:\\nfe_etl\\tmp"
  staging_dir:     "C:\\nfe_etl\\staging"

rules:
  min_year: 2026              # ignora NF-e com emissão anterior a este ano
  moving_window_months: 2     # reprocessa os últimos N meses a cada execução

performance:
  max_workers: 8              # threads paralelas para processamento de arquivos
  file_chunk_size: 2000       # arquivos por batch submetido ao pool
  record_chunk_size: 50000    # registros em memória antes de fazer flush para disco

checkpoint:
  sqlite_path: "C:\\nfe_etl\\checkpoint.sqlite"

logging:
  level: "INFO"               # DEBUG | INFO | WARNING | ERROR | CRITICAL
  json: true                  # true = JSON estruturado | false = texto legível
  file_path: "C:\\nfe_etl\\logs\\nfe_parquet.log"   # omitir para logar só no console
```

---

## Execução

Com o ambiente virtual ativo e o `config.yaml` configurado:

```bash
python -m nfe_parquet.cli config/config.yaml
```

Para desenvolvimento com logs legíveis, defina `json: false` no `config.yaml`:

```bash
# Saída no terminal (texto)
python -m nfe_parquet.cli config/config.yaml

# Redirecionando logs para arquivo manualmente (alternativa ao file_path no yaml)
python -m nfe_parquet.cli config/config.yaml 2>> logs/etl.log
```

---

## Logs

### Formato JSON (`json: true`) — produção

Cada evento é uma linha JSON independente, diretamente consumível por Splunk, Datadog, Loki, etc.

```json
{"ts": "2026-02-24T14:30:00.000+00:00", "level": "INFO", "logger": "nfe_parquet.orchestrator.pipeline_mt", "message": "scan_done", "source": "importados", "total": 1240, "xml": 800, "zip": 440, "elapsed_ms": 87}
{"ts": "2026-02-24T14:30:12.500+00:00", "level": "INFO", "logger": "nfe_parquet.orchestrator.pipeline_mt", "message": "compact_done", "source": "importados", "month": "202601", "parts": 4, "rows": 48321, "output": "L:\\...\\202601.parquet", "elapsed_ms": 312}
{"ts": "2026-02-24T14:30:12.800+00:00", "level": "INFO", "logger": "nfe_parquet.orchestrator.pipeline_mt", "message": "source_run_summary", "source": "importados", "ok": 48000, "filtered": 321, "skipped": 900, "errors": 0, "months_written": 2}
```

### Formato texto (`json: false`) — desenvolvimento

```
2026-02-24T14:30:00  INFO      nfe_parquet.orchestrator.pipeline_mt  scan_done
2026-02-24T14:30:12  INFO      nfe_parquet.orchestrator.pipeline_mt  compact_done
2026-02-24T14:30:12  INFO      nfe_parquet.orchestrator.pipeline_mt  source_run_summary
```

---

## Idempotência e reprocessamento

O checkpoint registra cada arquivo processado pelo fingerprint `tamanho|mtime_ns`. Na próxima execução, arquivos não modificados são ignorados (campo `skipped` no resumo).

A **janela móvel** (`moving_window_months`) garante que os últimos N meses sejam sempre reprocessados, reconciliando arquivos que chegaram com atraso. Arquivos fora da janela são descartados silenciosamente (`filtered`).

Para forçar o reprocessamento completo de tudo, basta apagar o arquivo SQLite do checkpoint:

```bash
del C:\nfe_etl\checkpoint.sqlite   # Windows
rm C:/nfe_etl/checkpoint.sqlite    # Linux / macOS
```

---

## Testes

```bash
pytest
```

---

## Dependências principais

| Pacote | Uso |
|---|---|
| `pyarrow` | Leitura e escrita de Parquet, schema tipado |
| `lxml` | Parse de XML sem namespace via localname |
| `python-dateutil` | Parse robusto de `dhEmi` (ISO 8601 com timezone) |
| `pyyaml` | Leitura do `config.yaml` |