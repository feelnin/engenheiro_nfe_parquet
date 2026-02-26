# nfe-parquet

Pipeline ETL para processamento de documentos fiscais brasileiros (NF-e e CT-e) em formato XML, convertendo-os em arquivos Parquet mensais particionados por período de emissão.

## Visão geral

O pipeline lê arquivos XML e ZIP de diretórios de entrada, extrai os campos fiscais relevantes, aplica filtros de janela móvel e escreve os dados em Parquet compactado. Cada execução é idempotente: arquivos já processados são ignorados via checkpoint SQLite baseado em fingerprint de tamanho + mtime.

```
XML / ZIP  ──►  Scanner  ──►  Parser  ──►  Filtros  ──►  Buffer  ──►  Flush  ──►  Compact  ──►  Parquet
                                                              │
                                                         Checkpoint
                                                          (SQLite)
```

## Funcionalidades

- Processamento paralelo com `ThreadPoolExecutor` configurável
- Suporte a NF-e (`nfeProc`, `NFe` direto, `dEmi` como fallback de data)
- Suporte a CT-e (`cteProc`) — detectado automaticamente por tag raiz via `is_cte_xml`
- Leitura de XMLs soltos e dentro de ZIPs
- Arquivos vazios (0 bytes ou só whitespace) descartados como `skipped` sem bloquear o pipeline
- Escrita atômica via arquivo `.tmp` + `os.replace`
- Limpeza automática do staging após compactação
- Logging estruturado JSON compatível com Datadog / Loki
- Exit code `1` quando qualquer origem reporta erros — compatível com Task Scheduler, Airflow e cron

## Estrutura do projeto

```
src/nfe_parquet/
├── cli.py                          # Ponto de entrada (argparse + exit code)
├── config/
│   ├── models.py                   # Dataclasses de configuração
│   └── loader.py                   # Carregamento do config.yaml
├── domain/
│   └── models.py                   # SourceMeta, ParseResult
├── io/
│   ├── scanner.py                  # Varredura de diretórios (WorkItem)
│   └── zip_extract.py              # Extração segura de ZIPs para tmp
├── parse/
│   ├── xml_utils.py                # parse_xml_bytes (defensivo, rejeita vazio)
│   ├── nfe_parser.py               # parse_nfe_xml
│   └── cte_parser.py               # parse_cte_xml, is_cte_xml
├── transform/
│   ├── filters.py                  # is_year_allowed
│   └── window.py                   # last_n_months_yyyymm
├── schema/
│   ├── parquet_schema.py           # get_arrow_schema (NF-e)
│   └── cte_schema.py               # get_cte_arrow_schema
├── checkpoint/
│   ├── fingerprint.py              # fingerprint_size_mtime
│   └── store_sqlite.py             # SQLiteCheckpointStore, CheckpointKey
├── write/
│   ├── parquet_writer.py           # write_monthly_parquet
│   └── atomic_commit.py            # atomic_replace
├── orchestrator/
│   ├── pipeline_mt.py              # run_once_mt, _run_source_mt
│   ├── pipeline_cte_mt.py          # run_cte_mt, _run_source_cte
│   └── chunking.py                 # chunked (iterador em lotes)
└── observability/
    └── setup.py                    # setup_logging, get_logger

tests/
├── conftest.py                     # Fixtures compartilhados
├── fixtures/
│   ├── nfe_nfeproc.xml             # NF-e completa (nfeProc + protNFe)
│   ├── nfe_direta_demi.xml         # NF-e sem nfeProc, com dEmi
│   ├── nfe_campos_opcionais_ausentes.xml
│   ├── cte_cteproc.xml             # CT-e completa (cteProc + protCTe)
│   ├── zip_com_xmls.zip            # Gerado por make_zip.py
│   └── make_zip.py
├── test_parser.py
├── test_transform.py
├── test_checkpoint.py
└── test_writer.py
```

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Dependências principais: `lxml`, `pyarrow`, `pyyaml`.
Dependências de desenvolvimento: `pytest`, `pytest-cov`.

## Configuração

Copie e ajuste `config.example.yaml`:

```yaml
paths:
  input_importados: C:\dados\importados
  input_processados: C:\dados\processados
  output_importados: C:\saida\importados
  output_processados: C:\saida\processados
  output_cte: C:\saida\cte
  staging_dir: C:\staging
  tmp_extract_dir: C:\staging\tmp_zip

checkpoint:
  sqlite_path: C:\staging\checkpoint.sqlite

rules:
  min_year: 2023
  moving_window_months: 2

performance:
  max_workers: 8
  file_chunk_size: 500
  record_chunk_size: 50000

logging:
  level: INFO
  format: json
```

## Execução

```bash
python -m nfe_parquet.cli config.yaml
```

**Exit codes:**

| Código | Significado |
|--------|-------------|
| `0` | Pipeline concluído sem erros |
| `1` | Pipeline concluído com erros em pelo menos uma origem |
| `1` | Erro fatal (exceção não tratada ou config inválida) |
| `130` | Interrompido por Ctrl+C |

## Testes

```bash
# Gerar o fixture ZIP (apenas na primeira vez)
python tests/fixtures/make_zip.py

# Rodar todos os testes unitários
pytest tests/ -v

# Com cobertura
pytest tests/ -v --cov=src/nfe_parquet --cov-report=term-missing
```

Consulte `Desc_Tecnica.md` para detalhes sobre a estratégia de testes e a integração com o pipeline de produção.

## Fluxo de execução detalhado

1. `cli.py` carrega o `config.yaml` e inicializa o logging
2. `run_once_mt` cria o `SQLiteCheckpointStore`, carrega o cache em RAM e calcula a janela móvel de meses
3. Para cada origem NF-e (`importados`, `processados`), `_run_source_mt` varre os arquivos, filtra por checkpoint e janela, parseia em paralelo, acumula em buffers mensais e grava parts intermediários em staging
4. `run_cte_mt` executa o mesmo fluxo para CT-e, compartilhando o mesmo store de checkpoint
5. Ao final de cada origem, os parts são compactados em um único Parquet por mês via `_compact_month`, e o staging é limpo
6. O checkpoint é commitado **somente se `errors == 0`**, garantindo idempotência
7. `run_once_mt` retorna `True` se qualquer origem teve erros; `cli.py` traduz isso em `sys.exit(1)`

## Garantias de produção

**Idempotência** — arquivos já processados não são reprocessados. Reprocessamento ocorre somente se o fingerprint (tamanho + mtime) mudar, o que indica modificação do arquivo na origem.

**Atomicidade** — o Parquet final só substitui o anterior após escrita completa via `os.replace`. Nunca há arquivo corrompido em disco.

**Arquivos vazios** — descartados silenciosamente como `skipped`, sem bloquear o checkpoint dos demais arquivos válidos da mesma execução.

**Exit code confiável** — agendadores externos (Task Scheduler, Airflow, cron) detectam falhas via código de saída `1`, sem depender de inspeção de logs.