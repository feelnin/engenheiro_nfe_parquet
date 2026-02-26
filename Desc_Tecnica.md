# Descrição Técnica — nfe-parquet

## 1. Arquitetura em camadas

O projeto segue separação estrita de responsabilidades entre as camadas:

```
cli.py
  └── orchestrator/pipeline_mt.py      (NF-e: importados + processados)
        └── orchestrator/pipeline_cte_mt.py  (CT-e: processados_cte)
              ├── io/scanner.py
              ├── io/zip_extract.py
              ├── parse/nfe_parser.py  /  parse/cte_parser.py
              ├── transform/filters.py
              ├── transform/window.py
              ├── checkpoint/store_sqlite.py
              └── write/parquet_writer.py  →  write/atomic_commit.py
```

Nenhuma camada acessa diretamente a camada acima dela. O orquestrador é o único lugar que coordena I/O, parsing, checkpoint e escrita.

---

## 2. Parsing de documentos fiscais

### NF-e (`nfe_parser.py`)

O parser suporta três variantes de documento:

**`nfeProc` com `protNFe`** — variante mais comum em produção. A chave `chNFe` é extraída do `protNFe`, garantindo que se trata de uma NF-e autorizada pela SEFAZ.

**`NFe` direto** — documento sem o envelope `nfeProc`. A chave é extraída do atributo `Id` do nó `infNFe` com remoção do prefixo `NFe`. Um warning é registrado no campo `parser_warnings` do record.

**`dEmi` em vez de `dhEmi`** — documentos mais antigos usam o campo de data sem horário. O parser aceita o fallback e registra warning. `ref_aaaamm` é derivado sempre da data de emissão, seja `dhEmi` ou `dEmi`.

Todos os campos monetários são extraídos como `Decimal` (sem arredondamento de float). Itens de produtos, duplicatas e pagamentos são armazenados como listas alinhadas por posição, o que permite join por índice na camada analítica.

### CT-e (`cte_parser.py`)

A função `is_cte_xml(bytes) -> bool` inspeciona a tag raiz do XML antes de qualquer parse completo. Isso permite que o pipeline de CT-e processe a mesma pasta `processados` que o pipeline NF-e, ignorando silenciosamente os documentos que não são CT-e.

---

## 3. Checkpoint e idempotência

O `SQLiteCheckpointStore` usa um padrão de dois modos de leitura:

**Com cache (modo normal de produção):** `load_cache()` é chamado uma vez no início da execução e popula um `set` em memória com todas as chaves já processadas. Durante o processamento paralelo, `was_processed(key)` consulta apenas esse set — nenhuma leitura em disco nas threads.

**Sem cache (fallback):** se `load_cache()` não foi chamado, `was_processed` faz uma query SQLite em tempo real. Útil em testes unitários onde não se quer pré-carregar o banco.

A chave de checkpoint é composta por `(source, source_file_path, source_entry_path, fingerprint)`. O fingerprint é `f"{tamanho}|{mtime_ns}"`. A combinação garante que um arquivo substituído na origem (mesmo nome, conteúdo diferente) seja reprocessado.

**Commit condicional:** o batch de checkpoints só é gravado se `errors == 0` na origem. Se houve qualquer erro de parsing, nenhum arquivo da execução é marcado como processado — todos serão reprocessados na próxima execução. Isso garante exatidão mesmo que o pipeline seja interrompido no meio.

---

## 4. Escrita Parquet

### Buffer e flush intermediário

Registros são acumulados em memória em `buffers[month]`. Quando o buffer atinge `record_chunk_size`, é descarregado em um part file dentro de `staging/parts/{source}/{month}/`. Isso evita picos de memória com volumes grandes.

### Compactação final

Ao final do processamento de uma origem, todos os parts de cada mês são consolidados em um único arquivo via `ParquetWriter`. A escrita ocorre para um `.tmp` em staging e depois `os.replace` o move atomicamente para o diretório de saída final. Os parts são removidos após o replace.

### Schema tipado

O schema Arrow é declarado estaticamente em `get_arrow_schema()` e `get_cte_arrow_schema()`. Isso garante que o tipo de cada coluna seja idêntico entre execuções, mesmo se um mês não tiver registros com certos campos opcionais. Colunas de tipo `list` (itens, duplicatas) são declaradas com o tipo do elemento interno.

---

## 5. Tratamento de erros e exit code

### Arquivos vazios

Antes de chamar qualquer parser, o pipeline lê os bytes do arquivo e verifica `if not xml_bytes.strip()`. Se vazio, registra um `WARNING` com o caminho e retorna `None` — que o orquestrador contabiliza como `skipped`, não como `error`. Isso evita que um arquivo vazio bloqueie o checkpoint de milhares de arquivos válidos.

O mesmo guard existe dentro do loop de extração de ZIPs, verificando cada entrada XML antes de parsear.

### Cadeia de retorno de `bool`

```
_run_source_mt()      ->  bool  (errors > 0)
_run_source_cte()     ->  bool  (errors > 0)
run_cte_mt()          ->  bool  (propaga _run_source_cte)
run_once_mt()         ->  bool  (OR de todas as origens)
cli.main()            ->  sys.exit(1) se True
```

Essa cadeia garante que o agendador externo receba exit code `1` se qualquer origem teve problemas, sem precisar inspecionar logs.

---

## 6. Estratégia de testes

### Filosofia

Os testes cobrem a **lógica de negócio** do pipeline por camada, de forma isolada, rápida e sem dependência de infraestrutura externa (sem S3, sem banco real, sem diretórios de produção). O `cli.py` não é testado diretamente porque não contém lógica própria — é apenas cola entre as camadas.

### Fixtures XML

Os fixtures em `tests/fixtures/` representam os documentos reais do ambiente de produção:

| Fixture | Variante coberta |
|---------|-----------------|
| `nfe_nfeproc.xml` | NF-e com `nfeProc + protNFe`, 2 itens, 2 duplicatas, transportadora |
| `nfe_direta_demi.xml` | NF-e sem `nfeProc`, usando `dEmi` (formato legado) |
| `nfe_campos_opcionais_ausentes.xml` | NF-e sem transportadora, sem cobr, destinatário PF sem CNPJ |
| `cte_cteproc.xml` | CT-e com `cteProc + protCTe`, ICMS00, 2 medidas de carga |
| `zip_com_xmls.zip` | ZIP contendo uma NF-e e um CT-e (gerado por `make_zip.py`) |

### Cobertura por arquivo de teste

**`test_parser.py`** — ~50 casos

Verifica que o parser extrai corretamente os campos críticos de cada variante de XML. Os pontos mais importantes são: extração de `chNFe` via `protNFe` vs. fallback no atributo `Id`; parsing de `dhEmi` com timezone vs. `dEmi` sem horário; `ref_aaaamm` derivado corretamente; alinhamento das listas de itens (todos os arrays `itens_*` devem ter o mesmo tamanho); `Decimal` preservado sem arredondamento; campos opcionais ausentes retornam `None` ou lista vazia, nunca levantam exceção; `is_cte_xml` distingue CT-e de NF-e e retorna `False` para bytes vazios.

**`test_transform.py`** — ~15 casos

Verifica os filtros de janela: `is_year_allowed` com `None`, anos abaixo/acima do mínimo; `last_n_months_yyyymm` com `n=1`, `n=2`, `n=12` e virada de ano (janeiro deve incluir dezembro do ano anterior).

**`test_checkpoint.py`** — ~20 casos

Verifica o ciclo completo do store: criação do SQLite com diretórios pai; `_cache is None` antes de `load_cache`; `was_processed` sem cache (fallback disco) retorna `False` para arquivo novo e `True` após `mark_processed`; fingerprint diferente retorna `False`; source diferente retorna `False`; `load_cache` popula o set em memória; cache não vê inserts posteriores ao `load_cache`; `mark_processed_batch` com lista vazia, com múltiplos, idempotente (INSERT OR REPLACE); `entry_path=None` salvo como string vazia e recuperado corretamente.

**`test_writer.py`** — ~15 casos

Verifica que o arquivo Parquet gerado tem o schema exato de `get_arrow_schema()`; número de linhas correto; valor de `chNFe` preservado como string; `Decimal` de `vBCST` preservado sem perda de precisão; lista de `itens_cProd` preservada; `None` salvo como null no Parquet; nenhum arquivo `.tmp` sobrevive após escrita bem-sucedida; segunda escrita sobrescreve sem erro; `atomic_replace` move o arquivo e remove a origem.

### Como rodar os testes

```bash
# Pré-requisito: gerar o fixture ZIP (uma vez)
python tests/fixtures/make_zip.py

# Todos os testes
pytest tests/ -v

# Com cobertura de código
pytest tests/ -v --cov=src/nfe_parquet --cov-report=term-missing

# Um módulo específico
pytest tests/test_parser.py -v

# Uma classe específica
pytest tests/test_parser.py::TestParseNfeNfeProc -v
```

### Integração dos testes com o `cli.py`

O `cli.py` **não é coberto pelos testes unitários** — isso é intencional. Ele não contém lógica de negócio: carrega config, inicializa logging e repassa o bool de `run_once_mt` para `sys.exit`. Testar isso exigiria subprocesso ou mock de `sys.exit`, com custo de manutenção alto para ganho baixo.

A confiança no exit code vem da **cadeia de retorno `bool`** que é exercida pelos testes de cada camada:

```
test_writer + test_checkpoint   →  _run_source_mt retorna errors > 0 corretamente
                                →  run_once_mt acumula os bools (visível no código)
                                →  cli.py chama sys.exit(1) se True
```

Para equipes que precisam de cobertura formal do `cli.py`, o caminho recomendado é um teste de integração separado com `subprocess.run`, marcado com `@pytest.mark.integration` e executado em ambiente configurado:

```python
# tests/test_integration.py
import subprocess, sys

def test_exit_code_zero_sem_erros(tmp_path, config_valida):
    result = subprocess.run(
        [sys.executable, "-m", "nfe_parquet.cli", str(config_valida)],
        capture_output=True,
    )
    assert result.returncode == 0

def test_exit_code_um_com_xml_corrompido(tmp_path, config_com_xml_corrompido):
    result = subprocess.run(
        [sys.executable, "-m", "nfe_parquet.cli", str(config_com_xml_corrompido)],
        capture_output=True,
    )
    assert result.returncode == 1
```

Esses testes são executados com `pytest tests/ -m integration`, separados da suíte unitária.

---

## 7. Limitações conhecidas e próximos passos

**Race condition em execuções paralelas** — se duas instâncias do pipeline rodarem simultaneamente com overlap de agendamento, ambas vão processar os mesmos arquivos (a cache RAM de cada uma reflete o estado no momento do `load_cache`). O `PRAGMA journal_mode=WAL` protege a escrita concorrente no SQLite, mas não resolve a duplicação lógica. A solução é garantir que apenas uma instância rode por vez via lock de arquivo ou configuração do agendador.

**Validação de diretórios de entrada** — se `input_importados` não existir, o `rglob` lança `OSError` genérico. Uma verificação explícita no início de `_run_source_mt` produziria uma mensagem mais clara no log.

