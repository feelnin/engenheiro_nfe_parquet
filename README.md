# NF-e Parquet — ETL Python (XML/ZIP → Parquet mensal)

Esqueleto do projeto para processar NF-e em XML (incluindo XMLs em ZIP) e gerar 1 Parquet por mês (AAAAMM), por origem, com idempotência (checkpoint) e observabilidade (logs JSON).

## Regras principais (resumo)
- Origens:
  - `C:\XML_SICOF\processados_importados`
  - `C:\XML_SICOF\processados_XML`
- Nunca alterar/excluir arquivos de origem.
- ZIP: extrair em temporário local no C: e limpar ao final.
- Filtrar por emissão: `ide/dhEmi` ou `ide/dEmi` com ano >= 2026.
- Saída:
  - `L:\Arquivos carga BI\PY\PARQUET_NFE\importados\AAAAMM.parquet`
  - `L:\Arquivos carga BI\PY\PARQUET_NFE\processados\AAAAMM.parquet`
- Opção A: 1 linha por NF-e; campos de itens/duplicatas/pagamentos em arrays.
- Reprocessar sempre janela móvel de 2 meses (reconciliação).

## Rodando (placeholder)
- Instalar deps: `pip install -e .`
- Ajuda da CLI: `python -m nfe_parquet.cli --help`
- Rodar MVP local (a ser implementado): `python -m nfe_parquet.cli run-mvp --input-dir C:\... --output-dir C:\...`

## Estrutura (camadas)
- config, io, parse, transform, schema, write, checkpoint, observability, orchestrator, cli