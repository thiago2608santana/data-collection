# CLAUDE.md — Contexto do projeto data-collection

## O que é esse projeto

Pipeline de coleta de dados financeiros via API Alpha Vantage [link](https://www.alphavantage.co/documentation/), com persistência no Unity Catalog do Databricks. Orquestrado como Databricks Asset Bundle (DAB), com deploy automático via GitHub Actions ao fazer merge na `main`.

## Estrutura

```
src/
  alpha_vantage.py       # Task 1: ingestão de notícias com sentimento (NEWS_SENTIMENT)
  company_overview.py    # Task 2: dados fundamentalistas por ticker (OVERVIEW)
databricks.yml           # Definição do bundle (jobs, tasks, schedule, ambiente)
pyproject.toml           # Dependências gerenciadas com uv
.env                     # Variáveis locais — nunca versionar
```

## Databricks

- **Workspace:** `https://dbc-cddd3ed8-7fec.cloud.databricks.com`
- **Catálogo:** `datacollection`
- **Schema:** `alpha_vantage`
- **Tabelas:**
  - `datacollection.alpha_vantage.news_sentiment` — artigos com sentimento por ticker
  - `datacollection.alpha_vantage.company_overview` — dados fundamentalistas por empresa
- **Secret scope:** `alpha-vantage` / chave `api-key` (Alpha Vantage API key)
- **Schedule:** diário às 08:00 (America/Sao_Paulo)
- **Compute:** Serverless (environment_version: "2", sem dependências declaradas no bundle)

## Dependência entre tasks

```
ingestao_alpha_vantage → ingestao_company_overview
```

`company_overview.py` lê os tickers únicos de `news_sentiment`, então sempre roda depois.

## Padrão de upsert

Todas as tabelas usam Delta MERGE:
- `news_sentiment`: chave `url`
- `company_overview`: chave `Symbol`

Na primeira execução (tabela inexistente) → `df.write.saveAsTable`. Nas seguintes → `DeltaTable.merge`.

## Ambiente local

```bash
uv sync          # instala dependências
```

A API key deve estar em `.env`:
```
ALPHA_VANTAGE_API_KEY=sua_chave_aqui
```

Os scripts detectam se estão rodando localmente (sem `dbutils`) e fazem fallback para `os.getenv`.

## Alpha Vantage — limites da API (plano gratuito)

- 25 requisições/dia
- ~75 requisições/minuto
- `company_overview.py` aplica `time.sleep(1)` entre chamadas
- Se o número de tickers únicos ultrapassar 25, as requisições excedentes são ignoradas silenciosamente pela API

## Convenções

- Scripts Python puros (não notebooks) para rodar no Databricks como `spark_python_task`
- Sem comentários óbvios — apenas comentários que explicam decisões não evidentes
- Campos numéricos da API (que chegam como string) são convertidos via `_safe_float` / `_safe_long`
- Nomes de coluna com caracteres inválidos no Spark (`52WeekHigh`) são renomeados (`WeekHigh52`)
- Commits em português, mensagem no estilo convencional (`feat:`, `fix:`, `refactor:`)
