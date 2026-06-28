# Dicionário de Dados — `news_sentiment`

**Tabela:** `datacollection.alpha_vantage.news_sentiment`  
**Fonte:** [Alpha Vantage — NEWS_SENTIMENT](https://www.alphavantage.co/documentation/#news-sentiment)  
**Chave de upsert:** `url`  
**Script de ingestão:** [`src/alpha_vantage.py`](../src/alpha_vantage.py)

---

## Colunas principais

| Coluna | Tipo | Descrição |
|---|---|---|
| `title` | String | Título do artigo |
| `url` | String | URL canônica do artigo — chave de upsert |
| `time_published` | Timestamp | Data e hora de publicação (convertida de `yyyyMMddTHHmmss` para Timestamp) |
| `authors` | String | Autores do artigo, concatenados por vírgula |
| `source` | String | Nome da fonte (ex: `Benzinga`) |
| `source_domain` | String | Domínio da fonte (ex: `www.benzinga.com`) |
| `summary` | String | Resumo do artigo |
| `overall_sentiment_score` | Float | Score de sentimento geral do artigo (de `-1.0` a `1.0`) |
| `overall_sentiment_label` | String | Rótulo do sentimento geral (ver tabela abaixo) |

## Coluna aninhada — `topics`

Array de structs. Cada elemento representa um tema identificado no artigo.

| Campo | Tipo | Descrição |
|---|---|---|
| `topic` | String | Nome do tema (ex: `Technology`, `Manufacturing`) |
| `relevance_score` | Float | Relevância do tema no artigo (de `0.0` a `1.0`) |

## Coluna aninhada — `ticker_sentiment`

Array de structs. Cada elemento representa um ticker mencionado no artigo.

| Campo | Tipo | Descrição |
|---|---|---|
| `ticker` | String | Ticker do ativo (ex: `AAPL`). Pode conter prefixos como `FOREX:USD` ou `CRYPTO:BTC` |
| `relevance_score` | Float | Relevância do ticker no artigo (de `0.0` a `1.0`) |
| `ticker_sentiment_score` | Float | Score de sentimento específico para o ticker (de `-1.0` a `1.0`) |
| `ticker_sentiment_label` | String | Rótulo do sentimento para o ticker (ver tabela abaixo) |

## Metadado de ingestão

| Coluna | Tipo | Descrição |
|---|---|---|
| `ingestion_ts` | Timestamp | Timestamp de quando o registro foi ingerido no pipeline |

---

## Referência de rótulos de sentimento

| Label | Score aproximado |
|---|---|
| `Bearish` | ≤ −0.35 |
| `Somewhat-Bearish` | −0.35 a −0.15 |
| `Neutral` | −0.15 a 0.15 |
| `Somewhat-Bullish` | 0.15 a 0.35 |
| `Bullish` | ≥ 0.35 |

---

## Notas

- O pipeline coleta apenas notícias do **dia anterior** (UTC), filtradas pelos tópicos `manufacturing` e `technology`.
- O campo `authors` é uma string com os nomes separados por vírgula (a API retorna um array).
- Tickers com prefixo (ex: `FOREX:USD`, `CRYPTO:BTC`) são excluídos na task `company_overview`, que lê desta tabela.
- O upsert usa `whenMatchedUpdateAll`, permitindo que scores de sentimento recalculados pelo servidor sejam atualizados.
