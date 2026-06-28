# Dicionário de Dados — `company_overview`

**Tabela:** `datacollection.alpha_vantage.company_overview`  
**Fonte:** [Alpha Vantage — OVERVIEW](https://www.alphavantage.co/documentation/#company-overview)  
**Chave de upsert:** `Symbol`  
**Script de ingestão:** [`src/company_overview.py`](../src/company_overview.py)

---

## Identificação da empresa

| Coluna | Tipo | Descrição |
|---|---|---|
| `Symbol` | String | Ticker da ação na bolsa (ex: `AAPL`) |
| `AssetType` | String | Tipo de ativo (ex: `Common Stock`) |
| `Name` | String | Nome completo da empresa |
| `Description` | String | Descrição do negócio da empresa |
| `CIK` | String | Código CIK da SEC (regulador americano) |
| `Exchange` | String | Bolsa de valores onde o ativo é negociado (ex: `NYSE`, `NASDAQ`) |
| `Currency` | String | Moeda das cotações (ex: `USD`) |
| `Country` | String | País de domicílio da empresa |
| `Sector` | String | Setor econômico (ex: `Technology`) |
| `Industry` | String | Subsetor/indústria (ex: `Semiconductors`) |
| `Address` | String | Endereço sede da empresa |
| `OfficialSite` | String | URL do site oficial |

## Calendário financeiro

| Coluna | Tipo | Descrição |
|---|---|---|
| `FiscalYearEnd` | String | Mês de encerramento do ano fiscal (ex: `December`) |
| `LatestQuarter` | String | Data do trimestre mais recente reportado (ex: `2024-09-30`) |
| `DividendDate` | String | Data do próximo pagamento de dividendo |
| `ExDividendDate` | String | Data ex-dividendo (prazo para ter direito ao dividendo) |

## Avaliação (valuation)

| Coluna | Tipo | Descrição |
|---|---|---|
| `MarketCapitalization` | Long | Capitalização de mercado em USD |
| `EBITDA` | Long | EBITDA em USD |
| `PERatio` | Float | P/L — Preço sobre Lucro |
| `PEGRatio` | Float | PEG — P/L ajustado pelo crescimento |
| `BookValue` | Float | Valor patrimonial por ação |
| `PriceToBookRatio` | Float | P/VPA — Preço sobre Valor Patrimonial |
| `PriceToSalesRatioTTM` | Float | P/S — Preço sobre Receita (últimos 12 meses) |
| `EVToRevenue` | Float | EV/Receita — Enterprise Value sobre Receita |
| `EVToEBITDA` | Float | EV/EBITDA — Enterprise Value sobre EBITDA |
| `TrailingPE` | Float | P/L trailing (baseado no lucro dos últimos 12 meses) |
| `ForwardPE` | Float | P/L forward (baseado na estimativa de lucro futuro) |
| `Beta` | Float | Beta da ação (volatilidade relativa ao índice de mercado) |
| `AnalystTargetPrice` | Float | Preço-alvo médio dos analistas |

## Dividendos e lucro por ação

| Coluna | Tipo | Descrição |
|---|---|---|
| `EPS` | Float | Lucro por ação (EPS) |
| `DilutedEPSTTM` | Float | EPS diluído (últimos 12 meses) |
| `DividendPerShare` | Float | Dividendo por ação |
| `DividendYield` | Float | Dividend yield (ex: `0.005` = 0,5%) |

## Receita e rentabilidade (TTM = últimos 12 meses)

| Coluna | Tipo | Descrição |
|---|---|---|
| `RevenueTTM` | Long | Receita total nos últimos 12 meses (USD) |
| `GrossProfitTTM` | Long | Lucro bruto nos últimos 12 meses (USD) |
| `RevenuePerShareTTM` | Float | Receita por ação (últimos 12 meses) |
| `ProfitMargin` | Float | Margem líquida (ex: `0.25` = 25%) |
| `OperatingMarginTTM` | Float | Margem operacional (últimos 12 meses) |
| `ReturnOnAssetsTTM` | Float | ROA — Retorno sobre ativos (últimos 12 meses) |
| `ReturnOnEquityTTM` | Float | ROE — Retorno sobre patrimônio (últimos 12 meses) |

## Crescimento

| Coluna | Tipo | Descrição |
|---|---|---|
| `QuarterlyEarningsGrowthYOY` | Float | Crescimento do lucro no trimestre vs. mesmo trimestre do ano anterior |
| `QuarterlyRevenueGrowthYOY` | Float | Crescimento da receita no trimestre vs. mesmo trimestre do ano anterior |

## Indicadores técnicos e ações

| Coluna | Tipo | Descrição |
|---|---|---|
| `WeekHigh52` | Float | Máxima dos últimos 52 semanas (campo original da API: `52WeekHigh`) |
| `WeekLow52` | Float | Mínima dos últimos 52 semanas (campo original da API: `52WeekLow`) |
| `MovingAverage50Day` | Float | Média móvel de 50 dias (campo original: `50DayMovingAverage`) |
| `MovingAverage200Day` | Float | Média móvel de 200 dias (campo original: `200DayMovingAverage`) |
| `SharesOutstanding` | Long | Quantidade de ações em circulação |

## Recomendações de analistas

| Coluna | Tipo | Descrição |
|---|---|---|
| `AnalystRatingStrongBuy` | String | Nº de analistas com recomendação "Compra Forte" |
| `AnalystRatingBuy` | String | Nº de analistas com recomendação "Compra" |
| `AnalystRatingHold` | String | Nº de analistas com recomendação "Neutro" |
| `AnalystRatingSell` | String | Nº de analistas com recomendação "Venda" |
| `AnalystRatingStrongSell` | String | Nº de analistas com recomendação "Venda Forte" |

## Metadado de ingestão

| Coluna | Tipo | Descrição |
|---|---|---|
| `ingestion_ts` | Timestamp | Timestamp de quando o registro foi ingerido no pipeline |

---

## Notas

- Campos com nomes inválidos no Spark foram renomeados: `52WeekHigh` → `WeekHigh52`, `52WeekLow` → `WeekLow52`, `50DayMovingAverage` → `MovingAverage50Day`, `200DayMovingAverage` → `MovingAverage200Day`.
- Valores `"None"` ou inválidos retornados pela API são convertidos para `null` via `_safe_float` / `_safe_long`.
- As colunas de ratings de analistas chegam como `String` da API (contagem inteira em texto).
