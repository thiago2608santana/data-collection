# Este arquivo é um script Python puro para execução no Databricks.
# Compatível com Databricks Runtime e execução local.

import os
import time
import requests
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, FloatType
)
from pyspark.sql.functions import current_timestamp, col, explode
from delta.tables import DeltaTable

# Carrega variáveis do arquivo .env (caso executado localmente)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Inicializa Spark e dbutils
if 'spark' not in globals():
    spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    api_key = dbutils.secrets.get(scope="alpha-vantage", key="api-key")
except Exception:
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

CATALOG    = "datacollection"
SCHEMA     = "alpha_vantage"
TABLE      = "company_overview"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.news_sentiment"

# --- Lê tickers únicos da tabela news_sentiment ---
# ticker_sentiment é um array de structs; explode para obter linhas individuais
tickers_df = (
    spark.table(SOURCE_TABLE)
    .select(explode(col("ticker_sentiment")).alias("ts"))
    .select(col("ts.ticker").alias("ticker"))
    .distinct()
    # Filtra apenas tickers de ações (ex: "AAPL"). Exclui FOREX:USD, CRYPTO:BTC, etc.
    .filter(~col("ticker").contains(":"))
)

tickers = [row["ticker"] for row in tickers_df.collect()]
print(f"Tickers únicos encontrados: {len(tickers)}")

# --- Schema da tabela company_overview ---
schema = StructType([
    StructField("Symbol",                        StringType(), True),
    StructField("AssetType",                     StringType(), True),
    StructField("Name",                          StringType(), True),
    StructField("Description",                   StringType(), True),
    StructField("CIK",                           StringType(), True),
    StructField("Exchange",                      StringType(), True),
    StructField("Currency",                      StringType(), True),
    StructField("Country",                       StringType(), True),
    StructField("Sector",                        StringType(), True),
    StructField("Industry",                      StringType(), True),
    StructField("Address",                       StringType(), True),
    StructField("OfficialSite",                  StringType(), True),
    StructField("FiscalYearEnd",                 StringType(), True),
    StructField("LatestQuarter",                 StringType(), True),
    StructField("MarketCapitalization",          LongType(),   True),
    StructField("EBITDA",                        LongType(),   True),
    StructField("PERatio",                       FloatType(),  True),
    StructField("PEGRatio",                      FloatType(),  True),
    StructField("BookValue",                     FloatType(),  True),
    StructField("DividendPerShare",              FloatType(),  True),
    StructField("DividendYield",                 FloatType(),  True),
    StructField("EPS",                           FloatType(),  True),
    StructField("RevenuePerShareTTM",            FloatType(),  True),
    StructField("ProfitMargin",                  FloatType(),  True),
    StructField("OperatingMarginTTM",            FloatType(),  True),
    StructField("ReturnOnAssetsTTM",             FloatType(),  True),
    StructField("ReturnOnEquityTTM",             FloatType(),  True),
    StructField("RevenueTTM",                    LongType(),   True),
    StructField("GrossProfitTTM",                LongType(),   True),
    StructField("DilutedEPSTTM",                 FloatType(),  True),
    StructField("QuarterlyEarningsGrowthYOY",    FloatType(),  True),
    StructField("QuarterlyRevenueGrowthYOY",     FloatType(),  True),
    StructField("AnalystTargetPrice",            FloatType(),  True),
    StructField("AnalystRatingStrongBuy",        StringType(), True),
    StructField("AnalystRatingBuy",              StringType(), True),
    StructField("AnalystRatingHold",             StringType(), True),
    StructField("AnalystRatingSell",             StringType(), True),
    StructField("AnalystRatingStrongSell",       StringType(), True),
    StructField("TrailingPE",                    FloatType(),  True),
    StructField("ForwardPE",                     FloatType(),  True),
    StructField("PriceToSalesRatioTTM",          FloatType(),  True),
    StructField("PriceToBookRatio",              FloatType(),  True),
    StructField("EVToRevenue",                   FloatType(),  True),
    StructField("EVToEBITDA",                    FloatType(),  True),
    StructField("Beta",                          FloatType(),  True),
    StructField("WeekHigh52",                    FloatType(),  True),
    StructField("WeekLow52",                     FloatType(),  True),
    StructField("MovingAverage50Day",            FloatType(),  True),
    StructField("MovingAverage200Day",           FloatType(),  True),
    StructField("SharesOutstanding",             LongType(),   True),
    StructField("DividendDate",                  StringType(), True),
    StructField("ExDividendDate",                StringType(), True),
])


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_long(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def fetch_overview(ticker: str, api_key: str) -> dict | None:
    url = (
        "https://www.alphavantage.co/query"
        f"?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    # API retorna {} ou {"Information": "..."} quando o ticker não é encontrado
    # ou quando o limite de requisições foi atingido
    if not data.get("Symbol"):
        print(f"  Sem dados para {ticker}: {data.get('Information') or data.get('Note') or 'resposta vazia'}")
        return None
    return data


# --- Chama a API para cada ticker e monta as linhas ---
rows = []
for i, ticker in enumerate(tickers):
    print(f"[{i + 1}/{len(tickers)}] Buscando overview de {ticker}...")
    data = fetch_overview(ticker, api_key)
    if data is None:
        continue

    rows.append(Row(
        Symbol=data.get("Symbol"),
        AssetType=data.get("AssetType"),
        Name=data.get("Name"),
        Description=data.get("Description"),
        CIK=data.get("CIK"),
        Exchange=data.get("Exchange"),
        Currency=data.get("Currency"),
        Country=data.get("Country"),
        Sector=data.get("Sector"),
        Industry=data.get("Industry"),
        Address=data.get("Address"),
        OfficialSite=data.get("OfficialSite"),
        FiscalYearEnd=data.get("FiscalYearEnd"),
        LatestQuarter=data.get("LatestQuarter"),
        MarketCapitalization=_safe_long(data.get("MarketCapitalization")),
        EBITDA=_safe_long(data.get("EBITDA")),
        PERatio=_safe_float(data.get("PERatio")),
        PEGRatio=_safe_float(data.get("PEGRatio")),
        BookValue=_safe_float(data.get("BookValue")),
        DividendPerShare=_safe_float(data.get("DividendPerShare")),
        DividendYield=_safe_float(data.get("DividendYield")),
        EPS=_safe_float(data.get("EPS")),
        RevenuePerShareTTM=_safe_float(data.get("RevenuePerShareTTM")),
        ProfitMargin=_safe_float(data.get("ProfitMargin")),
        OperatingMarginTTM=_safe_float(data.get("OperatingMarginTTM")),
        ReturnOnAssetsTTM=_safe_float(data.get("ReturnOnAssetsTTM")),
        ReturnOnEquityTTM=_safe_float(data.get("ReturnOnEquityTTM")),
        RevenueTTM=_safe_long(data.get("RevenueTTM")),
        GrossProfitTTM=_safe_long(data.get("GrossProfitTTM")),
        DilutedEPSTTM=_safe_float(data.get("DilutedEPSTTM")),
        QuarterlyEarningsGrowthYOY=_safe_float(data.get("QuarterlyEarningsGrowthYOY")),
        QuarterlyRevenueGrowthYOY=_safe_float(data.get("QuarterlyRevenueGrowthYOY")),
        AnalystTargetPrice=_safe_float(data.get("AnalystTargetPrice")),
        AnalystRatingStrongBuy=data.get("AnalystRatingStrongBuy"),
        AnalystRatingBuy=data.get("AnalystRatingBuy"),
        AnalystRatingHold=data.get("AnalystRatingHold"),
        AnalystRatingSell=data.get("AnalystRatingSell"),
        AnalystRatingStrongSell=data.get("AnalystRatingStrongSell"),
        TrailingPE=_safe_float(data.get("TrailingPE")),
        ForwardPE=_safe_float(data.get("ForwardPE")),
        PriceToSalesRatioTTM=_safe_float(data.get("PriceToSalesRatioTTM")),
        PriceToBookRatio=_safe_float(data.get("PriceToBookRatio")),
        EVToRevenue=_safe_float(data.get("EVToRevenue")),
        EVToEBITDA=_safe_float(data.get("EVToEBITDA")),
        Beta=_safe_float(data.get("Beta")),
        WeekHigh52=_safe_float(data.get("52WeekHigh")),
        WeekLow52=_safe_float(data.get("52WeekLow")),
        MovingAverage50Day=_safe_float(data.get("50DayMovingAverage")),
        MovingAverage200Day=_safe_float(data.get("200DayMovingAverage")),
        SharesOutstanding=_safe_long(data.get("SharesOutstanding")),
        DividendDate=data.get("DividendDate"),
        ExDividendDate=data.get("ExDividendDate"),
    ))

    # Plano gratuito: 25 req/dia e ~75 req/min. Aguarda 1 s entre chamadas.
    time.sleep(1)

print(f"\nEmpresas com dados retornados: {len(rows)}")

if not rows:
    print("Nenhum dado retornado pela API. Encerrando sem gravar.")
    raise SystemExit(0)

# --- Cria o DataFrame PySpark ---
df = spark.createDataFrame(rows, schema=schema)
df = df.withColumn("ingestion_ts", current_timestamp())

print(f"Total de registros: {df.count()}")
df.printSchema()

# --- Persiste no Unity Catalog via upsert (chave: Symbol) ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

if spark.catalog.tableExists(FULL_TABLE):
    delta_table = DeltaTable.forName(spark, FULL_TABLE)
    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.Symbol = source.Symbol"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Upsert concluído em {FULL_TABLE}")
else:
    (
        df.write
        .format("delta")
        .saveAsTable(FULL_TABLE)
    )
    print(f"✅ Tabela criada e dados inseridos em {FULL_TABLE}")
