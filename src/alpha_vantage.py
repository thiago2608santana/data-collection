# Este arquivo é um script Python puro para execução no Databricks.
# Compatível com Databricks Runtime e execução local.

import os
import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, ArrayType
)
from pyspark.sql.functions import to_timestamp, col, explode
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
    # Fallback para desenvolvimento local usando arquivo .env
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")



# Início de ontem em UTC no formato exigido pela API: YYYYMMDDTHHMM
yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
time_from = yesterday.strftime("%Y%m%dT%H%M")   # ex: 20260523T0000

url = (
    "https://www.alphavantage.co/query"
    "?function=NEWS_SENTIMENT"
    "&topics=manufacturing,technology"
    "&limit=1000"
    f"&time_from={time_from}"
    f"&apikey={api_key}"
)
r = requests.get(url)
data = r.json()



# --- Sub-schemas ---

topic_schema = StructType([
    StructField("topic",           StringType(), True),
    StructField("relevance_score", FloatType(),  True),
])

ticker_schema = StructType([
    StructField("ticker",                 StringType(), True),
    StructField("relevance_score",        FloatType(),  True),
    StructField("ticker_sentiment_score", FloatType(),  True),
    StructField("ticker_sentiment_label", StringType(), True),
])

# --- Schema principal ---

schema = StructType([
    StructField("title",                   StringType(),             True),
    StructField("url",                     StringType(),             True),
    StructField("time_published",          StringType(),             True),
    StructField("authors",                 StringType(),             True),
    StructField("source",                  StringType(),             True),
    StructField("source_domain",           StringType(),             True),
    StructField("summary",                 StringType(),             True),
    StructField("overall_sentiment_score", FloatType(),              True),
    StructField("overall_sentiment_label", StringType(),             True),
    StructField("topics",                  ArrayType(topic_schema),  True),
    StructField("ticker_sentiment",        ArrayType(ticker_schema), True),
])



# --- Monta as linhas a partir do JSON da API ---

feed = data.get("feed", [])

rows = []
for article in feed:
    topics = [
        Row(
            topic=t.get("topic"),
            relevance_score=float(t.get("relevance_score", 0.0)),
        )
        for t in article.get("topics", [])
    ]

    tickers = [
        Row(
            ticker=t.get("ticker"),
            relevance_score=float(t.get("relevance_score", 0.0)),
            ticker_sentiment_score=float(t.get("ticker_sentiment_score", 0.0)),
            ticker_sentiment_label=t.get("ticker_sentiment_label"),
        )
        for t in article.get("ticker_sentiment", [])
    ]

    rows.append((
        article.get("title"),
        article.get("url"),
        article.get("time_published"),
        ", ".join(article.get("authors", [])),
        article.get("source"),
        article.get("source_domain"),
        article.get("summary"),
        article.get("overall_sentiment_score"),
        article.get("overall_sentiment_label"),
        topics,
        tickers,
    ))



# --- Cria o DataFrame PySpark ---
# No Databricks, 'spark' já é a SparkSession ativa — não é necessário criá-la

from pyspark.sql.functions import current_timestamp

df = spark.createDataFrame(rows, schema=schema)

df = (
    df
    .withColumn("time_published", to_timestamp(col("time_published"), "yyyyMMdd'T'HHmmss"))
    .withColumn("ingestion_ts", current_timestamp())   # auditoria: quando foi ingerido
)

print(f"Total de artigos: {df.count()}")
df.printSchema()



# --- Persiste no Unity Catalog via upsert ---
# Chave de negócio: url (cada artigo tem URL única)
# - Se o artigo JÁ existe na tabela → ignora (whenNotMatchedInsertAll)
# - Se é NOVO → insere
# Na primeira execução (tabela ainda não existe) → cria diretamente

CATALOG    = "datacollection"
SCHEMA     = "alpha_vantage"
TABLE      = "news_sentiment"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# Garante que o schema existe antes de escrever
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

if spark.catalog.tableExists(FULL_TABLE):
    # Tabela já existe — faz MERGE pela url
    delta_table = DeltaTable.forName(spark, FULL_TABLE)
    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            "target.url = source.url"       # chave de negócio
        )
        .whenMatchedUpdateAll()             # atualiza registros existentes em caso de recálculo no servidor
        .whenNotMatchedInsertAll()          # insere apenas artigos novos
        .execute()
    )
    print(f"✅ Upsert concluído em {FULL_TABLE}")
else:
    # Primeira execução — cria a tabela Delta
    (
        df.write
        .format("delta")
        .saveAsTable(FULL_TABLE)
    )
    print(f"✅ Tabela criada e dados inseridos em {FULL_TABLE}")
