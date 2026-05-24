# Databricks notebook source
# Este arquivo foi gerado a partir de alpha_vantage.ipynb
# Compatível com Databricks Runtime — 'spark' já existe como variável global

# COMMAND ----------

import os
import requests
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, ArrayType
)
from pyspark.sql.functions import to_timestamp, col, explode

# COMMAND ----------

# Recupera a chave de API do Databricks Secrets
# Pré-requisito: criar o Secret Scope antes de executar
#   databricks secrets create-scope alpha-vantage
#   databricks secrets put-secret alpha-vantage api-key
api_key = dbutils.secrets.get(scope="alpha-vantage", key="api-key")

# COMMAND ----------

url = (
    "https://www.alphavantage.co/query"
    "?function=NEWS_SENTIMENT"
    "&topics=manufacturing,technology"
    f"&apikey={api_key}"
)
r = requests.get(url)
data = r.json()

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

# --- Cria o DataFrame PySpark ---
# No Databricks, 'spark' já é a SparkSession ativa — não é necessário criá-la

df = spark.createDataFrame(rows, schema=schema)

df = df.withColumn(
    "time_published",
    to_timestamp(col("time_published"), "yyyyMMdd'T'HHmmss")
)

print(f"Total de artigos: {df.count()}")
df.printSchema()

# COMMAND ----------

# Visualização dos primeiros registros
display(df)

# COMMAND ----------

# Explode tickers: uma linha por ticker por artigo
df_tickers = df.select(
    "title",
    "time_published",
    "overall_sentiment_label",
    explode("ticker_sentiment").alias("t")
).select(
    "title",
    "time_published",
    "overall_sentiment_label",
    col("t.ticker").alias("ticker"),
    col("t.ticker_sentiment_score").alias("sentiment_score"),
    col("t.ticker_sentiment_label").alias("sentiment_label"),
)

display(df_tickers)

# COMMAND ----------

# Explode topics: uma linha por tópico por artigo
df_topics = df.select(
    "title",
    "overall_sentiment_score",
    explode("topics").alias("tp")
).select(
    "title",
    "overall_sentiment_score",
    col("tp.topic").alias("topic"),
    col("tp.relevance_score").alias("topic_relevance"),
)

display(df_topics)

# COMMAND ----------

# Valores distintos de sentimento
df.select("overall_sentiment_label").distinct().display()
