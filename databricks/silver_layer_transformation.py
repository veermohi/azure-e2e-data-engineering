# Databricks notebook source
# ---------------------------------------------------------------------------
# Silver Layer Transformation
# Reads raw (Bronze) AdventureWorks CSV data from ADLS Gen2, cleans and
# transforms it with PySpark, and writes the results back to the Silver
# container in Delta/Parquet format.
# ---------------------------------------------------------------------------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
# Data access using a Service Principal (SPN)
# Replace the placeholders below with your own values, or better, pull them
# from Azure Key Vault via a Databricks secret scope instead of hardcoding.
# ---------------------------------------------------------------------------
spark.conf.set("fs.azure.account.auth.type.<datalake>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<datalake>.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<datalake>.dfs.core.windows.net", "<client_id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<datalake>.dfs.core.windows.net", "<client_secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<datalake>.dfs.core.windows.net",
                "https://login.microsoftonline.com/<tenant_id>/oauth2/token")

# ---------------------------------------------------------------------------
# Data Loading (Bronze -> DataFrames)
# ---------------------------------------------------------------------------
df_cal = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Calendar")

df_cus = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Customers")

df_procat = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Product_Categories")

df_pro = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Products")

df_ret = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Returns")

df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Sales*")

df_ter = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/AdventureWorks_Territories")

df_subcat = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@<datalake>.dfs.core.windows.net/Product_Subcategories")

# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

# --- Calendar: derive Month / Year columns ---
df_cal = df_cal.withColumn('Month', month(col('Date'))) \
               .withColumn('Year', year(col('Date')))

df_cal.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Calendar") \
    .save()

# --- Customers: build a single fullName column ---
df_cus = df_cus.withColumn('fullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

df_cus.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Customers") \
    .save()

# --- Product Subcategories: pass-through to Silver ---
df_subcat.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_SubCategories") \
    .save()

# --- Products: clean SKU and ProductName ---
df_pro = df_pro.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0]) \
               .withColumn('ProductName', split(col('ProductName'), ' ')[0])

df_pro.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Products") \
    .save()

# --- Returns: pass-through to Silver ---
df_ret.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Returns") \
    .save()

# --- Territories: pass-through to Silver ---
df_ter.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Territories") \
    .save()

# --- Sales: type-cast StockDate, normalize OrderNumber, derive line total ---
df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))
df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))
df_sales = df_sales.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# Quick sanity check: total orders per date
df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

df_sales.write.format('parquet') \
    .mode('append') \
    .option("path", "abfss://silver@<datalake>.dfs.core.windows.net/AdventureWorks_Sales") \
    .save()
