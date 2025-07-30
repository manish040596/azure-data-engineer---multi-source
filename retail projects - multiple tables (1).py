# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://retail@retailproject.blob.core.windows.net",
  mount_point = "/mnt/retail_project",
  extra_configs = {"fs.azure.account.key.retailproject.blob.core.windows.net":"secret access key"})



# COMMAND ----------

dbutils.fs.ls('/mnt/retail_project/bronze/transaction/')

# COMMAND ----------

# DBTITLE 1,read the bronze layer
# Read raw data from Bronze layer
df_transactions = spark.read.parquet('/mnt/retail_project/bronze/transaction/')
df_products = spark.read.parquet('/mnt/retail_project/bronze/product/')
df_stores = spark.read.parquet('/mnt/retail_project/bronze/store/')

df_customers = spark.read.parquet('/mnt/retail_project/bronze/customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/')
display(df_customers)




# COMMAND ----------

display(df_transactions)

# COMMAND ----------

# DBTITLE 1,create silver layer - data cleaning
from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------

# DBTITLE 1,join all data together
# Join all data
df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))


# COMMAND ----------

display(df_silver)

# COMMAND ----------

# DBTITLE 1,dump to adls location
silver_path = "/mnt/retail_project/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)


# COMMAND ----------

# DBTITLE 1,create silver dataset
spark.sql(f"""
CREATE TABLE retail_silver_cleaned
USING DELTA
LOCATION '/mnt/retail_project/silver/'
""")


# COMMAND ----------

# MAGIC %sql select * from retail_silver_cleaned

# COMMAND ----------

# DBTITLE 1,gold layer
# Load cleaned transactions from Silver layer
silver_df = spark.read.format("delta").load("/mnt/retail_project/silver/")


# COMMAND ----------

display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)


# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_path = "/mnt/retail_project/gold/"

gold_df.write.mode("overwrite").format("delta").save(gold_path)


# COMMAND ----------

spark.sql("""
CREATE TABLE retail_gold_sales_summary
USING DELTA
LOCATION '/mnt/retail_project/gold/' """)


# COMMAND ----------

# MAGIC %sql select * from retail_gold_sales_summary

# COMMAND ----------

