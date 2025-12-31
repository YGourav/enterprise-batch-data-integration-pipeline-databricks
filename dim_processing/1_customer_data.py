# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/FMCG_pipeline_Project/1_setup/utilities

# COMMAND ----------

print (bronze_schema)
print (silver_schema)
print (gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://fmcgproject-childcompany-data/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(base_path)
                .withColumn("read_timestamp", F.current_timestamp())
                .select("*", "_metadata.file_name", "_metadata.file_size")
                )

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
.mode("overwrite")\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f" SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter("count > 1")
display(df_duplicates)

# COMMAND ----------

print('Rows before dropping duplicates: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after dropping duplicates: ', df_silver.count())

# COMMAND ----------

##Check rows which have leading spaces

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)


# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name", F.trim(F.col("customer_name"))
)

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)


# COMMAND ----------

## Seeing list of distinct cities
df_silver.select('city').distinct().show()

# COMMAND ----------

# replace typos with correct names

city_mapping = {
    'Bengaluruu' : 'Bengaluru',
    'Bengalore' : 'Bengaluru',
    'Benglore' : 'Bengaluru',

    'Hyedarabadd': 'Hyderabad',
    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'

}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver.replace(city_mapping, subset=["city"])
    .withColumn("city", 
                F.when(F.col("city").isNull(), None)
                .when(F.col("city").isin(allowed), F.col("city"))
                .otherwise(F.col("city"))
))

# COMMAND ----------

##Check for distinct city names now
df_silver.select('city').distinct().show()

# COMMAND ----------

##Check for cutomer_name

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

#fix the title case issue
df_silver = df_silver.withColumn(
    "customer_name", 
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

#Check for customer_name after fixing
df_silver.select("customer_name").distinct().show()

# COMMAND ----------

#check for city null
df_silver.filter(F.col("city").isNull()).show(truncate = False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate = False)

# COMMAND ----------

#After confirmation from business head, we can correct up the cities

customer_city_fix = {
    789403: "New Delhi",
    789420: "Bengaluru",
    789521: "Hyderabad",
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

# COMMAND ----------

df_fix.display()

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")
    )
    .drop("fixed_city")
)

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))

print(df_silver.printSchema())

# COMMAND ----------

##Making this child company data similar to parent company table

df_silver = (
    df_silver
    .withColumn(
        "customer",
        F.concat_ws("-","customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
)

#Static attributes aligned with parent data model
.withColumn("market", F.lit("India"))
.withColumn("platform", F.lit("Sports Bar"))
.withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write\
.mode("overwrite")\
.format("delta")\
.option("delta.enableChangeDataFeed", "true")\
.option("mergeSchema", "true")\
.saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold Processing
# MAGIC

# COMMAND ----------

df_silver = spark.sql(f" SELECT * FROM {catalog}.{silver_schema}.{data_source}")

#For gold take only required columns
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

display(df_gold)

# COMMAND ----------

df_gold.write\
    .mode("overwrite")\
    .option("delta.enableChangeDataFeed", "true")\
    .format("delta")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merging the child company gold table with parent company's gold table

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_{data_source}")

df_child_customers = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

delta_table.toDF().count()

# COMMAND ----------

