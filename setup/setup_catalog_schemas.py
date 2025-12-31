# Databricks notebook source
# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists fmcg

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog fmcg

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmcg.gold;
# MAGIC create schema if not exists fmcg.silver;
# MAGIC create schema if not exists fmcg.bronze;

# COMMAND ----------

