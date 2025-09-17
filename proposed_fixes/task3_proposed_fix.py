# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Aurangabad")) 
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Min/Max Temperature Difference

# COMMAND ----------

from pyspark.sql import functions as F

df_with_diff = df.withColumn(
    "temp_diff", 
    F.col("temperatures_max") - F.col("temperatures_min")
)

display(df_with_diff.select("date", "district", "temperatures_min", "temperatures_max", "temp_diff"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hottest Districts (by Max Temp)

# COMMAND ----------

df_hottest = (
    df.groupBy("district")
      .agg(F.max("temperatures_max").alias("hottest_temp"))
      .orderBy(F.col("hottest_temp").desc())
      .limit(5)
)

display(df_hottest)