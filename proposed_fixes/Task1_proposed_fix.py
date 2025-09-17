# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jul_10_2025")
display(df)

# COMMAND ----------

df = spark.table("model.modelllm.weather_data_jul_10_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Aurangabad"))
display(filtered_df)

# COMMAND ----------

df = spark.table("model.modelllm.weather_data_jul_10_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Arwal"))
display(filtered_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, district, temperatures_min, temperatures_max
# MAGIC FROM model.modelllm.weather_data_jul_10_2025
# MAGIC LIMIT 10;

# COMMAND ----------

df_araria = df.filter((df.state == "Bihar") & (df.district == "Araria"))
display(df_araria.select("date", "temperatures_min", "temperatures_max", "precipitation_amount"))

# COMMAND ----------

from pyspark.sql import functions as F

df_agg = (
    df.groupBy("district")
      .agg(
          F.avg("temperatures_min").alias("avg_min_temp"),
          F.avg("temperatures_max").alias("avg_max_temp"),
          F.sum("precipitation_amount").alias("total_rain")
      )
      .orderBy(F.col("avg_max_temp").desc())
)

display(df_agg)

# COMMAND ----------