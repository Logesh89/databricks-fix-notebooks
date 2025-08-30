# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Aurangabad")) 
display(filtered_df)



# COMMAND ----------

df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Arwal")) 
display(filtered_df)