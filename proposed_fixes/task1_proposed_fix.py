# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2025")
display(df)

# COMMAND ----------

df_weather_data_jun_12_2025 = spark.table("model.modelllm.weather_data_jun_12_2025")