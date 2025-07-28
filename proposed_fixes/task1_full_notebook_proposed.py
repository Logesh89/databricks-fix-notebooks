# Databricks notebook source
spark = spark # Corrected variable name to'spark' 
df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Arwal")) # Changed "Aurangabad" to "Arwal"
display(filtered_df) # Corrected variable name to 'filtered_df'