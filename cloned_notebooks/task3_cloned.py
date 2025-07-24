# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Araria")) 
display(filtered_df)