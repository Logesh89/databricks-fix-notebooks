# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Araria"))  # Fix: changed "====" to "=="
display(filtered_df)  # Fix: Renamed "filter_df" to "filtered_df" to match assignment