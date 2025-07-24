# Databricks notebook source
df = spark.table("model.modelllm.weather_data_jun_12_2022")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Araria")) # corrected this line
display(filtered_df) # also corrected this line (filter_df -> filtered_df)