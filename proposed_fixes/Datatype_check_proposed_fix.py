# Databricks notebook source
# Load the CSV file into a DataFrame
df_new_data = spark.read.csv("/FileStore/tables/historical_data_50.csv", header=True, inferSchema=True)

# Load the existing data from the table
df_existing_data = spark.table("custoomer_data.table.historical_data")

# Union the new data with the existing data
df_combined_data = df_existing_data.union(df_new_data)

# Write the combined data back to the table
df_combined_data.write.mode("overwrite").saveAsTable("custoomer_data.table.historical_data")

# Display the updated table
display(spark.table("custoomer_data.table.historical_data"))