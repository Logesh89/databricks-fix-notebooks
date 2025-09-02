# Databricks notebook source

# Load the existing data from the table
df_existing_data = spark.table("custoomer_data.table.historical_data")

# Create a DataFrame for the new data
new_data = [(4, 'David', 400.5, 'something'),
            (5, 'Eve', 500.75, 'another'),
            (6, 'Frank', 600.1, 'test')]

columns = ['id', 'name', 'amount', 'extra_col']
df_new_data_increment = spark.createDataFrame(new_data, columns)

# Ensure schema consistency by casting amount column to double
from pyspark.sql.types import DoubleType
df_new_data_increment = df_new_data_increment.withColumn("amount", df_new_data_increment["amount"].cast(DoubleType()))

# Align the schema of the new data with the existing data
from pyspark.sql import functions as F
existing_columns = set(df_existing_data.columns)
new_columns = set(df_new_data_increment.columns)
common_columns = existing_columns.intersection(new_columns)
df_new_data_increment = df_new_data_increment.select([F.col(c) for c in common_columns])

# Union the new data with the existing data
df_combined_data = df_existing_data.unionByName(df_new_data_increment, allowMissingColumns=True)

# Write the combined data back to the table
df_combined_data.write.mode("overwrite").saveAsTable("custoomer_data.table.historical_data")

# Display the updated table
display(spark.table("custoomer_data.table.historical_data"))