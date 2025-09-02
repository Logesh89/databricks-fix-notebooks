# Create a DataFrame for the new data
new_data = [(4, 'David', 400.5, 'something'),
            (5, 'Eve', 500.75, 'another'),
            (6, 'Frank', 600.1, 'test')]

columns = ['id', 'name', 'amount', 'extra_col']
df_new_data_increment = spark.createDataFrame(new_data, columns)

# Load the existing data from the table
df_existing_data = spark.table("custoomer_data.table.historical_data")

# Ensure schema consistency by selecting only common columns
common_columns = list(set(df_existing_data.columns) & set(df_new_data_increment.columns))

df_combined_data = df_existing_data.select(common_columns).union(df_new_data_increment.select(common_columns))

# Write the combined data back to the table
df_combined_data.write.mode("overwrite").saveAsTable("custoomer_data.table.historical_data")

# Display the updated table
display(spark.table("custoomer_data.table.historical_data"))