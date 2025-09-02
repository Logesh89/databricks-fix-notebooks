# Create a DataFrame for the new data
new_data = [(4, 'David', 400.5, 'something'),
            (5, 'Eve', 500.75, 'another'),
            (6, 'Frank', 600.1, 'test')]

columns = ['id', 'name', 'amount', 'extra_col']
df_new_data_increment = spark.createDataFrame(new_data, columns)

# Load the existing data from the table
df_existing_data = spark.table("custoomer_data.table.historical_data")

# Rename the 'amount' column in df_new_data_increment to match the existing schema
df_new_data_increment = df_new_data_increment.withColumnRenamed('amount', 'amount_new')

# Union the new data with the existing data
df_combined_data = df_existing_data.unionByName(df_new_data_increment, allowMissingColumns=True)

# Write the combined data back to the table
df_combined_data.write.mode("overwrite").saveAsTable("custoomer_data.table.historical_data")

# Display the updated table
display(spark.table("custoomer_data.table.historical_data"))