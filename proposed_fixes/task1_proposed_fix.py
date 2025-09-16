df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Aurangabad")) 
display(filtered_df)

df = spark.table("model.modelllm.weather_data_jun_12_2025")
filtered_df = df.filter((df.state == "Bihar") & (df.district == "Arwal")) 
display(filtered_df)

df_bihar = df.filter((df.state == "Bihar") & (df.district == "Araria"))
display(df_bihar.select("date", "temperatures_min", "temperatures_max", "precipitation_amount"))

from pyspark.sql import functions as F

df_agg = (
    df.groupBy("district")
      .agg(
          F.avg("temperatures_min").alias("avg_min_temp"),
          F.avg("temperatures_max").alias("avg_max_temp"),
          F.sum("precipitation_amount").alias("total_rain")
      )
      .orderBy(F.desc("avg_max_temp"))
)

display(df_agg)