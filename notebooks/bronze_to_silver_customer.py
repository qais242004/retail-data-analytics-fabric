from pyspark.sql.functions import col, trim

df = spark.read.table("Bronze.dbo.customer")
df_clean = df.toDF(*[c.strip() for c in df.columns])
df_clean = df_clean.fillna({"Name": "Unknown"})
df_clean = df_clean.dropDuplicates()
df_clean = df_clean.withColumn("City", trim(col("City")))
df_clean.show()

df_clean.write.format("delta").mode("overwrite").saveAsTable("Silver.dbo.customer")