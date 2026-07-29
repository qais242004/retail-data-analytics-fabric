from pyspark.sql.functions import col

df_product = spark.read.table("Bronze.dbo.products")
df_product_clean = df_product.toDF(*[c.strip() for c in df_product.columns])
df_product_clean = df_product_clean.withColumn("Price", col("Price").cast("double"))
df_product_clean = df_product_clean.withColumn("Stock", col("Stock").cast("int"))
df_product_clean = df_product_clean.dropDuplicates()
df_product_clean.show()

df_product_clean.write.format("delta").mode("overwrite").saveAsTable("Silver.dbo.products")