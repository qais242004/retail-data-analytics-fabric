from pyspark.sql.functions import row_number, col, rand, floor
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# DIM_CUSTOMER
df_customer = spark.read.table("Silver.dbo.customer")
window1 = Window.orderBy("ID")
dim_customer = df_customer.withColumn("customer_key", row_number().over(window1))
dim_customer = dim_customer.select("customer_key", "ID", "Name", "City")
dim_customer.write.format("delta").mode("overwrite").saveAsTable("Gold.dbo.dim_customer")

# DIM_PRODUCT
df_product = spark.read.table("Silver.dbo.products")
window2 = Window.orderBy("Internal_ID")
dim_product = df_product.withColumn("product_key", row_number().over(window2))
dim_product = dim_product.select(
    "product_key", "Internal_ID", "Name", "Brand", "Category",
    "Price", "Currency", "Stock", "Color", "Size", "Availability"
)
dim_product.write.format("delta").mode("overwrite").saveAsTable("Gold.dbo.dim_product")

# FACT_SALES
dim_c = spark.read.table("Gold.dbo.dim_customer").select("customer_key")
dim_p = spark.read.table("Gold.dbo.dim_product").select("product_key", "Price")
customer_count = dim_c.count()
product_count = dim_p.count()
num_transactions = 50

fact_sales = spark.range(1, num_transactions + 1) \
    .withColumnRenamed("id", "sales_id") \
    .withColumn("customer_key", (floor(rand() * customer_count) + 1).cast(IntegerType())) \
    .withColumn("product_key", (floor(rand() * product_count) + 1).cast(IntegerType())) \
    .withColumn("Quantity", (floor(rand() * 5) + 1).cast(IntegerType()))

fact_sales = fact_sales.join(dim_p, "product_key", "left")
fact_sales = fact_sales.withColumn("Total_Amount", col("Quantity") * col("Price"))
fact_sales = fact_sales.select("sales_id", "customer_key", "product_key", "Quantity", "Price", "Total_Amount")

fact_sales.write.format("delta").mode("overwrite").saveAsTable("Gold.dbo.fact_sales")
fact_sales.show(10)