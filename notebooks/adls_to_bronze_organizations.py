df_organizations = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Files/organizations/organizations-100.csv")

df_organizations.printSchema()
df_organizations.show(5)

new_columns = [c.replace(" ", "_") for c in df_organizations.columns]
df_organizations_clean = df_organizations.toDF(*new_columns)

df_organizations_clean.write.format("delta").mode("overwrite").saveAsTable("Bronze.dbo.organizations")
spark.read.table("Bronze.dbo.organizations").show(5)