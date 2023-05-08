from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("price", IntegerType()),
    StructField("area", IntegerType()),
    StructField("bedrooms", IntegerType()),
    StructField("bathrooms", IntegerType()),
    StructField("stories", IntegerType()),
    StructField("mainroad", StringType()),
    StructField("guestroom", StringType()),
    StructField("basement", StringType()),
    StructField("hotwaterheating", StringType()),
    StructField("airconfitioning", StringType()),
    StructField("parking", IntegerType()),
    StructField("prefaea", StringType()),
    StructField("funishingstatus", StringType())
])

spark = SparkSession.builder.appName('uat').getOrCreate()

df = spark.read.csv("/FileStore/shared_uploads/madhusudantnm.1997@gmail.com/housing.csv", header=True, schema=schema)
df.write.format("delta").mode("overwrite").saveAsTable("test_table")
