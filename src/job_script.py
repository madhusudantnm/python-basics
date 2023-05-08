from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data_schema = StructType([
    StructField("price", IntegerType()),
    StructField("area", IntegerType()),
    StructField("bedrooms", IntegerType()),
    StructField("bathrooms", IntegerType()),
    StructField("stories", IntegerType()),
    StructField("mainroad", StringType()),
    StructField("guestroom", StringType()),
    StructField("basement", StringType()),
    StructField("hotwaterheating", StringType()),
    StructField("airconditioning", StringType()),
    StructField("parking", IntegerType()),
    StructField("prefarea", StringType()),
    StructField("furnishingstatus", StringType())
])
spark = SparkSession.builder.appName('UpdateDeltaTable').getOrCreate()
df = spark.read.csv("/FileStore/shared_uploads/madhusudantnm.1997@gmail.com/housing.csv", header=True, schema=data_schema)
df.write.format("delta").mode("overwrite").saveAsTable("housing_dataset")
