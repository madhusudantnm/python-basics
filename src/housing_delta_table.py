from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

table_schema = """
  price INT,
  area INT,
  bedrooms INT,
  bathrooms INT,
  stories INT,
  mainroad STRING,
  guestroom STRING,
  basement STRING,
  hotwaterheating STRING,
  airconditioning STRING,
  parking INT,
  prefarea STRING,
  furnishingstatus STRING
"""

spark = SparkSession.builder.appName('CreateDeltaTableIfNotExists').getOrCreate()

# Create the new Delta table
spark.sql(f"CREATE TABLE IF NOT EXISTS housing_dataset ({table_schema}) USING DELTA")
