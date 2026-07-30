# src/schemas.py
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

solar_wind_schema = StructType([
    StructField("time_tag", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("density", DoubleType(), True),
    StructField("b", DoubleType(), True),
    StructField("bz", DoubleType(), True),
    StructField("temp", IntegerType(), True)
])