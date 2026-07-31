from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

solar_wind_schema = StructType([
    StructField("time_tag", TimestampType(), True),
    StructField("solar_wind_speed", DoubleType(), True),
    StructField("proton_density", DoubleType(), True),
    StructField("magnetic_field_total", DoubleType(), True),
    StructField("magnetic_field_bz", DoubleType(), True),
    StructField("plasma_temperature", DoubleType(), True),
    StructField("status_code", StringType(), True) 
])