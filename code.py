import os
import sys
import pandas as pd
## importing libraries of SPARK
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("yarn") \
            .enableHiveSupport() \
            .getOrCreate()
            
path = "my input path"
df = spark.read.option("header","True").csv(path)

df.createOrReplaceTempView("weather")


# Writing the data to output location as Parquet file
# as the ask is to get Hottest day , temprature and Region-- I just added country as well 
spark.sql("""SELECT 
        date(ObservationDate) , 
        Region, Country, 
        MAX(ScreenTemperature) AS max_temp 
        FROM weather GROUP BY 1,2,3 """).createOrReplaceTempView("final")

spark.table("final").write.parquet("outputpath")


#  parquet file can be read as 
spark.read.parquet("outputpath").createOrReplaceTempView("weather_tbl")

# WHAT IS THE HOTTEST TEMPRATURE in these 2 months
spark.sql("""SELECT MAX(max_temp) FROM final""").show()


# Which date was the hottest day 
# What was the temperature on that day
# In which region was the hottest day
spark.sql("""SELECT 
        DISTINCT ObservationDate , 
        region , 
        max_temp FROM final WHERE max_temp = (SELECT MAX(max_temp) FROM final)
        """).show(200,truncate=False)
