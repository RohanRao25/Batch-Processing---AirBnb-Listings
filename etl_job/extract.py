from pyspark.sql import dataframe, SparkSession
import pandas as pd
from etl_job.schema_details import *

def Extract_data(pre_processed_data,spark: SparkSession,date_Format):
   print(pre_processed_data.columns)
   
   #Pre-Processing the data and then converting it into a spark data frame.
   listings_data_df = spark.createDataFrame(pre_processed_data,schema=listings_Data_schema)
   

   review_data_df = spark.read.format("csv").option("header","true").option("multiline","true"). \
    option("dateFormat",date_Format).option("mode","FAILFAST").schema(reviews_data_schema).load("data/reviews_details - Copy.csv")
   

   """raw_data = spark.read.format("csv").option("header","true") \
                    .option("quote","\"").option("escape","\""). \
                        option("delimiter",",").option("multiline","true") \
                            .option("encoding","UTF-8").option("mode","FAILFAST") \
                                .option("dateFormat",date_Format).load("data/listings_details - Copy.csv")"""
   
   return {"listing":listings_data_df,"reviews": review_data_df}

    