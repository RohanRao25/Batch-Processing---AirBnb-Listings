from pyspark.sql import dataframe, SparkSession
import pandas as pd

def Extract_data(pre_processed_data,spark: SparkSession,date_Format):
   data_df = spark.createDataFrame(pre_processed_data)
   data_df.show()
   """raw_data = spark.read.format("csv").option("header","true") \
                    .option("quote","\"").option("escape","\""). \
                        option("delimiter",",").option("multiline","true") \
                            .option("encoding","UTF-8").option("mode","FAILFAST") \
                                .option("dateFormat",date_Format).load("data/listings_details - Copy.csv")"""

