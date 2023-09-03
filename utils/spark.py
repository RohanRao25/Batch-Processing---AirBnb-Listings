from pyspark.sql import SparkSession
from pyspark import SparkConf
from utils.helpers import Read_Config_Files
from utils.logger import Log4j
import os


def Initialize_Spark():

    configs = Read_Config_Files("spark")
    if not configs:
        sparkSession = SparkSession.builder.appName("ETL_AirBnb_listing").getOrCreate()

    else:
        sparkConf = SparkConf()

        for key,val in configs.items():
            sparkConf.set(key,val) 

        sparkSession = SparkSession.builder.config(conf = sparkConf).getOrCreate()
    logger = Log4j(sparkSession)
    logger.info("Spark Session Started...")

    return sparkSession, logger


