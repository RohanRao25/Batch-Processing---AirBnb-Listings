from pyspark.sql import dataframe
from timeit import default_timer as timer

db_URL = ""
userName = ""
passWord = ""

def Load_df_To_DB(data_dict,logger):
    logger.info("Load operation started...")
    start = timer()
    for key,val in data_dict.items():
        val.write.format("com.microsoft.sqlserver.jdbc.spark").mode("overwrite").option("url",db_URL) \
        .option("dbtable",key).option("user",userName).option("password",passWord).save()
    end = timer()
    logger.info("Load operation completed! Total Time Elapsed - {}".format(str(end-start)))