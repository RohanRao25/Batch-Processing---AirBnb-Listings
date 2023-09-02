from pyspark.sql import SparkSession
from constants import root_name


class Log4j:

    def __init__(self, sparkSession: SparkSession):
        
        log4j = sparkSession._jvm.org.apache.log4j
        conf = sparkSession.sparkContext.getConf()
        app_name = conf.get('spark.app.name')
        """Root_name is used to map the logger to the logger.properties files"""
        self.logger = log4j.LogManager.getLogger(root_name+"."+app_name)

    def info(self,message):
        self.logger.info(message)
    
    def warn(self,message):
        self.logger.warn(message)

    def error(self,message):
        self.logger.error(message)

    def debug(self,message):
        self.logger.debug(message)
