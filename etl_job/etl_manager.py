
import sys
import os

# Add the root_directory to the Python path
root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_directory)
from utils.spark import Initialize_Spark
from utils.constants import date_Format
from etl_job.extract import Extract_data
from etl_job.data_pre_processing import Pre_Process_Data

def Manage_Pipeline():
    
    pre_Processed_Data = Pre_Process_Data()
    #Start the Spark Session
    sparkSession, logger = Initialize_Spark()
    Extract_data(pre_Processed_Data,sparkSession,date_Format)



