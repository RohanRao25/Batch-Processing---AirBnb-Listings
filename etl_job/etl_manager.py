
import sys
import os

# Add the root_directory to the Python path
root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_directory)
from utils.spark import Initialize_Spark


def Manage_Pipeline():
    
    #Start the Spark Session
    Initialize_Spark()

Manage_Pipeline()

