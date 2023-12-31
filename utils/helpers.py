from configparser import ConfigParser
from utils.constants import spark_config_rel_path, pattern
import os
import re
import pandas as pd


def Read_Config_Files(config_indicator):
    dict_config = {}

    if config_indicator == "spark":
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.normpath(os.path.join(script_dir,spark_config_rel_path))
    
    config_parser = ConfigParser()
    config_parser.read(config_file_path)
    for key,val in config_parser.items("spark_app_config"):
        dict_config.update({key:val})

    return dict_config


def Convert_Scientific_To_Real(val):
    return float(val)

def Remove_Spec_Chars(val):
    if not pd.isna(val):
        return re.sub(pattern," ",val)
    else:
        return val






