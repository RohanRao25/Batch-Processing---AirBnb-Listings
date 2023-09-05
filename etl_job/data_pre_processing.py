import pandas as pd
import os,sys

# Add the root_directory to the Python path
root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_directory)
from utils.helpers import Convert_Scientific_To_Real, Remove_Spec_Chars
from utils.constants import columns_To_Be_processed, columns_to_be_dropped

def Pre_Process__Listings_Data():
    raw_data = pd.read_csv("data/listings_details - Copy.csv")
   
  
    raw_data["scrape_id"] = raw_data["scrape_id"].apply(Convert_Scientific_To_Real)
    raw_data[columns_To_Be_processed] = raw_data[columns_To_Be_processed].applymap(Remove_Spec_Chars)
    raw_data["host_since"] = pd.to_datetime(raw_data["host_since"])
    raw_data.drop(columns=columns_to_be_dropped, inplace=True)

    return raw_data

