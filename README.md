# Batch-Processing---AirBnb-Listings
<b>Introduction</b>
 This is a batch processing application to extract AirBnb listings data from a  CSV file, cleanse it and store it in a SQL Server Database


Project Structure :-
root/
 |-- configs/
 |   |-- spark.conf
 |-- Data Model/
 |   |-- Data_Model.png
 |-- DDL Scripts/
 |-- |-- AirBnb_DDL.sql
 |-- etl_job/
 |   |-- data_pre_processing.py
 |   |-- etl_manager.py
 |   |-- extract.py
 |   |-- load.py
 |   |-- shema_details.py
 |   |-- transform.py
 |-- utils/
 |   |-- __init__.py
 |   |-- constants.py
 |   |-- helpers.py
 |   |-- logger.py
 |   |-- spark.py
 |-- log4j.properties
 |-- README.md
 |-- main.py



