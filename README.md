# Batch-Processing---AirBnb-Listings
<h3>Introduction</h3><hr></br>
 <p>This is a batch processing application to extract AirBnb listings data from a  CSV file, cleanse it and store it in a SQL Server Database</p>

 <h3>Architecture</h3></br><hr></br>

 ![image](https://github.com/RohanRao25/Batch-Processing---AirBnb-Listings/assets/53894044/821a161c-2259-4a69-9d8e-2d7559879d3f)


<h3>Project Structure</h3></br><hr></br>
<p>root/</p></br>
 <p>|-- configs/</p></br>
 <p>|   |-- spark.conf</p></br>
 <p>|-- Data Model/</p></br>
 <p>|   |-- Data_Model.png</p></br>
 <p>|-- DDL Scripts/</p></br>
 <p>|-- |-- AirBnb_DDL.sql</p></br>
 <p>|-- etl_job/</p></br>
 <p>|   |-- data_pre_processing.py</p></br>
 <p>|   |-- etl_manager.py</p></br>
 <p>|   |-- extract.py</p>br>
 <p>|   |-- load.py</p></br>
 <p>|   |-- schema_details.py</p></br>
 <p>|   |-- transform.py</p></br>
 <p>|-- utils/</p></br>
 <p>|   |-- __init__.py</p></br>
 <p>|   |-- constants.py</p></br>
 <p>|   |-- helpers.py</p></br>
 <p>|   |-- logger.py</p></br>
 <p>|   |-- spark.py</p>br>
 <p>|-- log4j.properties</p></br>
 <p>|-- README.md</p></br>
 <p>|-- main.py</p></br>


 <h3>Data Model</h3></br><hr></br>
 ![Data_Model](https://github.com/RohanRao25/Batch-Processing---AirBnb-Listings/assets/53894044/8539c5dc-0bdc-4d5c-8641-40d1ca272d24)

 <h3>Data Set</h3></br><hr></br>
 <p>Link :</p><a href="https://www.kaggle.com/datasets/erikbruin/airbnb-amsterdam">AirBnb Amsterdam Dataset</a>
 




