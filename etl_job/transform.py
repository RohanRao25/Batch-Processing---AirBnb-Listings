from pyspark.sql import dataframe,Window
from pyspark.sql.functions import to_date,split,explode,row_number, monotonically_increasing_id,trim,concat_ws,substring, col
import os,sys
from timeit import default_timer as timer
# Add the root_directory to the Python path
root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_directory)
from utils.constants import listing_df_column_name_dict,reviews_df_column_name_dict

def Rename_Columns(df,col_dict):
    
    for key in col_dict.keys():
        df = df.withColumnRenamed(key, col_dict.get(key))
        
    return df

def Convert_To_Date_Field(df,column_name):
    df = df.withColumn(column_name,to_date(column_name,'M/d/yyyy'))
    
    return df


def Create_Dim_And_Facts_Dataframes(df:dataframe,df_Name):
    df_dict = {}
    if df_Name == "listing":
        host_dim_df = df.dropDuplicates(["idn_host"]).select(concat_ws(",",df.txt_nam_street,df.txt_nam_country,df.txt_state).alias("txt_addr_host"),"idn_host","txt_url_host","txt_nam_host","txt_abt_host","ind_suprhost", \
                                                                 "txt_url_pic_host" \
                                             ,"ind_vrfd_host","txt_zip_host","cnt_lstng_host",df.txt_nam_country.alias("txt_loc_host"))
        
        host_dim_df = host_dim_df.select([substring(col(f),0,100).alias(f) for f in host_dim_df.columns])
        df_dict["dim.t_host_det_dim"] = host_dim_df
        
        #Code block for verification dimension table
        vrfctn_mthd_df = df.select("idn_host","txt_vrfctn_host")
        vrfctn_mthd_df = vrfctn_mthd_df.withColumn("txt_vrfctn_mthd",split(vrfctn_mthd_df["txt_vrfctn_host"],",")).select("idn_host",explode("txt_vrfctn_mthd").alias("txt_vrfctn_mthd"))
        vrfctn_mthd_df = vrfctn_mthd_df.withColumn("txt_vrfctn_mthd",trim(vrfctn_mthd_df.txt_vrfctn_mthd))
        vrfctn_mthd_dim_df = vrfctn_mthd_df.dropDuplicates(["txt_vrfctn_mthd"]).select("txt_vrfctn_mthd")
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.withColumn("idn_vrfctn_mthd",row_number().over(w))
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.drop("counter")
        df_dict["dim.t_vrfctn_mthd_dim"] = vrfctn_mthd_dim_df

        #Code block for verification dimension table ends here

        #host verification facts table code starts here
        host_vrfctn_mthd_facts_dim = vrfctn_mthd_df.join(vrfctn_mthd_dim_df,"txt_vrfctn_mthd","left")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.drop("txt_vrfctn_mthd")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.orderBy("idn_host")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.withColumn("idn_host_vrfctn",row_number().over(w))
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.drop("counter")
        
        df_dict["facts.t_host_vrfctn_fact"] = host_vrfctn_mthd_facts_dim
        #host verification facts table code ends here

        #Code block for aminites dimension table

        amnts_df = df.select("idn_lstng","txt_nam_amnts")
        amnts_df = amnts_df.withColumn("txt_nam_amnts", split(amnts_df.txt_nam_amnts,",")).select("idn_lstng",explode("txt_nam_amnts").alias("txt_nam_amnts"))
        amnts_df = amnts_df.withColumn("txt_nam_amnts",trim(amnts_df.txt_nam_amnts))
        amnts_dim_df = amnts_df.dropDuplicates(["txt_nam_amnts"]).select("txt_nam_amnts")
        amnts_dim_df = amnts_dim_df.withColumn("counter", monotonically_increasing_id())
        w = Window.orderBy("counter")
        amnts_dim_df = amnts_dim_df.withColumn("idn_amnts",row_number().over(w))
        amnts_dim_df = amnts_dim_df.drop("counter")
        
        df_dict["dim.t_amnts_dim "] = amnts_dim_df

        #Code block for aminites dimension table ends here

        #listing amnities facts table code starts from here
        lstng_amnts_facts_df = amnts_df.join(amnts_dim_df,"txt_nam_amnts","left")
        lstng_amnts_facts_df = lstng_amnts_facts_df.drop("txt_nam_amnts")
        lstng_amnts_facts_df = lstng_amnts_facts_df.orderBy("idn_lstng")
        lstng_amnts_facts_df = lstng_amnts_facts_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        lstng_amnts_facts_df = lstng_amnts_facts_df.withColumn("idn_prop_amnts",row_number().over(w))
        lstng_amnts_facts_df = lstng_amnts_facts_df.drop("counter")
        
        df_dict["facts.t_prop_amnts_fact"] = lstng_amnts_facts_df
        #listing amnities facts table code ends here


        print(df.columns)

        #Code block for prop listing dimension table
        

        listing_df = df.select("idn_lstng","txt_url_lstng","txt_nam_lstng","txt_desc_lstng","txt_dtl_nbrhd", \
                               "txt_dtl_trnst", "txt_rule_lstng", "txt_url_poc_lstng","txt_type_prop","txt_type_room" \
                                ,"accommodates","cnt_bathroom","cnt_bed","type_bed",\
                                    "lstng_price","security_deposit",concat_ws(",",df.latitude,df.longitude))

        listing_df = listing_df.select([substring(col(f),0,100).alias(f) for f in listing_df.columns])
        df_dict["dim.t_prop_lstng_dim"] = listing_df

        #Code block for prop listing dimension table ends here

        #code block for host listing fact table starts here
        host_listing_facts_df = df.select("idn_lstng","idn_host","dte_frm_host")

        host_listing_facts_df = host_listing_facts_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        host_listing_facts_df = host_listing_facts_df.withColumn("idn_host_lstng",row_number().over(w))
        host_listing_facts_df = host_listing_facts_df.drop("counter")
        
        df_dict["facts.t_host_lstng_fact"] = host_listing_facts_df
        

        #code block for host listing facts table ends here
    
    elif df_Name == "reviews":
         #Customer dim table starts from here

         customer_dim_df = df.select("idn_customer","txt_nam_customer")
         
         df_dict["dim.t_customer_dim"] = customer_dim_df
         print(df.columns)

         #Customer dim table code block ends here

         #customer review facts table starts here
         customer_rwview_facts_df = df.select("idn_listing","idn_review","dte_review","idn_customer","txt_review")
         
         df_dict["facts.t_customer_review_fact"] = customer_rwview_facts_df

         #customer review facts table ends here
    return df_dict






def Transform_data(data_Dict,logger):
    logger.info("Transformation Operation started...")
    start = timer()
    df_Dict = {}
    for key,val in data_Dict.items():
        if key == "listing":
            
            df2 = val.transform(Rename_Columns,listing_df_column_name_dict).transform(Convert_To_Date_Field,"dte_frm_host")
            
            listing_Dict = Create_Dim_And_Facts_Dataframes(df2,key)

        elif key == "reviews":
            df2 = val.transform(Rename_Columns,reviews_df_column_name_dict).transform(Convert_To_Date_Field,"dte_review")
            review_Dict = Create_Dim_And_Facts_Dataframes(df2,key)
        
        
    df_Dict = {**listing_Dict,**review_Dict}
    end = timer()
    logger.info("Transformation operation compplete! Time Elapsed - {}".format(str(end-start)))

    return df_Dict
 