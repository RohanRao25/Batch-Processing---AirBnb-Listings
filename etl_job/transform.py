from pyspark.sql import dataframe,Window
from pyspark.sql.functions import to_date,split,explode,row_number, monotonically_increasing_id,trim
import os,sys
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
        host_listing_dim_df = df.dropDuplicates(["idn_host"]).select("idn_host","txt_url_host","txt_nam_host","txt_abt_host","ind_suprhost", \
                                                                 "txt_url_pic_host","txt_vrfctn_host" \
                                             ,"ind_vrfd_host","txt_nam_street","txt_state","txt_zip_host","cnt_lstng_host","txt_nam_country")
        
        #Code block for verification dimension table
        vrfctn_mthd_df = df.select("idn_host","txt_vrfctn_host")
        vrfctn_mthd_df = vrfctn_mthd_df.withColumn("txt_vrfctn_mthd",split(vrfctn_mthd_df["txt_vrfctn_host"],",")).select("idn_host",explode("txt_vrfctn_mthd").alias("txt_vrfctn_mthd"))
        vrfctn_mthd_df = vrfctn_mthd_df.withColumn("txt_vrfctn_mthd",trim(vrfctn_mthd_df.txt_vrfctn_mthd))
        vrfctn_mthd_dim_df = vrfctn_mthd_df.dropDuplicates(["txt_vrfctn_mthd"]).select("txt_vrfctn_mthd")
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.withColumn("idn_vrfctn_mthd",row_number().over(w))
        vrfctn_mthd_dim_df = vrfctn_mthd_dim_df.drop("counter")
        df_dict["vfctn_mthd_dim"] = vrfctn_mthd_dim_df

        #Code block for verification dimension table ends here

        #host verification facts table code starts here
        host_vrfctn_mthd_facts_dim = vrfctn_mthd_df.join(vrfctn_mthd_dim_df,"txt_vrfctn_mthd","left")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.drop("txt_vrfctn_mthd")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.orderBy("idn_host")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.withColumn("idn_host_vrfctn",row_number().over(w))
        host_vrfctn_mthd_facts_dim = host_vrfctn_mthd_facts_dim.drop("counter")
        host_vrfctn_mthd_facts_dim.show()
        df_dict["host_vrfctn_mthd_facts"] = host_vrfctn_mthd_facts_dim
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
        amnts_dim_df.show()
        df_dict["amnts_dim_df"] = amnts_dim_df

        #Code block for aminites dimension table ends here

        #listing amnities facts table code starts from here
        lstng_amnts_facts_df = amnts_df.join(amnts_dim_df,"txt_nam_amnts","left")
        lstng_amnts_facts_df = lstng_amnts_facts_df.drop("txt_nam_amnts")
        lstng_amnts_facts_df = lstng_amnts_facts_df.orderBy("idn_lstng")
        lstng_amnts_facts_df = lstng_amnts_facts_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        lstng_amnts_facts_df = lstng_amnts_facts_df.withColumn("idn_prop_amnts",row_number().over(w))
        lstng_amnts_facts_df = lstng_amnts_facts_df.drop("counter")
        lstng_amnts_facts_df.show()
        df_dict["lstng_amnts_facts"] = lstng_amnts_facts_df
        #listing amnities facts table code ends here


        print(df.columns)

        #Code block for prop listing dimension table
        df.show()

        listing_df = df.select("idn_lstng","txt_url_lstng","txt_nam_lstng","txt_desc_lstng","txt_dtl_nbrhd", \
                               "txt_dtl_trnst", "txt_rule_lstng", "txt_url_poc_lstng","txt_type_prop","txt_type_room" \
                                ,"accommodates","cnt_bathroom","cnt_bed","type_bed","txt_nam_amnts","latitude" \
                                    ,"longitude","lstng_price","security_deposit")

        listing_df.show()
        df_dict["listing_dim"] = listing_df

        #Code block for prop listing dimension table ends here

        #code block for host listing fact table starts here
        host_listing_facts_df = df.select("idn_lstng","idn_host","dte_frm_host")

        host_listing_facts_df = host_listing_facts_df.withColumn("counter",monotonically_increasing_id())
        w = Window.orderBy("counter")
        host_listing_facts_df = host_listing_facts_df.withColumn("idn_host_lstng",row_number().over(w))
        host_listing_facts_df = host_listing_facts_df.drop("counter")
        host_listing_facts_df.show()
        df_dict["host_listing_facts"] = host_listing_facts_df
        host_listing_dim_df

        #code block for host listing facts table ends here
    
    elif df_Name == "reviews":
         #Customer dim table starts from here

         customer_dim_df = df.select("idn_customer","txt_nam_customer")
         customer_dim_df.show()
         df_dict["customer_dim"] = customer_dim_df
         print(df.columns)

         #Customer dim table code block ends here

         #customer review facts table starts here
         customer_rwview_facts_df = df.select("idn_listing","idn_review","dte_review","idn_customer","txt_review")
         customer_rwview_facts_df.show()
         df_dict["customer_rev_facts"] = customer_rwview_facts_df

         #customer review facts table ends here





def Transform_data(data_Dict):
    for key,val in data_Dict.items():
        if key == "listing":
            
            df2 = val.transform(Rename_Columns,listing_df_column_name_dict).transform(Convert_To_Date_Field,"dte_frm_host")
            
            Create_Dim_And_Facts_Dataframes(df2,key)

        elif key == "reviews":
            df2 = val.transform(Rename_Columns,reviews_df_column_name_dict).transform(Convert_To_Date_Field,"dte_review")
            Create_Dim_And_Facts_Dataframes(df2,key)
 