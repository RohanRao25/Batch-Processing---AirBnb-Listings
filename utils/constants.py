root_name = "spark_app"
spark_config_rel_path = "../configs/spark.conf"
date_Format = "mm/dd/yyyy"
columns_To_Be_processed = ["space","description","neighborhood_overview","notes","transit","access","interaction","house_rules" \
                           ,"host_about","host_verifications","amenities","security_deposit","cleaning_fee","extra_people","jurisdiction_names"]
pattern = r'[^a-zA-Z0-9:(),!{}\[\]?$.\-]'