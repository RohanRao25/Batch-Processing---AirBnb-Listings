root_name = "spark_app"

spark_config_rel_path = "../configs/spark.conf"

date_Format = "M/dd/yyyy"

columns_To_Be_processed = ["space","description","neighborhood_overview","notes","transit","access","interaction","house_rules" \
                           ,"host_about","host_verifications","amenities","price","security_deposit","cleaning_fee","extra_people","jurisdiction_names"]

columns_to_be_dropped = [
    "scrape_id", "last_scraped", "summary", "space", "experiences_offered", "notes", "access", "interaction",
    "thumbnail_url", "medium_url", "xl_picture_url", "host_response_time", "host_response_rate",
    "host_acceptance_rate", "host_thumbnail_url", "host_neighbourhood", "host_listings_count",
    "host_has_profile_pic", "street", "neighbourhood", "neighbourhood_group_cleansed"
    , "market", "smart_location", "country_code", "square_feet", "weekly_price",
    "is_location_exact", "cleaning_fee", "guests_included", "extra_people","monthly_price",
    "minimum_nights", "maximum_nights", "calendar_updated", "has_availability", "availability_30", "availability_60",
    "availability_90", "availability_365", "calendar_last_scraped", "number_of_reviews", "first_review", "last_review",
    "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin",
    "review_scores_communication", "review_scores_location", "review_scores_value", "requires_license", "license",
    "jurisdiction_names", "instant_bookable", "is_business_travel_ready", "cancellation_policy",
    "require_guest_profile_picture", "require_guest_phone_verification", "calculated_host_listings_count","host_location",
    "reviews_per_month"
]


pattern = r'[^a-zA-Z0-9:(),!?$.\-]'

reviews_df_column_name_dict = {"listing_id":"idn_listing",
                               "id":"idn_review",
                               "date":"dte_review",
                               "reviewer_id":"idn_customer",
                               "reviewer_name":"txt_nam_customer",
                               "comments":"txt_review"

}

listing_df_column_name_dict = {"id":"idn_lstng",
                               "listing_url":"txt_url_lstng",
                               "name":"txt_nam_lstng",
                               "description":"txt_desc_lstng",
                               "neighborhood_overview": "txt_dtl_nbrhd",
                               "transit":"txt_dtl_trnst",
                               "house_rule":"txt_rule_lstng",
                               "picture_url":"txt_url_poc_lstng",
                               "host_id":"idn_host",
                               "host_url":"txt_url_host",
                               "host_name":"txt_nam_host",
                               "host_since":"dte_frm_host",
                               "host_about":"txt_abt_host",
                               "host_is_superhost":"ind_suprhost",
                               "host_picture_url":"txt_url_pic_host",
                               "host_total_listings_count":"cnt_lstng_host",
                               "host_verifications":"txt_vrfctn_host",
                               "host_identity_verified":"ind_vrfd_host",
                               "neighbourhood_cleansed":"txt_nam_street",
                               "city":"txt_City",
                               "state":"txt_state",
                               "zipcode":"txt_zip_host",
                               "country":"txt_nam_country",
                               "latitude":"latitude",
                               "longitude":"longitude",
                               "property_type":"txt_type_prop",
                               "room_type":"txt_type_room",
                               "accomodates":"cnt_accmdt_person",
                               "bathrooms":"cnt_bathroom",
                               "bedrooms":"cnt_bedrms",
                               "beds":"cnt_bed",
                               "bed_type":"type_bed",
                               "amenities":"txt_nam_amnts",
                               "price": "lstng_price",
                               "security_deposit" : "security_deposit"
                               }



