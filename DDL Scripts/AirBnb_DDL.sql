-- Create a login for the database
CREATE LOGIN spark_login WITH PASSWORD = 'P@ssword1997'

GO

--Create a user for the login
CREATE USER spark_user FOR LOGIN spark_login

GO

CREATE DATABASE airbnb_data_db

GO

USE airbnb_data_db

GO

CREATE SCHEMA dim

GO

CREATE SCHEMA facts

GO

CREATE TABLE dim.t_host_det_dim (
idn_host int,
txt_url_host VARCHAR(200),
txt_nam_host VARCHAR(100),
txt_loc_host VARCHAR(100),
txt_abt_host VARCHAR(500),
txt_url_pic_host VARCHAR(200),
cnt_lstng_host int,
ind_suprhost VARCHAR(1),
ind_vrfd_host VARCHAR(1),
txt_addr_host VARCHAR(200),
txt_zip_host VARCHAR(10),
CONSTRAINT pk_idn_host PRIMARY KEY (idn_host)
) ON [PRIMARY]

GO

GRANT INSERT, SELECT, DELETE, UPDATE ON dim.t_host_det_dim TO spark_user

GO

CREATE TABLE dim.t_customer_dim (
idn_customer int,
txt_nam_customer VARCHAR(100)
CONSTRAINT pk_idn_customer PRIMARY KEY (idn_customer)
) ON [PRIMARY]

GO

GRANT INSERT, SELECT, DELETE, UPDATE ON dim.t_customer_dim TO spark_user

GO

CREATE TABLE dim.t_vrfctn_mthd_dim (
idn_vrfctn_mthd int,
txt_vrfctn_mthd VARCHAR(100)
CONSTRAINT pk_idn_vrfctn_mthd PRIMARY KEY (idn_vrfctn_mthd)
) ON [PRIMARY]

GO

GRANT SELECT, INSERT, DELETE, UPDATE ON dim.t_vrfctn_mthd_dim TO spark_user

GO

CREATE TABLE dim.t_amnts_dim (
idn_amnts int,
txt_nam_amnts VARCHAR(100),
CONSTRAINT pk_idn_amnts PRIMARY KEY (idn_amnts)
) ON [PRIMARY]

GO

GRANT SELECT , INSERT, DELETE, UPDATE ON dim.t_amnts_dim TO spark_user

GO

CREATE TABLE dim.t_prop_lstng_dim (
idn_lstng int,
txt_url_lstng VARCHAR(200),
txt_nam_lstng VARCHAR(500),
txt_desc_lstng VARCHAR(1000),
txt_dtl_nbrhd VARCHAR(200),
txt_dtl_trnst VARCHAR(500),
txt_rule_lstng VARCHAR(500),
txt_url_pic_lstng VARCHAR(200),
txt_geo_coordinates VARCHAR(200),
txt_type_prop VARCHAR(200),
txt_type_room VARCHAR(200),
cnt_accmdt_person int,
cnt_bathroom int,
cnt_bedroom int,
cnt_bed int,
lstng_price float,
security_deposit float
CONSTRAINT pk_idn_lstng PRIMARY KEY (idn_lstng)
) ON [PRIMARY]

GO

GRANT INSERT, SELECT, DELETE, UPDATE ON dim.t_prop_lstng_dim TO spark_user;

GO

-- Create the facts table now

CREATE TABLE facts.t_prop_amnts_fact (
idn_prop_amnts int,
idn_amnts int, 
idn_lstng int
CONSTRAINT pk_idn_prop_amnts PRIMARY KEY (idn_prop_amnts),
CONSTRAINT fk_idn_amnts FOREIGN KEY (idn_amnts)
REFERENCES dim.t_amnts_dim (idn_amnts),
CONSTRAINT fk_idn_lstng FOREIGN KEY (idn_lstng)
REFERENCES dim.t_prop_lstng_dim (idn_lstng)
) ON [PRIMARY]

GO

GRANT SELECT, INSERT, DELETE ON facts.t_prop_amnts_fact TO spark_user

GO

CREATE TABLE facts.t_host_vrfctn_fact (
idn_host_vrfctn int,
idn_vrfctn_mthd int,
idn_host int
CONSTRAINT pk_idn_host_vrfctn PRIMARY KEY (idn_host_vrfctn),
CONSTRAINT fk_idn_vrfctn_mthd FOREIGN KEY (idn_vrfctn_mthd)
REFERENCES dim.t_vrfctn_mthd_dim (idn_vrfctn_mthd),
CONSTRAINT fk_idn_host FOREIGN KEY (idn_host)
REFERENCES dim.t_host_det_dim (idn_host)
) ON [PRIMARY]

GO

GRANT SELECT, INSERT, DELETE ON facts.t_host_vrfctn_fact TO spark_user

GO

CREATE TABLE facts.t_host_lstng_fact (
idn_host_lstng int,
idn_host int,
idn_lstng int,
dte_frm_host date
CONSTRAINT pk_idn_host_lstng PRIMARY KEY (idn_host_lstng),
CONSTRAINT fk_idn_host_thlf FOREIGN KEY (idn_host)
REFERENCES dim.t_host_det_dim (idn_host),
CONSTRAINT fk_idn_lstng_thlf FOREIGN KEY (idn_lstng)
REFERENCES dim.t_prop_lstng_dim (idn_lstng)
) ON [PRIMARY]

GO

GRANT SELECT, INSERT, DELETE ON facts.t_host_lstng_fact TO spark_user

GO

CREATE TABLE facts.t_customer_review_fact (
idn_review int,
idn_lstng int,
idn_customer int,
txt_review VARCHAR(500),
dte_review date
CONSTRAINT pk_idn_review PRIMARY KEY (idn_review),
CONSTRAINT fk_idn_lstng_tcrf FOREIGN KEY (idn_lstng)
REFERENCES dim.t_prop_lstng_dim (idn_lstng),
CONSTRAINT fk_idn_customer_tcrf FOREIGN KEY (idn_customer)
REFERENCES dim.t_customer_dim (idn_customer)
) ON [PRIMARY]

GO

GRANT SELECT, INSERT, DELETE ON facts.t_customer_review_fact TO spark_user

GO





