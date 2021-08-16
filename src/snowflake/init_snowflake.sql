----IMPORTANT: PLEASE HELP MODIFY THE FILE PATHS TO SAVE UNLOAD CSV FILES IN GET TASK.
-- Set up Warehouse

CREATE or REPLACE WAREHOUSE FA_Project01_CloudDW WITH 
	WAREHOUSE_SIZE = 'XSMALL' 
	WAREHOUSE_TYPE = 'STANDARD' 
	AUTO_SUSPEND = 300 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1 
	MAX_CLUSTER_COUNT = 2 
	SCALING_POLICY = 'STANDARD';
-- Set up Database
/********************CREATE DATABASE***************************/
--CREATE DATABASE
CREATE or REPLACE DATABASE FA_Project01_DB;
/********************CREATE SCHEMA***************************/
CREATE SCHEMA AdsBI;
/********************CREATE TABLES***************************/
-- TABLES for Staging
CREATE or replace TABLE AdsBI.AdsHeaderDetails (
	AdsID int NOT NULL,
	AdsName nvarchar(30) NOT NULL,
	AdsCategory nvarchar(100) NOT NULL,
	AdsPlatform nvarchar(100) NOT NULL,
	StandardCost float(2) NOT NULL,
	Cost_Per_Click float(2) NOT NULL,
	CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsID)
);
CREATE or replace TABLE AdsBI.CustomerDetails (
	CustomerID int NOT NULL,
	CustomerName nvarchar(100) NOT NULL,
	Gender nvarchar(10) NOT NULL,
	Email nvarchar(100) NOT NULL,
	Address nvarchar(100) NOT NULL,
	Age int NOT NULL,
	Income int NOT NULL,
	City nvarchar(50) NOT NULL,
	Region nvarchar(100) NOT NULL,
	RegisteredDate date NOT NULL,
	CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
);
CREATE  or replace TABLE AdsBI.ProductDetails (
	ProductID int NOT NULL,
	ProductName nvarchar(200) NOT NULL,
	ProductCategory nvarchar(200) NOT NULL,
	ProductColor nvarchar(100) NOT NULL,
	Cost float(2) NOT NULL,
	Price float(2) NOT NULL,
	CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);
CREATE OR REPLACE TABLE AdsBI.AdsTransactionDetails(
	Date date NOT NULL,
	CustomerID int NOT NULL,
	ProductID int NOT NULL,
	AdsID int NOT NULL,
	TimeOnAdSite int NOT NULL,
	DailySpentOnPlaftForm float (2) NOT NULL,
	ClickTimes tinyint NOT NULL,
	NumberOfBoughtProduct tinyint NOT NULL,
	PurchaseRate float (2) NULL,
	CONSTRAINT PK_AdsFACT PRIMARY KEY (CustomerID, ProductID, AdsID),
	CONSTRAINT FK_Customer FOREIGN KEY (CustomerID) REFERENCES AdsBI.CustomerDetails(CustomerID),
	CONSTRAINT FK_Product FOREIGN KEY (ProductID) REFERENCES AdsBI.ProductDetails(ProductID),
	CONSTRAINT FK_Ads FOREIGN KEY (AdsID) REFERENCES AdsBI.AdsHeaderDetails(AdsID)
);


-- CREATE DIM/FACT TABLES

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT"
    (ProductKey int identity(1,1),
    ProductID int NOT NULL,
    ProductName nvarchar(200) NOT NULL,
    Cost number NOT NULL,
    Price number NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductKey));
   
CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS" (
    AdsKey int identity(1,1),
    AdsID int NOT NULL,
    AdsName nvarchar(30) NOT NULL ,
    AdsCategory nvarchar(100) NOT NULL,
    AdsPlatform nvarchar(100) NOT NULL,
    StandardCost number NOT NULL,
    Cost_Per_Click float NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsKey)
);


CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER" (
    CustomerKey int identity(1,1),
    CustomerID int NOT NULL,
    CustomerName nvarchar(100) NOT NULL,
    Gender nvarchar(10) NOT NULL,
    Age int NOT NULL,
    Income int NOT NULL,
    City nvarchar(50) NOT NULL,
    Region nvarchar(100) NOT NULL,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (Customerkey)
);


CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_DATE" (
   DATEKEY        int NOT NULL
   ,DATE          DATE        NOT NULL
   ,DAYOFMONTH       SMALLINT    NOT NULL
   ,WEEKDAYNAME    VARCHAR(10) NOT NULL
   ,WEEK     SMALLINT    NOT NULL
   ,DAYOFWEEK      VARCHAR(9)  NOT NULL
   ,MONTH            SMALLINT    NOT NULL
   ,MONTHNAME       CHAR(3)     NOT NULL
   ,QUARTER          SMALLINT NOT NULL
  ,YEAR             SMALLINT    NOT NULL,
  CONSTRAINT PK_DateDim PRIMARY KEY (DATEKEY)
)
AS
  WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2017-01-01') AS DATEKEY
      FROM TABLE(GENERATOR(ROWCOUNT=>2000))  
  )
  SELECT TO_CHAR(DATE(DATEKEY),'YYYYMMDD'),
         DATE(DATEKEY)
         ,DAY(DATEKEY),
         DECODE(DAYNAME(DATEKEY),
    'Mon','Monday','Tue','Tuesday',
    'Wed','Wednesday','Thu','Thursday',
    'Fri','Friday','Sat','Saturday',
          'Sun','Sunday')
         ,WEEKOFYEAR(DATEKEY)        
         ,DAYOFWEEK(DATEKEY)
         ,MONTH(DATEKEY)
        ,MONTHNAME(DATEKEY),
         QUARTER(DATEKEY)
        ,YEAR(DATEKEY)
    FROM CTE_MY_DATE;
    
CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS"(
    DateKey int NOT NULL,
    CustomerKey int NOT NULL,
    ProductKey int NOT NULL,
    AdsKey int NOT NULL,
    TimeOnAdSite int NOT NULL,
    DailySpentOnPlatForm float  NOT NULL,
    ClickTimes tinyint NOT NULL,
    NumberOfBoughtProduct tinyint NOT NULL,
    IsBoughtFlag boolean NULL,
    constraint pk_adsfact PRIMARY KEY (DateKey, CustomerKey, ProductKey, AdsKey),
    CONSTRAINT FK_date FOREIGN KEY (dateKey) REFERENCES ADSBI.DIM_date(DAteKey),
    CONSTRAINT FK_Customer FOREIGN KEY (CustomerKey) REFERENCES ADSBI.DIM_CUSTOMER(CustomerKey),
    CONSTRAINT FK_Product FOREIGN KEY (ProductKey) REFERENCES ADSBI.DIM_PRODUCT(ProductKey),
    CONSTRAINT FK_Ads FOREIGN KEY (AdsKey) REFERENCES ADSBI.DIM_ADS(AdsKey)
);


---CREATE A STORED PROCEDURE

CREATE OR REPLACE STREAM fact_ads_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS";

CREATE OR REPLACE PROCEDURE load_data_sp()
  returns string
  language javascript
  as     
  $$  
  var result;
  var sqlcommand0 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_PRODUCT;`;
  var sqlcommand1 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_ADS;`;
  var sqlcommand2 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_CUSTOMER;`;
  var sqlcommand3= `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.FACT_ADS;`;
  var sqlcommand4 = `INSERT INTO ADSBI.DIM_ADS (AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click) 
  SELECT AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click FROM Adsbi.AdsHeaderDetails;`;
  var sqlcommand5 = `INSERT INTO ADSBI.DIM_CUSTOMER (CustomerID,CustomerName,Gender,Age,Income,City,Region) 
  SELECT CustomerID,CustomerName,Gender,Age,Income,City,Region FROM AdsBi.CustomerDetails;`;
  var sqlcommand6= ` INSERT INTO ADSBI.DIM_PRODUCT(ProductID, ProductName,Cost,Price) 
  SELECT ProductID, ProductName,Cost,Price FROM AdsBI.ProductDetails;`;
  var sqlcommand7 = `INSERT INTO ADSBI.FACT_ADS(DateKey,CustomerKey,ProductKey,AdsKey, TimeOnAdSite, DailySpentOnPlaftForm,ClickTimes, NumberOfBoughtProduct, IsBoughtFlag) 
  SELECT dimdate.DateKey, customer.Customerkey, product.productkey, ads.adskey, transact.TimeOnAdSite, transact.DailySpentOnPlaftForm,transact.ClickTimes, transact.NumberOfBoughtProduct,
        CASE
        WHEN transact.NumberOfBoughtProduct >0 THEN True
        WHEN transact.NumberOfBoughtProduct <1 THEN False
        END
        AS IsBoughtFlag
 FROM fact_ads_stream AS transact
 JOIN adsbi.dim_ads AS ads ON (transact.adsid=ads.adsid)
 JOIN adsbi.dim_product AS product ON (transact.productid=product.productid)
 JOIN adsbi.dim_customer AS customer ON (transact.customerid=customer.customerid)
 JOIN adsbi.dim_date AS dimdate ON (transact.date=dimdate.date)
 WHERE transact.METADATA$ACTION = 'INSERT';`;

 try {
    snowflake.execute({sqlText: sqlcommand0 });        
    snowflake.execute({sqlText: sqlcommand1 });
    snowflake.execute({sqlText: sqlcommand2 });
    snowflake.execute({sqlText: sqlcommand3 });
    snowflake.execute({sqlText: sqlcommand4 });
    snowflake.execute({sqlText: sqlcommand5 });
    snowflake.execute({sqlText: sqlcommand6 });
    snowflake.execute({sqlText: sqlcommand7 });
    result = "Succeeded"
 }
 catch(err) {
 result = "Failed" + err;
 }
 return result;
  $$
;

CREATE OR REPLACE TASK ETL_To_WH
WAREHOUSE = FA_PROJECT01_CLOUDDW
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('fact_ads_stream')
AS
call load_data_sp();
ALTER TASK ETL_To_WH RESUME;


-----CREATE STORED PROCEDURE TO UNLOAD DATA


CREATE OR REPLACE STREAM unload_dimads_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS";
CREATE OR REPLACE STREAM unload_product_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT";
CREATE OR REPLACE STREAM unload_customer_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER";
CREATE OR REPLACE STREAM unload_factads_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS";

CREATE OR REPLACE PROCEDURE my_unload_sp()
  RETURNS string 
  LANGUAGE javascript
  as
  $$
    var result;
    var sql_command0 = `CREATE OR REPLACE TEMPORARY TABLE tmp_table_dimads AS SELECT ADSKEY,ADSID,ADSNAME,ADSCATEGORY,ADSPLATFORM,STANDARDCOST,COST_PER_CLICK FROM unload_dimads_stream WHERE metadata$action = 'INSERT';`;
    var sql_command1 = `COPY INTO @adsbi.%dim_ads from tmp_table_dimads file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true  overwrite=true;`;
    var sql_command2 = `GET @adsbi.%dim_ads file://D:\unload\table2;`;
    
    var sql_command3 = `CREATE OR REPLACE TEMPORARY TABLE tmp_table_product AS SELECT PRODUCTKEY,PRODUCTID,PRODUCTNAME,COST,PRICE FROM unload_product_stream WHERE metadata$action = 'INSERT';`;
    var sql_command4 = `COPY INTO @adsbi.%dim_product from tmp_table_product file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true  overwrite=true;`;
    var sql_command5 = `GET @adsbi.%dim_product file://D:\unload\table2`;
    
    var sql_command6 = `CREATE OR REPLACE TEMPORARY TABLE tmp_table_customer AS SELECT CUSTOMERKEY,CUSTOMERID,CUSTOMERNAME,GENDER,AGE,INCOME,CITY,REGION FROM unload_customer_stream WHERE metadata$action = 'INSERT';`;
    var sql_command7 = `COPY INTO @adsbi.%dim_customer from tmp_table_customer file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true  overwrite=true;`;
    var sql_command8 = `GET @adsbi.%dim_customer file://D:\unload\table3`;
    
    var sql_command9 = `CREATE OR REPLACE TEMPORARY TABLE tmp_table_factads AS SELECT DATEKEY,CUSTOMERKEY,PRODUCTKEY,ADSKEY,TIMEONADSITE,DAILYSPENTONPLATFORM,CLICKTIMES,NUMBEROFBOUGHTPRODUCT,ISBOUGHTFLAG FROM unload_factads_stream WHERE metadata$action = 'INSERT';`;
    var sql_command10 = `COPY INTO @adsbi.%fact_ads from tmp_table file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true  overwrite=true;`;
    var sql_command11 = `GET @adsbi.%fact_ads file://D:\unload\table4`;
    
    try {
    snowflake.execute ({sqlText: sql_command0});
    snowflake.execute ({sqlText: sql_command1});
    snowflake.execute ({sqlText: sql_command2});
    snowflake.execute ({sqlText: sql_command3});
    snowflake.execute ({sqlText: sql_command4});
    snowflake.execute ({sqlText: sql_command5});
    snowflake.execute ({sqlText: sql_command6});
    snowflake.execute ({sqlText: sql_command7});
    snowflake.execute ({sqlText: sql_command8});
    snowflake.execute ({sqlText: sql_command9});
    snowflake.execute ({sqlText: sql_command10});
     snowflake.execute ({sqlText: sql_command11});
    result = "Succeeded";
    }
    catch (err) {
    result = "Failed"+err;
    }
    return result;
  $$;

CREATE TASK unload_data_task
  WAREHOUSE = FA_PROJECT01_CLOUDDW
  SCHEDULE = '1 minute'
  WHEN SYSTEM$STREAM_HAS_DATA('unload_dimads_stream')
AS
  CALL my_unload_sp();
