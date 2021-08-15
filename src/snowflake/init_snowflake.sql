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
CREATE TABLE AdsBI.AdsHeaderDetails (
	AdsID int NOT NULL,
	AdsName nvarchar(30) NOT NULL,
	AdsCategory nvarchar(100) NOT NULL,
	AdsPlatform nvarchar(100) NOT NULL,
	StandardCost float(2) NOT NULL,
	Cost_Per_Click float(2) NOT NULL,
	ValidFlag Binary,
	CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsID)
);
CREATE TABLE AdsBI.CustomerDetails (
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
	ValidFlag Binary,
	CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
);
CREATE TABLE AdsBI.ProductDetails (
	ProductID int NOT NULL,
	ProductName nvarchar(200) NOT NULL,
	ProductCategory nvarchar(200) NOT NULL,
	ProductColor nvarchar(100) NOT NULL,
	Cost float(2) NOT NULL,
	Price float(2) NOT NULL,
	ValidFlag Binary,
	CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);
CREATE TABLE AdsBI.AdsTransactionDetails(
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

-- Set up Snowpipe
-- create pipe and change accessibility
ALTER PIPE AdsBI.AdsPipe  SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE AdsBI.ProductPipe SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE AdsBI.CustomerPipe SET PIPE_EXECUTION_PAUSED=true;
ALTER PIPE AdsBI.TransactionPipe SET PIPE_EXECUTION_PAUSED=true;

create pipe AdsBI.AdsPipe if not exists as copy into AdsBI.AdsHeaderDetails from @AdsBI.%AdsHeaderDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.ProductPipe if not exists as copy into AdsBI.ProductDetails from @AdsBI.%ProductDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.CustomerPipe if not exists as copy into AdsBI.CustomerDetails from @AdsBI.%CustomerDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.TransactionPipe if not exists as copy into AdsBI.AdsTransactionDetails from @AdsBI.%AdsTransactionDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
grant ownership on pipe AdsBI.AdsPipe to role accountadmin;
grant ownership on pipe AdsBI.ProductPipe to role accountadmin;
grant ownership on pipe AdsBI.CustomerPipe to role accountadmin;
grant ownership on pipe AdsBI.TransactionPipe to role accountadmin;
--Create dim/fact tables in warehouse
CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT"
    (ProductKey int identity(1,1),
    ProductID int NOT NULL,
    ProductName nvarchar(200) NOT NULL,
    Cost number NOT NULL,
    Price number NOT NULL,
    ValidFlag boolean NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductKey));
   
CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS" (
    AdsKey int identity(1,1),
    AdsID int NOT NULL,
    AdsName nvarchar(30) NOT NULL ,
    AdsCategory nvarchar(100) NOT NULL,
    AdsPlatform nvarchar(100) NOT NULL,
    StandardCost number NOT NULL,
    Cost_Per_Click float NOT NULL,
    ValidFlag boolean NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsKey)
);

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER" (
    CustomerKey int identity(1,1),
    CustomerID int NOT NULL,
    CustomerName nvarchar(100) NOT NULL,
    Gender nvarchar(10) NOT NULL,
    Age int NOT NULL,
    Email nvarchar(100) NOT NULL,
    Address nvarchar(100) NOT NULL,
    Income int NOT NULL,
    City nvarchar(50) NOT NULL,
    Region nvarchar(100) NOT NULL,
    RegisteredDate date NOT NULL,
    ValidFlag boolean,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
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
    DateKey char(8) NOT NULL,
    CustomerKey int NOT NULL,
    ProductKey int NOT NULL,
    AdsKey int NOT NULL,
    TimeOnAdSite int NOT NULL,
    DailySpentOnPlaftForm float  NOT NULL,
    ClickTimes tinyint NOT NULL,
    NumberOfBoughtProduct tinyint NOT NULL,
    IsBoughtFlag boolean NULL,
    PRIMARY KEY (DateKey, CustomerKey, ProductKey, AdsKey),
	CONSTRAINT FK_Customer FOREIGN KEY (CustomerKey) REFERENCES ADSBI.DIM_CUSTOMER(CustomerKey),
	CONSTRAINT FK_Product FOREIGN KEY (ProductKey) REFERENCES ADSBI.DIM_PRODUCT(ProductKey),
	CONSTRAINT FK_Ads FOREIGN KEY (AdsKey) REFERENCES ADSBI.DIM_ADS(AdsKey)
);

---CREATE AUTO TASK TO LOAD DATA
CREATE OR REPLACE STREAM load_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS"

CREATE OR REPLACE PROCEDURE load_data_sp()
  returns float not null
  language javascript
  as     
  $$  

  var sqlcommand = "
TRUNCATE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT";
TRUNCATE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS";
TRUNCATE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER";
TRUNCATE TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS";

INSERT INTO "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT" (ProductID,ProductName,Cost,Price,ValidFlag)
SELECT ProductID,ProductName,Cost,Price,hex_decode_string(to_char(ValidFlag)) FROM "FA_PROJECT01_DB"."ADSBI"."PRODUCTDETAILS";
INSERT INTO "FA_PROJECT01_DB"."ADSBI"."DIM_ADS" (AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click,ValidFlag)
SELECT AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click,hex_decode_string(to_char(ValidFlag))
FROM "FA_PROJECT01_DB"."ADSBI"."ADSHEADERDETAILS";
INSERT INTO "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER" (CustomerID,CustomerName,Gender,Age,Income,City,Region,ValidFlag)
SELECT CustomerID,CustomerName,Gender,Age,Income,City,Region,hex_decode_string(to_char(ValidFlag))
FROM "FA_PROJECT01_DB"."ADSBI"."CUSTOMERDETAILS";
CREATE OR REPLACE TEMPORARY TABLE tmp_table AS SELECT TO_CHAR(DATE(DATE),'YYYYMMDD') AS DATEKEY,CUSTOMERID AS CUSTOMERKEY,PRODUCTID AS PRODUCTKEY,ADSID AS ADSKEY,TIMEONADSITE, DAILYSPENTONPLATFORM ,CLICKTIMES, NUMBEROFBOUGHTPRODUCT,
IFF(NUMBEROFBOUGHTPRODUCT>0,TRUE,FALSE) AS ISBOUGHTFLAG FROM "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS";

INSERT INTO "FA_PROJECT01_DB"."ADSBI"."FACT_ADS"(DATEKEY,CUSTOMERKEY,PRODUCTKEY,ADSKEY,DAILYSPENTONPLATFORM, TIMEONADSITE, CLICKTIMES, NUMBEROFBOUGHTPRODUCT,ISBOUGHTFLAG)
SELECT DATEKEY,CUSTOMERKEY,PRODUCTKEY,ADSKEY,DAILYSPENTONPLATFORM,TIMEONADSITE,CLICKTIMES, NUMBEROFBOUGHTPRODUCT,ISBOUGHTFLAG FROM tmp_table;
 "
  var smtmt = snowflake.createStatement(
   {
   sqlText: sql_command,
   });
   RETURN;
  $$
;

CREATE OR REPLACE TASK load_data_task
  warehouse = FA_Project01_CloudDW
  schedule = '1 minute'
  when SYSTEM$STREAM_HAS_DATA('load_stream')
as
  call load_data_sp();
    alter task load_data_task resume


---UNLOAD DATA

--please help input 4 seperate paths for 4 tables and input 

CREATE OR REPLACE STREAM unload_stream
ON TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS";

CREATE OR REPLACE PROCEDURE unload_data_sp()
  returns float not null
  language javascript
  as     
  $$  
  var sqlcommand = "
	copy into @adsbi%dim_ads from ad_raw_stream file_format = (TYPE=CSV compression=none) header= true single=true max_file_size=4900000000;
    get @C:\Users\admin\Downloads\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\src\snowflake\unload_data\table1\* @ADSBI.%AdsHeaderDetails overwrite=true;
	
	copy into @adsbi%dim_ads from ad_raw_stream file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true max_file_size=4900000000;
    get @C:\Users\admin\Downloads\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\src\snowflake\unload_data\table2 @ADSBI.%AdsHeaderDetails overwrite=true;

	copy into @adsbi%dim_ads from ad_raw_stream file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true single=true max_file_size=4900000000;
    get @C:\Users\admin\Downloads\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\src\snowflake\unload_data\table3 @ADSBI.%AdsHeaderDetails overwrite=true;
	
	copy into @adsbi%dim_ads from ad_raw_stream file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8'compression=none) header= true single=true max_file_size=4900000000;
    get @C:\Users\admin\Downloads\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\Group8_FA_Project01_HCM21_FR_DATA01-khang-bulkload-SQL2SF\src\snowflake\unload_data\table4 @ADSBI.%AdsHeaderDetails overwrite=true;
	
 "
  var smtmt = snowflake.createStatement(
   {
   sqlText: sql_command,
   });
   RETURN;
  $$
;

CREATE OR REPLACE TASK unload_data_task
  warehouse = FA_Project01_CloudDW
  schedule = '1 minute'
  when SYSTEM$STREAM_HAS_DATA('unload_stream')
as
  call unload_data_sp();
  alter task unload_data_task resume


