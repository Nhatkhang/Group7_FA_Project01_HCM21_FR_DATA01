-- SET UP WAREHOUSE
CREATE
or REPLACE WAREHOUSE FA_Project01_CloudDW_LOADING WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE
or REPLACE WAREHOUSE FA_Project01_CloudDW_TRANSFORM WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE
or REPLACE WAREHOUSE FA_Project01_CloudDW_BI WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

-- SET UP DATABASE
-- CREATE DATABASE
CREATE
or REPLACE DATABASE FA_Project01_DB;

CREATE SCHEMA AdsBI;

-- TABLES for Staging
CREATE
or replace TABLE AdsBI.AdsHeaderDetails (
    AdsID int NOT NULL,
    AdsName nvarchar(30) NOT NULL,
    AdsCategory nvarchar(100) NOT NULL,
    AdsPlatform nvarchar(100) NOT NULL,
    StandardCost float(2) NOT NULL,
    Cost_Per_Click float(2) NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsID)
);

CREATE
or replace TABLE AdsBI.CustomerDetails (
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

CREATE
or replace TABLE AdsBI.ProductDetails (
    ProductID int NOT NULL,
    ProductName nvarchar(200) NOT NULL,
    ProductCategory nvarchar(200) NOT NULL,
    ProductColor nvarchar(100) NOT NULL,
    Cost float(2) NOT NULL,
    Price float(2) NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);

CREATE
OR REPLACE TABLE AdsBI.AdsTransactionDetails(
    Date date NOT NULL,
    CustomerID int NOT NULL,
    ProductID int NOT NULL,
    AdsID int NOT NULL,
    TimeOnAdSite int NOT NULL,
    DailySpentOnPlatForm float (2) NOT NULL,
    ClickTimes tinyint NOT NULL,
    NumberOfBoughtProduct tinyint NOT NULL,
    PurchaseRate float (2) NULL,
    CONSTRAINT PK_AdsFACT PRIMARY KEY (CustomerID, ProductID, AdsID),
    CONSTRAINT FK_Customer FOREIGN KEY (CustomerID) REFERENCES AdsBI.CustomerDetails(CustomerID),
    CONSTRAINT FK_Product FOREIGN KEY (ProductID) REFERENCES AdsBI.ProductDetails(ProductID),
    CONSTRAINT FK_Ads FOREIGN KEY (AdsID) REFERENCES AdsBI.AdsHeaderDetails(AdsID)
);

-- CREATE DIM/FACT TABLES
CREATE
OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT" (
    ProductKey int identity(1, 1),
    ProductID int NOT NULL,
    ProductName nvarchar(200) NOT NULL,
    ProductCategory nvarchar(200) NOT NULL,
    Cost number NOT NULL,
    Price number NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductKey)
);

CREATE
OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS" (
    AdsKey int identity(1, 1),
    AdsID int NOT NULL,
    AdsName nvarchar(30) NOT NULL,
    AdsCategory nvarchar(100) NOT NULL,
    AdsPlatform nvarchar(100) NOT NULL,
    StandardCost number NOT NULL,
    Cost_Per_Click float NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsKey)
);

CREATE
OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER" (
    CustomerKey int identity(1, 1),
    CustomerID int NOT NULL,
    CustomerName nvarchar(100) NOT NULL,
    Gender nvarchar(10) NOT NULL,
    Age int NOT NULL,
    Income int NOT NULL,
    City nvarchar(50) NOT NULL,
    Region nvarchar(100) NOT NULL,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (Customerkey)
);

CREATE
OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_DATE" (
    DATEKEY int NOT NULL,
    DATE DATE NOT NULL,
    DAYOFMONTH SMALLINT NOT NULL,
    WEEKDAYNAME VARCHAR(10) NOT NULL,
    WEEK SMALLINT NOT NULL,
    DAYOFWEEK VARCHAR(9) NOT NULL,
    MONTH SMALLINT NOT NULL,
    MONTHNAME CHAR(3) NOT NULL,
    QUARTER SMALLINT NOT NULL,
    YEAR SMALLINT NOT NULL,
    CONSTRAINT PK_DateDim PRIMARY KEY (DATEKEY)
) AS WITH CTE_MY_DATE AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2017-01-01') AS DATEKEY
    FROM
        TABLE(GENERATOR(ROWCOUNT => 2000))
)
SELECT
    TO_CHAR(DATE(DATEKEY), 'YYYYMMDD'),
    DATE(DATEKEY),
    DAY(DATEKEY),
    DECODE(
        DAYNAME(DATEKEY),
        'Mon',
        'Monday',
        'Tue',
        'Tuesday',
        'Wed',
        'Wednesday',
        'Thu',
        'Thursday',
        'Fri',
        'Friday',
        'Sat',
        'Saturday',
        'Sun',
        'Sunday'
    ),
    WEEKOFYEAR(DATEKEY),
    DAYOFWEEK(DATEKEY),
    MONTH(DATEKEY),
    MONTHNAME(DATEKEY),
    QUARTER(DATEKEY),
    YEAR(DATEKEY)
FROM
    CTE_MY_DATE;

CREATE
OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS"(
    DateKey int NOT NULL,
    CustomerKey int NOT NULL,
    ProductKey int NOT NULL,
    AdsKey int NOT NULL,
    TimeOnAdSite int NOT NULL,
    DailySpentOnPlatForm float NOT NULL,
    ClickTimes tinyint NOT NULL,
    NumberOfBoughtProduct tinyint NOT NULL,
    IsBoughtFlag boolean NULL,
    CONSTRAINT pk_adsfact PRIMARY KEY (DateKey, CustomerKey, ProductKey, AdsKey),
    CONSTRAINT FK_date FOREIGN KEY (dateKey) REFERENCES ADSBI.DIM_date(DAteKey),
    CONSTRAINT FK_Customer FOREIGN KEY (CustomerKey) REFERENCES ADSBI.DIM_CUSTOMER(CustomerKey),
    CONSTRAINT FK_Product FOREIGN KEY (ProductKey) REFERENCES ADSBI.DIM_PRODUCT(ProductKey),
    CONSTRAINT FK_Ads FOREIGN KEY (AdsKey) REFERENCES ADSBI.DIM_ADS(AdsKey)
);

-- CREATE CSV FILE FORMAT
CREATE
OR REPLACE FILE FORMAT csv_format TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8';

-- CREATE INTERNAL STAGE
create
or replace stage FA_Project01_DB.AdsBI.AdsHeaderDetails_stage;

create
or replace stage FA_Project01_DB.AdsBI.CustomerDetails_stage;

create
or replace stage FA_Project01_DB.AdsBI.ProductDetails_stage;

create
or replace stage FA_Project01_DB.AdsBI.AdsTransactionDetails_stage;

-- SETUP SNOWPIPE
create
or replace pipe FA_Project01_DB.AdsBI.AdsHeaderDetails_pipe as copy into FA_Project01_DB.AdsBI.AdsHeaderDetails
from
    (
        select
            t.*
        from
            @FA_Project01_DB.AdsBI.AdsHeaderDetails_stage t
    ) file_format = csv_format ON_ERROR = SKIP_FILE;

create
or replace pipe FA_Project01_DB.AdsBI.CustomerDetails_pipe as copy into FA_Project01_DB.AdsBI.CustomerDetails
from
    (
        select
            t.*
        from
            @FA_Project01_DB.AdsBI.CustomerDetails_stage t
    ) file_format = csv_format ON_ERROR = SKIP_FILE;

create
or replace pipe FA_Project01_DB.AdsBI.ProductDetails_pipe as copy into FA_Project01_DB.AdsBI.ProductDetails
from
    (
        select
            t.*
        from
            @FA_Project01_DB.AdsBI.ProductDetails_stage t
    ) file_format = csv_format ON_ERROR = SKIP_FILE;

create
or replace pipe FA_Project01_DB.AdsBI.AdsTransactionDetails_pipe as copy into FA_Project01_DB.AdsBI.AdsTransactionDetails
from
    (
        select
            t.*
        from
            @FA_Project01_DB.AdsBI.AdsTransactionDetails_stage t
    ) file_format = csv_format ON_ERROR = SKIP_FILE;

-- LOAD DATA STREAM
CREATE
OR REPLACE STREAM fact_ads_stream ON TABLE "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS";

-- CREATE A STORED PROCEDURE
CREATE
OR REPLACE PROCEDURE load_data_sp() returns string language javascript as $$ var result;

var sqlcommand0 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_PRODUCT;`;

var sqlcommand1 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_ADS;`;

var sqlcommand2 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_CUSTOMER;`;

var sqlcommand3 = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.FACT_ADS;`;

var sqlcommand4 = `INSERT INTO ADSBI.DIM_ADS (AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click) 
  SELECT AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click FROM Adsbi.AdsHeaderDetails;`;

var sqlcommand5 = `INSERT INTO ADSBI.DIM_CUSTOMER (CustomerID,CustomerName,Gender,Age,Income,City,Region) 
  SELECT CustomerID,CustomerName,Gender,Age,Income,City,Region FROM AdsBi.CustomerDetails;`;

var sqlcommand6 = ` INSERT INTO ADSBI.DIM_PRODUCT(ProductID, ProductName,ProductCategory, Cost,Price) 
  SELECT ProductID, ProductName, ProductCategory, Cost,Price FROM AdsBI.ProductDetails;`;

var sqlcommand7 = `INSERT INTO ADSBI.FACT_ADS(DateKey,CustomerKey,ProductKey,AdsKey, TimeOnAdSite, DailySpentOnPlatForm,ClickTimes, NumberOfBoughtProduct, IsBoughtFlag) 
  SELECT dimdate.DateKey, customer.Customerkey, product.productkey, ads.adskey, transact.TimeOnAdSite, transact.DailySpentOnPlatForm,transact.ClickTimes, transact.NumberOfBoughtProduct,
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

try { snowflake.execute({ sqlText: sqlcommand0 });

snowflake.execute({ sqlText: sqlcommand1 });

snowflake.execute({ sqlText: sqlcommand2 });

snowflake.execute({ sqlText: sqlcommand3 });

snowflake.execute({ sqlText: sqlcommand4 });

snowflake.execute({ sqlText: sqlcommand5 });

snowflake.execute({ sqlText: sqlcommand6 });

snowflake.execute({ sqlText: sqlcommand7 });

result = "Succeeded" } catch(err) { result = "Failed" + err;

} return result;

$$;

CREATE
OR REPLACE TASK ETL_To_WH WAREHOUSE = FA_Project01_CloudDW_TRANSFORM SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('fact_ads_stream') AS call load_data_sp();

ALTER TASK ETL_To_WH RESUME;