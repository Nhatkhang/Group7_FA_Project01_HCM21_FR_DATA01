-- SET UP WAREHOUSE
CREATE OR REPLACE WAREHOUSE FA_Project01_CloudDW_LOADING WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE FA_Project01_CloudDW_TRANSFORM WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE FA_Project01_CloudDW_BI WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

-- SET UP DATABASE
-- CREATE DATABASE
CREATE OR REPLACE DATABASE FA_Project01_DB;

CREATE SCHEMA AdsBI;

-- TABLES for Staging
CREATE OR REPLACE TABLE AdsBI.AdsHeaderDetails (
    AdsID INT NOT NULL,
    AdsName NVARCHAR(30) NOT NULL,
    AdsCategory NVARCHAR(100) NOT NULL,
    AdsPlatform NVARCHAR(100) NOT NULL,
    StandardCost FLOAT(2) NOT NULL,
    Cost_Per_Click FLOAT(2) NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsID)
);

CREATE OR REPLACE TABLE AdsBI.CustomerDetails (
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(100) NOT NULL,
    Gender NVARCHAR(10) NOT NULL,
    Email NVARCHAR(100) NOT NULL,
    Address NVARCHAR(100) NOT NULL,
    Age INT NOT NULL,
    Income INT NOT NULL,
    City NVARCHAR(50) NOT NULL,
    Region NVARCHAR(100) NOT NULL,
    RegisteredDate DATE NOT NULL,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
);

CREATE OR REPLACE TABLE AdsBI.ProductDetails (
    ProductID INT NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    ProductColor NVARCHAR(100) NOT NULL,
    Cost FLOAT(2) NOT NULL,
    Price FLOAT(2) NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);

CREATE OR REPLACE TABLE AdsBI.AdsTransactionDetails(
    Date DATE NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    AdsID INT NOT NULL,
    TimeOnAdSite INT NOT NULL,
    DailySpentOnPlatForm FLOAT (2) NOT NULL,
    ClickTimes TINYINT NOT NULL,
    NumberOfBoughtProduct TINYINT NOT NULL,
    PurchaseRate FLOAT(2) NULL,
    CONSTRAINT PK_AdsFACT PRIMARY KEY (CustomerID, ProductID, AdsID),
    CONSTRAINT FK_Customer FOREIGN KEY (CustomerID) REFERENCES AdsBI.CustomerDetails(CustomerID),
    CONSTRAINT FK_Product FOREIGN KEY (ProductID) REFERENCES AdsBI.ProductDetails(ProductID),
    CONSTRAINT FK_Ads FOREIGN KEY (AdsID) REFERENCES AdsBI.AdsHeaderDetails(AdsID)
);

-- CREATE DIM/FACT TABLES
CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_PRODUCT" (
    ProductKey INT IDENTITY(1, 1),
    ProductID INT NOT NULL,
    ProductName NVARCHAR(200) NOT NULL,
    ProductCategory NVARCHAR(200) NOT NULL,
    Cost NUMBER NOT NULL,
    Price NUMBER NOT NULL,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductKey)
);

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_ADS" (
    AdsKey INT IDENTITY(1, 1),
    AdsID INT NOT NULL,
    AdsName NVARCHAR(30) NOT NULL,
    AdsCategory NVARCHAR(100) NOT NULL,
    AdsPlatform NVARCHAR(100) NOT NULL,
    StandardCost NUMBER NOT NULL,
    Cost_Per_Click FLOAT NOT NULL,
    CONSTRAINT PK_AdsDIM PRIMARY KEY (AdsKey)
);

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_CUSTOMER" (
    CustomerKey INT identity(1, 1),
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(100) NOT NULL,
    Gender NVARCHAR(10) NOT NULL,
    Age INT NOT NULL,
    Income INT NOT NULL,
    City NVARCHAR(50) NOT NULL,
    Region NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (Customerkey)
);

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."DIM_DATE" (
    DATEKEY INT NOT NULL,
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

CREATE OR REPLACE TABLE "FA_PROJECT01_DB"."ADSBI"."FACT_ADS"(
    DateKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    AdsKey INT NOT NULL,
    TimeOnAdSite INT NOT NULL,
    DailySpentOnPlatForm FLOAT NOT NULL,
    ClickTimes TINYINT NOT NULL,
    NumberOfBoughtProduct TINYINT NOT NULL,
    IsBoughtFlag BOOLEAN NULL,
    CONSTRAINT pk_adsfact PRIMARY KEY (DateKey, CustomerKey, ProductKey, AdsKey),
    CONSTRAINT FK_date FOREIGN KEY (dateKey) REFERENCES ADSBI.DIM_date(DAteKey),
    CONSTRAINT FK_Customer FOREIGN KEY (CustomerKey) REFERENCES ADSBI.DIM_CUSTOMER(CustomerKey),
    CONSTRAINT FK_Product FOREIGN KEY (ProductKey) REFERENCES ADSBI.DIM_PRODUCT(ProductKey),
    CONSTRAINT FK_Ads FOREIGN KEY (AdsKey) REFERENCES ADSBI.DIM_ADS(AdsKey)
);

-- CREATE CSV FILE FORMAT
CREATE OR REPLACE FILE FORMAT csv_format TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8';

-- CREATE INTERNAL STAGE
CREATE OR REPLACE STAGE FA_Project01_DB.AdsBI.AdsHeaderDetails_stage;

CREATE OR REPLACE STAGE FA_Project01_DB.AdsBI.CustomerDetails_stage;

CREATE OR REPLACE STAGE FA_Project01_DB.AdsBI.ProductDetails_stage;

CREATE OR REPLACE STAGE FA_Project01_DB.AdsBI.AdsTransactionDetails_stage;

-- SETUP SNOWPIPE
CREATE OR REPLACE PIPE FA_Project01_DB.AdsBI.AdsHeaderDetails_pipe 
AS COPY INTO FA_Project01_DB.AdsBI.AdsHeaderDetails
FROM
    (
        SELECT
            t.*
        FROM
            @FA_Project01_DB.AdsBI.AdsHeaderDetails_stage t
    ) 
FILE_FORMAT = csv_format ON_ERROR = SKIP_FILE;

CREATE OR REPLACE PIPE FA_Project01_DB.AdsBI.CustomerDetails_pipe 
AS COPY INTO FA_Project01_DB.AdsBI.CustomerDetails
FROM
    (
        SELECT
            t.*
        FROM
            @FA_Project01_DB.AdsBI.CustomerDetails_stage t
    ) 
FILE_FORMAT = csv_format ON_ERROR = SKIP_FILE;

CREATE OR REPLACE PIPE FA_Project01_DB.AdsBI.ProductDetails_pipe 
AS COPY INTO FA_Project01_DB.AdsBI.ProductDetails
FROM
    (
        SELECT
            t.*
        FROM
            @FA_Project01_DB.AdsBI.ProductDetails_stage t
    ) 
FILE_FORMAT = csv_format ON_ERROR = SKIP_FILE;

CREATE OR REPLACE PIPE FA_Project01_DB.AdsBI.AdsTransactionDetails_pipe 
AS COPY INTO FA_Project01_DB.AdsBI.AdsTransactionDetails
FROM
    (
        SELECT
            t.*
        FROM
            @FA_Project01_DB.AdsBI.AdsTransactionDetails_stage t
    ) 
FILE_FORMAT = csv_format ON_ERROR = SKIP_FILE;

-- LOAD DATA STREAM
CREATE OR REPLACE STREAM fact_ads_stream ON TABLE "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS";

-- CREATE A STORED PROCEDURE
CREATE OR REPLACE PROCEDURE load_data_sp() 
RETURNS string LANGUAGE javascript 
AS 
$$ 
var result;

var truncate_dim_product = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_PRODUCT;`;

var truncate_dim_ads = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_ADS;`;

var truncate_dim_customer = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.DIM_CUSTOMER;`;

var truncate_fact_ads = `TRUNCATE TABLE FA_PROJECT01_DB.ADSBI.FACT_ADS;`;

var insert_dim_ads = `INSERT INTO ADSBI.DIM_ADS (AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click) 
  SELECT AdsID,AdsName,AdsCategory,AdsPlatform,StandardCost,Cost_Per_Click FROM Adsbi.AdsHeaderDetails;`;

var insert_dim_customer = `INSERT INTO ADSBI.DIM_CUSTOMER (CustomerID,CustomerName,Gender,Age,Income,City,Region) 
  SELECT CustomerID,CustomerName,Gender,Age,Income,City,Region FROM AdsBi.CustomerDetails;`;

var insert_dim_product = ` INSERT INTO ADSBI.DIM_PRODUCT(ProductID, ProductName,ProductCategory, Cost,Price) 
  SELECT ProductID, ProductName, ProductCategory, Cost,Price FROM AdsBI.ProductDetails;`;

var insert_fact_ads = `INSERT INTO ADSBI.FACT_ADS(DateKey,CustomerKey,ProductKey,AdsKey, TimeOnAdSite, DailySpentOnPlatForm,ClickTimes, NumberOfBoughtProduct, IsBoughtFlag) 
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

try { snowflake.execute({sqlText: truncate_dim_product});

snowflake.execute({sqlText: truncate_dim_ads});

snowflake.execute({sqlText: truncate_dim_customer});

snowflake.execute({sqlText: truncate_fact_ads});

snowflake.execute({sqlText: insert_dim_ads});

snowflake.execute({sqlText: insert_dim_customer});

snowflake.execute({sqlText: insert_dim_product});

snowflake.execute({sqlText: insert_fact_ads});

result = "Succeeded" } catch(err) { result = "Failed" + err;

} return result;

$$;

CREATE OR REPLACE TASK ETL_To_WH WAREHOUSE = FA_Project01_CloudDW_TRANSFORM SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('fact_ads_stream') AS CALL load_data_sp();

ALTER TASK ETL_To_WH RESUME;