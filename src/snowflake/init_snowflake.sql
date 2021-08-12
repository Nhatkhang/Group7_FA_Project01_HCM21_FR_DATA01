-- Set up Warehouse
create warehouse FA_Project01_CloudDW;

-- Set up Database
CREATE DATABASE FA_Project01_DB;
CREATE SCHEMA AdsBI;

-- set up table

CREATE TABLE AdsBI.AdsHeaderDetails (
    AdsID int ,
    AdsName nvarchar(30) NOT NULL ,
    AdsCategory nvarchar(100) NOT NULL,
    AdsPlatform nvarchar(100) NOT NULL,
    StandardCost number NOT NULL,
    Cost_Per_Click float NOT NULL,
    ValidFlag boolean NOT NULL,
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
    ValidFlag boolean,
    CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
);
CREATE TABLE AdsBI.ProductDetails (
    ProductID int NOT NULL,
    ProductName nvarchar(200) NOT NULL,
    ProductCategory nvarchar(200) NOT NULL,
    ProductColor nvarchar(100) NOT NULL,
    Cost number NOT NULL,
    Price number NOT NULL,
    ValidFlag boolean,
    CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);
CREATE TABLE AdsBI.AdsTransactionDetails(
    Date datetime NOT NULL,
    CustomerID int NOT NULL,
    ProductID int NOT NULL,
    AdsID int NOT NULL,
    TimeOnAdSite int NOT NULL,
    DailySpentOnPlaftForm float  NOT NULL,
    ClickTimes tinyint NOT NULL,
    NumberOfBoughtProduct tinyint NOT NULL,
    PurchaseRate float NULL,
    CONSTRAINT PK_AdsFACT PRIMARY KEY (CustomerID, ProductID, AdsID)
    
);

-- SET UP DIM/FACT TABLE
CREATE TABLE Ads_DIM
AS SELECT ADSID,ADSNAME,ADSCATEGORY,ADSPLATFORM,STANDARDCOST,COST_PER_CLICK
FROM "FA_PROJECT01_DB"."ADSBI"."ADSHEADERDETAILS"

CREATE TABLE Customer_DIM
AS SELECT CUSTOMERID,CUSTOMERNAME,GENDER,EMAIL,ADDRESS, AGE, INCOME,CITY,REGION 
FROM "FA_PROJECT01_DB"."ADSBI"."CUSTOMERDETAILS"

CREATE TABLE ADS_FACT
AS
SELECT TO_CHAR(DATE(DATE),'YYYYMMDD') AS DATEKEY,CUSTOMERID,PRODUCTID,ADSID,TIMEONADSITE,CLICKTIMES, NUMBEROFBOUGHTPRODUCT,
IFF(NUMBEROFBOUGHTPRODUCT>0,'1','0') AS ISBOUGHTFLAG
FROM "FA_PROJECT01_DB"."ADSBI"."ADSTRANSACTIONDETAILS";



CREATE TABLE Product_DIM
AS SELECT PRODUCTID,PRODUCTNAME,COST 
FROM "FA_PROJECT01_DB"."ADSBI"."PRODUCTDETAILS"


CREATE OR REPLACE TABLE DIM_DATE (
   DATEKEY          CHAR(8)        NOT NULL
   ,DATE          DATE        NOT NULL
   ,DAYOFMONTH       SMALLINT    NOT NULL
   ,WEEKDAYNAME    VARCHAR(10) NOT NULL
   ,WEEK     SMALLINT    NOT NULL
   ,DAYOFWEEK      VARCHAR(9)  NOT NULL
   ,MONTH            SMALLINT    NOT NULL
   ,MONTHNAME       CHAR(3)     NOT NULL
   ,QUARTER          SMALLINT NOT NULL
  ,YEAR             SMALLINT    NOT NULL
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
        
    FROM CTE_MY_DATE
;


-- Create users
CREATE USER trainers
PASSWORD = 'Thisisapassw0rd'
DEFAULT_ROLE = "ACCOUNTADMIN"
MUST_CHANGE_PASSWORD = TRUE;

CREATE USER nkang
PASSWORD = 'Thisisapassw0rdne'
DEFAULT_ROLE = "ACCOUNTADMIN"
MUST_CHANGE_PASSWORD = TRUE;

