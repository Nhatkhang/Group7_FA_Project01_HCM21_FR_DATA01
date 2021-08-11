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
-- Create Trigger

-- Set up Snowpipe

-- Task
