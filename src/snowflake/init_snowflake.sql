-- Set up Warehouse
CREATE WAREHOUSE FA_Project01_CloudDW WITH 
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
CREATE DATABASE FA_Project01_DB;
/********************CREATE SCHEMA***************************/
USE FA_Project01_CloudDB;
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

-- Create Trigger

-- Set up Snowpipe
-- create pipe and change accessibility
create pipe AdsBI.AdsPipe if not exists as copy into AdsBI.AdsHeaderDetails from @AdsBI.%AdsHeaderDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.ProductPipe if not exists as copy into AdsBI.ProductDetails from @AdsBI.%ProductDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.CustomerPipe if not exists as copy into AdsBI.CustomerDetails from @AdsBI.%CustomerDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
create pipe AdsBI.TransactionPipe if not exists as copy into AdsBI.AdsTransactionDetails from @AdsBI.%AdsTransactionDetails FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8') ON_ERROR = SKIP_FILE;
grant ownership on pipe AdsBI.AdsPipe to role accountadmin;
grant ownership on pipe AdsBI.ProductPipe to role accountadmin;
grant ownership on pipe AdsBI.CustomerPipe to role accountadmin;
grant ownership on pipe AdsBI.TransactionPipe to role accountadmin;
-- Task