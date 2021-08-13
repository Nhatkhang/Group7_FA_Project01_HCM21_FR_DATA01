-- Set up Warehouse
Alter session set date_input_format='yyyy-mm-dd';
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
-- set up table

-- Create Trigger

-- Set up Snowpipe

-- Task