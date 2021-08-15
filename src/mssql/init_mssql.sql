--MAIN SCRIPT--
SET NOCOUNT ON
/********************CREATE DATABASE***************************/
--CREATE DATABASE
CREATE DATABASE FA_Project01_DB;
GO
/********************CREATE SCHEMA***************************/
USE [FA_Project01_DB];
GO
CREATE SCHEMA AdsBI;
GO
/********************CREATE TABLES***************************/

-- TABLES
CREATE TABLE [AdsBI].[AdsHeaderDetails] (
	[AdsID] [int] NOT NULL,
	[AdsName] [nvarchar](30) NOT NULL,
	[AdsCategory] [nvarchar](100) NOT NULL,
	[AdsPlatform] [nvarchar](100) NOT NULL,
	[StandardCost] [money] NOT NULL,
	[Cost_Per_Click] [money] NOT NULL,
	[ValidFlag] [Bit],
	CONSTRAINT PK_AdsDIM PRIMARY KEY ([AdsID])
);
CREATE TABLE [AdsBI].[CustomerDetails] (
	[CustomerID] [int] NOT NULL,
	[CustomerName] [nvarchar](100) NOT NULL,
	[Gender] [nvarchar](10) NOT NULL,
	[Email] [nvarchar](100) NOT NULL,
	[Address] [nvarchar](100) NOT NULL,
	[Age] [int] NOT NULL,
	[Income] [int] NOT NULL,
	[City] [nvarchar](50) NOT NULL,
	[Region] [nvarchar](100) NOT NULL,
	[RegisteredDate] [date] NOT NULL,
	[ValidFlag] [Bit],
	CONSTRAINT PK_CustomerDIM PRIMARY KEY (CustomerID)
);
CREATE TABLE [AdsBI].[ProductDetails] (
	[ProductID] [int] NOT NULL,
	[ProductName] [nvarchar](200) NOT NULL,
	[ProductCategory] [nvarchar](200) NOT NULL,
	[ProductColor] [nvarchar](100) NOT NULL,
	[Cost] [money] NOT NULL,
	[Price] [money] NOT NULL,
	[ValidFlag] [Bit],
	CONSTRAINT PK_ProductDIM PRIMARY KEY (ProductID)
);
CREATE TABLE [AdsBI].[AdsTransactionDetails](
	[Date] [date] NOT NULL,
	[CustomerID] [int] NOT NULL,
	[ProductID] [int] NOT NULL,
	[AdsID] [int] NOT NULL,
	[TimeOnAdSite] [int] NOT NULL,
	[DailySpentOnPlaftForm] [float] (2) NOT NULL,
	[ClickTimes] [tinyint] NOT NULL,
	[NumberOfBoughtProduct] [tinyint] NOT NULL,
	[PurchaseRate] [float] (2) NULL,
	CONSTRAINT PK_AdsFACT PRIMARY KEY (CustomerID, ProductID, AdsID),
	CONSTRAINT FK_Customer FOREIGN KEY (CustomerID) REFERENCES [AdsBI].[CustomerDetails](CustomerID),
	CONSTRAINT FK_Product FOREIGN KEY (ProductID) REFERENCES [AdsBI].[ProductDetails](ProductID),
	CONSTRAINT FK_Ads FOREIGN KEY (AdsID) REFERENCES [AdsBI].[AdsHeaderDetails](AdsID),
);
/********************CREATE VIEW***************************/
/********************CREATE LOGEVENT***************************/
CREATE TABLE [AdsBI].[EventLog](
	[IDlog] [int] IDENTITY(1,1) NOT NULL,
	[Package] [nvarchar] (100) NOT NULL,
	[Task] [nvarchar] (100) NOT NULL,
	[EventDescription] [nvarchar] (max) NOT NULL,
	[Timelog] [date] NULL,
);




