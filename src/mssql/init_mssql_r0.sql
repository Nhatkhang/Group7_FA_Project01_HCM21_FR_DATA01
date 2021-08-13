/*PLEASE PERSONALIZE YOUR INFOR IN CREATE JOBS PART-------------
-----BEFORE RUNNING THIS SCRIPT OR ERRORs WILL SHOW UP*/--------
----------------------------------------------------------------
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
/********************CREATE JOBS/SCHEDULE***************************/
USE [msdb]
GO
DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'runDemo', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'FSOFT.FPT.VN\KhangNHN', 
		@notify_email_operator_name=N'FakeOperator', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'runDemo', @server_name = N'CVPKHANGNHN\KHANGNHN2019'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'runDemo', @step_name=N'RunMain', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\RunSSIS_StagingETL\SSIS_ETL_StagingData\AdsBI.dtsx\"" /SERVER "\"CVPKHANGNHN\KHANGNHN2019\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'master', 
		@flags=0
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'runDemo', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'FSOFT.FPT.VN\KhangNHN', 
		@notify_email_operator_name=N'FakeOperator', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'runDemo', @name=N'RunWeekly', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210820, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
/********************CREATE VIEW***************************/
/********************CREATE LOGEVENT***************************/
CREATE TABLE [AdsBI].[EventLog](
	[IDlog] [int] IDENTITY(1,1) NOT NULL,
	[Package] [nvarchar] (100) NOT NULL,
	[Task] [nvarchar] (100) NOT NULL,
	[EventDescription] [nvarchar] (max) NOT NULL,
	[Timelog] [date] NULL,
);




