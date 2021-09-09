/*PLEASE PERSONALIZE YOUR INFOR IN CREATE JOBS PART-------------
-----BEFORE RUNNING THIS SCRIPT OR ERRORs WILL SHOW UP*/--------
----------------------------------------------------------------
/********************CREATE A SQL Server Agent PROXY***************************/
-- creates credential   
USE msdb ;  
GO  
CREATE CREDENTIAL CredentialTest WITH 
IDENTITY = 'DESKTOP-NQAEQ4S\Ha Quyen',   
SECRET = '****';  

GO  
-- creates proxy "proxy test" and assigns
-- the credential 'CredentialTest' to it.  
EXEC dbo.sp_add_proxy  
    @proxy_name = ' proxy test',  
    @enabled = 1,  
    @description = '',  
    @credential_name = 'CredentialTest' ;  
GO  

EXEC dbo.sp_grant_proxy_to_subsystem  
    @proxy_name = N'proxy test',  
    @subsystem_id = 11 ;  
GO  
/********************CREATE OPERATOR***************************/
-- sets up the operator information for user 'QUYENMT'
USE msdb ;  
GO  

EXEC dbo.sp_add_operator  
    @name = N'OperatorTest',  
    @enabled = 1,  
    @email_address = N'QUYENMT',
    @pager_address = N'',  
    @weekday_pager_start_time = 080000,  
    @weekday_pager_end_time = 170000,  
    @pager_days = 62 ;  
GO  
/********************CREATE JOBS/SCHEDULE***************************/
USE [msdb]
GO
DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'Rundemo1', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'DESKTOP-NQAEQ4S\Ha Quyen', 
		@notify_email_operator_name=N'OperatorTest', @job_id = @jobId OUTPUT
SELECT @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'Rundemo1', @server_name = N'DESKTOP-NQAEQ4S'

GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'Rundemo1', @step_name=N'RunMain1', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/ISSERVER "\"\SSISDB\test\SSIS_ETL_StagingData\AdsBI.dtsx\"" /SERVER "\"DESKTOP-NQAEQ4S\"" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E', 
		@database_name=N'msdb', 
		@flags=0, 
		@proxy_name=N'proxy test'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'Rundemo1', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'DESKTOP-NQAEQ4S\Ha Quyen', 
		@notify_email_operator_name=N'OperatorTest', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'Rundemo1', @name=N'RunWeekly1', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210818, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
SELECT @schedule_id
GO

