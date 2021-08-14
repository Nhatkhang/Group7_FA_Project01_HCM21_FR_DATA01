# PROJECT FA 01

## I. DECSRIPTION OF THIS PROJECT

**Business Question:**
*Social Media- User behavior analysis (based on engagement, content, user's personnal information…) to maximize advertisement income*

***Prototype Solution***: Analysis of the parameters that influence the Ads Income such as: Platform, Ads Categories, .etc. In this report scope, the Ads income includes Standard cost for the Product advertised and the total cost per click charged on user/customer click on Ads.

This project is for initializing and creating data pipeline to answer the aboved Bussiness Question.

## II. WORKING DATA

Data is generated based on Python sript. The database (.csv flat files) will include:

1. CustomerData.csv

    This file includes basics data for a customer such as Customer_ID, Full_Name, Gender, Email,.ect. The list of customer ID is temporary saved in during the generating process.

2. ProductData.csv

    This file includes basics data for a product such as Product_ID, Product_Name, Product_Category, Cost,.ect. The list of product ID is temporary saved in during the generating process.

3. AdsHeaderData.csv

    This file includes basics data for a product such as Ads_ID, Ads_Name, Ads_Category, Platform,.ect. The list of advertisement ID is temporary saved in during the generating process.

4. AdsDetailData.csv

    This file contains trasaction information with the components:
    - Date: Indicates the date of record.
    - Customer_ID: this is ramdomly select from the customer ID list saved aboved to ensure that the data exist in      CustomerData.
    - Product_ID: this is ramdomly select from the product ID list saved aboved to ensure that the data exist in        ProductData.
    - Time_Spent_On_Ads: Average time user/customer spent on a specific Ads for a specific product, unit isminute.
    - Ads_ID: this is ramdomly select from the Advertisement ID list saved aboved to ensure that the data exist in      AdsHeaderData.
    - Daily_Internet_Usage: Average time user/customer spent for the Internet, unit is hour.
    - Number_ClicksAds: Total of clicks user/customer clicked on the Ad.
    - Number_ProductBought: Total number of product user/customer actually bought from the Ad.

**Each of the csv file includes *Modified_Date* to indicate the date Data is created.**

Please note that the Data will be replaced each time the python script is executed. User is highly recommended to change the variable *start_date* and *end_date* in config file to update the data chronologically.

## III. DETAIL OF WORK

0. Generate Data
    - [x] Generate rawdata and copy to $path/Working-Folder
1. Design data pipeline ![ProjectDesign](./docs/Project_Design.png)
    - [x] Create Drawio file
2. Build data model ![DataModel](./docs/data_model_SQL.png)
    - [x] Generate Dim/Facts Tables scheme
3. Ingest data from flat file csv
    - [x] Data Profiling and Data Staging
    - [x] Build a SSIS solution to do ETL and stage data in MSSQL
    - [x] Deploy package into MSSQL
    - [ ] Error Handling
4. Load data onto SNOWFLAKE and do DataWarehousing
5. Visualize your data using PowerBI
6. Update data

## VI. HOW TO SETUP

### Note

Before you run:

- Please ensure that you have snowsql on your machine. Your snowsql config has to be configured with our confidential information we provided and remember to set your role to accountadmin.
- Please install modules: faker (pip -install faker) and snowflake-connector (refer to this link https://docs.snowflake.com/en/user-guide/python-connector-install.html)
  
Then you can run the following:

1. Configure your [config.json](./resources/python_source/config.json) as you wish then generate data [main_gen.py](./resources/python_source/main_gen.py)  to generate data.

2. Login into MSSQL and run [init_mssql.sql](./src/mssql/init_mssql.sql)

3. Open SSIS solution:
    - Update *ConnectServer.DB* to with your servername;
    - Change project parameters to *WorkingFolderPath* with your path that contains **resources/Working-Folder** such as *$User/Working-Folder*;
    - Change variable *ErrorLogFolderPath* with  your path that you want to log error of failure rows in ETL staging process;
    - Change variable *ConfigsnowsqlPath* with  your path that contains your **snowsql config file**;
    - Change variable *SnowflakePath* with your path that contains **scr/snowflake** such as *$User/src/snowflake*;
    - Change variable *SnowsqlPath* with your your path that contains **snowsql.exe** file;
    - Run a trial then if error occurs you can track them down in EvenLog table in you newly created Database;
    - Deploy SSIS package to your SSISDB.

        **$your-path should not contain any spaces character**

4. Open [create_job_run_weekly.sql](./src/mssql/create_job_run_weekly.sql), then personalize your informations to     create job and weeky schedule.
    - Please ensure that your SQL Agent access right level is the same as your user account. Please note that, if you fail to do so, the executable file (.bat) can not be called.

5. Authen SnowSQL and run [init_snowflake.sql](./src/snowflake/init_snowfalke.sql)

6. Configure your [config.json](./resources/python_source/config.json) then regenerate data [main_gen.py](./resources/python_source/main_gen.py) to update your Database.

**Configuration for generating data**

- User should follow the format of config.json to update the data based on one's purpose.
