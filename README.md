# PROJECT FA 01

### Project Description 

**Business Question:**
*Social Media - User behavior analysis* (based on engagement, content, user's personnal information…) to maximize advertisement income.

**Prototype Solution**: 
Analysis of the parameters that influence the Ads Income such as: Platform, Ads Categories, .etc. In this report scope, the Ads income includes Standard cost for the Product advertised and the total cost per click charged on user/customer click on Ads.

This project is for initializing and creating data pipeline to answer the aboved Bussiness Question.

### Working data

Data is generated using `main_gen.py` sript. The database (`.csv` flat files) will include:

1. CustomerData.csv

    This file includes basics data for a customer such as `Customer_ID, Full_Name, Gender, Email, .etc`. The list of customer ID is temporary saved in during the generating process.

2. ProductData.csv

    This file includes basics data for a product such as `Product_ID, Product_Name, Product_Category, Cost, .etc`. The list of product ID is temporary saved in during the generating process.

3. AdsHeaderData.csv

    This file includes basics data for a product such as `Ads_ID, Ads_Name, Ads_Category, Platform, .etc`. The list of advertisement ID is temporary saved in during the generating process.

4. AdsDetailData.csv

    This file contains trasaction information with the components:
    - `Date`: Indicates the date of record.
    - `Customer_ID`: this is ramdomly select from the customer ID list saved aboved to ensure that the data exist in CustomerData.
    - `Product_ID`: this is ramdomly select from the product ID list saved aboved to ensure that the data exist in ProductData.
    - `Time_Spent_On_Ads`: Average time user/customer spent on a specific Ads for a specific product, unit isminute.
    - `Ads_ID`: this is ramdomly select from the Advertisement ID list saved aboved to ensure that the data exist in AdsHeaderData.
    - `Daily_Internet_Usage`: Average time user/customer spent for the Internet, unit is hour.
    - `Number_ClicksAds`: Total of clicks user/customer clicked on the Ad.
    - `Number_ProductBought`: Total number of product user/customer actually bought from the Ad.

**Each of the csv file includes `Modified_Date` to indicate the date Data is created.**

Please note that the Data will be replaced each time the python script is executed. User is highly recommended to change the variable `start_date` and `end_date` in config file to update the data chronologically.

## :earth_asia:	 Detail of Work

1. Generate Data
    - Generate rawdata and copy to $path/Working-Folder
2. Design data pipeline 
    - Create Drawio file
    ![ProjectDesign](./docs/Project_Design.png)
3. Build data model 
    - Generate Dim/Facts Tables scheme
    ![DataModel](./docs/data_model_SQL.png)
4. Ingest data from flat file csv
    - Data Profiling and Data Staging
    - Build a SSIS solution to do ETL and stage data in MSSQL
    - Deploy package into MSSQL
    - Error Handling
5. Load data onto SNOWFLAKE and do DataWarehousing
    - Create ODBC connection
    - Load data using put/copy as snowpipe
    - Update SSIS solution to run automatically in generating script files for uploading
    - Python connetion to snowpipe
    - Create DataWarehouse and auto update tasks.
6. Visualize your data using PowerBI
7. Update data

## :rocket: Getting Started

### Prerequisites
- Install [Snowsql](https://docs.snowflake.com/en/user-guide/snowsql.html) and add the following setting to `config` file (Windows: `config` file located at `C:\Users\<user-name>\.snowsql`). Your snowsql config has to be configured with our confidential information we provided and remember to set your role to accountadmin.
    ```
    [connections.project2]
    accountname = <accountname> # Ask the repo owner for the accountname
    username = <username> # Ask the repo owner for the username
    password = <password> # Ask the repo owner for the password
    warehousename = FA_Project01_CloudDW_LOADING
    dbname = FA_Project01_DB
    schemaname = AdsBI
    rolename = accountadmin
    ```
- Install python dependencies and make sure you can import libraries using `cmd`. We recommend installing [Anaconda](https://www.anaconda.com/) and add `conda` environment to windows `PATH`.
    ```
    pip install Faker
    pip install snowflake-connector-python
    ```
- Clone the repo and checkout `Group7_main` branch:
    ```
    git clone https://github.com/Nhatkhang/Group8_FA_Project01_HCM21_FR_DATA01.git
    cd Group8_FA_Project01_HCM21_FR_DATA01
    git checkout Group7_main
    ```
- Generate RSA key with the following command and set the password of the key: `12345` (Snowflake [document](https://docs.snowflake.com/en/user-guide/key-pair-auth.html)), you can change the directory to a temp folder so that it does not overwrite your existing key:
    ```bash
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    ```
    Send the `rsa_key.pub` file to the repo owner to add to your Snowflake user account or your can add with this command if your account has `accountadmin` role:
    ```
    ALTER USER <user-name> SET rsa_public_key='<rsa-public-key>';
    ```

### Usage 
1. Configure your [config.json](./resources/python_source/config.json) as you wish then generate data [main_gen.py](./resources/python_source/main_gen.py)  to generate data.

2. Login into MSSQL and run [init_mssql.sql](./src/mssql/init_mssql.sql)

3. Authen SnowSQL and run [init_snowflake.sql](./src/snowflake/init_snowfalke.sql)

4. Open SSIS solution:
    Note: `$your-path` should not contain any spaces character.

    - Update `ConnectServer.DB` to with your servername.
    - Change `ProjectFolderPath` parameter with your path that contains this repository.
    - Change `WorkingFolderPath` parameter with your path that contains `resources/Working-Folder` such as `$User/Working-Folder`.
    - Change `ErrorLogFolderPath` parameter with  your path that you want to log error of failure rows in ETL staging process.
    - Change `ConfigsnowsqlPath` parameter with  your path that contains your snowsql `config` file.
    - Change `SnowflakePath` parameter with your path that contains `scr/snowflake` such as `$User/src/snowflake`.
    - Change `PythonPath` parameter with your path that contains `python.exe` file.
    - Change `SRAPath` parameter with your path that contains the RSA key.
    - Run a trial then if error occurs you can track them down in EvenLog table in you newly created Database;
    - Deploy SSIS package to your SSISDB. Please ensure that your Integration services catalogs already have set up SSISDB.

5. Open SQL Server Management Studio (SSMS) - Integration Services Catalogs > SSISDB
   - Create Environment & set up Variables within the Environment which coincide with the Parameters from the SSIS project.
   - Configure Project.
   - Execute in SSMS Via the Catalog. You need to check the box and choose the correct environment.
   - Create Agent Job (see the index 6 for instruction) and Associate Job to the Environment. Open the Job properties > Configure page and specific which Environment the job should use

6. Open [create_job_run_weekly.sql](./src/mssql/create_job_run_weekly.sql), then personalize your informations to     create job and weekly schedule. 
    - Please ensure that your SQL Agent access right level is the same as your user account. Please note that, if you fail to do so, the executable file can not be called.
    - If your SQL Agent has already set up Proxy, Operator, please just execute `CREATE A JOB/SCHEDULE` part.
    - You need to personalize/modify your informations as required: 
    `IDENTITY, SECRET, email_address, owner_login_name, notify_email_operator_name, command (package path and server name)`.
7. Configure your [config.json](./resources/python_source/config.json) then regenerate data [main_gen.py](./resources/python_source/main_gen.py) to update your Database.

**Configuration for generating data**

- User should follow the format of `config.json` to update the data based on one's purpose.

## :round_pushpin: Roadmap

See the [open issues](https://github.com/dhuy237/fa-project-1-team-7/issues) for a list of proposed features (and known issues).

## :hammer: Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project.
2. Create your Feature Branch (`git checkout -b feature/Feature`).
3. Commit your Changes (`git commit -m 'Add some feature'`).
4. Push to the Branch (`git push origin feature/Feature`).
5. Open a Pull Request.

## :mailbox: Contact
- Huy Tran ([dhuy237](https://github.com/dhuy237)) - d.huy723@gmail.com
- Quyen Mai ([mtquyen](https://github.com/mtquyen)) - maithiquyen124@gmail.com