# Decsription of this project
**Business Question**: *Social Media	- User behavior analysis (based on engagement, content, user's personnal information…) to maximize advertisement income*

This project is for initialize=ing and creating data pipeline to answer the aboved Bussiness Question.
# Working data

Creating data based on Python sript. The database will include:

1. CustomerData.csv

This file includes basics data for a customer such as Customer_ID, Full_Name, Gender, Email,.ect. The list of customer ID is temporary saved in during the generating process.

2. ProductData.csv

This file includes basics data for a product such as Product_ID, Product_Name, Product_Category, Cost,.ect. The list of product ID is temporary saved in during the generating process.

3. AdsHeaderData.csv

This file includes basics data for a product such as Ads_ID, Ads_Name, Ads_Category, Platform,.ect. The list of advertisement ID is temporary saved in during the generating process.

4. AdsDetailData.csv

This file contains trasaction information with the components:

- Date: Indicates the date of record.
- Customer_ID: this is ramdomly select from the customer ID list saved aboved to ensure that the data exist in CustomerData.
- Product_ID: this is ramdomly select from the product ID list saved aboved to ensure that the data exist in ProductData.
- Time_Spent_On_Ads: Average time user/customer spent on a specific Ads for a specific product, unit is minute.
- Ads_ID: this is ramdomly select from the Advertisement ID list saved aboved to ensure that the data exist in AdsHeaderData.
- Daily_Internet_Usage: Average time user/customer spent for the Internet, unit is hour.
- Number_ClicksAds: Total of clicks user/customer clicked on the Ad.
- Number_ProductBought: Total number of product user/customer actually bought from the Ad.

**Each of the csv file includes *Modified_Date* to indicate the SCD data when data is loaded into DW.**
Please note that the Data will be accumulated saved to csv file each time the python script is executed. User is highly recommended to change the variable *start_date* and *end_date* in config file to update the data chronologically.

# Detail of Work

1. Design data pipeline [here](./docs/design.png "Architecture")
2. Normalize and Denormalize data
3. Build data model
4. Ingest data from flat file csv
5. Extract and Load into Data warehouse using SSIS
6. Load data onto Cloud with the transformation
7. Enrich data with different data sources
8. Visualize your data

# How to setup
1. Login into MSSQL and run [init_mssql.sql](./src/mssql/init_mssql.sql)
2. Authen SnowSQL and run [init_snowflake.sql](./src/mssql/init_snowfalke.sql)
3. Generate data: `python data-generator.py`
# Configuration for generating data
User should follow the format of config.json to update the data based on one's purpose.