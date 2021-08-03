# Decsription of this project
**Business Question**: *Social Media	- User behavior analysis (based on engagement, content, user's personnal information…) to maximize advertisement income*

This project is initialize and create data pipeline to answer the aboved Bussiness Question
# Working data

Crawling data from e-commerce website: [Tiki.vn](https/tiki.vn)

# Detail of Work

1. Design data pipeline [here](./docs/design.png "Architecture")
2. Normalize and Denormalize data
3. Build data model
4. Ingest data from flat file
5. Extract and Load into Data warehouse using SSIS
6. Load data onto Cloud with the transformation
7. Enrich data with different data sources
8. Visualize your data

# How to setup
1. Login into MSSQL and run [init_mssql.sql](./src/mssql/init_mssql.sql)
2. Authen SnowSQL and run [init_snowflake.sql](./src/mssql/init_snowfalke.sql)
3. Generate data: `python data-generator.py`