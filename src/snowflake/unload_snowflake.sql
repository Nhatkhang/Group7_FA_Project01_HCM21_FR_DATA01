----IMPORTANT: PLEASE HELP MODIFY THE FILE PATHS TO SAVE UNLOAD CSV FILES IN GET TASK.

COPY INTO @adsbi.%dim_ads from AdsBI.Dim_Ads file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true overwrite=true;
GET @adsbi.%dim_ads file://D:\\unload\\table1 ;
    
COPY INTO @adsbi.%dim_product from AdsBI.Dim_Product file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true overwrite=true;
GET @adsbi.%dim_product file://D:\\unload\\table2;
    
COPY INTO @adsbi.%dim_customer from AdsBI.Dim_Customer file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true overwrite=true;
GET @adsbi.%dim_customer file://D:\\unload\\table3 ;
    
COPY INTO @adsbi.%fact_ads from AdsBI.Fact_Ads file_format = (TYPE=CSV FIELD_DELIMITER = '|' BINARY_FORMAT = 'UTF-8' compression=none) header= true overwrite=true;
GET @adsbi.%fact_ads file://D:\\unload\\table4 ;
    
   