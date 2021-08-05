from distutils.dir_util import copy_tree
from generateDB import createCustomerDB, createProductDB
from generateDB import createAdsHeaderDB, createAdsDetailsDB
from configureParam import ParamObject

customer_record = ParamObject.customer_record
product_record = ParamObject.product_record
ads_header_record = ParamObject.ads_header_record
ads_detail_record = ParamObject.ads_detail_record


if __name__ == '__main__':
    print('Generating a fake data...')
    createCustomerDB(customer_record)
    createProductDB(product_record)
    createAdsHeaderDB(ads_header_record)
    createAdsDetailsDB(ads_detail_record, customer_record, product_record, ads_header_record)
    print('Done Generating Data...')
    print('Copy Data to Working Folder')
    copy_tree('./Raw-folder', './Working-folder')
    print('Everything is Done!')
    
