import json

class ParamObject:
    def __init__(self, customer_record, product_record, ads_header_record, ads_detail_record):
        self.customer_record = customer_record
        self.product_record = product_record
        self.ads_header_record = ads_header_record
        self.ads_detail_record = ads_detail_record

    def customer_record(self):
        return self.customer_record

    def product_record(self):
        return self.product_record

    def ads_header_record(self):
        return self.ads_header_record
        
    def ads_detail_record(self):
        return self.ads_detail_record


with open('config.json') as json_data_file:
    data = json.load(json_data_file)
    for i in data:
        ParamObject.customer_record = data['parameters']['customer_record']
        ParamObject.product_record = data['parameters']['product_record']
        ParamObject.ads_header_record = data['parameters']['ads_header_record']
        ParamObject.ads_detail_record = data['parameters']['ads_detail_record']