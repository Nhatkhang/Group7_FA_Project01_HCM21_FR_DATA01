import random, os 
from datetime import date, datetime
from faker import Faker
import csv



## DATA PATH ##
os.chdir('..')
os.makedirs('Working-Folder', exist_ok=True)
os.makedirs('Raw-Folder', exist_ok=True)
current_path = os.getcwd()
raw_path = current_path + '\Raw-Folder'
work_path = current_path+'\Working-Folder'

## VARIABLES ##
product_categories = ["Books", "Electronics", "Movies and TV", "CDs and Vinyl", 
                      "Clothing, Shoes and Jewelry","Home and Kitchen", "Kindle Store", 
                      "Sports and Outdoors", "Cell Phones and Accessories",
                      "Health and Personal Care", "Toys and Games", "Video Games", 
                      "Tools and Home Improvement", "Beauty", "Apps for Android", 
                      "Office Products", "Pet Supplies", "Automotive", 
                      "Grocery and Gourmet Food", "Patio, Lawn and Garden", "Baby",
                      "Digital Music", "Musical Instruments", "Instant Video"]
ads_platform = ["Facebook", "Instagram", "Youtube", "Google", "Tiktok"]
ads_categories = ["Video", "Gif file", "Sound clip", "Image"]
fake = Faker('en')
time_stampe = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
customer_id_list = []
ads_id_list = []
product_id_list = []


## CREATE DATA FOR CUSTOMER ## 
def createCustomerDB(CUSTOMER_RECORD):
    with open(f'{raw_path}\CustomerData.csv', "a", newline='') as csvfile:
        fieldnames = ['Customer_ID', 'Full_Name', 'Gender', 'Email', 'Address', 'City', 'Region',
                      'Age', 'Income', 'Reg_Date', 'Modified_Date']
        start_date = date(year=2013, month=1, day=1)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(CUSTOMER_RECORD):
            tmp_id = fake.random_int(1,92123)
            customer_id_list.append(tmp_id)
            writer.writerow(
                {
                    'Customer_ID': tmp_id,
                    'Full_Name': fake.name(),
                    'Gender': fake.random_element(elements=['Male', 'Female']),
                    'Email': fake.free_email(),
                    'Address': fake.address()[0:-10],
                    'City': fake.location_on_land()[4].split('/')[1],
                    'Region': fake.location_on_land()[4].split('/')[0],
                    'Age': fake.random_int(5,60),
                    'Income': fake.random_int(10000,200000,5000),
                    'Reg_Date': fake.date_between(start_date=start_date, end_date='+3y'),
                    'Modified_Date': time_stampe
                }
            )
    return 

## CREATE DATA FOR ADS HEADER ##
def createAdsHeaderDB(ADS_HEADER_RECORD):
    with open(f'{raw_path}\AdsHeaderData.csv', "a", newline='') as csvfile:
        fieldnames = ['Ads_ID', 'Ads_Name', 'Ads_Category', 'Platform', 'Standard_Cost', 
                      'Cost_Per_Click', 'Modified_Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(ADS_HEADER_RECORD):
            tmp_id = fake.random_int()
            ads_id_list.append(tmp_id)
            writer.writerow(
                {
                    'Ads_ID': tmp_id,
                    'Ads_Name': fake.bothify(text='ClickAds ?????_##'),
                    'Ads_Category': fake.random_element(elements=ads_categories),
                    'Platform': fake.random_element(elements=ads_platform),
                    'Standard_Cost': round(random.uniform(100,1000),2),
                    'Cost_Per_Click': round(random.uniform(0.1,2),2),
                    'Modified_Date': time_stampe
                }
            )
    return

## CREATE DATA FOR PRODUCT ##
def createProductDB(PRODUCT_RECORD):
    with open(f'{raw_path}\ProductData.csv', "a", newline='') as csvfile:
        fieldnames = ['Product_ID', 'Product_Name', 'Product_Category', 'Color', 'Cost',
                      'Price', 'Modified_Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(PRODUCT_RECORD):
            tmp=round(random.uniform(100,5000),2) # max/min cost: 5000/100
            tmp_id = fake.random_int(1,30288)
            product_id_list.append(tmp_id)
            writer.writerow(
                {
                    'Product_ID': tmp_id,
                    'Product_Name': fake.bothify(text=fake.company()+' ???_####'),
                    'Product_Category': fake.random_element(elements=product_categories),
                    'Color': fake.color_name(),
                    'Cost': tmp,
                    'Price': round(tmp*random.uniform(1.1,1.3),2),
                    'Modified_Date': time_stampe
                }
            )
    return

## CREATE DATA FOR ADS DETAILS ##
def createAdsDetailsDB(ADS_DETAILS_RECORD, START_DATE, END_DATE):
    with open(f'{raw_path}\AdsDetailsData.csv', "a", newline='') as csvfile:
        fieldnames = ['Date', 'Customer_ID', 'Product_ID','Time_Spent_On_Ads', 'Ads_ID', 
                      'Daily_Internet_Usage', 'Number_ClicksAds', 'Number_ProductBought',
                      'Modified_Date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(ADS_DETAILS_RECORD):
            writer.writerow(
                {
                    'Date': fake.date_between(start_date=START_DATE, end_date=END_DATE),
                    'Customer_ID': fake.random_element(elements=customer_id_list),
                    'Product_ID': fake.random_element(elements=product_id_list),
                    'Time_Spent_On_Ads': fake.random_int(1,15), #minutes
                    'Ads_ID': fake.random_element(elements=ads_id_list),
                    'Daily_Internet_Usage': round(random.uniform(0.5,10),1), #hours
                    'Number_ClicksAds': fake.random_int(1,30),
                    'Number_ProductBought': fake.random_int(0,5),
                    'Modified_Date': time_stampe
                }
            )
    return
