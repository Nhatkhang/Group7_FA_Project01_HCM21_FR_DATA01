import os
import snowflake.connector
import configparser
import argparse
from logging import getLogger
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import logging
import time
import datetime


def executesnowsql(processName, sqlCode, logFile, cursor, n_err):
    logFile.write('File sql process: '+ processName +'\n')

    for line in sqlCode:
        try:
            cursor.execute(line)
        except snowflake.connector.errors.ProgrammingError as e:
            logFile.write('Error {0}  ({1}): {2}  ({3})+\n'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            n_err = n_err + 1
    return n_err

def get_private_key_passphrase():
  return '12345'

def loadData(file_list_name, pipe, account, host, user, private_key_text, logger):
    fileList = os.listdir(args.workingFolderPath + '\\tmpCSV\\' + file_list_name)
    fileList_new = [a + ".gz" for a in fileList]
    # print(fileList_new)
    # fileList_new = ['Split200.csv.gz']
    ingest_manager = SimpleIngestManager(account=account,
                                        host=host,
                                        user=user,
                                        pipe=pipe,
                                        private_key=private_key_text)
    # List of files, but wrapped into a class
    staged_file_list = []
    for file_name in fileList_new:
        staged_file_list.append(StagedFile(file_name, None))

    try:
        resp = ingest_manager.ingest_files(staged_file_list)
    except HTTPError as e:
        # HTTP error, may need to retry
        logger.error(e)
        exit(1)

    # This means Snowflake has received file and will start loading
    assert(resp['responseCode'] == 'SUCCESS')

    return ingest_manager

def getHistoryReport(ingest_manager):
    while True:
        history_resp = ingest_manager.get_history()

        if len(history_resp['files']) > 0:
            print('Ingest Report:\n')
            print(history_resp)
            break
        else:
            # wait for 20 seconds
            time.sleep(20)

        hour = timedelta(hours=1)
        date = datetime.datetime.utcnow() - hour
        history_range_resp = ingest_manager.get_history_range(date.isoformat() + 'Z')

        print('\nHistory scan report: \n')
        print(history_range_resp)

def changeFilename(workingPath, timestamp):
    tmpPath = ['\\tmpCSV\\AdsHeaderSplit', '\\tmpCSV\\AdsTransactionSplit', '\\tmpCSV\\CustomerSplit', '\\tmpCSV\\ProductSplit']

    for tmp in tmpPath:
        path = workingPath + tmp
        files = os.listdir(path)

        # Add prefix (timestamp) to all filenames
        for file in files:
            # file: string before "_" will be removed
            os.rename(os.path.join(path, file), os.path.join(path, str(timestamp) + '_' + file.split("_")[-1]))

# Create arguments
parser = argparse.ArgumentParser()
parser.add_argument("config", help="SnowSQL folder path")
parser.add_argument("snowflakePath", help="Snowflake folder path")
parser.add_argument("workingFolderPath", help="Working folder path")
parser.add_argument("sraPath", help="SRA folder path")
args = parser.parse_args()

config = args.config + "\config"
parser = configparser.ConfigParser()
parser.read(config)

# Get config information
user = parser.get("connections.project2", "username")
account = parser.get("connections.project2", "accountname")
password = parser.get("connections.project2", "password")
role = parser.get("connections.project2", "rolename")
warehouse = parser.get("connections.project2", "warehousename")
database = parser.get("connections.project2", "dbname")
schema = parser.get("connections.project2", "schemaname")

# Define SRA key
sra_path = args.sraPath + '\\rsa_key.p8'
log_path = args.snowflakePath + '\\ingest.log'

# Define snowpipe
adsheader_pipe = database + '.' + schema + '.AdsHeaderDetails_pipe'
customer_pipe = database + '.' + schema + '.CustomerDetails_pipe'
product_pipe = database + '.' + schema + '.ProductDetails_pipe'
adstransaction_pipe = database + '.' + schema + '.AdsTransactionDetails_pipe'

# SQL code for truncate data on stage table
dropFileCode = [
    'USE DATABASE '+ database + ';',
    'TRUNCATE TABLE ' + schema + '.AdsHeaderDetails;',
    'TRUNCATE TABLE ' + schema + '.CustomerDetails;',
    'TRUNCATE TABLE ' + schema + '.ProductDetails;',
    'TRUNCATE TABLE ' + schema + '.AdsTransactionDetails;'
]

# SQL code for put data to stage table
putFileCode = [
    'USE DATABASE '+ database + ';',
    'USE WAREHOUSE ' + warehouse + ';',
    'PUT file://' + args.workingFolderPath + '\\tmpCSV\\AdsHeaderSplit\\*.csv @' + schema + '.AdsHeaderDetails_stage OVERWRITE = TRUE;',
    'PUT file://' + args.workingFolderPath + '\\tmpCSV\\ProductSplit\\*.csv @' + schema + '.ProductDetails_stage OVERWRITE = TRUE;',
    'PUT file://' + args.workingFolderPath + '\\tmpCSV\\CustomerSplit\\*.csv @' + schema + '.CustomerDetails_stage OVERWRITE = TRUE;',
    'PUT file://' + args.workingFolderPath + '\\tmpCSV\\AdsTransactionSplit\\*.csv @' + schema + '.AdsTransactionDetails_stage OVERWRITE = TRUE;'
]

# SQL code for remove data in internal stage
removeStageCode = [
    'USE DATABASE '+ database + ';',
    'REMOVE @' + schema + '.AdsHeaderDetails_stage;',
    'REMOVE @' + schema + '.CustomerDetails_stage;',
    'REMOVE @' + schema + '.ProductDetails_stage;',
    'REMOVE @' + schema + '.AdsTransactionDetails_stage;'
]

w = open(args.snowflakePath + '\\LogSnowPipe.log', 'w')

# Create timestamp
ts = int(time.time())

changeFilename(args.workingFolderPath, ts)

# Create snowflake connection
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    rolename = role
)

cs = conn.cursor()

n_err = 0

print('Removing data on stage table to snowflake')
n_err = executesnowsql("Drop File", dropFileCode, w, cs, n_err)

print('Removing internal stage data to snowflake')
n_err = executesnowsql("Remove Stage File", removeStageCode, w, cs, n_err)

print('Loading data to snowflake')
n_err = executesnowsql("Put File", putFileCode, w, cs, n_err)


logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG)
logger = getLogger(__name__)


with open(sra_path, 'rb') as pem_in:
    pemlines = pem_in.read()
    private_key_obj = load_pem_private_key(pemlines,
        get_private_key_passphrase().encode(),
        default_backend()
    )
    
private_key_text = private_key_obj.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')

print("Loading AdsHeader data")
ingest_manager_header = loadData('AdsHeaderSplit', 
    adsheader_pipe, 
    account, 
    account + '.snowflakecomputing.com', 
    user, 
    private_key_text, 
    logger
)

print("Loading Customer data")
ingest_manager_customer = loadData('CustomerSplit', 
    customer_pipe, 
    account, 
    account + '.snowflakecomputing.com', 
    user, 
    private_key_text, 
    logger
)

print("Loading Product data")
ingest_manager_product = loadData('ProductSplit', 
    product_pipe, 
    account, 
    account + '.snowflakecomputing.com', 
    user, 
    private_key_text, 
    logger
)

print("Loading AdsTransaction data")
ingest_manager_transaction = loadData('AdsTransactionSplit', 
    adstransaction_pipe, 
    account, 
    account + '.snowflakecomputing.com', 
    user, 
    private_key_text, 
    logger
)

n_err = executesnowsql("Remove Stage File", removeStageCode, w, cs, n_err)

cs.close()
conn.close()

w.write('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))
w.close()
print('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))