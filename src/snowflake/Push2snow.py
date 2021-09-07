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

ADSHEADER_PIPE = 'FA_PROJECT01_DB.AdsBI.AdsHeaderDetails_pipe'
CUSTOMER_PIPE = 'FA_PROJECT01_DB.AdsBI.CustomerDetails_pipe'
PRODUCT_PIPE = 'FA_PROJECT01_DB.AdsBI.ProductDetails_pipe'
ADSTRANSACTION_PIPE = 'FA_PROJECT01_DB.AdsBI.AdsTransactionDetails_pipe'

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

def loadData(file_list_name, pipe, account, host, user, private_key_text, logger, timestamp):
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
    # assert(resp['responseCode'] == 'SUCCESS')

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


# Create arguments
parser = argparse.ArgumentParser(description='Process some integers.')
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


dropFileCode = [
    'USE DATABASE '+ database + ';',
    'TRUNCATE TABLE ' + schema + '.AdsHeaderDetails;',
    'TRUNCATE TABLE ' + schema + '.CustomerDetails;',
    'TRUNCATE TABLE ' + schema + '.ProductDetails;',
    'TRUNCATE TABLE ' + schema + '.AdsTransactionDetails;'
]

putFileCode = [
    'USE DATABASE '+ database + ';',
    'USE WAREHOUSE ' + warehouse + ';',
    'put file://' + args.workingFolderPath + '\\tmpCSV\\AdsHeaderSplit\\*.csv @' + schema + '.AdsHeaderDetails_stage OVERWRITE = TRUE;',
    'put file://' + args.workingFolderPath + '\\tmpCSV\\ProductSplit\\*.csv @' + schema + '.ProductDetails_stage OVERWRITE = TRUE;',
    'put file://' + args.workingFolderPath + '\\tmpCSV\\CustomerSplit\\*.csv @' + schema + '.CustomerDetails_stage OVERWRITE = TRUE;',
    'put file://' + args.workingFolderPath + '\\tmpCSV\\AdsTransactionSplit\\*.csv @' + schema + '.AdsTransactionDetails_stage OVERWRITE = TRUE;'
]
#     'copy into AdsBI.AdsHeaderDetails from @ADSBI.AdsHeaderDetails_stage FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = "|" BINARY_FORMAT = "UTF-8") ON_ERROR = SKIP_FILE;',
#     'copy into AdsBI.ProductDetails from @ADSBI.ProductDetails_stage FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = "|" BINARY_FORMAT = "UTF-8") ON_ERROR = SKIP_FILE;',
#     'copy into AdsBI.CustomerDetails from @ADSBI.CustomerDetails_stage FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = "|" BINARY_FORMAT = "UTF-8") ON_ERROR = SKIP_FILE;',
#     'copy into AdsBI.AdsTransactionDetails from @ADSBI.AdsTransactionDetails_stage FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = "|" BINARY_FORMAT = "UTF-8") ON_ERROR = SKIP_FILE;'
# ]

removeStageCode = [
    'USE DATABASE '+ database + ';',
    'REMOVE @ADSBI.AdsHeaderDetails_stage;',
    'REMOVE @ADSBI.CustomerDetails_stage;',
    'REMOVE @ADSBI.ProductDetails_stage;',
    'REMOVE @ADSBI.AdsTransactionDetails_stage;'
]

SRA_PATH = args.sraPath + '\\rsa_key.p8'
LOG_PATH = args.snowflakePath + '\\ingest.log'

w = open(args.snowflakePath + '\\LogSnowPipe.log', 'w')

ts = int(time.time())

path = args.workingFolderPath + '\\tmpCSV\\AdsTransactionSplit'
files = os.listdir(path)

for index, file in enumerate(files):
    os.rename(os.path.join(path, file), os.path.join(path, str(ts) + '_' + file))

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

print('Loading data to snowflake')
n_err = executesnowsql("Drop File", dropFileCode, w, cs, n_err)
n_err = executesnowsql("Remove Stage File", removeStageCode, w, cs, n_err)
n_err = executesnowsql("Put File", putFileCode, w, cs, n_err)


logging.basicConfig(
        filename=LOG_PATH,
        level=logging.DEBUG)
logger = getLogger(__name__)

with open(SRA_PATH, 'rb') as pem_in:
    pemlines = pem_in.read()
    private_key_obj = load_pem_private_key(pemlines,
    get_private_key_passphrase().encode(),
    default_backend())
    
private_key_text = private_key_obj.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')

# loadData('AdsHeaderSplit', 
#     ADSHEADER_PIPE, 
#     account, 
#     account + '.snowflakecomputing.com', 
#     user, 
#     private_key_text, 
#     logger,
#     ts
# )

# loadData('CustomerSplit', 
#     CUSTOMER_PIPE, 
#     account, 
#     account + '.snowflakecomputing.com', 
#     user, 
#     private_key_text, 
#     logger,
#     ts
# )

# loadData('ProductSplit', 
#     PRODUCT_PIPE, 
#     account, 
#     account + '.snowflakecomputing.com', 
#     user, 
#     private_key_text, 
#     logger,
#     ts
# )

loadData('AdsTransactionSplit', 
    ADSTRANSACTION_PIPE, 
    account, 
    account + '.snowflakecomputing.com', 
    user, 
    private_key_text, 
    logger,
    ts
)

n_err = executesnowsql("Remove Stage File", removeStageCode, w, cs, n_err)

cs.close()
conn.close()

w.write('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))
w.close()
print('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))