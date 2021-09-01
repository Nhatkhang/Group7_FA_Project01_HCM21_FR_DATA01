import os
import snowflake.connector
import configparser
import argparse

def executesnowsql(sqlfile, logfile, cursor, n_err):
    logfile.write('File sql process: '+sqlfile +'\n')
    f= open(sqlfile, 'r', encoding='utf-8')
    lines = f.readlines()
    f.close()
    for line in lines:
        try:
            cursor.execute(line)
        except snowflake.connector.errors.ProgrammingError as e:
            logfile.write('Error {0}  ({1}): {2}  ({3})+\n'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            n_err = n_err + 1
    return n_err

# Create arguments
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("config", help="SnowSQL folder path")
parser.add_argument("snowflakePath", help="Snowflake folder path")
args = parser.parse_args()

config = args.config + "\config"
parser = configparser.ConfigParser()
parser.read(config)

user = parser.get("connections.project2", "username")
account = parser.get("connections.project2", "accountname")
password = parser.get("connections.project2", "password")
role = parser.get("connections.project2", "rolename")
warehouse = parser.get("connections.project2", "warehousename")
database = parser.get("connections.project2", "dbname")
schema = parser.get("connections.project2", "schemaname")

w = open(args.snowflakePath + '\\LogSnowPipe.log', 'w')

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
dropfile = args.snowflakePath + '\\TruncateData.sql'
putfile = args.snowflakePath + '\\PutSSIS.sql'
n_err = 0

print('Loading data to snowflake')
n_err = executesnowsql(dropfile, w, cs, n_err)
n_err = executesnowsql(putfile, w, cs, n_err)
cs.close()
conn.close()
w.write('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))
w.close()
print('Finish loading Data to SnowFlake with {0} Error'.format(str(n_err)))
