import sys
import os
import logging
import ConfigParser
import argparse
from pygresql.pg import DB

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("-t","--type", metavar="<type>", choices=['pg_dump','gpcrondump'],
                   required=True, action="store",help="Specify the type of backup")
parser.add_argument("-c","--config_file", required=True,
		           action="store",help="Specify the config file")
args = parser.parse_args()
config = ConfigParser.ConfigParser()
config.read(args.config_file)


# Source System Information
logging.info("Getting source database Information")
source_db = config.get("source","database")
source_host = config.get("source","host")
source_user = config.get("source","user")
source_port = config.get("source","port")
source_schema = config.get("source","schema")
backup_file = config.get("source","backup_file")

# Target System Information
logging.info("Getting target database Information")
target_db = config.get("target","database")
target_host = config.get("target","host")
target_user = config.get("target","user")
target_port = config.get("target","port")
target_schema = config.get("target","schema")

logging.info("============Backup Details============")
logging.info("Source Database = %s" %source_db)
logging.info("Source Schema   = %s" %source_schema)
logging.info("============Restore Details============")
logging.info("Target Database = %s" %target_db)
logging.info("Target Schema   = %s" %target_schema)



backup_command="gpcrondump -x %s -s %s -h -a" %(source_db,source_schema)

def pg_dump_backup():
    backup_command="pg_dump -d %s -h %s -U %s -n %s > %s" %(source_db,source_host,source_user,source_schema,backup_file)
    os.popen(backup_command)

def pg_dump_restore():
    restore_command="psql -d %s -h %s -U %s < %s" %(target_db,target_host,target_user,backup_file)
    os.popen(restore_command)

def gpcrondump_backup():
    backup_command="gpcrondump -x %s -s %s -h -a 2> /dev/null" %(source_db,source_schema)
    os.popen(backup_command)

def gpdbrestore_restore():
    restore_command="gpdbrestore -t %s --noanalyze --redirect %s -a 2> /dev/null" %(get_backupkey(),target_db)
    os.popen(restore_command)

def get_backupkey():
    con = DB()
    opts = backup_command[11:]
    key = con.query("SELECT dump_key FROM gpcrondump_history where options = '%s' AND exit_text = 'COMPLETED' ORDER BY dump_key desc limit 1" %opts)
    row = key.dictresult()
    dump_key = row[0]["dump_key"]
    return int(dump_key)

if __name__ == '__main__':
	if args.type == 'pg_dump':
		pg_dump_backup()
		pg_dump_restore()
	elif args.type == 'gpcrondump':
		gpcrondump_backup()
		gpdbrestore_restore()
	else:
		print("Invalid Backup Type. Please choose from pg_dump or gpcrondump")
