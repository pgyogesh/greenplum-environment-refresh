import sys
import os
import datetime
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

# Getting timestamp of script start, Later this timestamp will be used to compare with dump_key from gpcrondump_history. 
now = datetime.datetime.now()
start_timestamp = int(now.strftime("%Y%m%d%H%M%S"))

logging.info("Script Start Timestamp = %d" %start_timestamp)
logging.info("============Backup Details============")
logging.info("Source Database        = %s" %source_db)
logging.info("Source Schema          = %s" %source_schema)
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
    	backup_command="gpcrondump -x %s -s %s -h -a 2> dev/null" %(source_db,source_schema)
    	p = os.popen(backup_command)

def gpdbrestore_restore():
	logging.info("Checking if %s schema already exists in %s database" %(target_schema,target_db))
	con = DB(dbname=target_db, host=target_host, port=target_port, user=target_user)
	schema = con.query("SELECT nspname FROM pg_namespace where nspname = %s" %target_schema)
	row = schema.dictresult()
	if row:
		logging.info("%s schema already exists in %s database" %(target_schema,target_db))
		logging.info("Renaming %s schema to %s_hold" %(target_schema,target_schema))
		con.query("ALTER SCHEMA %s RENAME to %s_hold" %(target_schema,target_schema))
		schema = con.query("SELECT nspname FROM pg_namespace where nspname = %s" %target_schema)
		row = schema.dictresult()
		if row:
			logging.info("Failed to rename %s schema to %s_hold" %(target_scheme,target_schema))
		else:
			logging.info("%s schema renamed successfully to %s_hold" %(target_scheme,target_schema))
	else:
		logging.info("%s schema doesn't exists in %s. Good to restore backup" %(target_schema,target_db)
   	restore_command="gpdbrestore -t %s --noanalyze --redirect %s -a 2> /dev/null" %(get_backupkey(),target_db)
    	os.popen(restore_command)

def get_backupkey():
    	con = DB(dbname=source_db, host=source_host, port=source_port, user=source_user)
    	opts = backup_command[11:]
    	key = con.query("SELECT dump_key FROM gpcrondump_history where options = '%s' AND exit_text = 'COMPLETED' ORDER BY dump_key desc limit 1" %opts)
    	row = key.dictresult()
    	dump_key = row[0]["dump_key"]
    	return int(dump_key)

if __name__ == '__main__':
	if args.type == 'pg_dump':
		pg_dump_backup()
		pg_dump_restore()
	else:
		gpcrondump_backup()
		if get_backupkey() > start_timestamp:
			sys.exit("Backup is failed. Please check backup logs /home/gpadmin/gpAdminlogs/gpcrondump_%s.log" %now.strftime("%Y%m%d"))
		else:
			gpdbrestore_restore()
