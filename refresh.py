import sys
import os
import ConfigParser
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("-t","--type", metavar="<type>", choices=['pg_dump','gpcrondump'],
                   required=True, action="store",help="Specify the type of backup")
parser.add_argument("--redirect",
                   action="store_true",help="Specify if source_db and target_db name is diffrent")

config = ConfigParser.ConfigParser()
config.read("config_file.conf")

# Source System Information
source_db = config.get("source","database")
source_host = config.get("source","host")
source_user = config.get("source","user")
source_port = config.get("source","port")
source_schema = config.get("source","schema")
backup_file = config.get("source","backup_file")

# Target System Information
target_db = config.get("target","database")
target_host = config.get("target","host")
target_user = config.get("target","user")
target_port = config.get("target","port")

def pg_dump_backup():
    if type == 'pg_dump':
        backup_command="pg_dump -d %s -h %s -U %s -n %s > %s" %(source_db,source_host,source_user,source_schema,backup_file)
        os.popen(backup_command)
    elif type == 'gpcrondump':
        backup_command="gpcrondump -x %s -s %s"
        os.popen(backup_command)
    else:
        print("Invalid Backup Type")

def pg_dump_restore():
    if type == 'pg_dump':
        restore_command="psql -d %s -h %s -U %s < %s" %(target_db,target_host,target_user,backup_file)
        os.popen(restore_command)
    elif type == 'gpcrondump':
        restore_command="gpdbrestore "
def gpcrondump_backup():
    backup_command="gpcrondump -x %s -s %s -a" %(source_db,source_schema)
    os.popen(backup_command)

def gpdbrestore_restore():
    restore_command="gpdbrestore -t %s --noanalyze --redirect %s" %(backup_timestamp,target_db)
    os.popen(restore_command)


if __name__ == '__main__':
