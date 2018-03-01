import sys
import os
import re
import time
import datetime
import logging
import optparse
import ConfigParser
import smtplib
from pygresql.pg import DB

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.DEBUG)

#'''
# Below block of code adds command line options
# This script has 3 options:
#     1. -t / --type         - This is to choose between pg_dump and gpcrondump
#     2. -c / --config_file  - This to supply configuration file
#     3. -h / --help         - This is to print help
#,,,

parser = optparse.OptionParser()
parser.add_option("-t","--type", dest='type', choices=['pg_dump','gpcrondump'], action="store",help="Specify the type of backup")
parser.add_option("-c","--config_file", dest = 'config_file',
                    action="store",help="Specify the config file")
options, args = parser.parse_args()
config = ConfigParser.ConfigParser()
config.read(options.config_file)


# Source System Information

logging.info("Getting source database Information")
source_db = config.get("source","database")
source_host = config.get("source","host")
source_user = config.get("source","user")
source_port = config.get("source","port")
source_schemafile = config.get("source","schema-file")
source_environment = config.get("source","environment")
backup_file = config.get("source","backup-file")

# Target System Information

logging.info("Getting target database Information")
target_db = config.get("target","database")
target_host = config.get("target","host")
target_user = config.get("target","user")
target_port = config.get("target","port")
target_schemafile = config.get("target","schema-file")
target_environment = config.get("target","environment")

# Getting timestamp of script start, Later this timestamp will be used to compare with dump_key from gpcrondump_history.

logging.info("Source Database        = %s" %source_db)
logging.info("Target Database        = %s" %target_db)


now = datetime.datetime.now()
start_timestamp = int(now.strftime("%Y%m%d%H%M%S"))

#get_starttime():This is to get start time of function compare with dump key from gpcrondump_history table

def sendmail(body):
    ENVIRONMENT = source_db
    SENDER = '%s-gpadmin@yourdomain.com' %ENVIRONMENT
    RECEIVERS = 'DBA-Greenplum@yourdomain.com'
    sender = SENDER
    receivers = RECEIVERS

    message = """From: """ + SENDER + """
To: """ + RECEIVERS + """
MIME-Version: 1.0
Content-type: text/html
Subject: Environment refresh status \n"""
    message = message + body
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message)
    except SMTPException:
        logging.error("Unable to send email")

def get_starttime():
    return int(start_timestamp)

# pg_dump_backup(): This function takes pg_dump backup

def pg_dump_backup():
    backup_command="pg_dump -d %s -h %s -U %s %s > %s" %(source_db,source_host,source_user,schema_list_for_cmd('-n'),backup_file)
    os.popen(backup_command)
    sendmail("pg_dump backup completed")

# pg_dump_restore: This function restores backup taken by pg_dump
def pg_dump_restore():
    target_schema_check()
    restore_command="psql -d %s -h %s -U %s < %s" %(target_db,target_host,target_user,backup_file)
    os.popen(restore_command)
    schemas = ''
    schema_file = open(source_schemafile,'r')
    schema_file.seek(0)
    for num,line in enumerate(schema_file, 1):
        schema = line.rstrip('\n')
        con = DB(dbname=target_db)
        get_schema = con.query("SELECT nspname FROM pg_namespace where nspname = \'%s\'" %schema)
        row = get_schema.getresult()
        if row:
            logging.info("Restore completed for %s schema" %schema)
            sendmail("pg_dump restore completed")
        else:
            logging.error("Restore failed for %s schema" %schema)
            sendmail("Restore failed for %s schema" %schema)
            sys.exit()


#'''
# schema_list_for_cmd:
# This functions reads lines from schema file and converts it into below format
# Ex:
#   schema_list_for_cmd('-s')
#   -s schema1 -s schema2 -s schema2
# This would help us while running pg_dump backup as it doesn't support schema file option
#,,,

def schema_list_for_cmd(option):
    schemas = ''
    schema_file = open(source_schemafile,'r')
    schema_file.seek(0)
    for num,line in enumerate(schema_file, 1):
        schema = line.rstrip('\n')
        schemas = schemas + option + ' ' + schema + ' '
    schema_file.close()
    return schemas


#'''
# gpdbrestore_restore():
# This function restores backup taken with gpcrondump
# After gpdbrestore, It checks if schemas exists in database
# If schema doesn't exists, Script exits
#,,,

def gpdbrestore_restore():
    target_schema_check()
    restore_command="gpdbrestore -t %s --noanalyze --redirect %s -a 2> /dev/null" %(get_backupkey(),target_db)
    os.popen(restore_command)
    schemas = ''
    schema_file = open(source_schemafile,'r')
    schema_file.seek(0)
    for num,line in enumerate(schema_file, 1):
        schema = line.rstrip('\n')
        con = DB(dbname=target_db)
        get_schema = con.query("SELECT nspname FROM pg_namespace where nspname = \'%s\'" %schema)
        row = get_schema.getresult()
        if row:
            logging.info("Restore completed for %s schema" %schema)
        else:
            logging.error("Restore failed for %s schema" %schema)
            sendmail("Restore failed for %s schema" %schema)
            sys.exit()

#'''
# target_schema_check()
# This function checks if schema ready exists in target database
# If schema exists, It renamed in below format
# schemaname__hold_%Y%m%d%H%M%S
# It again checks if schemas are renamed. If not, Script exits
#,,,

def target_schema_check():
    con = DB(dbname=target_db)
    schema_list = open(source_schemafile,'r')
    schema_list.seek(0)
    for schema in schema_list:
        schema = schema.rstrip('\n')
        date = now.strftime("%Y%m%d%H%M%S")
        logging.info("Checking if %s schema exists in %s database" %(schema,target_db))
        query = "SELECT nspname FROM pg_namespace where nspname = \'%s\'" %schema
        print(query)
        get_schema = con.query(query)
        row = get_schema.getresult()
        print(row)
        if row:
            logging.info("%s schema already exists in %s database" %(schema,target_db))
            logging.info("Renaming %s schema to %s_hold_%s" %(schema,schema,date))
            con.query("ALTER SCHEMA %s RENAME to %s_hold_%s" %(schema,schema,date))
            nsp = con.query("SELECT nspname FROM pg_namespace where nspname = \'%s\'" %schema)
            row = nsp.getresult()
            if row:
                logging.error("Failed to rename %s schema to %s_hold_%s" %(schema,schema,date))
                logging.error("Please rename the schemas and run restore manually using timestamp: %s" %get_backupkey())
                sendmail("Failed to rename %s schema to %s_hold_%s, Please Check logs" %(schema,schema,date))
                sys.exit()
            else:
                logging.info("%s schema renamed successfully to %s_hold_%s" %(schema,schema,date))
        else:
            logging.info("%s schema doesn't exists in %s. Good to restore backup" %(schema,target_db))
    schema_list.close()
    con.close()

#'''
# get_dumpkey():
# This function gets backup_key from gpcrondump_history table
# where:
#   options = options provided to gpcrondump
#   exit_text = "COMPLETED"
# order by
#   dump_key desc
# limit 1
#,,,

def get_backupkey():
    con = DB(dbname=source_db, host=source_host, user=source_user)
    opts = backup_command[11:-13]
    key = con.query("SELECT dump_key FROM gpcrondump_history where options = '%s' AND exit_text = 'COMPLETED' ORDER BY dump_key desc limit 1" %opts)
    row = key.dictresult()
    dump_key = row[0]["dump_key"]
    return int(dump_key)

#'''
# permission_switch(schemaname):
# This function switches roles from source environment to target environement
# It takes --schema-only backup from restored schema from target database
# Then fetches GRANT, ALTER TABLE .. OWNER TO statements from schema only backup into temp files.
# Generates REVOKE statements by replacing REVOKE with GRANT and FROM with TO.
# Generates ALTER TABLE .. OWNER TO statements by replacing source environment to target environment (Ex: _prod_ to _uat_)
# Generated GRANT statements by replacing source enviironment to target environment(Ex: _prod_ to _uat_)
# Gathers all generated statements into one file and runs same on target database
#,,,

def permission_switch(schemaname):
    logging.info("Switching roles from %s to %s for %s schema" %(source_environment, target_environment,schemaname))
    temp_files = ['/tmp/grantfile.sql','/tmp/grantfile_temp.sql','/tmp/revokefile.sql','/tmp/revokefile_temp.sql','/tmp/ownerfile.sql','/tmp/ownerfile_temp.sql']
    logging.info("Checking if temp files already exists")

    for file in temp_files:
        if os.path.isfile(file):
            os.remove(file)

    sql_file='/tmp/%s_%s.sql' %(schemaname,now.strftime("%Y%m%d"))
    logging.info("Taking schema only backup from target database for %s schemas in %s" %(schemaname,sql_file))
    schema_backup_command = "pg_dump %s -n %s > %s" %(target_db,schemaname,sql_file)
    os.popen(schema_backup_command)
    v_sqlfile=open(sql_file,'r')
    logging.info("Fetching grant SQL statement from " + sql_file)
    v_grantfile=open("/tmp/grantfile.sql","a")
    for g_line in v_sqlfile:
        g_line = g_line.rstrip()
        if re.search('^GRANT',g_line):
            v_grantfile.writelines(g_line)
            v_grantfile.write('\n')
    v_grantfile.close()

    logging.info("Fetching 'ALTER TABLE .. OWNER TO' SQL statement from " + sql_file)
    v_ownerfile=open("/tmp/ownerfile.sql","a")
    for o_line in v_sqlfile:
        o_line = o_line.rstrip()
        if re.search("OWNER TO",o_line):
            v_ownerfile.writelines(o_line)
            v_ownerfile.write('\n')
    v_ownerfile.close()

    logging.info("Generating revoke statements from " + sql_file)
    v_grantfile=open("/tmp/grantfile.sql","r")
    v_revokefile=open("/tmp/revokefile.sql","a")
    for r_line in v_grantfile:
        revoke_line = r_line.replace("GRANT","REVOKE")
        from_line = revoke_line.replace(" TO ", " FROM ")
        v_revokefile.writelines(from_line)
    v_revokefile.close()
    v_grantfile.close()
    logging.info("Creating new GRANT statement's file with "+ target_environment + " roles")
    v_grantfile=open("/tmp/grantfile.sql","r")
    v_grantfile_temp=open("/tmp/grantfile_temp.sql","a")
    for r_line in v_grantfile:
        new_role_line = r_line.replace('_' + source_environment + '_rl', '_' + target_environment + '_rl')
        v_grantfile_temp.writelines(new_role_line)
    v_grantfile.close()
    v_grantfile_temp.close()
    logging.info("Creating new 'ALTER TABLE .. OWNER TO' statement file with "+ target_environment + " roles")
    v_ownerfile=open("/tmp/ownerfile.sql","r")
    v_ownerfile_temp=open("/tmp/ownerfile_temp.sql","a")
    for o_line in v_ownerfile:
        new_role_line = o_line.replace('_' + source_environment + '_rl', '_' + target_environment + '_rl')
        v_ownerfile_temp.writelines(new_role_line)
    v_ownerfile.close()
    v_ownerfile_temp.close()
    final_sql_file= '/tmp/' + target_environment + '_' + schema + "_refresh_%s.sql" %now.strftime("%Y%m%d")
    logging.info("Gathering all statements in one SQL file: %s" %final_sql_file)
    revoke_sql=open("/tmp/revokefile.sql","r")
    grant_sql=open("/tmp/grantfile_temp.sql","r")
    owner_sql=open("/tmp/ownerfile_temp.sql","r")
    sql_file=open(final_sql_file,"a+")
    sql_file.write("set search_path to " + schemaname + ';')

    sql_file.write("--------------------REVOKE STATEMENTS---------------------\n\n")
    for line in revoke_sql:
        sql_file.writelines(line)

    sql_file.write("\n\n--------------------GRANT STATEMENTS---------------------\n\n")
    for line in grant_sql:
        sql_file.writelines(line)

    sql_file.write("\n\n--------------------OWNER STATEMENTS---------------------\n\n")
    for line in owner_sql:
        sql_file.writelines(line)

    revoke_sql.close()
    grant_sql.close()
    owner_sql.close()
    sql_file.close()

    logging.info("Deleting temporary files")
    logging.info("Running generated SQL file")
    run_permissions = "psql -d %s -f %s > /tmp/permissions.out" %(target_db,final_sql_file)
    os.popen(run_permissions)
    for file in temp_files:
        if os.path.isfile(file):
            os.remove(file)

if __name__ == '__main__':
    get_starttime()
    if options.type == 'pg_dump':
        pg_dump_backup()
        pg_dump_restore()
    else:
        time.sleep(1)
        backup_command="gpcrondump -x %s %s-h -a 2> /dev/null" %(source_db,schema_list_for_cmd('-s'))
        os.popen(backup_command)
        if get_backupkey() < get_starttime(): # Checks if dump_key. If it is lesser than script start time, It considers the backup is failed and exits the script
            logging.error("Backup is failed. Please check backup log /home/gpadmin/gpAdminlogs/gpcrondump_%s.log" %now.strftime("%Y%m%d"))
            sendmail("Backup is failed. Please check backup log /home/gpadmin/gpAdminlogs/gpcrondump_%s.log" %now.strftime("%Y%m%d"))
            sys.exit()
        else:
            logging.info("Backup completed successfully")
            sendmail("Backup completed successfully")
            gpdbrestore_restore() # Runs the restore funtions
            # Below block of code gets the schema list from schemafile and runs permission_switch(schemaname) for every schema
    file = open(source_schemafile,'r')
    for schema in file:
        schema = schema.rstrip('\n')
        permission_switch(schema)
        logging.info("Environment refresh from %s to %s completed" %(source_environment,target_environment))
