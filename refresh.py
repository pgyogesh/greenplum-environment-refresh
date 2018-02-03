import sys
import os
import time
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
source_schemafile = config.get("source","schema-file")
source_environment = config.get("source","environment")

# Target System Information
logging.info("Getting target database Information")
target_db = config.get("target","database")
target_host = config.get("target","host")
target_user = config.get("target","user")
target_port = config.get("target","port")
target_schemafile = config.get("target","schema-file")
target_environment = config.get("target","environment")

# Getting timestamp of script start, Later this timestamp will be used to compare with dump_key from gpcrondump_history.

logging.info("Script Start Timestamp = %d" %start_timestamp)
logging.info("Source Database        = %s" %source_db)
logging.info("Target Database        = %s" %target_db)


#backup_command="gpcrondump -x %s -s %s -h -a" %(source_db,source_schema)

def pg_dump_backup():
	backup_command="pg_dump -d %s -h %s -U %s -n %s > %s" %(source_db,source_host,source_user,source_schema,backup_file)
    	os.popen(backup_command)

def pg_dump_restore():
    	restore_command="psql -d %s -h %s -U %s < %s" %(target_db,target_host,target_user,backup_file)
    	os.popen(restore_command)

def schema_list_for_cmd(option):
    schemas = ''
    for num,line in enumerate(open(source_schemafile,'r'), 1):
		schema = line.rstrip('\n')
		schemas = schemas + option + ' ' + schema + ' '
    return schemas

def gpdbrestore_restore():
	target_schema_check()
   	restore_command="gpdbrestore -t %s --noanalyze --redirect %s -a 2> /dev/null" %(get_backupkey(),target_db)
    	os.popen(restore_command)
	schemas = ''
	for num,line in enumerate(source_schemafile, 1):
		schema = line.rstrip('\n')
		schemas = schemas + "'" + schema + "'" + ","
	schemas = schemas.rstrip(',')
	con = DB(dbname=target_db, host=target_host, port=target_port, user=target_user)
	schema_count = con.query("SELECT count(nspname) FROM pg_namespace where nspname in ('%s' %schemas)")
        count = schema_count.getresult()[0]
        if count != num:
                logging.error("Restore is failed. %s schema restored out of %s schema" %(count,num))
        else:
                logging.info("Restore is completed successfully")
def target_schema_check():
	logging.info("Checking if %s schema already exists in %s database" %(target_schema,target_db))
	con = DB(dbname=target_db, host=target_host, port=target_port, user=target_user)
	schema = con.query("SELECT nspname FROM pg_namespace where nspname = %s" %target_schema)
	row = schema.getresult()
	if row:
		logging.info("%s schema already exists in %s database" %(target_schema,target_db))
		logging.info("Renaming %s schema to %s_hold" %(target_schema,target_schema))
		con.query("ALTER SCHEMA %s RENAME to %s_hold" %(target_schema,target_schema))
		schema = con.query("SELECT nspname FROM pg_namespace where nspname = %s" %target_schema)
		row = schema.getresult()
		if row:
			logging.info("Failed to rename %s schema to %s_hold" %(target_scheme,target_schema))
		else:
			logging.info("%s schema renamed successfully to %s_hold" %(target_scheme,target_schema))
	else:
		logging.info("%s schema doesn't exists in %s. Good to restore backup" %(target_schema,target_db))
	con.close()

def get_backupkey():
    con = DB(dbname=source_db, host=source_host, user=source_user)
    opts = backup_command[11:-13]
    key = con.query("SELECT dump_key FROM gpcrondump_history where options = '%s' AND exit_text = 'COMPLETED' ORDER BY dump_key desc limit 1" %opts)
    row = key.dictresult()
    dump_key = row[0]["dump_key"]
    return dump_key

def permission_switch(schemaname):
	temp_files = ['/tmp/grantfile.sql','/tmp/grantfile_temp.sql','/tmp/revokefile.sql','/tmp/revokefile_temp.sql','/tmp/ownerfile.sql','/tmp/ownerfile_temp.sql']
	logging.info("Checking if temp files already exists")

	for file in temp_files:
		if os.path.isfile(file):
			os.remove(file)

	sql_file='/tmp/%s_%s.sql' %(schemaname,now.strftime("%Y%m%d"))
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
		new_role_line = r_line.replace('_' + source_environment + '_', '_' + target_environment + '_')
		v_grantfile_temp.writelines(new_role_line)
	v_grantfile.close()
	v_grantfile_temp.close()

	logging.info("Creating new 'ALTER TABLE .. OWNER TO' statement file with "+ target_environment + " roles")
	v_ownerfile=open("/tmp/ownerfile.sql","r")
	v_ownerfile_temp=open("/tmp/ownerfile_temp.sql","a")
	for o_line in v_ownerfile:
		new_role_line = o_line.replace('_' + source_environment + '_', '_' + target_environment + '_')
		v_ownerfile_temp.writelines(new_role_line)
	v_ownerfile.close()
	v_ownerfile_temp.close()
	logging.info("Gathering all statements in one SQL file")
	final_sql_file= target_environment +"_refresh.sql"
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

	logging.info("Deleting temporary files")
        logging.info("Running generated SQL file")
        run_permissions = "psql -d %s -f %s > /tmp/permissions.out" %(target_db,sql_file)
        os.popen(run_permissions)
	for file in temp_files:
    		if os.path.isfile(file):
        		os.remove(file)

if __name__ == '__main__':
    now = datetime.datetime.now()
    start_timestamp = int(now.strftime("%Y%m%d%H%M%S"))
    if args.type == 'pg_dump':
        pg_dump_backup()
        pg_dump_restore()
    else:
		backup_command="gpcrondump -x %s %s-h -a 2> /dev/null" %(source_db,schema_list_for_cmd('-s'))
        time.sleep(1)
    	os.popen(backup_command)
        if get_backupkey() > int(start_timestamp):
            print(get_backupkey())
            print(start_timestamp)
            logging.error("Backup is failed. Please check backup log /home/gpadmin/gpAdminlogs/gpcrondump_%s.log" %now.strftime("%Y%m%d"))
            sys.exit()
        else:
            gpdbrestore_restore()
            permission_switch()
