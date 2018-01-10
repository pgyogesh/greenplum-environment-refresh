import optparse
import os
import sys
import re
import logging
from shutil import copyfile
parser = optparse.OptionParser()
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.DEBUG)
parser.add_option("-f","--file",dest="sql_file",
                  action="store",help="SQL dump file")
parser.add_option("--to_env",dest="to_env",
                  action="store",help="Environment to refresh")
parser.add_option("--from_env",dest="from_env",
                  action="store",help="Environment to refresh from")
parser.add_option("-s","--schema",dest="schema",
                  action="store",help="Schema name, This is just to set search_path in SQL file")

options, args = parser.parse_args()

#
# Checking if temp files already exists
#

temp_files = ['/tmp/grantfile.sql','/tmp/grantfile_temp.sql','/tmp/revokefile.sql','/tmp/revokefile_temp.sql','/tmp/ownerfile.sql','/tmp/ownerfile_temp.sql']
logging.info("Checking if temp files already exists")


for file in temp_files:
    if os.path.isfile(file):
        logging.error("%s file already exists! Exiting...!" %file)
        sys.exit()

v_sqlfile=open(options.sql_file)

#
# Final result will be in "/tmp/grantfile.sql"
#

logging.info("Fetching grant SQL statement from " + options.sql_file)
v_grantfile=open("/tmp/grantfile.sql","a")
for g_line in v_sqlfile:
    g_line = g_line.rstrip()
    if re.search('^GRANT',g_line):
       v_grantfile.writelines(g_line)
       v_grantfile.write('\n')
v_grantfile.close()
v_sqlfile.close()

#
# Final Result will be in "/tmp/ownerfile.sql"
#
v_sqlfile=open(options.sql_file)

logging.info("Fetching 'ALTER TABLE .. OWNER TO' SQL statement from " + options.sql_file)
v_ownerfile=open("/tmp/ownerfile.sql","a")
for o_line in v_sqlfile:
    o_line = o_line.rstrip()
    if re.search("OWNER TO",o_line):
       v_ownerfile.writelines(o_line)
       v_ownerfile.write('\n')
v_ownerfile.close()

#
# Final result will in "/tmp/revokefile_temp.sql"
#

logging.info("Generating revoke statements from " + options.sql_file)
v_grantfile=open("/tmp/grantfile.sql","r")
#copyfile("/tmp/grantfile.sql","/tmp/revokefile.sql")
v_revokefile=open("/tmp/revokefile.sql","a")
#v_revokefile_temp=open("/tmp/revokefile_temp.sql","a")
for r_line in v_grantfile:
    revoke_line = r_line.replace("GRANT","REVOKE")
    from_line = revoke_line.replace(" TO ", " FROM ")
    v_revokefile.writelines(from_line)
v_revokefile.close()
v_grantfile.close()
#
# Final Result will be in "/tmp/grantfile_temp.sql"
#

logging.info("Creating new GRANT statement's file with "+ options.to_env + " roles")
v_grantfile=open("/tmp/grantfile.sql","r")
v_grantfile_temp=open("/tmp/grantfile_temp.sql","a")
for r_line in v_grantfile:
    new_role_line = r_line.replace('_' + options.from_env + '_', '_' + options.to_env + '_')
    v_grantfile_temp.writelines(new_role_line)
    #v_grantfile_temp.write('\n')

v_grantfile.close()
v_grantfile_temp.close()


#
# Final result will be in "/tmp/ownerfile_temp.sql"
#

logging.info("Creating new 'ALTER TABLE .. OWNER TO' statement file with "+ options.to_env + " roles")
v_ownerfile=open("/tmp/ownerfile.sql","r")
v_ownerfile_temp=open("/tmp/ownerfile_temp.sql","a")
for o_line in v_ownerfile:
    new_role_line = o_line.replace('_' + options.from_env + '_', '_' + options.to_env + '_')
    v_ownerfile_temp.writelines(new_role_line)
    #v_ownerfile_temp.write('\n')

v_ownerfile.close()
v_ownerfile_temp.close()
#
# Gathering all statements into one SQL file
#

logging.info("Gathering all statements in one SQL file")
final_sql_file= options.to_env +"_refresh.sql"
revoke_sql=open("/tmp/revokefile.sql","r")
grant_sql=open("/tmp/grantfile_temp.sql","r")
owner_sql=open("/tmp/ownerfile_temp.sql","r")
sql_file=open(final_sql_file,"a+")

sql_file.write("set search_path to " + options.schema + ';')
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

#
# Deleting temp files
#

logging.info("Deleting temporary files")

for file in temp_files:
    if os.path.isfile(file):
        os.remove(file)
