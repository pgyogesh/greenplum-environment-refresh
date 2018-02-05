from pygresql.pg import DB
con = DB(dbname='icuat')
query = "SELECT nspname FROM pg_namespace where nspname = 'test2'"
get_schema = con.query(query)
row = get_schema.getresult()
if row:
    print('True')
else:
    print('false')
