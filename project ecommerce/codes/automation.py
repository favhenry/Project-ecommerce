import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import ibm_db
# Connect to MySQL
connection = mysql.connector.connect(user='root', password='',host='',database='sales')
cursor = connection.cursor()

# Connect to DB2 or PostgreSql
dsn_hostname = "" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = ""        # e.g. "abc12345"
dsn_pwd = ""      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "32286"                # e.g. "50000"
dsn_database = "bludb"            # i.e. "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}" # i.e. "{IBM DB2 ODBC DRIVER}"
dsn_protocol = "TCPIP"            # i.e. "TCPIP"
dsn_security = "SSL"              # i.e. "SSL"

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
conn = ibm_db.connect(dsn, "", "")
# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    query = "SELECT MAX(rowid) AS last_rowid FROM sales_data;"
    try:
        stmt = ibm_db.exec_immediate(conn, query)
        result = ibm_db.fetch_assoc(stmt)
        return result['LAST_ROWID'] if result and result['LAST_ROWID'] is not None else 0
    except Exception as e:
        print(f"Error fetching last row ID: {e}")
        return 0


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)


# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    query = "SELECT * FROM sales_data WHERE rowid > %s;"
    try:
        cursor.execute(query, (rowid,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching new records: {e}")
        return []

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    if not records:
        return

    query = """
    INSERT INTO sales_data (rowid, product_id, customer_id, quantity) 
    VALUES (?, ?, ?, ?);
    """  # Update column names as per your schema

    try:
        for record in records:
            stmt = ibm_db.prepare(conn, query)
            ibm_db.execute(stmt, record)
    except Exception as e:
        print(f"Error inserting records: {e}")

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
cursor.close()
connection.close()
# disconnect from DB2 or PostgreSql data warehouse
ibm_db.close(conn)
# End of program