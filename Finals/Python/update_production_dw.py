"""
Created on Thu Mar  2 09:00:02 2023

@author: vboxuser

This program must be run each time the staging data warehouse is updated.
Concept: records are updated in the staging data warehouse, then checked before the 
production data warehouse is updated.
"""

import ibm_db
import mysql.connector

def update_pdw():
    # connect to mysql database
    try: 
        mysql_conn = mysql.connector.connect(user='root',password='############', host='127.0.0.1',database='what_mic')
        print("Mysql connection established")
    except:
        print("Failed to connect to the staging data warehouse")
    cursor = mysql_conn.cursor()
    
    # connect to db2
    dsn_hostname = "################.databases.appdomain.cloud"
    dsn_uid = "dnl#####"
    dsn_pwd = "################"
    dsn_port = "#####"
    dsn_database = "bludb"
    dsn_driver = "{IBM DB2 ODBC DRIVER}"
    dsn_protocol = "TCPIP"
    dsn_security = "SSL"
    
    dsn = (
        "DRIVER={0};"
        "DATABASE={1};"
        "HOSTNAME={2};"
        "PORT={3};"
        "PROTOCOL={4};"
        "UID={5};"
        "PWD={6};"
        "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)
    
    try:
        db2_conn = ibm_db.connect(dsn, "", "")
        print ("DB2 connection established")
    except:
        print("Failed to connect to the production data warehouse")
    
    
    # find last row id of each table in the production data warehouse (bands, members, micUsed, facts)
    def get_last_band_id():
        SQL = "select max(band_id) from dimBand;"
        stmt = ibm_db.exec_immediate(db2_conn, SQL)
        result = ibm_db.fetch_tuple(stmt)
        return result
    
    last_band_id = get_last_band_id()
    print(f"Last band_id on production data warehouse = {last_band_id}")
    
    def get_last_member_id():
        SQL = "select max(member_id) from flakeMembers;"
        stmt = ibm_db.exec_immediate(db2_conn, SQL)
        result = ibm_db.fetch_tuple(stmt)
        return result
    
    last_member_id = get_last_member_id()
    print(f"Last member_id on production data warehouse = {last_member_id}")
    
    def get_last_mic_used_id():
        SQL = "select max(mic_used_id) from flakeMicUsed;"
        stmt = ibm_db.exec_immediate(db2_conn, SQL)
        result = ibm_db.fetch_tuple(stmt)
        return result
    
    last_mic_used_id = get_last_mic_used_id()
    print(f"Last mic_used_id on production data warehouse = {last_mic_used_id}")
    
    def get_last_fact_id():
        SQL = "select max(fact_id) from factResults;"
        stmt = ibm_db.exec_immediate(db2_conn, SQL)
        result = ibm_db.fetch_tuple(stmt)
        return result
    
    last_fact_id = get_last_fact_id()
    print(f"Last fact_id on production data warehouse = {last_fact_id}")
    
    
    # get new records from the staging data warehouse
    print("Retrieving new records from the staging data warehouse...")
    
    def get_new_band_records(band_id):
        cursor.execute("select * from what_mic.dimBand where band_id > %s",(band_id))
        results = cursor.fetchall()
        return results
    
    new_band_records = get_new_band_records(last_band_id)
    
    print(f"{len(new_band_records)} new band record(s) to be inserted into production data warehouse")
    
    def get_new_member_records(member_id):
        cursor.execute("select * from what_mic.flakeMembers where member_id > %s",(member_id))
        results = cursor.fetchall()
        return results
    
    new_member_records = get_new_member_records(last_member_id)
    
    print(f"{len(new_member_records)} new member record(s) to be inserted into production data warehouse")
    
    def get_new_mic_used_records(mic_used_id):
        cursor.execute("select * from what_mic.flakeMicUsed where mic_used_id > %s",(mic_used_id))
        results = cursor.fetchall()
        return results
    
    new_mic_used_records = get_new_mic_used_records(last_mic_used_id)
    
    print(f"{len(new_mic_used_records)} new mic_used record(s) to be inserted into production data warehouse")
    
    def get_new_fact_records(fact_id):
        cursor.execute("select * from factResults where fact_id > %s",(fact_id))
        results = cursor.fetchall()
        return results
    
    new_fact_records = get_new_fact_records(last_fact_id)
    
    print(f"{len(new_fact_records)} new fact record(s) to be inserted into production data warehouse")
    
    
    # insert the new records into the production dw
    print("This will take a moment...")
    def insert_band_records(records):
        for record in records:
            SQL = "insert into dimBand(band_id,band_name,num_members,style) values(?,?,?,?);"
            stmt = ibm_db.prepare(db2_conn, SQL)
            ibm_db.execute(stmt, record)
            
    insert_band_records(new_band_records)
    print("Band records updated")
            
    def insert_member_records(records):
        for record in records:
            SQL = "insert into flakeMembers(member_id,band_id,role) values(?,?,?);"
            stmt = ibm_db.prepare(db2_conn, SQL)
            ibm_db.execute(stmt, record)
            
    insert_member_records(new_member_records)
    print("Member records updated")
    
    def insert_mic_used_records(records):
        for record in records:
            SQL = "insert into flakeMicUsed(mic_used_id,member_id,source_id,mic_name,mic_id) values(?,?,?,?,?);"
            stmt = ibm_db.prepare(db2_conn, SQL)
            ibm_db.execute(stmt, record)
            
    insert_mic_used_records(new_mic_used_records)
    print("Mic_used records updated")
    
    def insert_fact_records(records):
        for record in records:
            SQL = "insert into factResults(fact_id,mic_id,band_id,venue_id,source_id,result,result_numeric) values(?,?,?,?,?,?,?);"
            stmt = ibm_db.prepare(db2_conn, SQL)
            ibm_db.execute(stmt, record)
            
    insert_fact_records(new_fact_records)
    print("Fact records updated")
    
    # close the connections
    mysql_conn.close()
    print("Mysql connection closed")
    ibm_db.close(db2_conn)
    print("DB2 connection closed")


if __name__=="__main__":
    update_pdw()

# End of program