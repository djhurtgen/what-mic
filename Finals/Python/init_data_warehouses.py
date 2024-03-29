"""
Created on Fri Feb 24 09:21:42 2023

@author: David Hurtgen

This program is to be run once to populate both the staging and production data warehouses with initial records
generated by records_generator.py
"""

import mysql.connector
import ibm_db
import pandas as pd

# import the .csv files and assign them to dataframes
sources = pd.read_csv('~/Documents/Sources.csv', index_col=False)
mics = pd.read_csv('~/Documents/Mics.csv', index_col=False)
bands = pd.read_csv('~/Documents/Bands.csv', index_col=False)
venues = pd.read_csv('~/Documents/Venues.csv', index_col=False)
members = pd.read_csv('~/Documents/Members.csv', index_col=False)
mics_used = pd.read_csv('~/Documents/MicsUsed.csv', index_col=False)
facts = pd.read_csv('~/Documents/Facts.csv', index_col=False)


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
    
# insert the data into the staging data warehouse
for i, row in sources.iterrows():
    SQL = "insert into what_mic.dimSource values (%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()

for i, row in mics.iterrows():
    SQL = "insert into what_mic.dimMics values (%s,%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()    

for i, row in bands.iterrows():
    SQL = "insert into what_mic.dimBand values (%s,%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()

for i, row in venues.iterrows():
    SQL = "insert into what_mic.dimVenue values (%s,%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()

for i, row in members.iterrows():
    SQL = "insert into what_mic.flakeMembers values (%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()

for i, row in mics_used.iterrows():
    SQL = "insert into what_mic.flakeMicUsed values (%s,%s,%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()
    
for i, row in facts.iterrows():
    SQL = "insert into what_mic.factResults values (%s,%s,%s,%s,%s,%s,%s);"
    cursor.execute(SQL, tuple(row))
    mysql_conn.commit()

# insert the data into the production data warehouse
# sources
def get_source_records():
    cursor.execute("select * from what_mic.dimSource;")
    results = cursor.fetchall()
    return results

new_source_records = get_source_records()

def insert_source_records(records):
    for record in records:
        SQL = "insert into dimSource(source_id,source_name) values(?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_source_records(new_source_records)

# mics
def get_mic_records():
    cursor.execute("select * from what_mic.dimMics;")
    results = cursor.fetchall()
    return results

new_mic_records = get_mic_records()

def insert_mic_records(records):
    for record in records:
        SQL = "insert into dimMics(mic_id,manufacturer,model,type) values(?,?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_mic_records(new_mic_records)

# bands
def get_band_records():
    cursor.execute("select * from what_mic.dimBand;")
    results = cursor.fetchall()
    return results

new_band_records = get_band_records()

def insert_band_records(records):
    for record in records:
        SQL = "insert into dimBand(band_id,band_name,num_members,style) values(?,?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_band_records(new_band_records)

# venues
def get_venue_records():
    cursor.execute("select * from what_mic.dimVenue;")
    results = cursor.fetchall()
    return results

new_venue_records = get_venue_records()

def insert_venue_records(records):
    for record in records:
        SQL = "insert into dimVenue(venue_id,venue_name,size,reverberance_seconds) values(?,?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_venue_records(new_venue_records)

# members
def get_member_records():
    cursor.execute("select * from what_mic.flakeMembers;")
    results = cursor.fetchall()
    return results

new_member_records = get_member_records()

def insert_member_records(records):
    for record in records:
        SQL = "insert into flakeMembers(member_id,band_id,role) values(?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_member_records(new_member_records)

# micUsed
def get_mics_used_records():
    cursor.execute("select * from what_mic.flakeMicUsed;")
    results = cursor.fetchall()
    return results

new_mics_used_records = get_mics_used_records()

def insert_mics_used_records(records):
    for record in records:
        SQL = "insert into flakeMicUsed(mic_used_id,member_id,source_id,mic_name,mic_id) values(?,?,?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_mics_used_records(new_mics_used_records)

# factResults
def get_facts_records():
    cursor.execute("select * from what_mic.factResults;")
    results = cursor.fetchall()
    return results

new_facts_records = get_facts_records()

def insert_facts_records(records):
    for record in records:
        SQL = "insert into factResults(fact_id,mic_id,band_id,venue_id,source_id,result,result_numeric) values(?,?,?,?,?,?,?);"
        stmt = ibm_db.prepare(db2_conn, SQL)
        ibm_db.execute(stmt, record)
        
insert_facts_records(new_facts_records)


print(f"{len(sources)} rows committed to staging data warehouse, table=dimSource")
print(f"{len(sources)} rows committed to production data warehouse, table=dimSource")
print(f"{len(mics)} rows committed to staging data warehouse, table=dimMics")
print(f"{len(mics)} rows committed to production data warehouse, table=dimMics")
print(f"{len(bands)} rows committed to staging data warehouse, table=dimBand")
print(f"{len(bands)} rows committed to production data warehouse, table=dimBand")
print(f"{len(venues)} rows committed to staging data warehouse, table=dimVenue")
print(f"{len(venues)} rows committed to production data warehouse, table=dimVenue")
print(f"{len(members)} rows committed to staging data warehouse, table=flakeMembers")
print(f"{len(members)} rows committed to production data warehouse, table=flakeMembers")
print(f"{len(mics_used)} rows committed to staging data warehouse, table=flakeMicUsed")
print(f"{len(mics_used)} rows committed to production data warehouse, table=flakeMicUsed")
print(f"{len(facts)} rows committed to staging data warehouse, table=factResults")
print(f"{len(facts)} rows committed to production data warehouse, table=factResults")

mysql_conn.close()
print("Mysql connection closed")
ibm_db.close(db2_conn)
print("DB2 connection closed")

# End of program