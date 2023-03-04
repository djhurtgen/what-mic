#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 09:21:42 2023

@author: vboxuser

This program must be run each time new records are created to update the staging datawarehouse
"""

import mysql.connector
import pandas as pd

def update_sdw():
    # import the .csv files and assign them to dataframes
    bands = pd.read_csv('~/Documents/Bands.csv', index_col=False)
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
    
    
    # insert the data into the staging data warehouse
    for i, row in bands.iterrows():
        SQL = "insert into what_mic.dimBand values (%s,%s,%s,%s);"
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
        SQL = "insert into what_mic.factResults values (%s,%s,%s,%s,%s,%s);"
        cursor.execute(SQL, tuple(row))
        mysql_conn.commit()
    
    
    print(f"{len(bands)} rows committed to staging data warehouse, table=dimBand")
    print(f"{len(members)} rows committed to staging data warehouse, table=flakeMembers")
    print(f"{len(mics_used)} rows committed to staging data warehouse, table=flakeMicUsed")
    print(f"{len(facts)} rows committed to staging data warehouse, table=factResults")
    
    
    mysql_conn.close()
    print("Mysql connection closed")

if __name__=="__main__":
    update_sdw()

# End of program