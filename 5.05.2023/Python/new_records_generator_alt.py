"""
Created on Wed Mar  1 14:55:46 2023

@author: David Hurtgen

This program is to be run repeatedly to generate new records
Simulates daily record creation from participants, it is the 'create' portion
of the airlfow DAG

5.05.202 - code added to upload .csv files to S3
"""

# imports
import random
import pandas as pd
from time import ctime, time
from pathlib import Path
import mysql.connector
import boto3
from botocore.exceptions import ClientError
import logging
from generator_functions import generate_fact_result,generate_band_name,generate_style,\
    generate_role,get_source_id,get_sid_drummers,choose_mic_generator,get_mic_id,result_to_numeric

# function for drums
def add_micused_fact_drums(source,mic_used_records,member_records,fact_records,band_id):
    # add mic used
    source_id=get_sid_drummers(source)
    mic_name=choose_mic_generator(source_id)
    mic_id=get_mic_id(mic_name)
    mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
    
    # add fact
    venue_id=random.randint(0, 99)
    result=generate_fact_result()
    fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])
  
def generate_new_records():
    # connect to mysql database
    try: 
        mysql_conn = mysql.connector.connect(user='root',password='############', host='127.0.0.1',database='what_mic')
        print("Mysql connection established")
    except:
        print("Failed to connect to the staging data warehouse")
    cursor = mysql_conn.cursor()
    
    
    # first band_id
    cursor.execute("select max(band_id) from what_mic.dimBand")
    first_band_id = int(cursor.fetchone()[0]) + 1
    
    # first member_id
    cursor.execute("select max(member_id) from what_mic.flakeMembers")
    first_member_id = int(cursor.fetchone()[0]) + 1
    
    # first mic_used_id
    cursor.execute("select max(mic_used_id) from what_mic.flakeMicUsed")
    first_mic_used_id = int(cursor.fetchone()[0]) + 1
    
    # first fact_id
    cursor.execute("select max(fact_id) from what_mic.factResults")
    first_fact_id = int(cursor.fetchone()[0]) + 1
    
    
    # first run of generation
    band_records = [[first_band_id, generate_band_name(), random.randint(4,10), generate_style()]]
    
    # add Lead Vocalist
    member_records = [[first_member_id, first_band_id, 'Lead Vocalist']]
    
    # add mic used
    source_id=get_source_id(member_records[-1][2])
    mic_name=choose_mic_generator(source_id)
    mic_id=get_mic_id(mic_name)
    mic_used_records = [[first_mic_used_id, member_records[-1][0], source_id, mic_name, mic_id]]
    
    # add fact
    venue_id=random.randint(0, 99)
    result=generate_fact_result()
    fact_records = [[first_fact_id, mic_id, first_band_id, venue_id, source_id, result]]
    
    # Drummer
    member_records.append([member_records[-1][0]+1, first_band_id, 'Drummer'])

    # bass drum
    add_micused_fact_drums('Kick',mic_used_records,member_records,fact_records,first_band_id)

    # snare drum
    add_micused_fact_drums('Snare',mic_used_records,member_records,fact_records,first_band_id)
    
    # hi-hats
    add_micused_fact_drums('Hi-hat',mic_used_records,member_records,fact_records,first_band_id)
    
    # toms
    add_micused_fact_drums('Toms',mic_used_records,member_records,fact_records,first_band_id)
    
    # overhead
    add_micused_fact_drums('Overhead',mic_used_records,member_records,fact_records,first_band_id)
    
    # Bassist
    member_records.append([member_records[-1][0]+1, first_band_id, 'Bassist'])
    
    # add mic used
    source_id=get_source_id(member_records[-1][2])
    mic_name=choose_mic_generator(source_id)
    mic_id=get_mic_id(mic_name)
    mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
    
    # add fact
    venue_id=random.randint(0, 99)
    result=generate_fact_result()
    fact_records.append([fact_records[-1][0]+1, mic_id, first_band_id, venue_id, source_id, result])
    
    # Any one of other members
    member_records.append([member_records[-1][0]+1, first_band_id, generate_role()])
    
    # add mic used
    source_id=get_source_id(member_records[-1][2])
    mic_name=choose_mic_generator(source_id)
    mic_id=get_mic_id(mic_name)
    mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
    
    # add fact
    venue_id=random.randint(0, 99)
    result=generate_fact_result()
    fact_records.append([fact_records[-1][0]+1, mic_id, first_band_id, venue_id, source_id, result])
    
    # add the rest of the members based on num_members
    for i in range(4, band_records[0][2]):
        # generate band member
        member_records.append([member_records[-1][0]+1, first_band_id, generate_role()])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, first_band_id, venue_id, source_id, result])
        
    # all subsequent runs
    for i in range(24):
        # generate the band
        band_id = band_records[-1][0]+1
        band_records.append([band_id, generate_band_name(), random.randint(4,10), generate_style()])
        
        # generate members
        # all bands start with 4 members by default
        # Lead Vocalist
        member_records.append([member_records[-1][0]+1, band_id, 'Lead Vocalist'])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])
        
        # Drummer
        member_records.append([member_records[-1][0]+1, band_id, 'Drummer'])
        
        # individual drum mics w/ facts
        # bass drum
        add_micused_fact_drums('Kick',mic_used_records,member_records,fact_records,band_id)

        # snare drum
        add_micused_fact_drums('Snare',mic_used_records,member_records,fact_records,band_id)
        
        # hi-hats
        add_micused_fact_drums('Hi-hat',mic_used_records,member_records,fact_records,band_id)
        
        # toms
        add_micused_fact_drums('Toms',mic_used_records,member_records,fact_records,band_id)
        
        # overhead
        add_micused_fact_drums('Overhead',mic_used_records,member_records,fact_records,band_id)
        
        # Bassist
        member_records.append([member_records[-1][0]+1, band_id, 'Bassist'])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])
        
        # Any one of other members
        member_records.append([member_records[-1][0]+1, band_id, generate_role()])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])
        
        # add the rest of the members based on num_members
        for j in range(4, band_records[-1][2]):
            # generate band member
            member_records.append([member_records[-1][0]+1, band_id, generate_role()])
            
            # add mic used
            source_id=get_source_id(member_records[-1][2])
            mic_name=choose_mic_generator(source_id)
            mic_id=get_mic_id(mic_name)
            mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
            
            # add fact
            venue_id=random.randint(0, 99)
            result=generate_fact_result()
            fact_records.append([fact_records[-1][0]+1, mic_id, band_id, venue_id, source_id, result])
    
            
    bands=pd.DataFrame(band_records, columns=['band_id','band_name','num_members','style'])
    bands.to_csv('~/Documents/what-mic/CSV/Bands.csv',index=False)
    print(f"Bands.csv created with {len(bands)} records.")
    # create a client object for s3
    s3_client = boto3.client('s3')
    # record current time for naming
    now = ctime(time())
    now_formatted = now.replace(" ", "-")
    # upload to S3
    path = Path("/home/david/Documents/what-mic/CSV/")
    object_name = f"Bands-{now_formatted}"
    try:
        s3_client.upload_file(f'{path}/Bands.csv', 'dhurtgen-what-mic', object_name)
    except ClientError as e:
        logging.error(e)
    print(f"Object {object_name} uploaded to S3, bucket dhurtgen-what-mic")
    
    members=pd.DataFrame(member_records, columns=['member_id','band_id','role'])
    members.to_csv('~/Documents/what-mic/CSV/Members.csv',index=False)
    print(f"Members.csv created with {len(members)} records.")
    # upload to S3
    object_name = f"Members-{now_formatted}"
    try:
        s3_client.upload_file(f'{path}/Members.csv', 'dhurtgen-what-mic', object_name)
    except ClientError as e:
        logging.error(e)
    print(f"Object {object_name} uploaded to S3, bucket dhurtgen-what-mic")
    
    mics_used=pd.DataFrame(mic_used_records, columns=['mic_used_id','member_id','source_id','mic_name','mic_id'])
    mics_used.to_csv('~/Documents/what-mic/CSV/MicsUsed.csv',index=False)
    print(f"MicsUsed.csv created with {len(mics_used)} records.")
    # upload to S3
    object_name = f"MicsUsed-{now_formatted}"
    try:
        s3_client.upload_file(f'{path}/MicsUsed.csv', 'dhurtgen-what-mic', object_name)
    except ClientError as e:
        logging.error(e)
    print(f"Object {object_name} uploaded to S3, bucket dhurtgen-what-mic")
    
    facts=pd.DataFrame(fact_records, columns=['fact_id','mic_id','band_id','venue_id','source_id','result'])
    facts['result_numeric']=facts['result'].map(result_to_numeric)
    facts.to_csv('~/Documents/what-mic/CSV/Facts.csv',index=False)
    print(f"Facts.csv created with {len(facts)} records.")
    # upload to S3
    object_name = f"Facts-{now_formatted}"
    try:
        s3_client.upload_file(f'{path}/Facts.csv', 'dhurtgen-what-mic', object_name)
    except ClientError as e:
        logging.error(e)
    print(f"Object {object_name} uploaded to S3, bucket dhurtgen-what-mic")
    
    
    mysql_conn.close()
    print("Mysql connection closed")

if __name__=="__main__":
    generate_new_records()

# End of program