#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 14:55:46 2023

@author: vboxuser

This program is to be run repeatedly to generate new records
Simulates daily record creation from participants
"""

# imports
import random
import pandas as pd
import mysql.connector
from generator_functions import generate_fact_result,generate_band_name,generate_style,\
    generate_role,get_source_id,get_sid_drummers,choose_mic_generator,get_mic_id

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
    for i in range(9):
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
    bands.to_csv('~/Documents/Bands.csv',index=False)
    print(f"Bands.csv created with {len(bands)} records.")
    
    members=pd.DataFrame(member_records, columns=['member_id','band_id','role'])
    members.to_csv('~/Documents/Members.csv',index=False)
    print(f"Members.csv created with {len(members)} records.")
    
    mics_used=pd.DataFrame(mic_used_records, columns=['mic_used_id','member_id','source_id','mic_name','mic_id'])
    mics_used.to_csv('~/Documents/MicsUsed.csv',index=False)
    print(f"MicsUsed.csv created with {len(mics_used)} records.")
    
    facts=pd.DataFrame(fact_records, columns=['fact_id','mic_id','band_id','venue_id','source_id','result'])
    facts.to_csv('~/Documents/Facts.csv',index=False)
    print(f"Facts.csv created with {len(facts)} records.")
    
    
    mysql_conn.close()
    print("Mysql connection closed")

if __name__=="__main__":
    generate_new_records()

# End of program