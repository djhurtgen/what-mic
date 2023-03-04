#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 15:34:14 2023

@author: vboxuser

This program is to be run once to create initial records as .csv files
"""
# imports
import random
import pandas as pd
from generator_functions import mic_records,source_records,generate_venue_name,generate_venue_size,generate_fact_result,generate_band_name,\
    generate_style,generate_role,generate_reverberance_length,get_source_id,get_sid_drummers,choose_mic_generator,get_mic_id

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
    
# main function    
def generate_records():
    # create mics table
    mics=pd.DataFrame(mic_records, columns=['mic_id','manufacturer','model','type'])
    mics.to_csv('Mics.csv',index=False)
    print(f"Mics.csv created with {len(mics)} records.")
    
    # create source table
    sources=pd.DataFrame(source_records, columns=['source_id','source_name'])
    sources.to_csv('Sources.csv',index=False)
    print(f"Sources.csv created with {len(sources)} records.")
    
    # create venue table   
    venue_records = []
    for i in range(100):
        venue_name=generate_venue_name()
        venue_size=generate_venue_size()
        venue_reverberance_length=str(generate_reverberance_length(venue_size)) + " " + "seconds"
        venue_records.append([i, venue_name, venue_size, venue_reverberance_length])
        
    venues=pd.DataFrame(venue_records, columns=['venue_id','venue_name','size','reverberance'])
    venues.to_csv('Venues.csv',index=False)
    print(f"Venues.csv created with {len(venues)} records.")
    
    
    # Here's the main creation algorithm for bands, members, micUsed, and facts
    # band creation
    band_records = []
    
    # member creation, dummy entry is for first run only
    member_records = [[-1,0,'Drop Me']]
    
    # mic used creation, dummy entry for first run only
    mic_used_records = [[-1,0,0,'Drop Me',0]]
    
    # fact creation, dummy entry for first run only
    fact_records = [[-1,0,0,0,0,'Drop Me']]
      
    for i in range(10):
        # generate the band
        band_records.append([i, generate_band_name(), random.randint(4,10), generate_style()])
        # generate members
        # all bands start with 4 members by default
        # Lead Vocalist
        member_records.append([member_records[-1][0]+1, i, 'Lead Vocalist'])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, i, venue_id, source_id, result])
        
        # Drummer
        member_records.append([member_records[-1][0]+1, i, 'Drummer'])
        
        # individual drum mics w/ facts
        # bass drum
        add_micused_fact_drums('Kick', mic_used_records, member_records, fact_records, i)
       
        # snare drum
        add_micused_fact_drums('Snare', mic_used_records, member_records, fact_records, i)
        
        # hi-hats
        add_micused_fact_drums('Hi-hat', mic_used_records, member_records, fact_records, i)
        
        # toms
        add_micused_fact_drums('Toms', mic_used_records, member_records, fact_records, i)
        
        # overhead
        add_micused_fact_drums('Overhead', mic_used_records, member_records, fact_records, i)
        
        # Bassist
        member_records.append([member_records[-1][0]+1, i, 'Bassist'])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, i, venue_id, source_id, result])
        
        # Any one of other members
        member_records.append([member_records[-1][0]+1, i, generate_role()])
        
        # add mic used
        source_id=get_source_id(member_records[-1][2])
        mic_name=choose_mic_generator(source_id)
        mic_id=get_mic_id(mic_name)
        mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
        
        # add fact
        venue_id=random.randint(0, 99)
        result=generate_fact_result()
        fact_records.append([fact_records[-1][0]+1, mic_id, i, venue_id, source_id, result])
        
        # add the rest of the members based on num_members
        for j in range(4, band_records[i][2]):
            # generate band member
            member_records.append([member_records[-1][0]+1, i, generate_role()])
            
            # add mic used
            source_id=get_source_id(member_records[-1][2])
            mic_name=choose_mic_generator(source_id)
            mic_id=get_mic_id(mic_name)
            mic_used_records.append([mic_used_records[-1][0]+1, member_records[-1][0], source_id, mic_name, mic_id])
            
            # add fact
            venue_id=random.randint(0, 99)
            result=generate_fact_result()
            fact_records.append([fact_records[-1][0]+1, mic_id, i, venue_id, source_id, result])
    
            
    bands=pd.DataFrame(band_records, columns=['band_id','band_name','num_members','style'])
    bands.to_csv('Bands.csv',index=False)
    print(f"Bands.csv created with {len(bands)} records.")
    
    members=pd.DataFrame(member_records, columns=['member_id','band_id','role']).drop(0)
    members.to_csv('Members.csv',index=False)
    print(f"Members.csv created with {len(members)} records.")
    
    mics_used=pd.DataFrame(mic_used_records, columns=['mic_used_id','member_id','source_id','mic_name','mic_id']).drop(0)
    mics_used.to_csv('MicsUsed.csv',index=False)
    print(f"MicsUsed.csv created with {len(mics_used)} records.")
    
    facts=pd.DataFrame(fact_records, columns=['fact_id','mic_id','band_id','venue_id','source_id','result']).drop(0)
    facts.to_csv('Facts.csv',index=False)
    print(f"Facts.csv created with {len(facts)} records.")

if __name__=="__main__":
    generate_records()

# End of program