#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 17:34:31 2023

@author: vboxuser

This program contains all the function necessary for records generation
"""

import random

# band name nouns and adjectives
noun_list = ['Anemone','Bandwagon','Concoction','Doldrums','Epiphany',\
             'Flummery','Gambit','Halfwit','Interloper','Jabberwocky',\
                 'Kibosh','Loophole','Misanthrope','Nudnik','Oxymoron',\
                     'Pedagogue','Ruckus','Sobriquet','Typhoon','Windbag']
    
adjective_list = ['Apoplectic','Blubbering','Cantankerous','Diaphanous','Extraterrestrial',\
                  'Flummoxed','Gobsmacked','Harebrained','Indomitable','Lugubrious',\
                      'Mellifluous','Orotund','Pulchritudinous','Quiddling','Ragtag',\
                          'Slipshod','Toothsome','Vainglorious','Wuthering','Zigzag']

# mics by model
vox_mics = ['OM7','M 88 TG','d:facto','SR314','767a N/D','PR35','KMS 105','e935','Beta 58A','SM58','M80']
                
inst_mics = ['i5','M 69 TG','M 88 TG','M 160','M 201 TG','SR25','RE16','RE20','PR30','R-121','e906','MD 421','MD 441 U','Beta 57A',\
             'SM7B','SM57']
    
kick_mics = ['D6','PL35','RE20','e902','Beta 52A']

snare_mics = ['i5','M 201 TG','MD 441 U','SM57']
  
tom_mics = ['D2','e904','MD 421','Beta 98AD/C']
    
area_mics = ['C414 XLII','AT4040','635a','KM 184','CMC64','SM81']

# roles
roles = ['Backup Vocalist','Guitarist','Horn Player','Keyboardist','Percussionist','String Player']

# venue size
venue_sizes = ['Small','Medium','Large']

# results
results = ['Poor','Mediocre','Average','Good','Excellent']

# band styles
styles = ['Folk','Hip-Hop','Jazz','Pop','Rock']

# mic list for dimMics
mic_records = [[0,'AKG','C414 XLII','Condenser'], [1,'Audio-Technica','AT4040','Condenser'], [2,'Audix','D2','Dynamic'], \
               [3,'Audix','D6','Dynamic'], [4,'Audix','i5','Dynamic'], [5,'Audix','OM7','Dynamic'], \
                   [6,'Beyerdynamic','M 69 TG','Dynamic'], [7,'Beyerdynamic','M 88 TG','Dynamic'], [8,'Beyerdynamic','M 160','Ribbon'], \
                       [9,'Beyerdynamic','M 201 TG','Dynamic'], [10,'DPA','d:facto','Condenser'], [11,'Earthworks','SR25','Condenser'], \
                           [12,'Earthworks','SR314','Condenser'], [13,'Electro-Voice','635a','Dynamic'], \
                               [14,'Electro-Voice','767a N/D','Dynamic'], [15,'Electro-Voice','PL35','Dynamic'], \
                                   [16,'Electro-Voice','RE16','Dynamic'], [17,'Electro-Voice','RE20','Dynamic'], \
                                       [18,'Heil','PR30','Dynamic'], [19,'Heil','PR35','Dynamic'], [20,'Neumann','KM 184','Condenser'], \
                                           [21,'Neumann','KMS 105','Condenser'], [22,'Royer','R-121','Ribbon'], \
                                               [23,'Schoeps','CMC64','Condenser'], [24,'Sennheiser','e902','Dynamic'], \
                                                   [25,'Sennheiser','e904','Dynamic'], [26,'Sennheiser','e906','Dynamic'], \
                                                       [27,'Sennheiser','e935','Dynamic'], [28,'Sennheiser','MD 421','Dynamic'], \
                                                           [29,'Sennheiser','MD 441 U','Dynamic'], [30,'Shure','Beta 52A','Dynamic'], \
                                                               [31,'Shure','Beta 57A','Dynamic'], [32,'Shure','Beta 58A','Dynamic'], \
                                                                   [33,'Shure','Beta 98AD/C','Condenser'], [34,'Shure','SM7B','Dynamic'], \
                                                                       [35,'Shure','SM57','Dynamic'], [36,'Shure','SM58','Dynamic'], \
                                                                           [37,'Shure','SM81','Condenser'], [38,'Telefunken','M80','Dynamic']]

source_records = [[0,'Backup Vocal'], [1,'Bass'], [2,'Drums, Kick'], [3,'Drums, Snare'], [4,'Drums, Hi-hat'], [5,'Drums, Toms'], \
               [6,'Drums, Overhead'], [7,'Guitar'], [8,'Horn'], [9,'Keyboards'], [10,'Lead Vocal'], [11,'Percussion'], [12,'Strings']]

# generator functions
# function to generate venue name
def generate_venue_name():
    venue_name = "The" + " " + random.choice(adjective_list) + " " + random.choice(noun_list)
    return venue_name

# function to generate venue size
def generate_venue_size():
    venue_size = random.choice(venue_sizes)
    return venue_size

# function to generate fact result
def generate_fact_result():
    fact_result = random.choice(results)
    return fact_result

# function to generate random band names
def generate_band_name():
    band_name = random.choice(adjective_list) + " " + random.choice(noun_list)
    return band_name

# function to generate band's musical style
def generate_style():
    style = random.choice(styles)
    return style

# function to generate member role
def generate_role():
    role = random.choice(roles)
    return role

# functions to generate microphones, return a list
def generate_vox_mic():
    vox_mic = random.choice(vox_mics)
    return vox_mic

def generate_inst_mic():
    inst_mic = random.choice(inst_mics)
    return inst_mic

def generate_kick_mic():
    kick_mic = random.choice(kick_mics)
    return kick_mic

def generate_snare_mic():
    snare_mic = random.choice(snare_mics)
    return snare_mic

def generate_tom_mic():
    tom_mic = random.choice(tom_mics)
    return tom_mic

def generate_area_mic():
    area_mic = random.choice(area_mics)
    return area_mic

# function to generate reverberance length in seconds based on venue size
def generate_reverberance_length(venue_size):
    if venue_size=='Small':
        mu=1.0
        sigma=0.2
        reverberance_length = round(random.normalvariate(mu, sigma), 2)
        return reverberance_length
    elif venue_size=='Medium':
        mu=1.5
        sigma=0.3
        reverberance_length = round(random.normalvariate(mu, sigma), 2)
        return reverberance_length
    else:
        mu=2.0
        sigma=0.5
        reverberance_length = round(random.normalvariate(mu, sigma), 2)
        return reverberance_length

# These are funcions necessary for the main alogrithm
# function to get source_id, excludes Drummers
def get_source_id(role):
    if role=='Backup Vocalist':
        return 0
    elif role=='Bassist':
        return 1
    elif role=='Guitarist':
        return 7
    elif role=='Horn Player':
        return 8
    elif role=='Keyboardist':
        return 9
    elif role=='Lead Vocalist':
        return 10
    elif role=='Percussionist':
        return 11
    else:
        return 12

# function to get source_id for Drummers only
def get_sid_drummers(drum):
    if drum=='Kick':
        return 2
    elif drum=='Snare':
        return 3
    elif drum=='Hi-hat':
        return 4
    elif drum=='Toms':
        return 5
    else:
        return 6
    
# also need a function to generate mics based on source_id
def choose_mic_generator(source_id):
    if source_id==0 or source_id==10:
        result=generate_vox_mic()
        return result
    elif source_id in [1,7,8]:
        result=generate_inst_mic()
        return result
    elif source_id==2:
        result=generate_kick_mic()
        return result
    elif source_id==3:
        result=generate_snare_mic()
        return result
    elif source_id==5:
        result=generate_tom_mic()
        return result
    else:
        result=generate_area_mic()
        return result

# function to get mic_id
def get_mic_id(mic_used):
    for mic in mic_records:
        if mic[2]==mic_used:
            return mic[0]
            break

# End of program