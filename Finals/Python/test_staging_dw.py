"""
Created on Fri July 21 09:00:01 2023

@author: David Hurtgen

This program tests newly inserted records in the staging data warehouse
and generates a log file (.txt) with results.
If all checks out, the records can be inserted into the production data warehouse.
It is the TEST portion of the Airflow DAG.
"""

import mysql.connector
import pandas as pd
from time import time, ctime

# main function
def test_sdw(ti):
    """
    Tests that the new number of records in dimBand, flakeMembers, flakeMicUsed, factResults
    matches the old number of records plus the new records created by new_records_generator_alt.py
    Also tests for data validity where programs may run with semantic errors
    :param ti: an object for pulling initial count data from XCom
    """

    # establish testing variables pre-insertion
    counts = ti.xcom_pull(key='initial_counts', task_ids=['get_counts'])
    init_num_records_bands = counts[0][0]
    init_num_records_members = counts[0][1]
    init_num_records_mics_used = counts[0][2]
    init_num_records_facts = counts[0][3]

    # import the .csv files and assign them to dataframes
    bands = pd.read_csv('~/Documents/what-mic/CSV/Bands.csv', index_col=False)
    members = pd.read_csv('~/Documents/what-mic/CSV/Members.csv', index_col=False)
    mics_used = pd.read_csv('~/Documents/what-mic/CSV/MicsUsed.csv', index_col=False)
    facts = pd.read_csv('~/Documents/what-mic/CSV/Facts.csv', index_col=False)
    
    
    # connect to mysql database
    try: 
        mysql_conn = mysql.connector.connect(user='root',password='############', host='127.0.0.1',database='what_mic')
        print("Mysql connection established")
    except:
        print("Failed to connect to the staging data warehouse")
    cursor = mysql_conn.cursor()
    
        
    # variables for testing stage, post dw update
    cursor.execute("select count(*) from what_mic.dimBand")
    final_num_records_bands = int(cursor.fetchone()[0])
    
    # count of members records
    cursor.execute("select count(*) from what_mic.flakeMembers")
    final_num_records_members = int(cursor.fetchone()[0])
    
    # count of mics_used records
    cursor.execute("select count(*) from what_mic.flakeMicUsed")
    final_num_records_mics_used = int(cursor.fetchone()[0])
    
    # count of facts records
    cursor.execute("select count(*) from what_mic.factResults")
    final_num_records_facts = int(cursor.fetchone()[0])

    # TEST stage
    # two broad categories for testing: 
    # 1) correct number of rows inserted
    # 2) data is valid (i.e., no null values, strings are valid, etc.)
    now = ctime(time())
    records_added_bands = final_num_records_bands - init_num_records_bands
    records_added_members = final_num_records_members - init_num_records_members
    records_added_mics_used = final_num_records_mics_used - init_num_records_mics_used
    records_added_facts = final_num_records_facts - init_num_records_facts
    with open("../test_log.txt", "w") as log_file:
        # dimBand
        log_file.write(f"Testing information for what_mic staging db, {now}:\n")
        log_file.write("\nRecords creation, dimBand:\n")
        log_file.write(f"\tThere should be {len(bands)} new records added to the table. \n")
        log_file.write(f"\tActual number of records added: {records_added_bands}\n")
        log_file.write("Data validity, dimBand:\n")

        # band_id must start at the correct number and advance by 1 sequentially
        cursor.execute(f"with cte as (select band_id, lag(band_id, 1) over(order by band_id) prev_band_id from dimBand) \
                        select count(*) from cte where band_id >= {init_num_records_bands} \
                        and band_id = prev_band_id + 1;")
        correct_band_id = int(cursor.fetchone()[0])
        if correct_band_id == len(bands):
            log_file.write("\tThe band_id data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(bands) - correct_band_id} records with invalid band_id data!\n")

        # not so concerned with band names

        # num_members can't be negative or 0
        cursor.execute(f"select count(*) from what_mic.dimBand \
                        where band_id >= {init_num_records_bands} and num_members > 0")
        correct_num_members = int(cursor.fetchone()[0])
        if correct_num_members == len(bands):
            log_file.write("\tThe num_members data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(bands) - correct_num_members} records with invalid num_members data!")

        # style must be a valid category
        cursor.execute(f"select count(*) from dimBand where band_id >= {init_num_records_bands} and \
                        (style in ('Folk', 'Hip-Hop', 'Jazz', 'Pop', 'Rock'))")
        correct_style = int(cursor.fetchone()[0])
        if correct_style == len(bands):
            log_file.write("\tThe style data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(bands) - correct_style} records with invalid style data!")


        # flakeMembers
        log_file.write("\nRecords creation, flakeMembers:\n")
        log_file.write(f"\tThere should be {len(members)} new records added to the table. \n")
        log_file.write(f"\tActual number of records added: {records_added_members}\n")
        log_file.write("Data validity, flakeMembers:\n")

        # member_id must start at the correct number and advance by 1 sequentially
        cursor.execute(f"with cte as (select member_id, lag(member_id, 1) over(order by member_id) prev_member_id \
                        from flakeMembers) \
                        select count(*) from cte where member_id >= {init_num_records_members} \
                        and member_id = prev_member_id + 1;")
        correct_member_id = int(cursor.fetchone()[0])
        if correct_member_id == len(members):
            log_file.write("\tThe member_id data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(members) - correct_member_id} records with invalid member_id data!\n")

        # band_id validity alread enforced as foreign key

        # role must be a valid category
        cursor.execute(f"select count(*) from flakeMembers where member_id >= {init_num_records_members} and \
                        (role in ('Backup Vocalist', 'Bassist', 'Drummer', 'Guitarist', 'Horn Player', 'Keyboardist', \
                        'Lead Vocalist', 'Percussionist', 'String Player'))")
        correct_role = int(cursor.fetchone()[0])
        if correct_role == len(members):
            log_file.write("\tThe role data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(members) - correct_role} records with invalid role data!")

        
        # flakeMicUsed
        log_file.write("\nRecords creation, flakeMicUsed:\n")
        log_file.write(f"\tThere should be {len(mics_used)} new records added to the table. \n")
        log_file.write(f"\tActual number of records added: {records_added_mics_used}\n")
        log_file.write("Data validity, flakeMicUsed:\n")

        # mic_used_id must start at the correct number and advance by 1 sequentially
        cursor.execute(f"with cte as (select mic_used_id, lag(mic_used_id, 1) over(order by mic_used_id) prev_mic_used_id \
                        from flakeMicUsed) \
                        select count(*) from cte where mic_used_id >= {init_num_records_mics_used} \
                        and mic_used_id = prev_mic_used_id + 1;")
        correct_mic_used_id = int(cursor.fetchone()[0])
        if correct_mic_used_id == len(mics_used):
            log_file.write("\tThe mic_used_id data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(mics_used) - correct_mic_used_id} records with invalid mic_used_id data!\n")

        # member_id, source_id, mic_id validity alread enforced as foreign keys

        # mic_name must be a valid category
        cursor.execute(f"select count(*) from flakeMicUsed where mic_used_id >= {init_num_records_mics_used} and \
                        (mic_name in (select distinct model from dimMics))")
        correct_mic_name = int(cursor.fetchone()[0])
        if correct_mic_name == len(mics_used):
            log_file.write("\tThe mic_name data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(mics_used) - correct_mic_name} records with invalid mic_name data!")


        # factResults
        log_file.write("\nRecords creation, factResults:\n")
        log_file.write(f"\tThere should be {len(facts)} new records added to the table. \n")
        log_file.write(f"\tActual number of records added: {records_added_facts}\n")
        log_file.write("Data validity, factResults:\n")

        # mic_used_id must start at the correct number and advance by 1 sequentially
        cursor.execute(f"with cte as (select fact_id, lag(fact_id, 1) over(order by fact_id) prev_fact_id \
                        from factResults) \
                        select count(*) from cte where fact_id >= {init_num_records_facts} \
                        and fact_id = prev_fact_id + 1;")
        correct_fact_id = int(cursor.fetchone()[0])
        if correct_fact_id == len(facts):
            log_file.write("\tThe fact_id data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(facts) - correct_fact_id} records with invalid fact_id data!\n")

        # mic_id, band_id, venue_id, source_id validity already enforced as foreign keys

        # Result must be a valid category
        cursor.execute(f"select count(*) from factResults where fact_id >= {init_num_records_facts} and \
                        (result in ('Poor', 'Mediocre', 'Average', 'Good', 'Excellent'))")
        correct_result = int(cursor.fetchone()[0])
        if correct_result == len(facts):
            log_file.write("\tThe result data is valid.\n")
        else:
            log_file.write(f"\tThere are {len(facts) - correct_result} records with invalid result data!")

    mysql_conn.close()
    print("Mysql connection closed")

if __name__=="__main__":
    test_sdw()

# End of program