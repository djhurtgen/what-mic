"""
Created on Fri Mar  3 13:10:41 2023

@author: David Hurtgen

This program is an Airflow DAG for creating, extracting, loading, and testing
data for the staging data warehouse.
"""

import sys
sys.path.append('/home/david/Documents/what-mic/Finals/Python')
from new_records_generator_alt import generate_new_records
from update_staging_dw import update_sdw
from test_staging_dw import test_sdw
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
# from airflow.utils.dates import days_ago

def _get_initial_records_counts(ti):
    """
    Retrieve the current number of records from dimBand, flakeMembers, flakeMicUsed, factResults
    Store with Airflow metadata, XCom's
    Used to compare against number of records post-staging dw insertion
    :param ti: an object for pushing data to XCom
    """
    
    # connect to mysql database
    try: 
        mysql_conn = mysql.connector.connect(user='root',password='############', host='127.0.0.1',database='what_mic')
        print("Mysql connection established")
    except:
        print("Failed to connect to the staging data warehouse")
    cursor = mysql_conn.cursor()

    # variables for testing stage, pre dw update
    # count of bands records
    cursor.execute("select count(*) from what_mic.dimBand")
    init_num_records_bands = int(cursor.fetchone()[0])

    # count of members records
    cursor.execute("select count(*) from what_mic.flakeMembers")
    init_num_records_members = int(cursor.fetchone()[0])

    # count of mics_used records
    cursor.execute("select count(*) from what_mic.flakeMicUsed")
    init_num_records_mics_used = int(cursor.fetchone()[0])

    # count of facts records
    cursor.execute("select count(*) from what_mic.factResults")
    init_num_records_facts = int(cursor.fetchone()[0])

    # push to Airflow xcom
    irc = [init_num_records_bands, init_num_records_members, init_num_records_mics_used, init_num_records_facts]
    ti.xcom_push(key='initial_counts', value=irc)

    mysql_conn.close()
    print("Mysql connection closed")


default_args = {
    'owner': 'David Hurtgen',
    'start_date': datetime(2023,3,9),
    'email': ['nobody@noemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    }

dag = DAG(
    dag_id='create_extract_load_test',
    catchup=False,
    default_args=default_args,
    description='Create, extract, load, & test new records',
    schedule_interval='0/5 17,18 * * *',
    )

# get count of records from each table before new records
# are inserted
get_counts = PythonOperator(
    task_id='get_counts',
    python_callable=_get_initial_records_counts,
    dag=dag,
)

# CREATE new records
create = PythonOperator(
    task_id='create',
    python_callable=generate_new_records,
    dag=dag,
    )

# EXTRACT the records from the .csv files created above 
# and LOAD them into the staging dw
extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=update_sdw,
    dag=dag,
    )

# test for correct number of records added
# and valid data
test_data = PythonOperator(
    task_id='test_data',
    python_callable=test_sdw,
    dag=dag
)

# pipeline
get_counts >> create >> extract_and_load >> test_data

# End of program