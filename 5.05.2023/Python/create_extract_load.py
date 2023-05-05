"""
Created on Fri Mar  3 13:10:41 2023

@author: David Hurtgen
"""

import sys
sys.path.append('/home/david/Documents/what-mic/3.11.2023/Python')
import new_records_generator_alt
import update_staging_dw
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# from airflow.utils.dates import days_ago
    
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
    dag_id='create_extract_load',
    catchup=False,
    default_args=default_args,
    description='Create, extract, and load records into sdw',
    schedule_interval='0/5 17,18 * * *',
    )

# 1st task, trigger new_records_generator_alt to CREATE new records
create = PythonOperator(
    task_id='create',
    python_callable=new_records_generator_alt.generate_new_records,
    dag=dag,
    )

# 2nd task, EXTRACT the records from the .csv files created above and 
# LOAD the new records into the staging data warehouse
extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=update_staging_dw.update_sdw,
    dag=dag,
    )

# pipeline
create >> extract_and_load

# End of program