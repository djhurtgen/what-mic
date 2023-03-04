#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  3 13:10:41 2023

@author: vboxuser
"""

import sys
sys.path.append('/home/vboxuser/Documents')
import new_records_generator_alt
import update_staging_dw
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
    
default_args = {
    'owner': 'David Hurtgen',
    'start_date': days_ago(0),
    'email': ['nobody@noemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    dag_id='create_and_load',
    default_args=default_args,
    description='Create and load records into sdw',
    schedule_interval=timedelta(days=1),
    )

# 1st task, trigger new_records_generator_alt for new records
create = PythonOperator(
    task_id='create',
    python_callable=new_records_generator_alt.generate_new_records,
    dag=dag,
    )

# 2nd task, load the new records into the staging data warehouse
load = PythonOperator(
    task_id='load',
    python_callable=update_staging_dw.update_sdw,
    dag=dag,
    )

# pipeline
create >> load

# End of program