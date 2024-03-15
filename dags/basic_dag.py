import os
import json
import tempfile
import sys
import pandas as pd 
import google.auth
import calendar
import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from pandas_gbq import to_gbq
from connect_mongo import load_mongo_client

# Fetch the connection object by its connection ID

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'datalake.video_games')




def six_month_ago(now_date):

    curr_month = now_date.month
    curr_day = now_date.day
    
    months = []
    # decremente les 6 derniers mois et les ajoutes dans une liste 
    for i in range(7): 
        months.append(curr_month)
        if curr_month != 1:
            curr_month -= 1
        else:
            curr_month = 12

    # liste correspondante des nombres de jours 
    day_count = []
    for i in months: 
        num_days = calendar.monthrange(now_date.year, i)[1]
        day_count.append(num_days)

    # est ce que il y a six mois était l'année dernière 
    last_year = months[0] < months[-1]


    if last_year : 
        if now_date.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(now_date.year - 1, months[-1], now_date.day)
        else:
            six_mounth_ago = datetime.datetime(now_date.year - 1, months[-1] , day_count[-1])
    else:
        if now_date.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(now_date.year, months[-1], now_date.day )
        else:
            six_mounth_ago = datetime.datetime(now_date.year, months[-1] , day_count[-1])

    # gerer le cas ou le jour est 31 ou 30 et n'existe pas dans le mois d'il ya 6mois

    six_mounth_ago_unix = six_mounth_ago.timestamp() 
      
    return six_mounth_ago_unix



def pd_df_processing(df):

    df['average_overall'] = df.groupby(by='asin')['overall'] \
            .transform('mean')
    
    df['game_id'] = df['asin'] 
    df['avg_note'] = df['average_overall']
    df['user_note'] = df['overall']
    df['latest_note'] = df.groupby('asin')['unixReviewTime'].transform('max').astype(int)
    df['oldest_note'] = df.groupby('asin')['unixReviewTime'].transform('min').astype(int)
    

    to_drop = [col for col in df.columns if col not in ['game_id', 'avg_note', 'user_note', 'latest_note', 'oldest_note']]

    df.drop(columns=to_drop, inplace=True)

    df['user_note'] = df['user_note'].astype(int)
    df['game_id'] = (df['game_id'].astype('category').cat.codes + 1).astype(int)

    return df 



def super_simple(): 
    print('hello max')

def simple(): 
    print('hello-world')


dag = DAG(
    'simple_dag',
    # start_date=days_ago(0),
    # schedule_interval='0 0 * * *',
    catchup=False
)

with dag:

    simple_task = PythonOperator(
        task_id='simple_task',
        python_callable=simple, 
      
    )
    super_simple_task = PythonOperator(
        task_id='super_simple_task', 
        python_callable=super_simple,
        provide_context=True
    )

    simple_task >> super_simple_task

