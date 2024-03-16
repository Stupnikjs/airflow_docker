import os
import tempfile
import pandas as pd 
import google.auth
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow import DAG
from google.cloud import storage
from stupnikjs.common_package.connect_mongo import load_mongo_client
from stupnikjs.common_package.six_month_ago import six_month_ago_ts

# Fetch the connection object by its connection ID

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'datalake')
BIGQUERY_TABLE = "video_games"


credentials, project_id = google.auth.default()

def pd_df_15_best(df):

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

    df = df.sort_values('avg_note').head(15)

    return df 



def fetch_mongo_to_gc_storage_fl(**kwargs):

    logical_date = kwargs['logical_date']
    six_mounth_ago = six_month_ago_ts(logical_date)
    client = load_mongo_client()
    db = client.get_database('Cluster0')
    col = db.get_collection('games_rating')
    
    projection = {'_id': False, 'summary': False, 'verified': False, 'reviewText': False, 'reviewTime': False }
    result = col.find({'unixReviewTime': {'$lt': six_mounth_ago}}, projection)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        df = pd.DataFrame(list(result))
        file_path = os.path.join(temp_dir,str(logical_date).replace(" ", "") + '.csv')
        file_path_bucket = str(six_mounth_ago).replace(" ", "") + '.csv'

        df = pd_df_15_best(df)
        df.to_csv(os.path.join(temp_dir,file_path), index=False)
      
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        
        client = storage.Client()
        bucket = client.bucket(kwargs['bucket'])

        blob = bucket.blob(file_path_bucket)  # name of the object in the bucket 
        blob.upload_from_filename(file_path)

        blob_url = blob.public_url

        bucket_file = kwargs['bucket']
        kwargs['ti'].xcom_push(key='blob_url', value=blob_url)
        kwargs['ti'].xcom_push(key='bucket', value=file_path_bucket)
        kwargs['ti'].xcom_push(key='six_month_ago', value=str(six_mounth_ago).replace(" ", ""))
    

        
    


dag = DAG(
    'daily_update_dag',
    # start_date=days_ago(0),
    # schedule_interval='0 0 * * *',
    catchup=False
)

with dag:

    fetch_mongo_gc_storage_task = PythonOperator(
        task_id='fetch_mongo_gcstorage_task',
        python_callable=fetch_mongo_to_gc_storage_fl, 
        op_kwargs={
            "bucket": BUCKET,
        },
        dag=dag
    )
 
    insert_or_update_task = 
   

    fetch_mongo_gc_storage_task >> insert_or_update_task

