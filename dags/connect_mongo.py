from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os 

def load_mongo_client() -> MongoClient:
    uri = os.getenv('AIRFLOW__DATABASE__MONGO_CONN')
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    if client.is_mongos:
        print('Client connected to mongodb')
    return client
    