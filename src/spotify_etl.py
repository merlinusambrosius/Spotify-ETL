from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator   # ← change to this
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import json

def extract_spotify_metadata(**kwargs):
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    results = sp.search(q='artist:Radiohead', type='track', limit=5)  # Example query
    return results['tracks']['items']  # Raw metadata

def transform_metadata(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract')
    cleaned = [{'name': track['name'], 'artist': track['artists'][0]['name'], 'album': track['album']['name']} for track in raw_data]
    return cleaned  # Simple clean: just extract basics

def load_to_s3(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='transform')
    s3 = boto3.client('s3')   # boto3 will now pick up the env vars automatically
    s3.put_object(
        Bucket='justin-music-ai-experiments-2026',
        Key=f'spotify_cleaned_{datetime.now().strftime("%Y%m%d")}.json',  # timestamped
        Body=json.dumps(cleaned_data)
    )

with DAG('spotify_etl_dag', start_date=datetime(2026, 1, 1), schedule='@daily') as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_spotify_metadata)
    transform = PythonOperator(task_id='transform', python_callable=transform_metadata)
    load = PythonOperator(task_id='load', python_callable=load_to_s3)

    extract >> transform >> load  # Task order