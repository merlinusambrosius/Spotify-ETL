# Spotify ETL
Spotify's API ETL project.  DAG scheduled ETL in Airflow/Docker that pulls live music metadata.
Build Your First Workflow (2-4 hours):
- Create a simple DAG file (Python script in your airflow/dags folder) that:
- Extracts: Pulls song metadata from Spotify's API. (Get a free Spotify dev account at https://developer.spotify.com/, grab client ID/secret. Use spotipy library: pip install spotipy—it's easy for querying tracks/artists.)
- Transforms: Cleans the data (e.g., normalize genres, fix missing albums like in your dummy script).
- Loads: Uploads the cleaned JSON/CSV to your S3 bucket (using boto3, which you already know).

Example skeleton DAG:Python
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
import json

def extract_spotify_metadata(**kwargs):
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id='your_id', client_secret='your_secret'))
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
    s3 = boto3.client('s3')
    s3.put_object(Bucket='your-bucket-name', Key='spotify_cleaned.json', Body=json.dumps(cleaned_data))

with DAG('spotify_etl_dag', start_date=datetime(2026, 1, 1), schedule_interval='@daily') as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_spotify_metadata)
    transform = PythonOperator(task_id='transform', python_callable=transform_metadata)
    load = PythonOperator(task_id='load', python_callable=load_to_s3)

    extract >> transform >> load  # Task order

# Verify by parsing .json data using view_spotify.py script:

🎵 Spotify ETL Data Viewer
======================================================================
✅ Loaded from local file (3 tracks)

======================================================================
 1. Let Down
    🎤 Artist : Radiohead
    💿 Album  : OK Computer
────────────────────────────────────────────────────────────
 2. Creep
    🎤 Artist : Radiohead
    💿 Album  : Pablo Honey
────────────────────────────────────────────────────────────
 3. No Surprises
    🎤 Artist : Radiohead
    💿 Album  : OK Computer
────────────────────────────────────────────────────────────
======================================================================