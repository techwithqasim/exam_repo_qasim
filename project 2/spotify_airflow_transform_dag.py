from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
import pandas as pd
from io import StringIO
import boto3
from datetime import datetime
from airflow.utils.dates import days_ago

# Initialize AWS clients
s3 = boto3.client('s3',
    aws_access_key_id='*******',
    aws_secret_access_key='********',
    region_name='us-east-1')
s3_resource = boto3.resource('s3')
glue_client = boto3.client('glue')

# S3 and Glue configurations
BUCKET_NAME = 'spotify-airflow-s3'
RAW_DATA_PREFIX = 'raw_data/to_process/'
TRANSFORMED_DATA_PREFIX = 'transformed_data/'
PROCESSED_DATA_PREFIX = 'raw_data/processed/'
CRAWLER_NAME = 'spotify-airflow-crawler'

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to process album data
def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                            'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    return album_list

# Function to process artist data
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'spotify_url': artist['external_urls']['spotify']}
                    artist_list.append(artist_dict)
    return artist_list

# Function to process song data
def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration_mins = round((row['track']['duration_ms']/(60*1000)),2)
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_mins':song_duration_mins,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
        
    return song_list

# Task: Process raw data and save transformed data
def transform_data():
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=BUCKET_NAME, Prefix=RAW_DATA_PREFIX)['Contents']:
        file_key = file['Key']
        if file_key.endswith('.json'):
            # Read raw data from S3
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            data = json.loads(response['Body'].read())

            spotify_data.append(data)
            spotify_keys.append(file_key)
            
            # Process data
            album_df = pd.DataFrame(album(data)).drop_duplicates(subset=['album_id'])
            artist_df = pd.DataFrame(artist(data)).drop_duplicates(subset=['artist_id'])
            song_df = pd.DataFrame(songs(data))
            
            # Convert date columns
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
            song_df['song_added'] = pd.to_datetime(song_df['song_added'])
            
            # Save transformed data to S3
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            for name, df in [('songs', song_df), ('album', album_df), ('artist', artist_df)]:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                s3.put_object(Bucket=BUCKET_NAME, Key=f'{TRANSFORMED_DATA_PREFIX}{name}_data/{name}_transformed_{timestamp}.csv', Body=buffer.getvalue())
            
    # Move processed file to "processed" folder
    s3_resource.meta.client.copy(
        {'Bucket': BUCKET_NAME, 'Key': file_key},
        BUCKET_NAME, f'{PROCESSED_DATA_PREFIX}{file_key.split("/")[-1]}'
    )
    s3_resource.Object(BUCKET_NAME, file_key).delete()

# Task: Trigger Glue Crawler
def trigger_glue_crawler():
    response = glue_client.start_crawler(Name=CRAWLER_NAME)
    print(f"Glue Crawler triggered: {response}")

# Define the DAG
dag = DAG(
    'spotify_airflow_transform_dag',
    default_args=default_args,
    description='ETL pipeline for Spotify data using Airflow',
    schedule_interval=None,  # Manual trigger for testing
    start_date=datetime(2025, 1, 6),
    catchup=False,
)

# Define Airflow tasks
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

glue_crawler_task = PythonOperator(
    task_id='trigger_glue_crawler',
    python_callable=trigger_glue_crawler,
    dag=dag,
)

# Task dependencies
transform_task >> glue_crawler_task