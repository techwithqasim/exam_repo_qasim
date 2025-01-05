import boto3
import json
import csv
import io
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# AWS Credentials
aws_access_key_id='**********'
aws_secret_access_key='****************'
AWS_REGION = 'us-east-1'
S3_BUCKET = 'spotify-airflow-s3'
INPUT_KEY = 'spotify/spotify_data_2025-01-05_19-22-49_869433015a4f42db88bd51320ce8c571.json'
OUTPUT_KEY = 'spotify/transformed_data.csv'

# S3 Client Initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=AWS_REGION
)

def extract_data():
    """Extract JSON data from S3."""
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
    data = json.loads(response['Body'].read().decode('utf-8'))
    logging.info("Data extracted successfully.")
    return data

def transform_data(data):
    """Transform JSON data into a CSV format."""
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Write header
    csv_writer.writerow(['Playlist Name', 'Playlist ID', 'Owner Name', 'Total Tracks', 'Image URL'])

    # Write rows
    for playlist in data['items']:
        playlist_name = playlist.get('name', 'N/A')
        playlist_id = playlist.get('id', 'N/A')
        owner_name = playlist.get('owner', {}).get('display_name', 'N/A')
        total_tracks = playlist.get('tracks', {}).get('total', 0)
        image_url = playlist.get('images', [{}])[0].get('url', 'N/A')
        csv_writer.writerow([playlist_name, playlist_id, owner_name, total_tracks, image_url])
    
    logging.info("Data transformed successfully.")
    csv_buffer.seek(0)
    return csv_buffer

def load_data(csv_buffer):
    """Load the transformed data back into S3."""
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    logging.info(f"Transformed data loaded to s3://{S3_BUCKET}/{OUTPUT_KEY}")

# Define the Airflow DAG
dag = DAG(
    'spotify_etl_pipeline',
    description='Extract, Transform, and Load Spotify Data using S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

# Task 1: Extract
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

# Task 2: Transform
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=lambda: transform_data(extract_task.output),
    dag=dag
)

# Task 3: Load
load_task = PythonOperator(
    task_id='load_data',
    python_callable=lambda: load_data(transform_task.output),
    dag=dag
)

# DAG Execution Flow
extract_task >> transform_task >> load_task