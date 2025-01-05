import json
import boto3
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract bucket and object key details from the EventBridge event
        detail = event.get('detail', {})
        bucket_name = detail.get('bucket', {}).get('name')
        object_key = detail.get('object', {}).get('key')

        # Ensure the event contains the required details
        if not bucket_name or not object_key:
            return {
                'statusCode': 400,
                'body': "Error: Event is missing required bucket or object key details."
            }

        # Filter objects based on the prefix
        if not object_key.startswith('extracted_data/'):
            print(f"Ignoring object {object_key} as it does not match the prefix 'extracted_data/'.")
            return {
                'statusCode': 200,
                'body': f"Ignored object: {object_key}"
            }

        # Fetch the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        # Parse the JSON data
        top_tracks = json.loads(file_content)

        # Transform the data
        transformed_data = []
        for track in top_tracks.get('tracks', []):
            transformed_data.append({
                'track_name': track['name'],
                'album_name': track['album']['name'],
                'popularity': track['popularity'],
            })

        # Convert the transformed data to JSON
        transformed_json = '\n'.join([json.dumps(record) for record in transformed_data])

        # Define the destination file key
        destination_file = f'transformed_data/{object_key.split("/")[-1].split(".")[0]}_transformed_{str(datetime.now()).replace(" ", "_")}.json'

        # Upload the transformed data back to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=destination_file,
            Body=transformed_json,
            ContentType='application/json'
        )

        print(f"Transformed data uploaded to S3: {bucket_name}/{destination_file}")
        return {
            'statusCode': 200,
            'body': f"Transformed data uploaded to S3: {bucket_name}/{destination_file}"
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }