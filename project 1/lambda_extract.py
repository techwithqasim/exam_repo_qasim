import json
import boto3
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Spotify API credentials
    CLIENT_ID = '38a7fb16ec084ef380d93ad5f9b2b15b'
    CLIENT_SECRET = '66858833cb9f48a9a23acf91dcde8aa0'

    # Authenticate with Spotify API
    auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    sp = Spotify(auth_manager=auth_manager)

    # Extract artist_name from the event
    artist_name = event.get('artist_name')
    if not artist_name:
        return {
            'statusCode': 400,
            'body': "Error: 'artist_name' is required in the event payload."
        }

    try:
        # Search for the artist
        result = sp.search(q=artist_name, type='artist', limit=1)
        artists = result.get('artists', {}).get('items', [])
        if not artists:
            return {
                'statusCode': 404,
                'body': f"Error: No artist found with the name '{artist_name}'."
            }

        artist_id = artists[0]['id']

        # Fetch the artist's top tracks
        top_tracks = sp.artist_top_tracks(artist_id, country='IN')

        # Convert to JSON
        top_tracks_json = json.dumps(top_tracks)

        # Upload to S3
        bucket_name = 'spotify-snowflake-data'
        file_name = f'extracted_data/spotify_artist_{artist_name}_top_tracks_{str(datetime.now()).replace(" ", "_")}.json'
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=top_tracks_json)

        return {
            'statusCode': 200,
            'body': f"Top tracks for '{artist_name}' uploaded to S3: {bucket_name}/{file_name}"
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }