import pandas as pd
import spotipy
import os
import time

from spotify_client import initialize_spotify_client

OUTPUT_DIR = 'output_files'
RATE_LIMIT_DELAY = 0.05

def complete_artist_data(spotify_artist_id, spotify_client):
    if spotify_artist_id == 'NOT_FOUND' or spotify_artist_id is None:
        return None, None

    try:
        artist_data = spotify_client.artist(spotify_artist_id)
        
        genres = artist_data['genres']
        followers = artist_data['followers'].get('total')
        
        return genres, followers
        
    except Exception as e:
        # Log the error for debugging
        print(f"Error fetching data for Artist ID {spotify_artist_id}: {e}")
        return None, None # Return None for both on error

def process_all_artists():
    
    artists_file_path = os.path.join(OUTPUT_DIR, 'artists.csv')

    if not os.path.exists(artists_file_path):
        print(f"Error: Artists file not found at {artists_file_path}. Run ETL first.")
        return

    # 1. Load Data
    artists_df = pd.read_csv(artists_file_path)

    sp = initialize_spotify_client()

    # Prepare lists to hold the new data
    new_genres_data = []
    new_followers_data = []

    for index, row in artists_df.iterrows():
        spotify_artist_id = row['id']
        
        # Check for the 'NOT_FOUND' sentinel if it somehow made it into artists.csv
        if spotify_artist_id == 'NOT_FOUND':
             genres, followers = None, None
        else:
             genres, followers = complete_artist_data(spotify_artist_id, sp)
        
        new_genres_data.append(genres)
        new_followers_data.append(followers)
        
        # Rate limit delay to prevent API key blocking
        time.sleep(RATE_LIMIT_DELAY) 

    # 4. Merge New Data Back to DataFrame
    artists_df['genres'] = new_genres_data
    artists_df['followers'] = new_followers_data

    # 5. Save Updated File
    artists_df.to_csv(artists_file_path, index=False)
    print(f"\nArtists data enrichment complete. File updated at {artists_file_path}")
    print(artists_df.head())

# --- Execute the Process (Example) ---
process_all_artists()