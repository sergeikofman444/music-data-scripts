import pandas as pd
import spotipy
import os
import time

from spotify_client import initialize_spotify_client

OUTPUT_DIR = 'output_files'
RATE_LIMIT_DELAY = 0.15

def complete_artist_data(spotify_artist_id, spotify_client):
    if spotify_artist_id == 'NOT_FOUND' or spotify_artist_id is None:
        return None, None

    try:
        artist_data = spotify_client.artist(spotify_artist_id)
        
        genres = artist_data['genres']
        followers = artist_data['followers'].get('total')
        
        return genres, followers
        
    except Exception as e:
        print(f"Error fetching data for Artist ID {spotify_artist_id}: {e}")
        return None, None

def process_all_artists():
    
    artists_file_path = os.path.join(OUTPUT_DIR, 'artists.csv')

    if not os.path.exists(artists_file_path):
        print(f"Error: Artists file not found at {artists_file_path}. Run ETL first.")
        return

    artists_df = pd.read_csv(artists_file_path)

    sp = initialize_spotify_client()

    new_genres_data = []
    new_followers_data = []

    for index, row in artists_df.iterrows():
        spotify_artist_id = row['id']
        
        if spotify_artist_id == 'NOT_FOUND':
             genres, followers = None, None
        elif pd.notna(row.get('genres')) and pd.notna(row.get('followers')):
            print(f"Skipping {spotify_artist_id} - already enriched.")
            genres, followers = row['genres'], row['followers']
        else:
             genres, followers = complete_artist_data(spotify_artist_id, sp)
             time.sleep(RATE_LIMIT_DELAY) 
        
        new_genres_data.append(genres)
        new_followers_data.append(followers)

    artists_df['genres'] = new_genres_data
    artists_df['followers'] = new_followers_data

    artists_df.to_csv(artists_file_path, index=False)
    print(f"\nArtists data enrichment complete. File updated at {artists_file_path}")
    print(artists_df.head())

if __name__ == "__main__":
    process_all_artists()