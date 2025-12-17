import pandas as pd
import spotipy
from datetime import datetime
import os
import time

from spotify_client import initialize_spotify_client

OUTPUT_DIR = 'output_files'
RATE_LIMIT_DELAY = 0.05

def parse_spotify_date(date_string):
    """Parses a date string from Spotify into a datetime.date object."""
    if not date_string:
        return None
        
    try:
        if len(date_string) == 10:  # YYYY-MM-DD
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        elif len(date_string) == 7:  # YYYY-MM
            # Assume the 1st day of the month
            return datetime.strptime(date_string, '%Y-%m').date()
        elif len(date_string) == 4:  # YYYY
            # Assume January 1st of that year
            return datetime.strptime(date_string, '%Y').date()
        else:
            return None
    except Exception:
        # Fallback for unexpected formats
        return None

def complete_track_data(spotify_track_id, spotify_client):
    if spotify_track_id == 'NOT_FOUND' or spotify_track_id is None:
        return None, None

    try:
        track_data = spotify_client.track(spotify_track_id)
        
        album = track_data['album']['name']
        release_date = parse_spotify_date(track_data['album']['release_date'])
        
        return album, release_date
        
    except Exception as e:
        # Log the error for debugging
        print(f"Error fetching data for track ID {spotify_track_id}: {e}")
        return None, None # Return None for both on error
    
def process_all_tracks():
    
    tracks_file_path = os.path.join(OUTPUT_DIR, 'tracks.csv')

    if not os.path.exists(tracks_file_path):
        print(f"Error: Tracks file not found at {tracks_file_path}. Run ETL first.")
        return

    # 1. Load Data
    tracks_df = pd.read_csv(tracks_file_path)

    sp = initialize_spotify_client()

    # Prepare lists to hold the new data
    new_album_data = []
    new_release_date_data = []

    for index, row in tracks_df.iterrows():
        spotify_track_id = row['spotify_track_id']
        
        # Check for the 'NOT_FOUND' sentinel if it somehow made it into tracks.csv
        if spotify_track_id == 'NOT_FOUND':
             album, release_date = None, None
        else:
             album, release_date = complete_track_data(spotify_track_id, sp)
        
        new_album_data.append(album)
        new_release_date_data.append(release_date)
        
        # Rate limit delay to prevent API key blocking
        time.sleep(RATE_LIMIT_DELAY) 

    # 4. Merge New Data Back to DataFrame
    tracks_df['album'] = new_album_data
    tracks_df['release_date'] = new_release_date_data

    # 5. Save Updated File
    tracks_df.to_csv(tracks_file_path, index=False)
    print(f"\nTracks data enrichment complete. File updated at {tracks_file_path}")
    print(tracks_df.head())

# --- Execute the Process (Example) ---
process_all_tracks()



