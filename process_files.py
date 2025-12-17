import pandas as pd
import time
import os
from datetime import datetime

from isrc_lookup import get_track_data_from_spotify
from spotify_client import initialize_spotify_client

INPUT_FILE = 'historic_data.csv'
OUTPUT_FILE = 'historic_data_with_isrc.csv'
RATE_LIMIT_DELAY = 0.05 

INPUT_DIR = 'input_files'
OUTPUT_DIR = 'output_files'

all_tracks_data = []
all_chart_entries_data = []
all_artists_data = []
all_artist_tracks_data = []

# Sets for efficient primary key tracking
tracks_seen = set()
chart_entries_seen = set()
artists_seen = set()
artist_tracks_seen = set()

# --- Spotify API Setup ---

def prepare_files_list(input_dir, output_dir):
    chart_files_list = [
        f for f in os.listdir(input_dir) 
        if f.endswith('.csv') and not f.startswith('Unnamed')
    ]
    
    if not chart_files_list:
        print(f"Error: No CSV files found in {input_dir}. Exiting.")
        return

    unique_chart_dates = {
        datetime.strptime(f.split('_')[-1].replace('.csv', ''), '%Y-%m-%d').date() 
        for f in chart_files_list
    }
    
    chart_instance_df = pd.DataFrame([
        {'id': instance.isoformat(), 'date': instance.isoformat()} 
        for instance in unique_chart_dates
    ]).reset_index(drop=True)

    os.makedirs(output_dir, exist_ok=True)

    return {
        'chart_files_list': chart_files_list,
        'chart_instance_df': chart_instance_df
    }

def process_file(chart_df, chart_instance_id, sp_client, all_tracks_data, tracks_seen, all_chart_entries_data, chart_entries_seen, all_artists_data, artists_seen, all_artist_tracks_data, artist_tracks_seen):
    for index, row in chart_df.iterrows():
            
        track_name = row['track']
        artist_name = row['artist']
        position = row['position']
        
        track_data = get_track_data_from_spotify(track_name, artist_name, sp_client)

        if track_data is not None:
            isrc = track_data['isrc']

            if isrc != 'NOT_FOUND':
                if isrc not in tracks_seen:
                    all_tracks_data.append({
                        'id': isrc,
                        'spotify_track_id': track_data['spotify_track_id'],
                        'name': track_name,
                        # Other track metadata (album, genre) will be filled later via Spotify API
                    })
                    tracks_seen.add(isrc)

                artists_list = track_data['artists']
                
                for artist in artists_list:

                    artist_name = artist['name']
                    artist_id = artist['id']
                    
                    if artist_id not in artists_seen:
                        all_artists_data.append({
                            'id': artist_id, 
                            'name': artist_name,
                            # ... other fields filled later
                        })
                        artists_seen.add(artist_id)
                    
                    artist_track_key = (artist_id, isrc)
                    if artist_track_key not in artist_tracks_seen:
                        all_artist_tracks_data.append({
                            'artist_id': artist_id,
                            'track_isrc': isrc
                        })
                        artist_tracks_seen.add(artist_track_key)
            
            entry_key = (chart_instance_id, isrc)
            if entry_key not in chart_entries_seen:
                all_chart_entries_data.append({
                    'chart_instance_id': chart_instance_id,
                    'track_isrc': isrc,
                    'position': position
                })
                chart_entries_seen.add(entry_key)
            
            

        time.sleep(RATE_LIMIT_DELAY)

def prepare_chart_files(chart_files_list, input_dir):

    # initialize spotify api client
    sp = initialize_spotify_client()

    # --- Loop through all input files ---
    for filename in chart_files_list:
        if not filename.endswith('.csv'):
            continue
            
        file_path = os.path.join(input_dir, filename)
        
        # Determine the chart date from the filename (e.g., hot100_YYYY-MM-DD.csv)
        try:
            date_str = filename.split('_')[-1].replace('.csv', '')
            chart_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            chart_instance_id = chart_date.isoformat() 
        except Exception:
            print(f"Skipping {filename}: Could not parse date from filename. Filename must end in YYYY-MM-DD.csv")
            continue
            
        print(f"\n--- Processing Chart: {chart_instance_id} ---")

        # 2. Load and Clean Chart Data
        chart_df = pd.read_csv(file_path)
        chart_df = chart_df.drop(columns=[col for col in chart_df.columns if col.startswith('Unnamed:')])

        if 'position' not in chart_df.columns:
            chart_df['position'] = chart_df.index + 1


        process_file(chart_df, chart_instance_id, sp, all_tracks_data, tracks_seen, all_chart_entries_data, chart_entries_seen, all_artists_data, artists_seen, all_artist_tracks_data, artist_tracks_seen)

files = prepare_files_list(INPUT_DIR, OUTPUT_DIR)

chart_files_list = files['chart_files_list']
chart_instance_df = files['chart_instance_df']

prepare_chart_files(chart_files_list, INPUT_DIR)

tracks_df = pd.DataFrame(all_tracks_data).reset_index(drop=True)
artists_df = pd.DataFrame(all_artists_data).reset_index(drop=True)
chart_entries_df = pd.DataFrame(all_chart_entries_data).reset_index(drop=True)
artist_tracks_df = pd.DataFrame(all_artist_tracks_data).reset_index(drop=True)


chart_instance_df.to_csv(os.path.join(OUTPUT_DIR, 'chart_instance.csv'), index=False)
tracks_df.to_csv(os.path.join(OUTPUT_DIR, 'tracks.csv'), index=False)
artists_df.to_csv(os.path.join(OUTPUT_DIR, 'artists.csv'), index=False)
chart_entries_df.to_csv(os.path.join(OUTPUT_DIR, 'chart_entries.csv'), index=False)
artist_tracks_df.to_csv(os.path.join(OUTPUT_DIR, 'artist_tracks.csv'), index=False)
