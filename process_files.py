import pandas as pd
import time
import os
from datetime import datetime
from isrc_lookup import get_track_data_from_spotify
from spotify_client import initialize_spotify_client

# --- Configuration ---
INPUT_DIR = 'input_files'
OUTPUT_DIR = 'output_files'
RATE_LIMIT_DELAY = 0.25

def load_existing_ids(filename, id_column):
    """Reads a CSV and returns a set of unique IDs to prevent duplicates."""
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            return set(existing_df[id_column].dropna().astype(str).unique())
        except Exception as e:
            print(f"Note: Could not load existing data from {filename}: {e}")
    return set()

def load_existing_composite_keys(filename, col1, col2):
    """Special loader for mapping tables (like artist_tracks) using tuples."""
    file_path = os.path.join(OUTPUT_DIR, filename)
    seen = set()
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            seen.add((str(row[col1]), str(row[col2])))
    return seen

def save_data(data_list, filename):
    """Appends a list of dictionaries to a CSV file."""
    if not data_list:
        return
        
    df = pd.DataFrame(data_list)
    file_path = os.path.join(OUTPUT_DIR, filename)
    file_exists = os.path.isfile(file_path)
    
    # Append mode 'a' prevents overwriting previous batches
    df.to_csv(file_path, mode='a', index=False, header=not file_exists)
    print(f"Saved {len(data_list)} new rows to {filename}")

def process_charts():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sp = initialize_spotify_client()
    
    # 1. Initialize "Seen" sets from existing CSVs
    print("Initializing memory from existing files...")
    tracks_seen = load_existing_ids('tracks.csv', 'id')
    artists_seen = load_existing_ids('artists.csv', 'id')
    charts_seen = load_existing_ids('chart_instance.csv', 'id')
    at_seen = load_existing_composite_keys('artist_tracks.csv', 'artist_id', 'track_isrc')
    ce_seen = load_existing_composite_keys('chart_entries.csv', 'chart_instance_id', 'track_isrc')

    chart_files = sorted([f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')])

    for filename in chart_files:
        try:
            date_str = filename.split('_')[-1].replace('.csv', '')
            chart_id = datetime.strptime(date_str, '%Y-%m-%d').date().isoformat()
        except:
            continue

        # Skip if this chart date was already processed in a previous year/batch
        if chart_id in charts_seen:
            print(f"Skipping {chart_id} - already processed.")
            continue

        print(f"\n--- Processing: {chart_id} ---")
        chart_df = pd.read_csv(os.path.join(INPUT_DIR, filename))
        
        # Batch containers for this specific file
        b_tracks, b_artists, b_ce, b_at = [], [], [], []

        for _, row in chart_df.iterrows():
            track_name, artist_name = row['track'], row['artist']
            pos = row.get('position', _ + 1)
            
            # Spotify lookup
            track_data = get_track_data_from_spotify(track_name, artist_name, sp)
            if not track_data or track_data['isrc'] == 'NOT_FOUND':
                continue

            isrc = track_data['isrc']

            # Deduplicate Tracks
            if isrc not in tracks_seen:
                b_tracks.append({
                    'id': isrc,
                    'spotify_track_id': track_data['spotify_track_id'],
                    'name': track_name
                })
                tracks_seen.add(isrc)

            # Deduplicate Artists & Mappings
            for artist in track_data['artists']:
                a_id = artist['id']
                if a_id not in artists_seen:
                    b_artists.append({'id': a_id, 'name': artist['name']})
                    artists_seen.add(a_id)
                
                if (a_id, isrc) not in at_seen:
                    b_at.append({'artist_id': a_id, 'track_isrc': isrc})
                    at_seen.add((a_id, isrc))

            # Deduplicate Entries
            if (chart_id, isrc) not in ce_seen:
                b_ce.append({
                    'chart_instance_id': chart_id,
                    'track_isrc': isrc,
                    'position': pos
                })
                ce_seen.add((chart_id, isrc))

            time.sleep(RATE_LIMIT_DELAY)

        # 2. Immediate Save (Atomic batches)
        save_data([{'id': chart_id, 'date': chart_id}], 'chart_instance.csv')
        save_data(b_tracks, 'tracks.csv')
        save_data(b_artists, 'artists.csv')
        save_data(b_ce, 'chart_entries.csv')
        save_data(b_at, 'artist_tracks.csv')
        
        charts_seen.add(chart_id)

if __name__ == "__main__":
    process_charts()