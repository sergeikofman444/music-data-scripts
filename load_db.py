# load_db.py
import pandas as pd
import os
import sys
from psycopg2.extras import execute_batch
import psycopg2

# --- CONFIGURATION (Consolidating Connection Here for Simplicity) ---
# NOTE: Replace 'output_files' with the correct absolute path if needed!
CSV_DIR = 'output_files' 

DB_DETAILS = {
    "host": "localhost",
    "database": "music_charts",  # Your POSTGRES_DB value from docker-compose
    "user": "postgres",          # Your POSTGRES_USER value
    "password": "postgres",      # Your POSTGRES_PASSWORD value
    "port": "5432"
}

def get_db_connection():
    """Returns an active connection object to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_DETAILS)
        return conn
    except psycopg2.OperationalError as e:
        print(f"FATAL: Error connecting to database. Is Docker running? ({e})")
        sys.exit(1)

def load_data_from_csv(table_name):
    """General function to load a DataFrame from a CSV file."""
    csv_path = os.path.join(CSV_DIR, f'{table_name}.csv')
    if not os.path.exists(csv_path):
        print(f"Error: Required CSV file not found at {csv_path}. Skipping.")
        return None
    
    return pd.read_csv(csv_path)

def bulk_insert(conn, df, table_name, columns, conflict_key=None):
    """
    Handles bulk insertion using execute_batch with optional conflict handling.
    """
    if df.empty:
        print(f"Skipping {table_name}: DataFrame is empty.")
        return

    # 1. Prepare Data for Insertion
    # Fill NaN values with a space or empty string to match TEXT/VARCHAR/ARRAY types
    # Pandas NaN (float) can cause issues when inserting into text columns.
    df = df.fillna('') 
    
    # 2. Extract data in the correct column order
    data_to_insert = df[columns].values.tolist()

    # 3. Define the SQL INSERT statement
    placeholders = ', '.join(['%s'] * len(columns))
    sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Add ON CONFLICT clause for idempotency
    if conflict_key:
        # If conflict_key is passed as a tuple/list, convert it to (col1, col2) string
        if isinstance(conflict_key, (list, tuple)):
            key_str = f"({', '.join(conflict_key)})"
        else:
            key_str = f"({conflict_key})" # Single column
            
        sql_insert += f" ON CONFLICT {key_str} DO NOTHING" # <--- Enforces proper syntax
    
    cursor = conn.cursor()
    
    print(f"  -> Loading {len(data_to_insert)} rows into '{table_name}'...")
    
    # 4. Execute Batch Insertion
    execute_batch(cursor, sql_insert, data_to_insert)
    
    conn.commit()
    print(f"  ✅ Data loaded for '{table_name}'.")

# ----------------------------------------------------------------------
# INDIVIDUAL TABLE LOADING FUNCTIONS (Matches seed.sql structure)
# ----------------------------------------------------------------------

def load_chart_instance(conn):
    df = load_data_from_csv('chart_instance')
    if df is not None:
        # Columns: date
        bulk_insert(
            conn, df, 
            table_name='chart_instance', 
            columns=['date'], 
            conflict_key='date'
        )

def load_artists(conn):
    df = load_data_from_csv('artists')
    if df is not None:
        # Columns: id, name, genres, followers
        # NOTE: Genres and followers will be NULL/empty initially, 
        # unless you ran the enrichment script first.
        bulk_insert(
            conn, df, 
            table_name='artists', 
            columns=['id', 'name'], # genres and followers will use the default value for now
            conflict_key='id'
        )

def load_tracks(conn):
    df = load_data_from_csv('tracks')
    if df is not None:
        # Columns: id, spotify_track_id, name, album, release_date
        bulk_insert(
            conn, df, 
            table_name='tracks', 
            columns=['id', 'spotify_track_id', 'name', 'album', 'release_date'], 
            conflict_key='id'
        )
        
def load_artist_tracks(conn):
    df = load_data_from_csv('artist_tracks')
    if df is not None:
        # Columns: artist_id, track_isrc
        # Composite primary key means ON CONFLICT (artist_id, track_isrc)
        bulk_insert(
            conn, df, 
            table_name='artist_tracks', 
            columns=['artist_id', 'track_isrc'], 
            conflict_key=('artist_id', 'track_isrc') # <-- PASS AS TUPLE
        )
        
def load_chart_entries(conn):
    df = load_data_from_csv('chart_entries')
    if df is not None:
        # Columns: chart_instance_id, track_isrc, position
        # CRITICAL: Since 'track_isrc' is a TEXT column to allow 'NOT_FOUND', 
        # we still use the composite PK for conflict handling.
        bulk_insert(
            conn, df, 
            table_name='chart_entries', 
            columns=['chart_instance_id', 'track_isrc', 'position'], 
            conflict_key=('chart_instance_id', 'track_isrc') # <-- PASS AS TUPLE
        )

# ----------------------------------------------------------------------
# MAIN ORCHESTRATION
# ----------------------------------------------------------------------

def main_load():
    conn = get_db_connection()
    
    # Load Dimension Tables first (they have no FK dependency on other tables)
    print("--- 1. Loading Dimension Tables ---")
    load_chart_instance(conn)
    load_artists(conn)
    
    # Load Tracks (It depends on Artists only via the artist_tracks join, 
    # but tracks and artists are the primary entities)
    print("\n--- 2. Loading Entity Tables ---")
    load_tracks(conn)
    
    # Load Fact/Join Tables (These depend on the entities loaded above)
    print("\n--- 3. Loading Fact and Join Tables ---")
    load_artist_tracks(conn)
    load_chart_entries(conn)
    
    print("\n*** ALL DATA LOADING COMPLETE ***")
    
if __name__ == '__main__':
    main_load()