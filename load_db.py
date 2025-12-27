import pandas as pd
import os
import sys
from psycopg2.extras import execute_batch
import psycopg2

CSV_DIR = 'output_files' 

DB_DETAILS = {
    "host": "localhost",
    "database": "music_charts",
    "user": "postgres",
    "password": "postgres",
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

    df = df.fillna('') 
    
    data_to_insert = df[columns].values.tolist()

    placeholders = ', '.join(['%s'] * len(columns))
    sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    if conflict_key:
        if isinstance(conflict_key, (list, tuple)):
            key_str = f"({', '.join(conflict_key)})"
        else:
            key_str = f"({conflict_key})"
            
        sql_insert += f" ON CONFLICT {key_str} DO NOTHING"
    
    cursor = conn.cursor()
    
    print(f"  -> Loading {len(data_to_insert)} rows into '{table_name}'...")
    
    execute_batch(cursor, sql_insert, data_to_insert)
    
    conn.commit()
    print(f"  ✅ Data loaded for '{table_name}'.")

def load_chart_instance(conn):
    df = load_data_from_csv('chart_instance')
    if df is not None:
        bulk_insert(
            conn, df, 
            table_name='chart_instance', 
            columns=['date'], 
            conflict_key='date'
        )

def load_artists(conn):
    df = load_data_from_csv('artists')
    if df is not None:
        bulk_insert(
            conn, df, 
            table_name='artists', 
            columns=['id', 'name'],
            conflict_key='id'
        )

def load_tracks(conn):
    df = load_data_from_csv('tracks')
    if df is not None:
        bulk_insert(
            conn, df, 
            table_name='tracks', 
            columns=['id', 'spotify_track_id', 'name', 'album', 'release_date'], 
            conflict_key='id'
        )
        
def load_artist_tracks(conn):
    df = load_data_from_csv('artist_tracks')
    if df is not None:
        bulk_insert(
            conn, df, 
            table_name='artist_tracks', 
            columns=['artist_id', 'track_isrc'], 
            conflict_key=('artist_id', 'track_isrc')
        )
        
def load_chart_entries(conn):
    df = load_data_from_csv('chart_entries')
    if df is not None:
        bulk_insert(
            conn, df, 
            table_name='chart_entries', 
            columns=['chart_instance_id', 'track_isrc', 'position'], 
            conflict_key=('chart_instance_id', 'track_isrc')
        )


def main_load():
    conn = get_db_connection()
    
    print("--- 1. Loading Dimension Tables ---")
    load_chart_instance(conn)
    load_artists(conn)
    
    print("\n--- 2. Loading Entity Tables ---")
    load_tracks(conn)
    
    print("\n--- 3. Loading Fact and Join Tables ---")
    load_artist_tracks(conn)
    load_chart_entries(conn)
    
    print("\n*** ALL DATA LOADING COMPLETE ***")
    
if __name__ == '__main__':
    main_load()