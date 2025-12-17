import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def initialize_spotify_client():
    load_dotenv()
    CLIENT_ID = os.environ.get('SPOTIPY_CLIENT_ID')
    CLIENT_SECRET = os.environ.get('SPOTIPY_CLIENT_SECRET')
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        ))
        print("Spotify API client initialized successfully.")
        return sp
    except Exception as e:
        print(f"Error initializing Spotify client. Check your CLIENT_ID and CLIENT_SECRET. Error: {e}")
        exit()