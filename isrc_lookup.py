def clean_artist_name(artist_name):
    if 'featuring' in artist_name.lower():

        lower_name = artist_name.lower()
        if 'featuring' in lower_name:
            index = lower_name.find('featuring')
            return artist_name[:index].strip()
        
    if 'with' in artist_name.lower():
        lower_name = artist_name.lower()
        if 'with' in lower_name:
            index = lower_name.find('with')
            return artist_name[:index].strip()
    
    return artist_name.strip()

def get_track_data_from_spotify(track_name, artist_name, sp_client):
    """Searches Spotify for a track and returns its ISRC."""

    artist_name = clean_artist_name(artist_name)
    
    query = f'track:"{track_name}" artist:"{artist_name}"'
    
    try:
        results = sp_client.search(q=query, type='track', limit=1)
        
        if results and results['tracks']['items']:
            first_track = results['tracks']['items'][0]
            found_isrc = first_track['external_ids'].get('isrc')
            
            if found_isrc:
                artists_list = [
                    {'id': artist['id'], 'name': artist['name']}
                    for artist in first_track['artists']
                ]
                return {
                    'isrc': found_isrc,
                    'spotify_track_id': first_track.get('id'),
                    'track_name': track_name,
                    'artists': artists_list
                }

        return {
            'isrc': 'NOT_FOUND',
            'spotify_track_id': 'NOT_FOUND',
            'track_name': track_name,
            'artists': [{'id': 'NOT_FOUND', 'name': artist_name}]
        }

    except Exception as e:
        print(f"--- API Error for {track_name} by {artist_name}: {e} ---")
        return None
    
    return None
