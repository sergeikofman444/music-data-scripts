
import os
import certifi
import pandas as pd
import time
import musicbrainzngs
import os

os.environ['SSL_CERT_FILE'] = certifi.where()
musicbrainzngs.set_useragent("MyMusicETL", "1.0", "sergeikofman444@gmail.com")
MB_DELAY = 1.1 

def fetch_mb_date(isrc):
    try:
        result = musicbrainzngs.get_recordings_by_isrc(isrc, includes=["releases"])
        recs = result.get('isrc', {}).get('recording-list', [])
        
        specific_dates = []
        vague_dates = []
        
        for r in recs:
            for rel in r.get('release-list', []):
                d = rel.get('date')
                if not d:
                    continue
                
                if len(d) == 10:
                    specific_dates.append(d)
                else:
                    vague_dates.append(d)
        
        if specific_dates:
            return min(specific_dates)
        
        if vague_dates:
            d = min(vague_dates)

            if len(d) == 4: return f"{d}-01-01"
            if len(d) == 7: return f"{d}-01"
            return d
            
        return None
    except Exception:
        return None

def process_all_tracks():
    path = os.path.join('output_files', 'tracks.csv')
    if not os.path.exists(path): return
    
    df = pd.read_csv(path)
    total = len(df)

    print(f"Rewriting release_date for {total} tracks using MusicBrainz...")

    for i, row in df.iterrows():
        isrc = row['id']
        track_name = row['name']
        
        verified_date = fetch_mb_date(isrc)
        
        if verified_date:
            print(f"[{i+1}/{total}] UPDATED: {track_name} -> {verified_date}")
            df.at[i, 'release_date'] = verified_date
        else:
            print(f"[{i+1}/{total}] KEEPING SPOTIFY: {track_name} -> {row['release_date']}")
        
        time.sleep(MB_DELAY)
        
        if i % 20 == 0:
            df.to_csv(path, index=False)

    df.to_csv(path, index=False)
    print("\nEnrichment complete. The release_date column has been overwritten with verified data.")

if __name__ == "__main__":
    process_all_tracks()
