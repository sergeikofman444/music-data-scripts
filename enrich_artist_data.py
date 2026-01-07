import pandas as pd
import os
import time
import certifi
import musicbrainzngs

# Configuration
os.environ['SSL_CERT_FILE'] = certifi.where()
musicbrainzngs.set_useragent("MyMusicETL", "1.0", "your-email@example.com")
MB_DELAY = 1.1 

def search_artist(artist_name):
    RED_FLAGS = ['tribute', 'cover', 'parody', 'fictional', 'karaoke', 'fan']

    try:
        # Pass artist_name directly to the search
        result = musicbrainzngs.search_artists(artist=artist_name)
        artists = result.get("artist-list", [])
        
        if not artists:
            return None

        top_match = artists[0]
        # Safely convert score to int
        try:
            score = int(top_match.get("ext:score", 0))
        except (ValueError, TypeError):
            score = 0
            
        disambiguation = top_match.get("disambiguation", "").lower()
        
        needs_review = False
        if score < 90:
            needs_review = True
        if any(word in disambiguation for word in RED_FLAGS):
            needs_review = True
        if len(artists) > 1:
            try:
                second_score = int(artists[1].get("ext:score", 0))
                if second_score == score:
                    needs_review = True
            except:
                pass

        return {
            'musicbrainz_id': top_match.get("id"),
            'artist_type': top_match.get("type"),
            'country_of_origin': top_match.get("country"),
            'year_of_origin': top_match.get("life-span", {}).get("begin"),
            'disambiguation': top_match.get("disambiguation", ""),
            'needs_review': needs_review
        }
    except Exception as e:
        print(f"Error during API call for {artist_name}: {e}")
        return None

def process_all_artists():
    path = os.path.join('output_files', 'artists.csv')
    if not os.path.exists(path): 
        print(f"Error: File not found at {path}")
        return
    
    df = pd.read_csv(path)
    total = len(df)

    # Initialize columns
    new_cols = ['musicbrainz_id', 'type', 'country_of_origin', 'year_of_origin', 'disambiguation', 'needs_review']
    for col in new_cols:
        if col not in df.columns:
            # Initialize needs_review as boolean, others as object/string
            df[col] = False if col == 'needs_review' else None

    print(f"Enriching data for {total} artists...")

    for i, row in df.iterrows():
        # Check if already processed (handles resuming after a crash)
        if pd.notna(df.at[i, 'musicbrainz_id']):
            continue

        artist_name = row['name']
        enriched = search_artist(artist_name)
        
        if enriched:
            # Use .loc for multi-column assignment
            df.loc[i, new_cols] = [
                enriched['musicbrainz_id'],
                enriched['artist_type'],
                enriched['country_of_origin'],
                enriched['year_of_origin'],
                enriched['disambiguation'],
                enriched['needs_review']
            ]
            
            status = "⚠️ REVIEW" if enriched['needs_review'] else "✅ OK"
            print(f"[{i+1}/{total}] {status}: {artist_name}")
        else:
            print(f"[{i+1}/{total}] ❌ NOT FOUND: {artist_name}")
        
        time.sleep(MB_DELAY)
        
        # Periodic Save
        if i % 20 == 0:
            df.to_csv(path, index=False)

    # Final Save
    df.to_csv(path, index=False)
    print("\nEnrichment complete. CSV updated.")

if __name__ == "__main__":
    process_all_artists()