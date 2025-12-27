import pandas as pd
import os

INPUT_FILE = '2010-2014_charts.csv'
OUTPUT_DIR = 'input_files'
OUTPUT_FILENAME_PREFIX = 'hot100_'

def split_charts():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Reading data from: {INPUT_FILE}")

    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    OUTPUT_COLUMNS = ['track', 'artist', 'position']
    
    chart_groups = df.groupby('date')
    
    print(f"Found {len(chart_groups)} unique dates to process.")

    for date, group_df in chart_groups:
        
        filename = f"{OUTPUT_FILENAME_PREFIX}{date}.csv"
        output_path = os.path.join(OUTPUT_DIR, filename)

        group_df[OUTPUT_COLUMNS].to_csv(
            output_path, 
            header=True, 
            index=False
        )
        
        print(f"Saved: {output_path}")

    print("\n--- Processing Complete ---")

if __name__ == "__main__":
    split_charts()