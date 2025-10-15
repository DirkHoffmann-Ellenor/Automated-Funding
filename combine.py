import os
import pandas as pd

# Define the root directory where folders are located
root_dir = 'Scraped'
output_file = 'fund_results_final.csv'

# List to hold all dataframes
all_dfs = []

# Walk through all subdirectories
for dirpath, dirnames, filenames in os.walk(root_dir):
    for filename in filenames:
        if filename == 'fund_result.csv':  # match exact filename
            file_path = os.path.join(dirpath, filename)
            print(f"Reading: {file_path}")
            try:
                df = pd.read_csv(file_path)
                df['source_folder'] = os.path.basename(dirpath)  # optional: add source info
                all_dfs.append(df)
            except Exception as e:
                print(f"⚠️ Could not read {file_path}: {e}")

# Combine all dataframes
if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.to_csv(output_file, index=False)
    print(f"\n✅ Combined CSV saved as: {output_file}")
else:
    print("❌ No 'fund_result.csv' files found in the Scraped folder.")
