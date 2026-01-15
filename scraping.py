import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

def run_scraper():
    # --- 1. CONFIGURATION ---
    url = "https://www.bot.go.tz/ExchangeRate/excRates?lang=en"
    file_name = "bot_historical_rates.csv"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # --- 2. EXTRACTION ---
    print(f"Fetching data from {url}...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to reach BoT site. Error: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    
    if not table:
        print("Table not found on page.")
        return

    rows = table.find_all('tr')
    clean_data = []

    # Loop through rows (skipping header)
    for row in rows:
        cells = row.find_all("td")
        if len(cells) > 0:
            clean_data.append([
                cells[1].text.strip(), # Currency
                cells[2].text.strip(), # Buying
                cells[3].text.strip(), # Selling
                cells[4].text.strip(), # Mean
                cells[5].text.strip()  # Date
            ])

    # --- 3. TRANSFORMATION ---
    heads = ["Currency", "Buying", "Selling", "Mean", "Date"]
    df = pd.DataFrame(clean_data, columns=heads)

    # Convert numeric columns (remove commas first)
    cols_to_fix = ['Buying', 'Selling', 'Mean']
    for col in cols_to_fix:
        df[col] = df[col].str.replace(',', '').astype(float)

    # Convert Date column
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Add metadata
    df['extracted_at'] = pd.Timestamp.now()
    df['source_url'] = url

    # --- 4. SMART SAVING (History Tracking) ---
    if os.path.exists(file_name):
        # Load existing data to check for duplicates
        existing_df = pd.read_csv(file_name)
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        
        # Only add data if the Date in the new scrape is NEWER than the max date in history
        if df['Date'].max() > existing_df['Date'].max():
            df.to_csv(file_name, mode='a', index=False, header=False)
            print(f"Success: New data for {df['Date'].max().date()} added to history.")
        else:
            print("Status: No new data found. History is already up to date.")
    else:
        # Create the file for the first time
        df.to_csv(file_name, index=False)
        print(f"Success: Initial history file created with {len(df)} rows.")

if __name__ == "__main__":
    run_scraper()