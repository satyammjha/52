import requests
import csv
import sqlite3
import io
from pymongo import MongoClient

# --- Database Setup ---
sqlite_db = '52lowstocks.db'

mongo_client = MongoClient("mongodb+srv://gundeepsingh2005:bNYMmhsrIxUR6xpL@cluster0.ewzqcvi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client['52HIGH_LOW']
mongo_collection = mongo_db['52weeklow']

# --- Download and Parse CSV ---
def download_csv(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.bseindia.com/'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # The file is NOT gzip, just plain CSV
    text_data = response.content.decode('utf-8')
    reader = csv.DictReader(io.StringIO(text_data))
    return list(reader)

# --- SQLite Helpers ---
def create_sqlite_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            scrip_code TEXT PRIMARY KEY,
            scrip_name TEXT,
            ltp TEXT,
            week_low TEXT,
            prev_week_low TEXT,
            prev_week_low_date TEXT,
            all_time_low_price TEXT,
            all_time_low_date TEXT,
            stock_group TEXT
        )
    ''')
    conn.commit()

def upsert_sqlite(conn, row):
    cursor = conn.cursor()
    query = '''
        INSERT INTO stocks (scrip_code, scrip_name, ltp, week_low, prev_week_low, prev_week_low_date, all_time_low_price, all_time_low_date, stock_group)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scrip_code) DO UPDATE SET
            scrip_name = excluded.scrip_name,
            ltp = excluded.ltp,
            week_low = excluded.week_low,
            prev_week_low = excluded.prev_week_low,
            prev_week_low_date = excluded.prev_week_low_date,
            all_time_low_price = excluded.all_time_low_price,
            all_time_low_date = excluded.all_time_low_date,
            stock_group = excluded.stock_group
    '''
    cursor.execute(query, (
        row['Scrip Code'],
        row['Scrip Name'],
        row['LTP'],
        row['52 Weeks Low'],
        row['Previous 52 Weeks Low'],
        row['Previous 52 Weeks Low Date'],
        row['All Time Low Price'],
        row['All Time Low Date'],
        row['Group']
    ))
    conn.commit()

# --- MongoDB Upsert ---
def upsert_mongo(row):
    mongo_collection.replace_one(
        {'Scrip Code': row['Scrip Code']},
        row,
        upsert=True
    )

# --- Main Function ---
def main():
    url = "https://api.bseindia.com/BseIndiaAPI/api/HLDownloadCSVNew/w?scripcode=&HLflag=L&Grpcode=&indexcode=&EQflag=1"
    conn = sqlite3.connect(sqlite_db)
    create_sqlite_table(conn)

    rows = download_csv(url)
    for row in rows:
        upsert_sqlite(conn, row)
        upsert_mongo(row)
        print(f"Upserted: {row['Scrip Name']}")

    conn.close()

if __name__ == '__main__':
    main()
