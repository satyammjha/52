import requests
import csv
import sqlite3
import io
from pymongo import MongoClient

sqlite_db = '52highstocks.db'

mongo_client = MongoClient("mongodb+srv://gundeepsingh2005:bNYMmhsrIxUR6xpL@cluster0.ewzqcvi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client['52HIGH_LOW']
mongo_collection = mongo_db['52weekhigh']

# --- Download and Parse CSV ---
def download_csv(url):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.bseindia.com/'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

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
            week_high TEXT,
            prev_week_high TEXT,
            prev_week_high_date TEXT,
            all_time_high_price TEXT,
            all_time_high_date TEXT,
            stock_group TEXT
        )
    ''')
    conn.commit()

def upsert_sqlite(conn, row):
    cursor = conn.cursor()
    query = '''
        INSERT INTO stocks (scrip_code, scrip_name, ltp, week_high, prev_week_high, prev_week_high_date, all_time_high_price, all_time_high_date, stock_group)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scrip_code) DO UPDATE SET
            scrip_name = excluded.scrip_name,
            ltp = excluded.ltp,
            week_high = excluded.week_high,
            prev_week_high = excluded.prev_week_high,
            prev_week_high_date = excluded.prev_week_high_date,
            all_time_high_price = excluded.all_time_high_price,
            all_time_high_date = excluded.all_time_high_date,
            stock_group = excluded.stock_group
    '''
    cursor.execute(query, (
        row['Security Code'],
        row['Security Name'],
        row['LTP'],
        row['52 Weeks High'],
        row['Previous 52 Weeks High'],
        row['Previous 52 Weeks High Date'],
        row['All Time High Price'],
        row['All Time High Date'],
        row['Group']
    ))
    conn.commit()

# --- MongoDB Upsert ---
def upsert_mongo(row):
    mongo_collection.replace_one(
        {'Security Code': row['Security Code']},
        row,
        upsert=True
    )

# --- Main Function ---
def main():
    url = "https://api.bseindia.com/BseIndiaAPI/api/HLDownloadCSVNew/w?scripcode=&HLflag=H&Grpcode=&indexcode=&EQflag=1"
    conn = sqlite3.connect(sqlite_db)
    create_sqlite_table(conn)

    rows = download_csv(url)
    for row in rows:
        upsert_sqlite(conn, row)
        upsert_mongo(row)
        print(f"Upserted: {row['Security Name']}")

    conn.close()

if __name__ == '__main__':
    main()