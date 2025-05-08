import requests
import csv
import sqlite3
import io
from pymongo import MongoClient

print("Starting script...")

# --- Database Setup ---
sqlite_db = '52lowstocks.db'

mongo_client = MongoClient("mongodb+srv://admin:Wt9cPRsB3eZazNeA@cluster0.hmuzu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client['52HIGH_LOW']
mongo_collection = mongo_db['52weeklow']

def download_csv(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.bseindia.com/'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    text_data = response.content.decode('utf-8-sig')  # Handle BOM
    print("Raw CSV content preview:\n", text_data[:500])  # Optional debug

    reader = csv.DictReader(io.StringIO(text_data), delimiter=',')  # ✅ FIXED HERE

    # Normalize headers: strip and lower case
    normalized_fieldnames = [name.strip().lower() for name in reader.fieldnames]
    reader.fieldnames = normalized_fieldnames

    cleaned_rows = []
    for row in reader:
        cleaned_row = {k.strip().lower(): str(v).strip() for k, v in row.items()}
        cleaned_rows.append(cleaned_row)

    return cleaned_rows

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
        INSERT INTO stocks (scrip_code, scrip_name, ltp, week_low, prev_week_low, 
                            prev_week_low_date, all_time_low_price, all_time_low_date, stock_group)
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
        row.get('scrip code', ''),
        row.get('scrip name', ''),
        row.get('ltp', ''),
        row.get('52 weeks low', ''),
        row.get('previous 52 weeks low', ''),
        row.get('previous 52 weeks low date', ''),
        row.get('all time low price', ''),
        row.get('all time low date', ''),
        row.get('group', '')
    ))
    conn.commit()

# --- MongoDB Upsert ---
def upsert_mongo(row):
    if not row.get('scrip code'):
        return
    mongo_collection.replace_one(
        {'scrip code': row['scrip code']},
        row,
        upsert=True
    )

# --- Main Function ---
def main():
    url = "https://api.bseindia.com/BseIndiaAPI/api/HLDownloadCSVNew/w?scripcode=&HLflag=L&Grpcode=&indexcode=&EQflag=1"
    conn = sqlite3.connect(sqlite_db)
    create_sqlite_table(conn)

    try:
        rows = download_csv(url)
        print(f"Total rows fetched from CSV: {len(rows)}")

        seen = set()

        for row in rows:
            code = row.get('scrip code', '').strip()
            name = row.get('scrip name', '').strip()
            if not code or code in seen:
                continue
            seen.add(code)
            upsert_sqlite(conn, row)
            upsert_mongo(row)
            print(f"Upserted: {code} - {name}")

    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()
