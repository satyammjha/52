import requests
import csv
import sqlite3
import io
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError

sqlite_db = '52highstocks.db'

mongo_client = MongoClient("mongodb+srv://gundeepsingh2005:bNYMmhsrIxUR6xpL@cluster0.ewzqcvi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client['52HIGH_LOW']
mongo_collection = mongo_db['52weekhigh']

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%d-%b-%y')
    except (ValueError, TypeError):
        return None

def process_row(raw_row):
    processed = {}
    processed['Security Code'] = safe_int(raw_row.get('Security Code', ''))
    processed['Security Name'] = raw_row.get('Security Name', '').strip()
    processed['LTP'] = safe_float(raw_row.get('LTP', ''))
    processed['52 Weeks High'] = safe_float(raw_row.get('52 Weeks High', ''))
    processed['Previous 52 Weeks High'] = safe_float(raw_row.get('Previous 52 Weeks High', ''))
    processed['Previous 52 Weeks High Date'] = parse_date(raw_row.get('Previous 52 Weeks High Date', ''))
    processed['All Time High Price'] = safe_float(raw_row.get('All Time High Price', ''))
    processed['All Time High Date'] = parse_date(raw_row.get('All Time High Date', ''))
    processed['Group'] = raw_row.get('Group', '').strip()
    return processed

def download_csv(url):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.bseindia.com/'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    text_data = response.content.decode('utf-8')
    reader = csv.DictReader(io.StringIO(text_data))
    return [process_row(row) for row in reader]

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
        str(row['Security Code']),
        row['Security Name'],
        str(row.get('LTP', '')),
        str(row.get('52 Weeks High', '')),
        str(row.get('Previous 52 Weeks High', '')),
        row['Previous 52 Weeks High Date'].strftime('%d-%b-%y') if row.get('Previous 52 Weeks High Date') else '',
        str(row.get('All Time High Price', '')),
        row['All Time High Date'].strftime('%d-%b-%y') if row.get('All Time High Date') else '',
        row['Group']
    ))
    conn.commit()

def upsert_mongo(row):
    try:
        filter_criteria = {'Security Code': row['Security Code']}
        mongo_collection.replace_one(
            filter_criteria,
            row,
            upsert=True
        )
    except PyMongoError as e:
        print(f"Error upserting document {row['Security Code']}: {str(e)}")

def main():
    url = "https://api.bseindia.com/BseIndiaAPI/api/HLDownloadCSVNew/w?scripcode=&HLflag=H&Grpcode=&indexcode=&EQflag=1"
    conn = sqlite3.connect(sqlite_db)
    create_sqlite_table(conn)

    try:
        rows = download_csv(url)
        for row in rows:
            if row['Security Code'] is None:
                continue  # Skip invalid rows
            upsert_sqlite(conn, row)
            upsert_mongo(row)
            print(f"Processed: {row['Security Name']} ({row['Security Code']})")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()