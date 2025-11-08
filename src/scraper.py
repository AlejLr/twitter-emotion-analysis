"""
Twitter Scraper Module
It uses twscrape to retrieve tweets and store them in a local SQLite database.

Call examples:
    - python src/scraper.py --q "AI" --max-tweets 500
    - python src/scraper.py --q "Python programming" --since "2025-11-01" --until "2025-11-31" --max-tweets 1000
    - python src/scraper.py --q "#Netherlands" --csv "data/netherlands_tweets.csv"
    
DataBase Schema:
    - SQLite file: tweets.db
    - Table: tweets
    - Unique key: tweet_id

"""

import argparse
import os
import re
import sqlite3
import asyncio
from datetime import datetime
from dateutil import parser as date_parser
import pandas as pd
from tqdm import tqdm
from twscrape import API, gather
from twscrape.logger import set_log_level

URL_PATTERN = re.compile(r"http[s]?://\S+")
MULTISPACE = re.compile(r"\s+")

def ensure_directory_exists():
    os.makedirs('data', exist_ok=True)
    os.makedirs("secrets", exist_ok=True)

def create_database(db_path='data/tweets.db'):
    ensure_directory_exists()
    conn = sqlite3.connect(db_path)
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_id TEXT PRIMARY KEY,
            query TEXT,
            date_utc TEXT,
            user TEXT,
            username TEXT,
            content TEXT,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            quotes INTEGER,
            lang TEXT,
            source TEXT,
            url TEXT,
            latitude REAL,
            longitude REAL
        );
    ''')

    conn.execute("CREATE INDEX IF NOT EXISTS idx_query_date ON tweets (query, date_utc);")
    return conn

def clean_tweet_content(content):
    
    content = URL_PATTERN.sub("", content)
    content = content.replace("\n", " ").replace("\r", " ")
    content = MULTISPACE.sub(" ", content).strip()
    return content

def search_query(q, since = None, until = None):
    
    parts = [q]
    if since:
        _ = date_parser.parse(since)
        parts.append(f"since:{since}")
    if until:
        _ = date_parser.parse(until)
        parts.append(f"until:{until}")
        
    query = " ".join(parts)
    return query

def to_row(i, q):
    
    coords = getattr(i, 'coordinates', None)
    lat, lon = (None, None)
    
    if coords and getattr(coords, "latitude", None) is not None and getattr(coords, "longitude", None) is not None:
        lat = coords.latitude
        lon = coords.longitude

    row = {
        'tweet_id': str(i.id),
        'query': q,
        'date_utc': i.date.strftime("%Y-%m-%d %H:%M:%S"),
        'user': getattr(i.user, 'displayname', None),
        'username': getattr(i.user, 'username', None),
        'content': clean_tweet_content(i.rawContent or ""),
        "likes": int(getattr(i, 'likeCount', 0) or 0),
        "retweets": int(getattr(i, 'retweetCount', 0) or 0),
        "replies": int(getattr(i, 'replyCount', 0) or 0),
        'quotes': int(getattr(i, 'quoteCount', 0) or 0),
        'lang': getattr(i, 'lang', None),
        'source': getattr(i, 'sourceLabel', None),
        'url': getattr(i, 'url', None),
        'latitude': lat,
        'longitude': lon
    }
    return row

def insert_rows(conn, rows):
    
    if not rows:
        return 0
    
    cols = [
        'tweet_id', 'query', 'date_utc', 'user', 'username', 'content',
        'likes', 'retweets', 'replies', 'quotes', 'lang', 'source',
        'url', 'latitude', 'longitude'
    ]

    placeholders = ','.join(['?'] * len(cols))
    sql = f"""
        INSERT OR IGNORE INTO tweets ({','.join(cols)})
        VALUES ({placeholders});
    """
    cur = conn.cursor()
    cur.executemany(sql, [[row.get(col) for col in cols] for row in rows])
    conn.commit()
    return cur.rowcount

async def scrape_tweets(q, limit, since=None, until=None, cookies_path=None, csv_path=None, db_path='data/tweets.db'):
    
    set_log_level("WARNING")
    api = API()
    try:
        api.pool.add_cookies_from_file(cookies_path)
    except Exception as e:
        print(f"Warning: Could not load cookies from {cookies_path}. Proceeding without cookies.")
        pass
    
    search = search_query(q, since, until)
    rows, count = [], 0
    
    async for tweet in api.search(search):
        try:
            rows.append(to_row(tweet, q))
            count += 1
            if limit and count >= limit:
                break
        except Exception as e:
            continue
    
    if csv_path:
        df = pd.DataFrame(rows)
        if rows:
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path, index=False)
    conn = create_database(db_path)
    inserted_count = insert_rows(conn, rows)
    conn.close()
    return len(rows), inserted_count

def parse_arguments():
    ap = argparse.ArgumentParser(description="Twitter Scraper with snscrape and store in SQLite database.")
    ap.add_argument('--q', required=True, help="Query/Keyword or hashtag to search for. E.g., 'AI', '#Netherlands'")
    ap.add_argument('--limit', type=int, default=500, help="Maximum number of tweets to scrape (default is 500, 0 for no limit).")
    ap.add_argument('--since', type=str, default=None, help="Start date (YYYY-MM-DD) to scrape tweets from.")
    ap.add_argument('--until', type=str, default=None, help="End date (YYYY-MM-DD) to scrape tweets until.")
    ap.add_argument('--csv', type=str, default=None, help="Optional path to save scraped tweets as CSV file.")
    ap.add_argument('--db', type=str, default='data/tweets.db', help="SQLite database path (default is 'data/tweets.db').")
    return ap.parse_args()

def main():
    args = parse_arguments()
    total_scraped, total_inserted = asyncio.run(scrape_tweets(
        q=args.q,
        limit=args.limit,
        since=args.since,
        until=args.until,
        cookies_path='secrets/twitter_cookies.txt',
        csv_path=args.csv,
        db_path=args.db
    ))
    
    print(f"Fetched: {total_scraped} tweets | Inserted: {total_inserted} new tweets into the database.")
    
if __name__ == "__main__":
    main()