"""

Outsourced SQL Schema for usage in both data sources

"""

import os
import sqlite3
import pandas as pd
from contextlib import contextmanager

BD_PATH = "data/posts.db"

@contextmanager
def connect():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(BD_PATH)
    try: yield conn
    finally: conn.close()
    
def initialize_db():
    with connect() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            source TEXT,
            author TEXT,
            text TEXT,
            created_utc TEXT,
            url TEXT,
            keyword TEXT,
            score INT,
            extras TEXT
        )
        """)
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_time ON posts(source, created_utc)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_keyword ON posts(keyword)")
        conn.commit()

def upsert_df(df: pd.DataFrame):
    if df.empty:
        return 0
    
    with connect() as conn:
        
        cols = ["id","source","author","text","created_utc","url","keyword","score","extras"]
        
        df = df.copy()
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        
        rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
        
        before = conn.total_changes
        conn.executemany("""
        INSERT OR REPLACE INTO posts (id, source, author, text, created_utc, url, keyword, score, extras)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        
        conn.commit()
        after = conn.total_changes
    
    return after - before