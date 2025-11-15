import os
import argparse
import pandas as pd
from src.storage import initialize_db, upsert_df
from src.scrapers.reddit_scrapper import redit_client, scrape_reddit
from src.scrapers.mastodon_scraper import scrape_mastodon
from src.labeling.sentiment_model import analyze_sentiment, add_translations

def run_scraper(query, use_mastodon, use_reddit, limit, min_score):
    initialize_db()
    frames = []
    
    if use_mastodon:
        frames.append(
            scrape_mastodon(query, limit=limit, min_score=min_score)
        )
    if use_reddit:
        rc = redit_client()
        frames.append(
            scrape_reddit(rc, query, limit=limit, min_score=min_score)
        )
    if not frames:
        return 0
    
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"])
    
    df = add_translations(df)
    df = analyze_sentiment(df)
    
    needed = ["id","source","author","text","created_utc","url","keyword","score","extras"]
    df = df.copy()
    for col in needed:
        if col not in df.columns:
            df[col] = None
    df = df[needed]
    
    df = df.drop_duplicates(subset=["id"]).copy()
    inserted = upsert_df(df)
    return inserted

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True, help="Keyword to search for on the social media platforms")
    ap.add_argument("--mastodon", action="store_true")
    ap.add_argument("--reddit", action="store_true")
    ap.add_argument("--limit", type=int, default=200, help="Number of posts to fetch from each platform")
    ap.add_argument("--min_score", type=int, default=0, help="Minimum score (likes/upvotes) to consider a post")
    args = ap.parse_args()
    
    inserted = run_scraper(
        query=args.query,
        use_mastodon=args.mastodon,
        use_reddit=args.reddit,
        limit=args.limit,
        min_score=args.min_score,
    )
    print(f"Inserted: {inserted} rows.")
    
if __name__ == "__main__":
    main()