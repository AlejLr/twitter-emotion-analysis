import os
import argparse
import pandas as pd
from storage import initialize_db, upsert_df
from scrapers.reddit_scrapper import redit_client, scrape_reddit
from scrapers.mastodon_scraper import scrape_mastodon
from labeling.sentiment_model import analyze_sentiment

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True, help="Keyword to search for on the social media platforms")
    ap.add_argument("--mastodon", action="store_true")
    ap.add_argument("--reddit", action="store_true")
    ap.add_argument("--limit", type=int, default=200, help="Number of posts to fetch from each platform")
    ap.add_argument("--min_score", type=int, default=0, help="Minimum score (likes/upvotes) to consider a post")
    args = ap.parse_args()
    
    initialize_db()
    frames = []
    
    if args.reddit:
        print(f"Scraping Reddit for '{args.query}'...")
        rc = redit_client(
            os.getenv("REDDIT_CLIENT_ID"),
            os.getenv("REDDIT_CLIENT_SECRET"),
            os.getenv("REDDIT_USER_AGENT", "alejlr-portfolio-app")
        )
        frames.append(scrape_reddit(rc, args.query, limit=args.limit, min_score=args.min_score))
    
    if args.mastodon:
        print(f"Scraping Mastodon for '{args.query}'...")
        frames.append(scrape_mastodon(args.query, limit=args.limit, min_score=args.min_score))
        
    if not frames:
        print("Nothing to do. Add --reddit and/or --mastodon for scraping.")
        return
    
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["id"])
    df = analyze_sentiment(df)
    
    # Remove later
    os.makedirs("data", exist_ok=True)
    df.to_csv(f"data/snapshot_{args.query}.csv", index=False)
    
    n = upsert_df(df[["id","source","author","text","created_utc","url","keyword","score","extras"]])
    print(f"Inserted: {len(df)} rows." + "\n" + f"New: {n} rows.")
    
if __name__ == "__main__":
    main()