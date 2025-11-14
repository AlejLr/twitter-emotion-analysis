import praw
import pandas as pd

def redit_client(client_id, client_secret, user_agent):
    return praw.Reddit(client_id=client_id,
                       client_secret=client_secret,
                        user_agent=user_agent)

def scrape_reddit(rc, query, subreddit="all", limit=100):
    data = []
    for submission in rc.subreddit(subreddit).search(query, limit=limit):
        data.append({
            "id": f"reddit_{submission.id}",
            "source": "reddit",
            "author": str(submission.author) if submission.author else "[deleted]",
            "text": (submission.title or "") + "\n" + (submission.selftext or ""),
            "created_utc": str(pd.to_datetime(submission.created_utc, unit='s')),
            "url": f"https://www.reddit.com{submission.permalink}",
            "keyword": query,
            "score": int(submission.score or 0),
            "extras": None
        })
    return pd.DataFrame(data)