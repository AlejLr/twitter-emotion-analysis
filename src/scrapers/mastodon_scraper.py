from mastodon import Mastodon
import pandas as pd
from html import unescape
import re

def clean_text(text):
    text = re.sub(r"<.*?>", "", text or "")
    text = unescape(text).strip()
    return text

def scrape_mastodon(query, limit, instace_url="https://mastodon.social", token=None):
    api = Mastodon(api_base_url=instace_url, access_token=token)
    results = api.timeline_hashtag(query.lstrip("#"), limit=limit)
    data = []
    for text in results:
        data.append({
            "id": str(text["id"]),
            "source": "mastodon",
            "author": text["account"]["acct"],
            "text": clean_text(text["content"]),
            "created_at": str(text["created_at"]),
            "url": text["url"],
            "keyword": query,
            "score": int(text.get("favourites_count") or 0),
            "extras": None
        })
    
    return pd.DataFrame(data)
        