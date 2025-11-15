from mastodon import Mastodon, MastodonError, MastodonNetworkError
import pandas as pd
from html import unescape
import re

BRIDGE_DOMAINS = {"web.brid.gy", "brid.gy"}

def clean_text(text):
    text = re.sub(r"<.*?>", "", text or "")
    text = unescape(text).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text

def is_bridge_domain(account):
    
    acct = (account or "").split("@")
    return len(acct) == 2 and acct[-1] in BRIDGE_DOMAINS


def scrape_mastodon(query, limit, instace_url="https://mastodon.social", token=None, min_score=0, hastag = True):
    
    api = Mastodon(api_base_url=instace_url, access_token=token)
    
    try:
        if hastag:
            results = api.timeline_hashtag(query.lstrip("#"), limit= limit)
        else:
            results = api.search_v2(q=query, resolve=True, type="statuses", limit= limit)["statuses"]
    except MastodonNetworkError as e:
        print(f"Mastodon Network error: {e}")
        return []
    
    data = []
    seen = set()
    
    def add_status(s):
        if s.get("reblog"):
            return
        acct = s["account"]["acct"]
        if is_bridge_domain(acct):
            return
        favs = int(s.get("favourites_count") or 0)
        if favs < min_score:
            return
        text = clean_text(s.get("content"))
        
        if not text:
            return
        sid = str(s["id"])
        if sid in seen:
            return
        
        seen.add(sid)
        data.append({
            "id": sid,
            "source": "mastodon",
            "author": acct,
            "text": text,
            "created_utc": str(s["created_at"]),
            "url": s.get("url"),
            "keyword": query,
            "score": favs,
            "extras": None
        })
    
    while True:
        for status in results:
            add_status(status)
            if len(data) >= limit:
                break
            
        if len(data) >= limit:
            break
        
        try:
            results = api.fetch_next(results)
            if not results:
                break
        except Exception:
            break
        
    return pd.DataFrame(data)
        