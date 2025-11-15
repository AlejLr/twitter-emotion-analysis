import sqlite3
import pandas as pd
import streamlit as st

from src.labeling.sentiment_model import add_translations, analyze_sentiment
from src.fetcher import run_scraper

DB_PATH = "data/posts.db"

@st.cache_data(show_spinner=False)
def load_raw_data(limit = 2000):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM posts ORDER BY datetime(created_utc) DESC LIMIT ?;", 
        conn,
        params=(limit,),
        )
    conn.close()
    return df

@st.cache_data(show_spinner=True)
def process_posts(df, use_translations=True):
    if df.empty:
        return df
    
    if use_translations:
        df = add_translations(df)
    else:
        df = df.copy()
        df["text_en"] = df["text"]
        df["lang"] = "en"
        
    df = analyze_sentiment(df)
    df["created_utc"] = pd.to_datetime(df["created_utc"], errors="coerce")
    return df

st.set_page_config(
    page_title="Social Media Sentiment Analysis",
    layout="wide",
)
st.title("Social Media Sentiment Analysis Dashboard")
st.caption(
    "Originally designed for Twitter; due to to API restrictions this version uses Mastodon (and optionally Reddit) posts."
    "The pipeline is source-agnostic and easily extendable."
)

col1, col2, col3, col4 = st.columns([2, 1.5, 1.2, 1.2])

with col1:
    keyword_filter = st.text_input("Filter by Keyword (from DB)", value="")
with col2:
    sources = st.multiselect(
        "Sources",
        options=["mastodon","reddit"],
        default=["mastodon","reddit"],
    )
with col3:
    limit = st.slider("Max rows from DB", min_value=200, max_value=5000, value=1500, step=100)
with col4:
    use_translations = st.checkbox("Translate non-English posts -> English", value=True)
    
st.write("---")

fetch_col1, fetch_col2, fetch_col3 = st.columns([2, 1, 1])

with fetch_col1:
    fetch_limit = st.number_input("Fetch limit (per search)", min_value=20, max_value=500, value=200, step=10)

with fetch_col2:
    min_score = st.number_input("Min amount of likes (0 recomended))", min_value=0, max_value=50, value=0, step=1)

with fetch_col3:
    do_fetch = st.button("Fetch / refresh data for this keyword")
    
if do_fetch:
    if not keyword_filter.strip():
        st.error("Please enter a keyword before fetching.")
    else:
        use_mastodon = "mastodon" in sources
        use_reddit = "reddit" in sources
        if not (use_mastodon or use_reddit):
            st.error("Please select at least one source (Mastodon and/or Reddit).")
        else:
            with st.spinner("Fetching data... This may take a while."):
                inserted = run_scraper(
                    query=keyword_filter.strip(),
                    use_mastodon=use_mastodon,
                    use_reddit=use_reddit,
                    limit=int(fetch_limit),
                    min_score=int(min_score),
                )
            st.success(f"Fetch complete. Inserted up to {inserted} new rows into the database.")
            st.rerun()

raw_df = load_raw_data(limit=limit)

if keyword_filter:
    raw_df = raw_df[raw_df["keyword"].str.contains(keyword_filter, case=False, na=False)]

if sources:
    raw_df = raw_df[raw_df["source"].isin(sources)]
    
st.write(f"Loaded {len(raw_df)} rows from the database.")

if raw_df.empty:
    st.info("No data matches the current filters. Try different keywords or sources.")
    st.stop()

df = process_posts(raw_df, use_translations=use_translations)

col_a, col_b, col_c = st.columns(3)

with col_a:
    st.metric("Total Posts", f"{len(df)}")
    
with col_b:
    counts = df["sentiment_label"].value_counts()
    pos = int(counts.get("positive", 0))
    neg = int(counts.get("negative", 0))
    neu = int(counts.get("neutral", 0))
    st.metric("Sentiment split", f"+{pos} / {neu} / -{neg}")
    
with col_c:
    avg_score = df["sentiment_score"].mean()
    st.metric("Average Sentiment Score", f"{avg_score:.3f}")

st.write("---")

tab1, tab2, tab3 = st.tabs(["Sentiment Distribution", "Time series", "Samples"])

with tab1:
    st.subheader("Sentiment Distribution")
    sent_counts = (
        df.groupby(["source", "sentiment_label"])["id"]
        .count()
        .reset_index(name="count")
    )
    if not sent_counts.empty:
        st.bar_chart(
            sent_counts.pivot(index="sentiment_label", columns="source", values="count")
        )
    else:
        st.write("No sentiment data to display.")
    
with tab2:
    st.subheader("Sentiment over time")
    ts = df.copy()
    ts = ts.dropna(subset=["created_utc"])
    ts = (ts.groupby([pd.Grouper(key="created_utc", freq="H"), "source"])["id"].count().reset_index(name="count"))
    if not ts.empty:
        st.line_chart(ts.pivot(index="created_utc", columns="source", values="count"))
    else:
        st.write("No time series data to display.")
    
with tab3:
    st.subheader("Recent Posts (with Sentiment)")
    show_n = st.slider("Row to display", min_value=5, max_value=50, value=15, step=5)
    display_cols = [
        "source",
        "author",
        "created_utc",
        "lang",
        "sentiment_label",
        "sentiment_score",
        "text_en",
        "url",
    ]
    existing_cols = [col for col in display_cols if col in df.columns]
    st.dataframe(df[existing_cols].head(show_n), use_container_width=True)
    
st.write("---")
st.caption(
    "Posts are fetched from public Mastodon (and optionally Reddit) APIs, translated when necessary, "
    "and labeled with VADER sentiment. No user-identifiable profiling beyond public content."
)