import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(df):
    if df.empty:
        return df
    
    scores = df["text"].fillna("").apply(lambda x: analyzer.polarity_scores(x)["compound"])
    df = df.copy()
    df["sentiment_score"] = scores
    df["sentiment_label"] = pd.cut(
        df["sentiment_score"], bins=[-1.0, -0.05, 0.05, 1.0], labels=["negative", "neutral", "positive"]
    )
    return df


# Additional function to aggregate sentiment results with Bert
def aggregate_sentiment(df):
    pass
    