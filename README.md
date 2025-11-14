# Twitter Emotion Analysis

## Project Overview

**Twitter is not possible due to scrappers block and official API price**

**-> Reddit and Mastodon were used instead**

This project realizes an emotional analyzis of recent tweets around a given keyword.
It collects tweets in real time, processes the text, and applies sentiment and emotion analysis to them.
The results will be visualized in charts and dashboards.

## Goals

- Collect tweets from Twitter API.
- Perform baseline sentiment analysis.
- Extend to emotion classification.
- Build interactive dashboard for keyword-based emotion exploration.

## Project Structure

- `data/` : datasets of collected tweets
- `notebooks/` : exploratory notebooks
- `src/` : core Python scripts
- `requirements.txt` : dependencies

## Current Status (v0.1)

- Repo initialized
- Project and Scope Defined

## Currently Under Development

- Tweet Collection
- Storage on JSON foramt
- Data Cleaning

## Project Description

### Data Collection Tool

- snscrape 

### Data Storage

- Tweets will be stored in JSON format
- Tweets will be cleaned and saved in CSV format
- (v2.0 verion) Data will be loaded into SQLite in order to connect PowerBI

### Model Phases

The model will have three phases:

- Phase 1 (v1.0/Baseline) - Sentiment with VADER/TextBlob (Quick analysis, not too reliable)
- Phase 2 (v2.0/Upgrade) - Emotion with HuggingFace pretrained model (possibly BERT)
- Phase 3 (v3.0/Final version) - model coded and trained on my own

### Process

- Input: keyword → fetch 250/500 recent tweets when looking the keyword
- Output: Chart with sentiment / emotion analysis broken down
- Advanced: track evolution of emotions over time (compare recent with one week old tweets)
- Advanced visualization: dashboard for interactive exploration (probably Streamlit)

### Emotions

Emotions the model will look for:

- Positive/Negative/Neutral
- Joy / Excitement
- Anger / Frustration
- Fear / Uncertainty

The scope of the emotion will be kept at a maximum of 6 in order to reduce ambiguity and the possibility of misclassification.

The analysis will report 3 different sentiment classes (Positive, Negative and Neutral) and three emotion classes (Joy, Anger, Fear).
