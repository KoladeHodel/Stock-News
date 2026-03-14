#!/usr/bin/env python3
# (Same header as prior version - omitted for brevity)
# ... keep all imports unchanged ...
import os
import time
import hashlib
import urllib.parse
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import re
import json
from email.message import EmailMessage
import smtplib

# ---------------- Config (unchanged) ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# ... keep rest of config variables unchanged ...
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "200"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR_MORNING = int(os.getenv("DAILY_DIGEST_HOUR_MORNING", "5"))
DAILY_DIGEST_HOUR_EVENING = int(os.getenv("DAILY_DIGEST_HOUR_EVENING", "18"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")
RECENT_DAYS = int(os.getenv("RECENT_DAYS", "3"))
UPCOMING_DAYS = int(os.getenv("UPCOMING_DAYS", "5"))

CACHE_DIR = ".cache"
SEEN_FILE = os.path.join(CACHE_DIR, "seen.json")
MORNING_SNAPSHOT_FILE = os.path.join(CACHE_DIR, "morning_snapshot.json")

# (Keep AI_INTENT_KEYPHRASES, PRODUCT_KEYWORDS, etc. unchanged — copy from the previous version)
AI_INTENT_KEYPHRASES = [
    r"\binvest(s|ed|ing)?\b.*\b(ai|artificial intelligence|generative ai|ml|machine learning)\b",
    r"\bcommit(s|ted)?\b.*\b(ai|artificial intelligence)\b",
    r"\bfund(s|ed|ing)?\b.*\b(ai|artificial intelligence)\b",
    r"\b(raises|raised)\b.*\bfor\b.*\b(ai|artificial intelligence)\b",
    r"\b(acquires?|acquired|acquisition of)\b.*\b(ai startup|ai company|ai firm)\b",
    r"\b(orders|orders? of|purchases?|buys?)\b.*\b(gpu|gpus|a100|h100|accelerator|tensor core)\b",
    r"\b(opens|opening|launches|announces)\b.*\b(ai lab|research lab|ai center|ai initiative|ai program)\b",
    r"\b(partner(s)? with|partners with|partners? with|partners? to)\b.*\b(OpenAI|Anthropic|NVIDIA|Cohere|Meta|Google Cloud|AWS|Microsoft)\b",
    r"\b(integrat(es|ed|ing)?|powered by)\b.*\b(gpt|llm|large language model|openai|anthropic|gpt-4|gpt-4o)\b",
    r"\b(build(s|ing)?|develop(s|ing)?|deploy(s|ing)?)\b.*\b(large language model|llm|generative model|ai model)\b",
]
PRODUCT_KEYWORDS = ["launch","launches","unveil","introduce","introduces","new product","releases","announces new","unveils"]
SCANDAL_KEYWORDS = ["scandal","allegation","fraud","lawsuit","investigation","probe","charged","indicted","recall"]
DEAL_KEYWORDS = ["partnership","partners with","signs deal","strategic partnership","contract worth","agreement with","signed a deal"]
MNA_KEYWORDS = ["acquir","acquisition","merger","takeover","s-4","will acquire","to buy","agrees to buy"]
EARNINGS_KEYWORDS = ["earnings","quarterly results","eps","revenue","beats","misses"]

# ---------------- Helpers, classification, poll, Yahoo earnings, notify -- unchanged ----------------
# (Copy all helper functions, parse_entry_published, is_recent_entry, get_sp500_list, get_nasdaq100_list,
#  build_google_news_rss, poll_feed, detect_ai_intent, classify, is_scandal_after_launch,
#  fetch_yahoo_earnings_for_date, fetch_upcoming_earnings, notify_telegram_digest, send_email)
# Use the exact implementations from the previous twice-daily script.

# ---------------- Main (only changed logic shown here) ----------------
def main():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required.")
        return

    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    local_hour = now_local.hour

    # Manual-run detection & override
    github_event_name = os.getenv("GITHUB_EVENT_NAME", "")  # GitHub sets this
    manual_mode_env = os.getenv("MANUAL_MODE", "")  # allow direct env override
    # Workflow will pass MANUAL_MODE via input (see workflow below)
    manual_run = (github_event_name == "workflow_dispatch")

    # Determine digest roles:
    # If manual_run and MANUAL_MODE set to 'morning' or 'evening', obey it.
    # If manual_run and MANUAL_MODE == 'auto' or empty -> default to morning full digest.
    # Otherwise (not manual), operate only at configured local hours.
    if manual_run:
        mm = (manual_mode_env or "").strip().lower()
        if mm == "evening":
            is_morning = False
            is_evening = True
            print("Manual run requested: EVENING (delta) mode.")
        else:
            # default manual or 'morning' or 'auto' -> morning/full digest
            is_morning = True
            is_evening = False
            print("Manual run requested: MORNING (full) mode.")
    else:
        is_morning = (local_hour == DAILY_DIGEST_HOUR_MORNING)
        is_evening = (local_hour == DAILY_DIGEST_HOUR_EVENING)
        print(f"Local hour {local_hour}. is_morning={is_morning} is_evening={is_evening}")

    # Load caches
    ensure_cache_dir()
    seen = load_json_set(SEEN_FILE)
    morning_snapshot = load_json_set(MORNING_SNAPSHOT_FILE)
    # Fetch upcoming earnings always
    upcoming = fetch_upcoming_earnings(UPCOMING_DAYS)

    # If not a digest hour and not manual, exit early (cheap)
    if not (is_morning or is_evening):
        print("Not a digest hour and not a manual run — exiting (no heavy collection).")
        save_json_set(SEEN_FILE, seen)
        return

    # proceed with rotated collection + classification (unchanged logic from prior script)
    # ... collect feeds into categories, add fingerprints to seen and added_this_run ...
    # Use exact code for rotation, collection, categorization, message building, snapshot updates
    # (the same code used previously in the twice-daily script)

    # For brevity here: copy the remainder of the previously supplied twice-daily script unchanged,
    # i.e. rotation, feed polling, categories population, morning/evening message building,
    # sending via notify_telegram_digest/send_email, persisting seen and morning snapshot.

if __name__ == "__main__":
    main()
