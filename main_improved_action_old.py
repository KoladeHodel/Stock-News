#!/usr/bin/env python3
"""
Digest-only improved poller (drop-in).

Behavior:
 - Auto S&P500 + NASDAQ-100 (Wikipedia)
 - Rotation by GITHUB_RUN_NUMBER / time to process the universe in chunks
 - Filters items older than RECENT_DAYS (default 7)
 - Persists seen fingerprints to .cache/seen.json (restored by actions/cache)
 - Collects events by category during the run
 - Sends ONE Telegram message per run with grouped events (no per-item alerts)
 - Optionally sends a daily email digest at DAILY_DIGEST_HOUR
"""

import os
import time
import hashlib
import urllib.parse
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from email.message import EmailMessage
import smtplib
import re
import json

# ---------------- Config ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")  # optional (not used per-item)
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")

MAX_TICKERS = int(os.getenv("MAX_TICKERS", "200"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR = int(os.getenv("DAILY_DIGEST_HOUR", "8"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")
RECENT_DAYS = int(os.getenv("RECENT_DAYS", "7"))  # only accept items within this many days

CACHE_DIR = ".cache"
SEEN_FILE = os.path.join(CACHE_DIR, "seen.json")

# keywords
KEYWORDS = [
    "earnings", "quarterly results", "eps", "revenue", "beats", "misses",
    "acquire", "acquisition", "merger", "takeover", "s-4", "will acquire",
    "launch", "launches", "introduce", "unveil", "release", "new product",
    "partnership", "partners with", "joint venture", "signs deal", "agreement",
    "scandal", "allegation", "fraud", "lawsuit", "investigation", "probe",
    "venture failed", "venture success", "failed", "success", "abandons"
]
AI_KEYWORDS = [
    "artificial intelligence", "generative ai", "ai partnership", "ai platform",
    "ai model", "machine learning", "openai", "anthropic", "nvidia", "copilot",
    "gpt", "llm", "large language model"
]

# target labels
TARGET_LABELS = {"earnings", "takeover", "scandal", "product_launch", "venture_result", "major_deal", "ai_special"}

# ---------------- Helpers ----------------
def fingerprint(title, link, published):
    return hashlib.sha256(f"{title}|{link}|{published}".encode()).hexdigest()

def safe_get(url, timeout=15):
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, timeout=timeout, headers=headers)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[HTTP] GET error for {url}: {e}")
        return None

def load_seen():
    try:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR, exist_ok=True)
        if os.path.exists(SEEN_FILE):
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
                elif isinstance(data, dict):
                    return set(data.keys())
        return set()
    except Exception as e:
        print("Error loading seen file:", e)
        return set()

def save_seen(seen_set):
    try:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = SEEN_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen_set), f)
        os.replace(tmp, SEEN_FILE)
    except Exception as e:
        print("Error saving seen file:", e)

def parse_entry_published(entry, target_tz):
    try:
        if entry.get("published_parsed"):
            ts = time.mktime(entry["published_parsed"])
            dt = datetime.fromtimestamp(ts, pytz.UTC).astimezone(target_tz)
            return dt
    except Exception:
        pass
    pub = entry.get("published") or entry.get("updated") or ""
    if pub:
        s = pub.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return dt.astimezone(target_tz)
        except Exception:
            m = re.search(r"(20\d{2})", s)
            if m:
                try:
                    year = int(m.group(1))
                    return datetime(year, 1, 1, tzinfo=target_tz)
                except Exception:
                    pass
    return None

def is_recent_entry(entry, target_tz, days=RECENT_DAYS):
    dt = parse_entry_published(entry, target_tz)
    if not dt:
        return False
    now = datetime.now(target_tz)
    return (now - dt) <= timedelta(days=days)

def build_google_news_rss(ticker, name):
    company_phrase = f'"{name}"'
    keywords_or = " OR ".join(KEYWORDS + AI_KEYWORDS)
    query = f"({ticker} OR {company_phrase}) ({keywords_or})"
    encoded = urllib.parse.quote(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

def classify(title, summary):
    txt = (title + " " + (summary or "")).lower()
    # AI first (special callout)
    if any(k in txt for k in AI_KEYWORDS):
        return "ai_special"
    if re.search(r"\b(earnings|quarterly results|eps|revenue|beats|misses)\b", txt):
        return "earnings"
    if re.search(r"\b(acquir|acquisition|merger|takeover|will acquire|s-4)\b", txt):
        return "takeover"
    if re.search(r"\b(scandal|allegation|fraud|lawsuit|investigation|probe)\b", txt):
        return "scandal"
    if re.search(r"\b(launch|launches|unveil|introduce|new product|releases|announces new)\b", txt):
        return "product_launch"
    if re.search(r"\b(joint venture|spin-off|pilot|venture|funding)\b", txt) and re.search(r"\b(success|succeed|failed|failure|abandons)\b", txt):
        return "venture_result"
    if re.search(r"\b(partnership|partners with|signs deal|strategic partnership|contract worth|agreement with)\b", txt):
        return "major_deal"
    return "other"

def poll_feed(url):
    try:
        parsed = feedparser.parse(url)
        return parsed.entries
    except Exception as e:
        print("feedparser error for", url, e)
        return []

def notify_telegram_digest(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("[Telegram missing] would send digest text length:", len(text))
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=10)
    except Exception as e:
        print("Telegram send error:", e)

def send_email(subject, body_html):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO):
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_EMAIL_TO
    msg.set_content(body_html, subtype="html")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
    except Exception as e:
        print("Email send error:", e)

# ---------------- Fetch indices ----------------
def fetch_wikipedia_table(url):
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    rows = []
    for t in tables:
        for tr in t.find_all("tr")[1:]:
            cols = tr.find_all(["td", "th"])
            if len(cols) >= 2:
                rows.append(cols)
    return rows

def get_sp500_list():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html = safe_get(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "constituents"}) or soup.find("table", class_="wikitable")
        out = []
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all(["td", "th"])
            if len(cols) >= 2:
                ticker = cols[0].get_text(strip=True)
                name = cols[1].get_text(strip=True)
                out.append((ticker.replace(".", "-"), name))
        print(f"SP500 count fetched: {len(out)}")
        return out
    except Exception as e:
        print("get_sp500_list error:", e)
        return []

def get_nasdaq100_list():
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        html = safe_get(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        out = []
        for table in soup.find_all("table", class_="wikitable"):
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 2:
                    a = tds[0].get_text(strip=True)
                    b = tds[1].get_text(strip=True)
                    if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", b):
                        name = a; ticker = b
                    elif re.fullmatch(r"[A-Z0-9\.\-]{1,10}", a):
                        name = b; ticker = a
                    else:
                        continue
                    out.append((ticker.replace(".", "-"), name))
        print(f"NASDAQ-100 count fetched: {len(out)}")
        return out
    except Exception as e:
        print("get_nasdaq100_list error:", e)
        return []

# ---------------- Main ----------------
def main():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required environment variables.")
        return

    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    date_iso = now_local.strftime("%Y-%m-%d")

    seen = load_seen()
    print(f"Loaded {len(seen)} seen fingerprints from cache.")

    sp = get_sp500_list()
    nas = get_nasdaq100_list()
    combined = sp + nas
    uniq = {}
    for t, n in combined:
        key = t.upper()
        if key not in uniq:
            uniq[key] = (t, n)
    universe = list(uniq.values())
    total = len(universe)
    if total == 0:
        print("No tickers found; exiting.")
        return

    run_num = os.getenv("GITHUB_RUN_NUMBER")
    try:
        run_index = int(run_num) if run_num else int(time.time() // (60*30))
    except Exception:
        run_index = int(time.time() // (60*30))

    chunk_size = max(1, min(MAX_TICKERS, total))
    offset = (run_index * chunk_size) % total

    def slice_window(lst, off, size):
        if size >= len(lst):
            return lst
        end = off + size
        if end <= len(lst):
            return lst[off:end]
        return lst[off:len(lst)] + lst[0:end - len(lst)]

    selected = slice_window(universe, offset, chunk_size)
    print(f"Universe size: {total}, processing {len(selected)} tickers (offset {offset}, run {run_index})")

    feeds = [(t, n, build_google_news_rss(t, n)) for (t, n) in selected]

    # collect per-category
    categories = {
        "ai_special": [],
        "earnings": [],
        "takeover": [],
        "scandal": [],
        "product_launch": [],
        "venture_result": [],
        "major_deal": []
    }

    # iterate feeds and collect (no per-item sends)
    for ticker, cname, rss in feeds:
        entries = poll_feed(rss)
        if not entries:
            time.sleep(THROTTLE_SECONDS)
            continue
        for entry in entries:
            if not is_recent_entry(entry, tz, RECENT_DAYS):
                continue
            title = entry.get("title", "") or ""
            link = entry.get("link", "") or ""
            summary = entry.get("summary", "") or entry.get("description", "") or ""
            published_str = entry.get("published") or entry.get("updated") or ""
            fp = fingerprint(title, link, published_str)
            if fp in seen:
                continue
            # Accept this item and mark seen (so we don't include it in future runs)
            seen.add(fp)
            label = classify(title, summary)
            if label in categories:
                # store compact record
                categories[label].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published_str})
            # else ignore non-target labels
        time.sleep(THROTTLE_SECONDS)

    # include today's scheduled earnings (Yahoo)
    try:
        yahoo_items = fetch_yahoo_earnings_for_date(date_iso)
        for it in yahoo_items:
            fake_entry = {"title": it["title"], "link": it["link"], "published": it["published"]}
            if not is_recent_entry(fake_entry, tz, RECENT_DAYS):
                continue
            published_str = it["published"]
            fp = fingerprint(it["title"], it["link"], published_str)
            if fp in seen:
                continue
            seen.add(fp)
            categories["earnings"].append({"ticker": "", "company": "", "title": it["title"], "link": it["link"], "published": published_str})
            time.sleep(0.02)
    except Exception as e:
        print("Yahoo earnings error:", e)

    # Build digest message (one message per run). If no items, do nothing.
    total_items = sum(len(v) for v in categories.values())
    if total_items == 0:
        print("No new recent items to include in digest this run.")
    else:
        header = f"📊 US Market Event Digest — {now_local.strftime('%Y-%m-%d %H:%M %Z')}\n"
        # Window info: we processed chunk_size tickers starting at offset
        header += f"Processed {len(selected)} tickers (rotation window). New items: {total_items}\n\n"

        # Order categories: AI special first, then earnings, partnerships/deals, product, takeover, scandal, ventures
        ordering = ["ai_special", "earnings", "major_deal", "product_launch", "takeover", "scandal", "venture_result"]
        pretty = {
            "ai_special": "🧠 AI Innovation / Partnerships",
            "earnings": "💰 Earnings",
            "major_deal": "🤝 Major Deals / Partnerships",
            "product_launch": "🚀 Product Launches",
            "takeover": "🏢 M&A / Takeovers",
            "scandal": "⚠️ Scandals / Investigations",
            "venture_result": "🔬 Venture results"
        }
        parts = [header]
        # cap per category to avoid massive messages
        PER_CAT_LIMIT = 8
        for cat in ordering:
            items = categories.get(cat, [])
            if not items:
                continue
            parts.append(pretty.get(cat, cat) + "\n")
            for idx, it in enumerate(items[:PER_CAT_LIMIT]):
                ticker_str = it["ticker"] + " " if it.get("ticker") else ""
                parts.append(f"• {ticker_str.strip()} — {it['title']}\n")
            more = max(0, len(items) - PER_CAT_LIMIT)
            if more > 0:
                parts.append(f"  ...and +{more} more in {pretty.get(cat,cat)}\n")
            parts.append("\n")

        message = "".join(parts)
        # Telegram message max ~4096 chars; truncate gracefully if needed
        if len(message) > 3800:
            message = message[:3800] + "\n\n(Truncated — check email for full digest if configured.)"

        # send single Telegram digest message
        notify_telegram_digest(message)

        # also send email digest (single email) if configured and at digest hour, or always as optional
        # We'll send email if SMTP is configured and either it's digest hour or the message was large.
        if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
            send_email("US Market Event Digest", "<pre>" + message + "</pre>")

    # persist seen cache
    try:
        save_seen(seen)
        print(f"Saved {len(seen)} fingerprints to {SEEN_FILE}")
    except Exception as e:
        print("Error saving seen cache at end:", e)

    print("Run complete.")

# ---------------- Yahoo earnings scraper ----------------
def fetch_yahoo_earnings_for_date(date_iso):
    url = f"https://finance.yahoo.com/calendar/earnings?day={date_iso}"
    html = safe_get(url)
    items = []
    if not html:
        return items
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tbody tr")
    for tr in rows[:500]:
        tds = tr.find_all("td")
        if len(tds) >= 6:
            ticker = tds[0].get_text(strip=True)
            name = tds[1].get_text(strip=True)
            eps_est = tds[2].get_text(strip=True)
            time_of_day = tds[4].get_text(strip=True)
            title = f"Earnings scheduled: {ticker} ({name}) {time_of_day}"
            items.append({"title": title, "link": url, "summary": f"EPS est: {eps_est}", "published": date_iso})
    return items

if __name__ == "__main__":
    main()
