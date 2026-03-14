#!/usr/bin/env python3
"""
main_improved_action.py
Improved GitHub Actions one-shot poller:
 - Auto S&P500 + NASDAQ-100 (Wikipedia)
 - Rotation by GITHUB_RUN_NUMBER / time to process the universe in chunks
 - Focused Google News RSS queries per ticker/company + event keywords
 - Classifies only your 6 event types and alerts via Telegram (required) + optional Slack/Email
 - No persistence (dedupe only in-run)
 - Daily digest at configured local hour (Europe/Paris by default)

Config (env):
 - TELEGRAM_BOT_TOKEN (required)
 - TELEGRAM_CHAT_ID  (required)
 - SLACK_WEBHOOK (optional)
 - SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_EMAIL_TO (optional)
 - MAX_TICKERS (how many tickers to process this run, default 250)
 - TIMEZONE (default Europe/Paris)
 - DAILY_DIGEST_HOUR (default 8)
 - THROTTLE_SECONDS (delay between feed polls, default 0.4)
 - USER_AGENT (optional override for HTTP)
 - GITHUB_RUN_NUMBER (GitHub Actions provides this automatically; used for rotation)
"""

import os
import time
import hashlib
import urllib.parse
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from email.message import EmailMessage
import smtplib
import re

# --------------- Configuration ---------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")

MAX_TICKERS = int(os.getenv("MAX_TICKERS", "250"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR = int(os.getenv("DAILY_DIGEST_HOUR", "8"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")

# keywords used to build Google News query - grouped to match your 6 types
KEYWORDS = [
    "earnings", "quarterly results", "eps", "revenue", "beats", "misses",
    "acquire", "acquisition", "merger", "takeover", "s-4", "will acquire",
    "launch", "launches", "introduce", "unveil", "release", "new product",
    "partnership", "partners with", "joint venture", "signs deal", "agreement",
    "scandal", "allegation", "fraud", "lawsuit", "investigation", "probe",
    "venture failed", "venture success", "failed", "success", "abandons"
]

# targeted event labels (only these generate alerts)
TARGET_LABELS = {"earnings", "takeover", "scandal", "product_launch", "venture_result", "major_deal"}

# --------------- Utilities ---------------
def fingerprint(title, link, published):
    return hashlib.sha256(f"{title}|{link}|{published}".encode()).hexdigest()

def safe_get(url, timeout=15):
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(url, timeout=timeout, headers=headers)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"HTTP GET error for {url}: {e}")
        return None

def post_json(url, json_payload, timeout=10):
    try:
        r = requests.post(url, json=json_payload, timeout=timeout)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"POST error to {url}: {e}")
        return False

# --------------- Notify channels ---------------
def notify_telegram(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("[Telegram missing] would send:", text)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        if resp.status_code != 200:
            print("Telegram response error:", resp.status_code, resp.text)
    except Exception as e:
        print("Telegram send error:", e)

def notify_slack(text):
    if not SLACK_WEBHOOK:
        return
    post_json(SLACK_WEBHOOK, {"text": text})

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

# --------------- Fetch indices ---------------
def fetch_wikipedia_table(url, table_id_hint=None):
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_="wikitable")
    if not tables:
        return []
    rows = []
    for t in tables:
        for tr in t.find_all("tr")[1:]:
            cols = tr.find_all(["td", "th"])
            if len(cols) < 2:
                continue
            rows.append(cols)
    return rows

def get_sp500_list():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        rows = fetch_wikipedia_table(url)
        out = []
        for cols in rows:
            # symbol usually in first column, name usually second
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
        # Table with constituents often has header 'Ticker' or column arrangement; we'll look for tables with tickers
        out = []
        for table in soup.find_all("table", class_="wikitable"):
            for tr in table.find_all("tr")[1:]:
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 2:
                    # some tables list company then ticker, others reverse; try both
                    a = tds[0].get_text(strip=True)
                    b = tds[1].get_text(strip=True)
                    # heuristics: if second cell is all-uppercase ticker-like
                    if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", b):
                        name = a
                        ticker = b
                    elif re.fullmatch(r"[A-Z0-9\.\-]{1,10}", a):
                        name = b
                        ticker = a
                    else:
                        # fallback - skip ambiguous rows
                        continue
                    out.append((ticker.replace(".", "-"), name))
        print(f"NASDAQ-100 count fetched: {len(out)}")
        return out
    except Exception as e:
        print("get_nasdaq100_list error:", e)
        return []

# --------------- Build Google News RSS query ---------------
def build_google_news_rss(ticker, name):
    # Query: (TICKER OR "Company Name") (keyword1 OR keyword2 ...)
    # We URL-encode the full query and prefer US news (hl=en-US&gl=US&ceid=US:en)
    company_phrase = f'"{name}"'
    keywords_or = " OR ".join(KEYWORDS)
    query = f"({ticker} OR {company_phrase}) ({keywords_or})"
    encoded = urllib.parse.quote(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

# --------------- Classifier (strict patterns) ---------------
def classify(title, summary):
    txt = (title + " " + (summary or "")).lower()

    # Earnings: explicit keywords near quarter/year or EPS terms
    if re.search(r"\b(earnings|quarterly results|q[1-4]\s?\s?results|eps|earnings per share|announces results|reports results|reports q)", txt):
        return "earnings"

    # Takeover / M&A: "acquire", "acquisition", "merger", "will acquire", "to buy", "agreement to buy"
    if re.search(r"\b(acquir|acquisition|merger|takeover|will acquire|to buy|agrees to buy|agreed to buy|s-4|s-4 filing)\b", txt):
        return "takeover"

    # Scandal: legal/regulatory language
    if re.search(r"\b(scandal|allegation|fraud|lawsuit|investigation|probe|charged|indicted|settlement|regulator)\b", txt):
        return "scandal"

    # Product launch: "launch", "unveil", "introduce", "new product", "releases"
    if re.search(r"\b(launch|launches|unveil|introduce|introduces|new product|releases|announces new)\b", txt):
        return "product_launch"

    # Venture result: mention of pilot/joint venture + success/failure words
    if re.search(r"\b(joint venture|spin-off|pilot|venture|funding|partnership pilot)\b", txt) and re.search(r"\b(success|succeed|failed|failure|abandon|abandons|halts)\b", txt):
        return "venture_result"

    # Major deal/partnership: sign deal, strategic partnership, contract worth $X
    if re.search(r"\b(partnership|partners with|signs deal|strategic partnership|contract worth|agreement with|signed a deal)\b", txt):
        return "major_deal"

    return "other"

# --------------- RSS poll & routing ---------------
def poll_feed(url):
    items = []
    try:
        parsed = feedparser.parse(url)
        for e in parsed.entries:
            title = e.get("title", "")
            link = e.get("link", "")
            summary = e.get("summary", "") or e.get("description", "")
            published = e.get("published", e.get("updated", datetime.utcnow().isoformat()))
            items.append({"title": title, "link": link, "summary": summary, "published": published})
    except Exception as e:
        print("feedparser error for", url, e)
    return items

# --------------- Main logic ---------------
def main():
    # basic validation
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required environment variables.")
        return

    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    date_iso = now_local.strftime("%Y-%m-%d")

    # fetch ticker lists
    sp = get_sp500_list()
    nas = get_nasdaq100_list()
    combined = sp + nas

    # dedupe by ticker key (ticker->(ticker,name))
    uniq = {}
    for t, n in combined:
        key = t.upper()
        if key not in uniq:
            uniq[key] = (t, n)
    universe = list(uniq.values())
    total = len(universe)
    if total == 0:
        print("No tickers found. Exiting.")
        return

    # rotation calculation: derive a run_index from GITHUB_RUN_NUMBER or time
    run_num = os.getenv("GITHUB_RUN_NUMBER")
    try:
        run_index = int(run_num) if run_num else int(time.time() // (60*30))  # half-hour epoch-based fallback
    except Exception:
        run_index = int(time.time() // (60*30))
    # compute chunk start based on run_index
    chunk_size = max(1, min(MAX_TICKERS, total))
    # rotate window offset every run so we cycle through full universe
    offset = (run_index * chunk_size) % total
    # build rotated slice (wrap around)
    def slice_window(lst, off, size):
        if size >= len(lst):
            return lst
        end = off + size
        if end <= len(lst):
            return lst[off:end]
        return lst[off:len(lst)] + lst[0:end - len(lst)]

    selected = slice_window(universe, offset, chunk_size)
    print(f"Universe size: {total}, processing {len(selected)} tickers (offset {offset}, run {run_index})")

    # build feeds
    feeds = []
    for ticker, cname in selected:
        feeds.append((ticker, cname, build_google_news_rss(ticker, cname)))

    # in-run dedupe
    seen = set()
    digest = []

    # poll feeds sequentially with light throttling
    for ticker, cname, rss in feeds:
        # guard: sometimes Google News blocks suspicious queries — skip on fail
        items = poll_feed(rss)
        if not items:
            time.sleep(THROTTLE_SECONDS)
            continue
        for it in items:
            fp = fingerprint(it["title"], it["link"], it["published"])
            if fp in seen:
                continue
            seen.add(fp)
            label = classify(it["title"], it.get("summary", ""))
            if label == "other":
                continue
            # only alert on target labels
            if label in TARGET_LABELS:
                # create a compact alert message
                alert_text = f"[{label.upper()}] {ticker} — {it['title']}\n{it.get('link')}"
                # add a short tag for company name if exists
                if cname:
                    alert_text = f"[{label.upper()}] {ticker} / {cname} — {it['title']}\n{it.get('link')}"
                print("ALERT:", alert_text)
                notify_telegram(alert_text)
                notify_slack(alert_text)
                # send email for highest severity (takeover/scandal/major_deal)
                if label in {"takeover", "scandal", "major_deal"}:
                    send_email(f"[ALERT] {label.upper()} — {ticker}", f"<p>{alert_text}</p>")
                # add to digest with score heuristic
                score = 90 if label in {"takeover", "scandal", "major_deal"} else 60
                digest.append({"label": label, "title": it["title"], "link": it["link"], "ticker": ticker, "score": score})
            # small per-item throttle
            time.sleep(0.08)
        # throttle between feeds
        time.sleep(THROTTLE_SECONDS)

    # also include today's scheduled earnings (Yahoo) for broader capture
    try:
        yahoo_items = fetch_yahoo_earnings_for_date(date_iso)
        for it in yahoo_items:
            fp = fingerprint(it["title"], it["link"], it["published"])
            if fp in seen:
                continue
            seen.add(fp)
            # treat these as earnings
            alert_text = f"[EARNINGS] {it['title']}\n{it['link']}"
            print("EARNINGS SCHEDULE:", alert_text)
            notify_telegram(alert_text)
            notify_slack(alert_text)
            digest.append({"label": "earnings", "title": it["title"], "link": it["link"], "ticker": "", "score": 50})
            time.sleep(0.05)
    except Exception as e:
        print("Yahoo earnings error:", e)

    # send daily digest at configured hour
    if now_local.hour == DAILY_DIGEST_HOUR:
        if digest:
            html = "<h2>Daily Market Event Digest</h2><ul>"
            for it in sorted(digest, key=lambda x: -x["score"]):
                html += f"<li><b>{it['label']}</b> — {it.get('ticker','')} {it['title']} (<a href='{it['link']}'>link</a>)</li>"
            html += "</ul>"
            send_email("Daily Market Event Digest", html)
            notify_telegram("Daily Market Event Digest sent (check email if configured).")
            notify_slack("Daily Market Event Digest sent (check email if configured).")
        else:
            print("Digest hour but no items to include.")
    else:
        print(f"Run complete at {now_local.isoformat()}; not digest hour ({DAILY_DIGEST_HOUR}).")

# helper: Yahoo earnings scraper (kept local to avoid external dependency)
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

# --------------- run ---------------
if __name__ == "__main__":
    main()
