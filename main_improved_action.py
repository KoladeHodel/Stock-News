#!/usr/bin/env python3
"""
main_improved_action.py — drop-in final with:
 - batch processing across universe (optionally single batch)
 - robust upcoming earnings (calendar + per-ticker JSON -> HTML fallback)
 - safe_get_json with HTTP status counters
 - per-ticker failure cache to avoid repeated retries
 - Telegram splitter (no truncation) + full-email attachment
 - morning-only upcoming earnings (not marked seen)
 - manual workflow_dispatch support (MANUAL_MODE)
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
import re
import json
from email.message import EmailMessage
import smtplib

# ---------------- Config (env) ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")

MAX_TICKERS = int(os.getenv("MAX_TICKERS", "200"))   # per-batch size
PROCESS_ALL_BATCHES = os.getenv("PROCESS_ALL_BATCHES", "true").lower() in ("1","true","yes")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR_MORNING = int(os.getenv("DAILY_DIGEST_HOUR_MORNING", "5"))
DAILY_DIGEST_HOUR_EVENING = int(os.getenv("DAILY_DIGEST_HOUR_EVENING", "18"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")
RECENT_DAYS = int(os.getenv("RECENT_DAYS", "7"))
UPCOMING_DAYS = int(os.getenv("UPCOMING_DAYS", "7"))

CACHE_DIR = ".cache"
SEEN_FILE = os.path.join(CACHE_DIR, "seen.json")
MORNING_SNAPSHOT_FILE = os.path.join(CACHE_DIR, "morning_snapshot.json")

# ---------------- Keyword groups / patterns ----------------
AI_INTENT_KEYPHRASES = [
    r"\binvest(s|ed|ing)?\b.*\b(ai|artificial intelligence|generative ai|ml|machine learning)\b",
    r"\bcommit(s|ted)?\b.*\b(ai|artificial intelligence)\b",
    r"\bfund(s|ed|ing)?\b.*\b(ai|artificial intelligence)\b",
    r"\b(raises|raised)\b.*\bfor\b.*\b(ai|artificial intelligence)\b",
    r"\b(acquires?|acquired|acquisition of)\b.*\b(ai startup|ai company|ai firm)\b",
    r"\b(orders|orders? of|purchases?|buys?)\b.*\b(gpu|gpus|a100|h100|accelerator|tensor core)\b",
    r"\b(opens|opening|launches|announces)\b.*\b(ai lab|research lab|ai center|ai initiative|ai program)\b",
    r"\b(partner(s)? with|partners with|partners? to)\b.*\b(OpenAI|Anthropic|NVIDIA|Cohere|Meta|Google Cloud|AWS|Microsoft)\b",
    r"\b(integrat(es|ed|ing)?|powered by)\b.*\b(gpt|llm|large language model|openai|anthropic|gpt-4|gpt-4o)\b",
    r"\b(build(s|ing)?|develop(s|ing)?|deploy(s|ing)?)\b.*\b(large language model|llm|generative model|ai model)\b",
]
PRODUCT_KEYWORDS = ["launch","launches","unveil","introduce","introduces","new product","releases","announces new","unveils"]
SCANDAL_KEYWORDS = ["scandal","allegation","fraud","lawsuit","investigation","probe","charged","indicted","recall"]
DEAL_KEYWORDS = ["partnership","partners with","signs deal","strategic partnership","contract worth","agreement with","signed a deal"]
MNA_KEYWORDS = ["acquir","acquisition","merger","takeover","s-4","will acquire","to buy","agrees to buy"]
EARNINGS_KEYWORDS = ["earnings","quarterly results","eps","revenue","beats","misses"]

# ---------------- per-run stats & caches ----------------
PER_TICKER_STATS = {"json_401": 0, "json_403": 0, "page_404": 0, "page_other_errors": 0, "json_other": 0}
PER_TICKER_CACHE_FAIL = {}  # ticker -> short reason for skipping further attempts this run

# ---------------- Helpers ----------------
def ensure_cache_dir():
    try:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception as e:
        print("Warning: could not create cache dir:", e)

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

def safe_get_json(url, timeout=15):
    """
    Improved safe JSON fetch that records 401/403/404 and other statuses in PER_TICKER_STATS,
    returns None on error so callers will fall back to HTML scraping.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = requests.get(url, timeout=timeout, headers=headers)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as he:
        status = None
        try:
            status = he.response.status_code
        except Exception:
            pass
        if status == 401:
            PER_TICKER_STATS["json_401"] += 1
        elif status == 403:
            PER_TICKER_STATS["json_403"] += 1
        else:
            PER_TICKER_STATS["json_other"] += 1
        print(f"[HTTP JSON] GET error for {url}: {he} (status={status})")
        return None
    except Exception as e:
        PER_TICKER_STATS["json_other"] += 1
        print(f"[HTTP JSON] GET error for {url}: {e}")
        return None

def load_json_set(path):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
                elif isinstance(data, dict):
                    return set(data.keys())
        return set()
    except Exception as e:
        print("Error loading json set:", path, e)
        return set()

def save_json_set(path, s):
    try:
        ensure_cache_dir()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(s), f)
        os.replace(tmp, path)
    except Exception as e:
        print("Error saving json set:", path, e)

# ---------------- Date parsing & recency ----------------
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

# ---------------- Indices ----------------
def get_sp500_list():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "constituents"}) or soup.find("table", class_="wikitable")
    out = []
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all(["td","th"])
        if len(cols) >= 2:
            ticker = cols[0].get_text(strip=True); name = cols[1].get_text(strip=True)
            out.append((ticker.replace(".", "-"), name))
    print(f"Fetched S&P500: {len(out)}")
    return out

def get_nasdaq100_list():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    out = []
    for table in soup.find_all("table", class_="wikitable"):
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all(["td","th"])
            if len(tds) >= 2:
                a = tds[0].get_text(strip=True); b = tds[1].get_text(strip=True)
                if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", b):
                    name=a; ticker=b
                elif re.fullmatch(r"[A-Z0-9\.\-]{1,10}", a):
                    name=b; ticker=a
                else:
                    continue
                out.append((ticker.replace(".","-"), name))
    print(f"Fetched NASDAQ-100: {len(out)}")
    return out

# ---------------- Build feed query & poll ----------------
def build_google_news_rss(ticker, name):
    company_phrase = f'"{name}"'
    q_terms = ["earnings","launch","product","partnership","invest","ai","gpu","acquire","merger","lawsuit","investigation"]
    keywords_or = " OR ".join(q_terms)
    query = f"({ticker} OR {company_phrase}) ({keywords_or})"
    encoded = urllib.parse.quote(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

def poll_feed(url):
    try:
        parsed = feedparser.parse(url)
        return parsed.entries
    except Exception as e:
        print("feedparser error for", url, e)
        return []

# ---------------- Yahoo calendar (global) ----------------
def fetch_yahoo_earnings_for_date_json(date_iso):
    url = f"https://query1.finance.yahoo.com/v7/finance/calendar/earnings?day={date_iso}"
    data = safe_get_json(url)
    if not data:
        return []
    results = []
    try:
        calendar = data.get("calendar") or {}
        if isinstance(calendar, dict) and "result" in calendar:
            res = calendar.get("result")
            if isinstance(res, list):
                for item in res:
                    maybe = item.get("earnings") or item.get("earningsDate") or []
                    if isinstance(maybe, list):
                        for e in maybe:
                            sym = e.get("symbol") or e.get("ticker") or e.get("shortName") or ""
                            name = e.get("shortName") or e.get("company") or ""
                            time_of_day = e.get("time", "") or e.get("timeOfDay","")
                            title = f"Earnings scheduled: {sym} ({name}) {time_of_day}".strip()
                            results.append({"title": title, "link": url, "published": date_iso, "ticker": sym, "company": name})
        if not results:
            earnings = data.get("earnings") or {}
            res = earnings.get("result") or earnings.get("calendar") or None
            if isinstance(res, list):
                for e in res:
                    sym = e.get("symbol") or e.get("ticker") or ""
                    name = e.get("shortName") or e.get("company") or ""
                    time_of_day = e.get("time", "") or e.get("timeOfDay","")
                    title = f"Earnings scheduled: {sym} ({name}) {time_of_day}".strip()
                    results.append({"title": title, "link": url, "published": date_iso, "ticker": sym, "company": name})
        if not results:
            def walk_find(obj):
                found = []
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        if isinstance(v, list):
                            for item in v:
                                if isinstance(item, dict) and ("symbol" in item or "time" in item or "shortName" in item):
                                    sym = item.get("symbol") or item.get("ticker") or ""
                                    name = item.get("shortName") or item.get("company") or ""
                                    time_of_day = item.get("time", "") or item.get("timeOfDay","")
                                    title = f"Earnings scheduled: {sym} ({name}) {time_of_day}".strip()
                                    found.append({"title": title, "link": url, "published": date_iso, "ticker": sym, "company": name})
                        elif isinstance(v, dict):
                            found.extend(walk_find(v))
                return found
            results = walk_find(data)
    except Exception as e:
        print("Yahoo JSON parse error:", e)
    seen_local = set(); uniq = []
    for it in results:
        key = (it.get("ticker","").upper(), it.get("published",""))
        if key not in seen_local:
            seen_local.add(key); uniq.append(it)
    return uniq

def fetch_yahoo_earnings_for_date_html(date_iso):
    url = f"https://finance.yahoo.com/calendar/earnings?day={date_iso}"
    html = safe_get(url)
    items = []
    if not html:
        return items
    try:
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("table tbody tr")
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 6:
                ticker = tds[0].get_text(strip=True)
                name = tds[1].get_text(strip=True)
                time_of_day = tds[4].get_text(strip=True)
                title = f"Earnings scheduled: {ticker} ({name}) {time_of_day}"
                items.append({"title": title, "link": url, "published": date_iso, "ticker": ticker, "company": name})
    except Exception as e:
        print("Yahoo HTML parse failed:", e)
    return items

def fetch_upcoming_earnings(days=UPCOMING_DAYS):
    tz = pytz.timezone(TIMEZONE)
    out = []
    for d in range(days):
        day = (datetime.now(tz) + timedelta(days=d)).strftime("%Y-%m-%d")
        items = fetch_yahoo_earnings_for_date_json(day)
        if not items:
            items = fetch_yahoo_earnings_for_date_html(day)
        if items:
            out.extend(items)
        time.sleep(0.12)
    seen_local = set(); uniq = []
    for it in out:
        key = (it.get("ticker","").upper(), it.get("published",""))
        if key not in seen_local:
            seen_local.add(key); uniq.append(it)
    return uniq

# ---------------- Per-ticker earnings (robust JSON -> HTML scraping fallback) ----------------
def _parse_earnings_text_to_datetime(text):
    if not text:
        return None
    text = re.sub(r"\b(after|before) (market )?(close|open)\b", "", text, flags=re.IGNORECASE).strip()
    m_iso = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if m_iso:
        try:
            dt = datetime.fromisoformat(m_iso.group(1))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return dt.astimezone(pytz.UTC)
        except Exception:
            pass
    m = re.search(r"([A-Za-z]+ \d{1,2}, 20\d{2})", text)
    if m:
        for fmt in ("%B %d, %Y","%b %d, %Y"):
            try:
                dt = datetime.strptime(m.group(1), fmt)
                return dt.replace(tzinfo=pytz.UTC)
            except Exception:
                pass
    m2 = re.search(r"([A-Za-z]+) (\b20\d{2}\b)", text)
    if m2:
        mon = m2.group(1); yr = int(m2.group(2))
        for fmt in ("%B %d %Y","%b %d %Y"):
            try:
                dt = datetime.strptime(f"{mon} 1 {yr}", fmt)
                return dt.replace(tzinfo=pytz.UTC)
            except Exception:
                pass
    return None

def _find_date_in_text_blob(text_blob):
    if not text_blob:
        return None
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", text_blob)
    if m:
        try:
            dt = datetime.fromisoformat(m.group(1))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
            return dt.astimezone(pytz.UTC)
        except Exception:
            pass
    m2 = re.search(r"([A-Z][a-z]+ \d{1,2}, 20\d{2})", text_blob)
    if m2:
        try:
            dt = datetime.strptime(m2.group(1), "%B %d, %Y")
            return dt.replace(tzinfo=pytz.UTC)
        except Exception:
            pass
    m3 = re.search(r"([A-Z][a-z]{2,8} 20\d{2})", text_blob)
    if m3:
        for fmt in ("%B %Y","%b %Y"):
            try:
                dt = datetime.strptime(m3.group(1), fmt)
                return dt.replace(tzinfo=pytz.UTC)
            except Exception:
                pass
    return None

def fetch_earnings_for_ticker_yahoo(ticker):
    ticker = (ticker or "").upper()
    if ticker in PER_TICKER_CACHE_FAIL:
        return None

    # Try JSON endpoint first (may be 401/403)
    json_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=calendarEvents"
    j = safe_get_json(json_url)
    if j:
        try:
            q = j.get("quoteSummary", {}).get("result")
            if isinstance(q, list) and q:
                ev = q[0].get("calendarEvents", {}).get("earnings", {})
                ed = ev.get("earningsDate")
                shortName = q[0].get("shortName") or q[0].get("symbol") or ticker
                ts = None
                if ed:
                    if isinstance(ed, list) and ed and isinstance(ed[0], dict) and "raw" in ed[0]:
                        ts = int(ed[0]["raw"])
                    elif isinstance(ed, dict) and "raw" in ed:
                        ts = int(ed["raw"])
                if ts:
                    dt = datetime.fromtimestamp(ts, pytz.UTC)
                    return {"ticker": ticker, "company": shortName, "earnings_dt": dt}
        except Exception as e:
            print(f"[per-ticker json parse error {ticker}]: {e}")

    # JSON failed or returned nothing -> scrape quote page
    quote_url = f"https://finance.yahoo.com/quote/{ticker}"
    try:
        r = requests.get(quote_url, timeout=15, headers={"User-Agent": USER_AGENT})
        if r.status_code == 404:
            PER_TICKER_STATS["page_404"] += 1
            PER_TICKER_CACHE_FAIL[ticker] = "404"
            print(f"[HTTP] GET 404 for {quote_url} (ticker likely invalid/delisted). Skipping.")
            return None
        if r.status_code >= 400:
            PER_TICKER_STATS["page_other_errors"] += 1
            PER_TICKER_CACHE_FAIL[ticker] = f"status_{r.status_code}"
            print(f"[HTTP] GET error for {quote_url}: {r.status_code} {r.reason}")
            return None
        html = r.text
    except Exception as e:
        PER_TICKER_STATS["page_other_errors"] += 1
        PER_TICKER_CACHE_FAIL[ticker] = "request_exception"
        print(f"[HTTP] GET error for {quote_url}: {e}")
        return None

    soup = BeautifulSoup(html, "lxml")

    # Heuristic: find 'Earnings Date' label and parse nearby text
    try:
        label_nodes = soup.find_all(string=re.compile(r"\bEarnings Date\b", flags=re.IGNORECASE))
        for node in label_nodes:
            parent = node.parent
            try:
                sib = parent.find_next_sibling()
                if sib:
                    cand = sib.get_text(" ", strip=True)
                    dt = _parse_earnings_text_to_datetime(cand)
                    if dt:
                        return {"ticker": ticker, "company": None, "earnings_dt": dt}
                grand = parent.find_parent()
                if grand:
                    blob = grand.get_text(" ", strip=True)
                    dt = _find_date_in_text_blob(blob)
                    if dt:
                        return {"ticker": ticker, "company": None, "earnings_dt": dt}
            except Exception:
                pass
    except Exception as e:
        print(f"[scrape heuristic error {ticker}]:", e)

    # fallback: search entire page
    txt = soup.get_text(" ", strip=True)
    dt = _find_date_in_text_blob(txt)
    if dt:
        return {"ticker": ticker, "company": None, "earnings_dt": dt}

    # nothing found: cache minor failure for this run
    PER_TICKER_CACHE_FAIL[ticker] = "not_found"
    return None

# ---------------- AI detection & classification ----------------
def detect_ai_intent(title, summary):
    txt = (title + " " + (summary or "")).lower()
    for patt in AI_INTENT_KEYPHRASES:
        try:
            if re.search(patt, txt, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    infra_terms = ["orders gpus","purchases gpus","buys gpus","orders a100","orders h100","announces ai lab","opens ai lab","builds ai team"]
    for t in infra_terms:
        if t in txt:
            return True
    return False

def classify(title, summary):
    txt = (title + " " + (summary or "")).lower()
    if detect_ai_intent(title, summary):
        return "ai_special"
    if any(k in txt for k in PRODUCT_KEYWORDS):
        return "product_launch"
    if any(k in txt for k in SCANDAL_KEYWORDS):
        return "scandal"
    if any(k in txt for k in DEAL_KEYWORDS):
        return "major_deal"
    if any(k in txt for k in MNA_KEYWORDS):
        return "takeover"
    return "other"

def is_scandal_after_launch(title, summary):
    txt = (title + " " + (summary or "")).lower()
    patterns = [
        r"post-?launch", r"after (the )?launch", r"following (the )?(release|launch|unveil|announcement)",
        r"shortly after (the )?(release|launch|announcement|unveil)"
    ]
    return any(re.search(p, txt) for p in patterns)

# ---------------- Notify digest helpers ----------------
def notify_telegram_digest(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("[Telegram missing] would send digest length:", len(text))
        return

    CHUNK_SIZE = 3800
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    if len(text) <= CHUNK_SIZE:
        try:
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}, timeout=10)
        except Exception as e:
            print("Telegram send error:", e)
        return

    parts = []
    remaining = text
    while remaining:
        if len(remaining) <= CHUNK_SIZE:
            parts.append(remaining)
            break
        slice_ = remaining[:CHUNK_SIZE]
        idx = slice_.rfind("\n\n")
        if idx == -1:
            idx = slice_.rfind("\n")
        if idx == -1:
            idx = CHUNK_SIZE
        parts.append(remaining[:idx].rstrip())
        remaining = remaining[idx:].lstrip()

    for p in parts:
        try:
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": p, "disable_web_page_preview": True}, timeout=10)
            time.sleep(0.35)
        except Exception as e:
            print("Telegram send error (chunk):", e)

def send_email(subject, full_text):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO):
        print("Email not sent (SMTP not configured).")
        return

    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL_TO

        html_body = "<html><body><pre style='font-family:monospace;white-space:pre-wrap;'>" + \
                    (full_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")) + \
                    "</pre></body></html>"
        msg.set_content(full_text)
        msg.add_alternative(html_body, subtype="html")

        attachment_filename = "full_digest.txt"
        msg.add_attachment(full_text.encode("utf-8"), maintype="text", subtype="plain", filename=attachment_filename)

        print(f"Email payload prepared: subject='{subject}', body_length={len(full_text)}, attachment='{attachment_filename}'")

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print("Email send error:", e)

# ---------------- Utilities for batching ----------------
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# ---------------- Main ----------------
def main():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required.")
        return

    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    local_hour = now_local.hour

    # Manual-run detection & override
    github_event_name = os.getenv("GITHUB_EVENT_NAME", "")
    manual_mode_env = os.getenv("MANUAL_MODE", "")
    manual_run = (github_event_name == "workflow_dispatch")

    if manual_run:
        mm = (manual_mode_env or "").strip().lower()
        if mm == "evening":
            is_morning = False; is_evening = True
            print("Manual run requested: EVENING (delta) mode.")
        else:
            is_morning = True; is_evening = False
            print("Manual run requested: MORNING (full) mode.")
    else:
        is_morning = (local_hour == DAILY_DIGEST_HOUR_MORNING)
        is_evening = (local_hour == DAILY_DIGEST_HOUR_EVENING)
        print(f"Local hour {local_hour}. is_morning={is_morning} is_evening={is_evening}")

    ensure_cache_dir()
    seen = load_json_set(SEEN_FILE)
    morning_snapshot = load_json_set(MORNING_SNAPSHOT_FILE)

    # Build universe once
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
    print(f"Universe total tickers: {total}")

    # 1) calendar-based upcoming earnings
    upcoming = fetch_upcoming_earnings(UPCOMING_DAYS)
    print(f"DEBUG: calendar-based upcoming earnings fetched: {len(upcoming)} items.")

    # 2) aggressive per-ticker supplement
    per_ticker_upcoming = []
    existing_tickers = set((it.get("ticker") or "").upper() for it in upcoming if it.get("ticker"))
    need_full_check = (len(upcoming) < max(3, min(20, len(universe)//30)))
    if need_full_check:
        print(f"Calendar returned {len(upcoming)} items — performing FULL per-ticker earnings checks across {len(universe)} tickers")
    else:
        print(f"Calendar returned {len(upcoming)} items — performing selective per-ticker checks for tickers not present in calendar")

    checked_cnt = 0
    found_cnt = 0
    now_utc = datetime.now(pytz.UTC)
    for ticker, name in universe:
        tk = ticker.upper()
        if not need_full_check and tk in existing_tickers:
            continue
        res = fetch_earnings_for_ticker_yahoo(tk)
        checked_cnt += 1
        if res and "earnings_dt" in res:
            edt = res["earnings_dt"]
            delta_days = (edt - now_utc).total_seconds() / 86400.0
            if 0 <= delta_days <= UPCOMING_DAYS:
                published_iso = edt.astimezone(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
                title = f"Earnings scheduled: {res['ticker']} ({res.get('company','')}) {published_iso}"
                per_ticker_upcoming.append({
                    "title": title,
                    "link": f"https://finance.yahoo.com/quote/{tk}/calendar?p={tk}",
                    "published": published_iso,
                    "ticker": tk,
                    "company": res.get("company") or name
                })
                found_cnt += 1
        time.sleep(0.12)  # polite throttle

    # Print per-ticker stats for debugging
    print(f"Per-ticker checks completed: checked={checked_cnt}, found={found_cnt}")
    print("Per-ticker HTTP stats:", PER_TICKER_STATS)
    print("Per-ticker cache fail examples (first 10):", list(PER_TICKER_CACHE_FAIL.items())[:10])

    # merge & dedupe upcoming
    all_upcoming = upcoming + per_ticker_upcoming
    dedup_upcoming = []
    seen_u = set()
    for it in all_upcoming:
        key = ((it.get("ticker") or "").upper(), it.get("published",""))
        if key not in seen_u:
            seen_u.add(key)
            dedup_upcoming.append(it)
    upcoming = dedup_upcoming
    print(f"DEBUG: total upcoming earnings after per-ticker supplement: {len(upcoming)} items. Sample: {upcoming[:6]}")

    # If not digest hour and not manual: exit early
    if not (is_morning or is_evening):
        print(f"Not a digest hour and not manual. Exiting. local_hour={local_hour}")
        save_json_set(SEEN_FILE, seen)
        return

    # Prepare categories
    categories = {
        "upcoming_earnings": [],
        "ai_special": [],
        "product_launch": [],
        "scandal_after_launch": [],
        "major_deal": [],
        "takeover": []
    }
    added_this_run = set()

    # include upcoming earnings in morning only (do NOT mark seen)
    if is_morning:
        for it in upcoming:
            categories["upcoming_earnings"].append({
                "title": it.get("title",""),
                "link": it.get("link",""),
                "published": it.get("published",""),
                "ticker": it.get("ticker"),
                "company": it.get("company")
            })

    # Process universe in batches
    batches = list(chunk_list(universe, MAX_TICKERS))
    print(f"Processing {len(batches)} batch(es) of up to {MAX_TICKERS} tickers (PROCESS_ALL_BATCHES={PROCESS_ALL_BATCHES})")
    if not PROCESS_ALL_BATCHES and batches:
        batches = [batches[0]]

    for batch_index, batch in enumerate(batches, start=1):
        print(f"Batch {batch_index}/{len(batches)} — tickers in this batch: {len(batch)}")
        feeds = [(t, n, build_google_news_rss(t, n)) for (t, n) in batch]
        for ticker, cname, rss in feeds:
            entries = poll_feed(rss)
            if not entries:
                time.sleep(THROTTLE_SECONDS)
                continue
            for entry in entries:
                if not is_recent_entry(entry, tz, RECENT_DAYS):
                    continue
                title = entry.get("title","") or ""
                link = entry.get("link","") or ""
                summary = entry.get("summary","") or entry.get("description","") or ""
                published = entry.get("published") or entry.get("updated") or ""
                fp = fingerprint(title, link, published)
                if fp in seen:
                    continue
                label = classify(title, summary)
                if label == "ai_special":
                    seen.add(fp); added_this_run.add(fp)
                    categories["ai_special"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published})
                    continue
                if label == "product_launch":
                    seen.add(fp); added_this_run.add(fp)
                    categories["product_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published})
                    continue
                if label == "scandal" and is_scandal_after_launch(title, summary):
                    seen.add(fp); added_this_run.add(fp)
                    categories["scandal_after_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published})
                    continue
                if label == "major_deal":
                    seen.add(fp); added_this_run.add(fp)
                    categories["major_deal"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published})
                    continue
                if label == "takeover":
                    seen.add(fp); added_this_run.add(fp)
                    categories["takeover"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": published})
                    continue
            time.sleep(THROTTLE_SECONDS)
        if batch_index < len(batches):
            print(f"Completed batch {batch_index}; sleeping briefly before next batch...")
            time.sleep(1.2)

    # Build & send digests
    if is_morning:
        total_new = sum(len(v) for v in categories.values())
        if total_new == 0:
            print("Morning: no new items to send.")
        else:
            header = f"📊 Morning Focused Digest — {now_local.strftime('%Y-%m-%d %H:%M %Z')}\nProcessed {total} tickers across {len(batches)} batch(es).\nNew items: {total_new}\n\n"
            parts = [header]
            if categories["upcoming_earnings"]:
                parts.append(f"💰 Upcoming earnings (next {UPCOMING_DAYS} days)\n")
                for it in categories["upcoming_earnings"][:400]:
                    ticker_str = (it.get("ticker") or "").strip()
                    parts.append(f"• {ticker_str} {it['title']}\n" if ticker_str else f"• {it['title']}\n")
                parts.append("\n")
            if categories["ai_special"]:
                parts.append("🧠 AI Intentional Investments / Partnerships\n")
                for it in categories["ai_special"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["product_launch"]:
                parts.append("🚀 Product Launches\n")
                for it in categories["product_launch"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["scandal_after_launch"]:
                parts.append("⚠️ Scandals linked to product launches/events\n")
                for it in categories["scandal_after_launch"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["major_deal"]:
                parts.append("🤝 Major Deals / Partnerships\n")
                for it in categories["major_deal"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["takeover"]:
                parts.append("🏢 M&A / Takeovers\n")
                for it in categories["takeover"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            message = "".join(parts)
            notify_telegram_digest(message)
            if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
                send_email("Morning Focused Market Digest", message)
        save_json_set(MORNING_SNAPSHOT_FILE, seen)
        print("Morning snapshot saved.")

    elif is_evening:
        if not morning_snapshot:
            delta_fps = added_this_run.copy()
        else:
            delta_fps = {fp for fp in added_this_run if fp not in morning_snapshot}
        delta_categories = {k: [] for k in categories.keys()}
        def item_fp(it):
            return fingerprint(it.get("title",""), it.get("link",""), it.get("published","") or "")
        for cat, items in categories.items():
            for it in items:
                if item_fp(it) in delta_fps:
                    delta_categories[cat].append(it)
        total_delta = sum(len(v) for v in delta_categories.values())
        if total_delta == 0:
            print("Evening: no new delta items since morning snapshot. No message sent.")
        else:
            header = f"📊 Evening Delta Digest — {now_local.strftime('%Y-%m-%d %H:%M %Z')}\nProcessed {total} tickers across {len(batches)} batch(es).\nNew since morning: {total_delta}\n\n"
            parts = [header]
            if delta_categories["upcoming_earnings"]:
                parts.append(f"💰 Upcoming earnings (next {UPCOMING_DAYS} days)\n")
                for it in delta_categories["upcoming_earnings"][:400]:
                    parts.append(f"• {it['title']}\n")
                parts.append("\n")
            if delta_categories["ai_special"]:
                parts.append("🧠 AI Intentional Investments / Partnerships (new)\n")
                for it in delta_categories["ai_special"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["product_launch"]:
                parts.append("🚀 Product Launches (new)\n")
                for it in delta_categories["product_launch"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["scandal_after_launch"]:
                parts.append("⚠️ Scandals linked to launches/events (new)\n")
                for it in delta_categories["scandal_after_launch"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["major_deal"]:
                parts.append("🤝 Major Deals / Partnerships (new)\n")
                for it in delta_categories["major_deal"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["takeover"]:
                parts.append("🏢 M&A / Takeovers (new)\n")
                for it in delta_categories["takeover"][:400]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            message = "".join(parts)
            notify_telegram_digest(message)
            if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
                send_email("Evening Delta Market Digest", message)
        save_json_set(MORNING_SNAPSHOT_FILE, seen)
        print("Morning snapshot updated after evening run.")

    save_json_set(SEEN_FILE, seen)
    print(f"Saved seen fingerprints: {len(seen)}")
    print("Done.")

if __name__ == "__main__":
    main()
