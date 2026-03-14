#!/usr/bin/env python3
"""
main_improved_action.py

Twice-daily digest:
 - Morning 05:00 Europe/Paris -> sends full digest
 - Evening 18:00 Europe/Paris -> sends only delta since morning
 - Rotation over S&P500 + NASDAQ-100 via GITHUB_RUN_NUMBER
 - Persistent seen fingerprints in .cache/seen.json (actions/cache)
 - Morning snapshot in .cache/morning_snapshot.json for delta calculation
 - Improved AI detection for intentional AI investments/initiatives
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

# ---------------- Config ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")

MAX_TICKERS = int(os.getenv("MAX_TICKERS", "200"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR_MORNING = int(os.getenv("DAILY_DIGEST_HOUR_MORNING", "5"))   # local hour for morning digest
DAILY_DIGEST_HOUR_EVENING = int(os.getenv("DAILY_DIGEST_HOUR_EVENING", "18"))  # local hour for evening digest
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")
RECENT_DAYS = int(os.getenv("RECENT_DAYS", "7"))    # window for news scraping
UPCOMING_DAYS = int(os.getenv("UPCOMING_DAYS", "7"))# upcoming earnings window

CACHE_DIR = ".cache"
SEEN_FILE = os.path.join(CACHE_DIR, "seen.json")
MORNING_SNAPSHOT_FILE = os.path.join(CACHE_DIR, "morning_snapshot.json")

# Keyword groups
AI_INTENT_KEYPHRASES = [
    # investment / infrastructure / strategic moves
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

def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)

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

# ---------------- Date parsing & recency filter ----------------
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

# ---------------- Build feed query ----------------
def build_google_news_rss(ticker, name):
    company_phrase = f'"{name}"'
    keywords = AI_INTENT_KEYPHRASES + PRODUCT_KEYWORDS + SCANDAL_KEYWORDS + DEAL_KEYWORDS + MNA_KEYWORDS + EARNINGS_KEYWORDS
    # flatten keyphrases to safe tokens (keywords may include regex-like strings; for query we will use a simple OR of common terms)
    # use a conservative set for query building to avoid failing encoding
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

# ---------------- Classification (improved AI intent detection) ----------------
def detect_ai_intent(title, summary):
    txt = (title + " " + (summary or "")).lower()
    # test each intent regex
    for patt in AI_INTENT_KEYPHRASES:
        if re.search(patt, txt, flags=re.IGNORECASE):
            return True
    # also check for supplier partnerships known to be AI infra-related
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
    # earnings are handled by Yahoo upcoming (not via feed)
    return "other"

def is_scandal_after_launch(title, summary):
    txt = (title + " " + (summary or "")).lower()
    patterns = [
        r"post-?launch", r"after (the )?launch", r"following (the )?(release|launch|unveil|announcement)",
        r"shortly after (the )?(release|launch|announcement|unveil)"
    ]
    return any(re.search(p, txt) for p in patterns)

# ---------------- Yahoo earnings (upcoming window) ----------------
def fetch_yahoo_earnings_for_date(date_iso):
    url = f"https://finance.yahoo.com/calendar/earnings?day={date_iso}"
    html = safe_get(url)
    items = []
    if not html:
        return items
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tbody tr")
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) >= 6:
            ticker = tds[0].get_text(strip=True)
            name = tds[1].get_text(strip=True)
            eps_est = tds[2].get_text(strip=True)
            time_of_day = tds[4].get_text(strip=True)
            title = f"Earnings scheduled: {ticker} ({name}) {time_of_day}"
            items.append({"title": title, "link": url, "published": date_iso})
    return items

def fetch_upcoming_earnings(days=UPCOMING_DAYS):
    tz = pytz.timezone(TIMEZONE)
    out = []
    for d in range(days):
        day = (datetime.now(tz) + timedelta(days=d)).strftime("%Y-%m-%d")
        try:
            out += fetch_yahoo_earnings_for_date(day)
            time.sleep(0.2)
        except Exception:
            pass
    seen_local = set()
    uniq = []
    for it in out:
        key = (it["title"], it["link"])
        if key not in seen_local:
            seen_local.add(key); uniq.append(it)
    return uniq

# ---------------- Notify digest ----------------
def notify_telegram_digest(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("[Telegram missing] would send digest length:", len(text))
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

# ---------------- Main ----------------
def main():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required.")
        return

    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    local_hour = now_local.hour

    # For DST robustness: workflow will run multiple UTC times; we only act on the two local hours below
    is_morning = (local_hour == DAILY_DIGEST_HOUR_MORNING)
    is_evening = (local_hour == DAILY_DIGEST_HOUR_EVENING)

    # If run outside the two target hours, just update seen cache (no send) to keep cache fresh and exit.
    # This allows workflow to run multiple UTC times for DST alignment.
    # But we will still run collection only at target hours to save work.
    # Load seen and morning snapshot
    ensure_cache_dir()
    seen = load_json_set(SEEN_FILE)
    morning_snapshot = load_json_set(MORNING_SNAPSHOT_FILE)

    # Fetch upcoming earnings always (no rotation) — these are high-value, actionable
    upcoming = fetch_upcoming_earnings(UPCOMING_DAYS)

    # If not morning or evening: exit early (no heavy collection)
    if not (is_morning or is_evening):
        print(f"Local hour {local_hour} — not a digest hour ({DAILY_DIGEST_HOUR_MORNING}/{DAILY_DIGEST_HOUR_EVENING}). Exiting after ensuring cache exists.")
        # save seen back (no changes likely)
        save_json_set(SEEN_FILE, seen)
        return

    # At digest hours, fetch rotated subset
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
    run_index = int(run_num) if run_num and run_num.isdigit() else int(time.time() // (60*30))
    chunk_size = max(1, min(MAX_TICKERS, total))
    offset = (run_index * chunk_size) % total

    def slice_window(lst, off, size):
        if size >= len(lst): return lst
        end = off + size
        if end <= len(lst): return lst[off:end]
        return lst[off:len(lst)] + lst[0:end - len(lst)]

    selected = slice_window(universe, offset, chunk_size)
    print(f"Universe size: {total}, processing {len(selected)} tickers (offset {offset}).")

    feeds = [(t, n, build_google_news_rss(t, n)) for (t, n) in selected]

    # categories
    categories = {
        "upcoming_earnings": [],
        "ai_special": [],
        "product_launch": [],
        "scandal_after_launch": [],
        "major_deal": [],
        "takeover": []
    }

    added_this_run = set()  # fingerprints added in this run

    # include upcoming earnings first — treat them as new if not seen
    for it in upcoming:
        title = it["title"]; link = it["link"]; published = it["published"]
        fp = fingerprint(title, link, published)
        if fp in seen:
            continue
        seen.add(fp); added_this_run.add(fp)
        categories["upcoming_earnings"].append({"title": title, "link": link, "published": published})

    # collect from feeds
    for ticker, cname, rss in feeds:
        entries = poll_feed(rss)
        if not entries:
            time.sleep(THROTTLE_SECONDS); continue
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
            # AI first
            if label == "ai_special":
                seen.add(fp); added_this_run.add(fp)
                categories["ai_special"].append({"ticker": ticker, "company": cname, "title": title, "link": link})
                continue
            # product launch
            if label == "product_launch":
                seen.add(fp); added_this_run.add(fp)
                categories["product_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link})
                continue
            # scandals that explicitly mention post-launch
            if label == "scandal" and is_scandal_after_launch(title, summary):
                seen.add(fp); added_this_run.add(fp)
                categories["scandal_after_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link})
                continue
            # major deals
            if label == "major_deal":
                seen.add(fp); added_this_run.add(fp)
                categories["major_deal"].append({"ticker": ticker, "company": cname, "title": title, "link": link})
                continue
            # takeover / m&a
            if label == "takeover":
                seen.add(fp); added_this_run.add(fp)
                categories["takeover"].append({"ticker": ticker, "company": cname, "title": title, "link": link})
                continue
        time.sleep(THROTTLE_SECONDS)

    # Now decide what to send:
    # - Morning (05:00): send full digest of items we added_this_run (and upcoming earnings)
    # - Evening (18:00): send only items that are new since the morning snapshot (i.e., added_this_run ∖ morning_snapshot)
    #   If morning_snapshot missing, treat evening as "send new items since last run" (best-effort)
    message = None
    if is_morning:
        # Full morning digest: everything added_this_run
        total_new = sum(len(v) for v in categories.values())
        if total_new == 0:
            print("Morning: no new items to send.")
        else:
            header = f"📊 Morning Focused Digest — {now_local.strftime('%Y-%m-%d %H:%M %Z')}\nProcessed {len(selected)} tickers.\nNew items: {total_new}\n\n"
            parts = [header]
            # order sections
            if categories["upcoming_earnings"]:
                parts.append(f"💰 Upcoming earnings (next {UPCOMING_DAYS} days)\n")
                for it in categories["upcoming_earnings"][:50]:
                    parts.append(f"• {it['title']}\n")
                parts.append("\n")
            if categories["ai_special"]:
                parts.append("🧠 AI Intentional Investments / Partnerships\n")
                for it in categories["ai_special"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["product_launch"]:
                parts.append("🚀 Product Launches\n")
                for it in categories["product_launch"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["scandal_after_launch"]:
                parts.append("⚠️ Scandals linked to product launches/events\n")
                for it in categories["scandal_after_launch"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["major_deal"]:
                parts.append("🤝 Major Deals / Partnerships\n")
                for it in categories["major_deal"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if categories["takeover"]:
                parts.append("🏢 M&A / Takeovers\n")
                for it in categories["takeover"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            message = "".join(parts)
            if len(message) > 3800:
                message = message[:3800] + "\n\n(Truncated — full digest emailed if configured.)"
            # After sending morning digest, update morning snapshot to current seen (so evening compares to this)
            notify_telegram_digest(message)
            if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
                send_email("Morning Focused Market Digest", "<pre>" + message + "</pre>")
            # update morning snapshot file
            save_json_set(MORNING_SNAPSHOT_FILE, seen)
            print("Morning snapshot saved.")

    elif is_evening:
        # Evening: compute delta = added_this_run - morning_snapshot
        if not morning_snapshot:
            # no snapshot available (maybe morning job failed); treat evening like normal: send items added_this_run only
            delta_fps = added_this_run.copy()
        else:
            delta_fps = {fp for fp in added_this_run if fp not in morning_snapshot}
        # Build categories filtered by delta_fps
        delta_categories = {k: [] for k in categories.keys()}
        # Check each category item and include only if its fingerprint in delta_fps
        def item_fp(it):
            return fingerprint(it.get("title",""), it.get("link",""), it.get("published","") or "")
        for cat, items in categories.items():
            for it in items:
                fp_val = item_fp(it)
                if fp_val in delta_fps:
                    delta_categories[cat].append(it)
        total_delta = sum(len(v) for v in delta_categories.values())
        if total_delta == 0:
            print("Evening: no new delta items since morning snapshot. No message sent.")
        else:
            header = f"📊 Evening Delta Digest — {now_local.strftime('%Y-%m-%d %H:%M %Z')}\nProcessed {len(selected)} tickers.\nNew since morning: {total_delta}\n\n"
            parts = [header]
            if delta_categories["upcoming_earnings"]:
                parts.append(f"💰 Upcoming earnings (next {UPCOMING_DAYS} days)\n")
                for it in delta_categories["upcoming_earnings"][:50]:
                    parts.append(f"• {it['title']}\n")
                parts.append("\n")
            if delta_categories["ai_special"]:
                parts.append("🧠 AI Intentional Investments / Partnerships (new)\n")
                for it in delta_categories["ai_special"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["product_launch"]:
                parts.append("🚀 Product Launches (new)\n")
                for it in delta_categories["product_launch"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["scandal_after_launch"]:
                parts.append("⚠️ Scandals linked to launches/events (new)\n")
                for it in delta_categories["scandal_after_launch"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["major_deal"]:
                parts.append("🤝 Major Deals / Partnerships (new)\n")
                for it in delta_categories["major_deal"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            if delta_categories["takeover"]:
                parts.append("🏢 M&A / Takeovers (new)\n")
                for it in delta_categories["takeover"][:20]:
                    parts.append(f"• {it['ticker']} — {it['title']}\n")
                parts.append("\n")
            message = "".join(parts)
            if len(message) > 3800:
                message = message[:3800] + "\n\n(Truncated — full digest emailed if configured.)"
            notify_telegram_digest(message)
            if SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO:
                send_email("Evening Delta Market Digest", "<pre>" + message + "</pre>")
            # update morning snapshot to current seen so next evening compares to next morning
            save_json_set(MORNING_SNAPSHOT_FILE, seen)
            print("Morning snapshot updated after evening delta send.")

    # Persist seen (always)
    save_json_set(SEEN_FILE, seen)
    print(f"Saved seen fingerprints: {len(seen)}")
    print("Done.")

if __name__ == "__main__":
    main()
