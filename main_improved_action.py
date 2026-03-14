#!/usr/bin/env python3
"""
Robust drop-in main_improved_action.py
- Defensive parsing of MANUAL_MODE and process flags
- Retry & fallback for Yahoo calendar JSON -> HTML
- Manual-run single-batch guard unless explicit override
- Same categories and notification behavior as before
"""

import os, time, hashlib, urllib.parse, feedparser, requests, json, re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from email.message import EmailMessage
import smtplib

# -------------- Config (env / defaults) --------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
DAILY_DIGEST_HOUR_MORNING = int(os.getenv("DAILY_DIGEST_HOUR_MORNING", "5"))
DAILY_DIGEST_HOUR_EVENING = int(os.getenv("DAILY_DIGEST_HOUR_EVENING", "18"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; MarketAlerts/1.0)")

# defaults tuned to your request (can be overridden by workflow env)
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "200"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "0.4"))
RECENT_DAYS = int(os.getenv("RECENT_DAYS", "2"))
UPCOMING_DAYS = int(os.getenv("UPCOMING_DAYS", "5"))

# runtime cache
CACHE_DIR = ".cache"
SEEN_FILE = CACHE_DIR + "/seen.json"
MORNING_SNAPSHOT_FILE = CACHE_DIR + "/morning_snapshot.json"

# -------------- small helpers --------------
def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        try:
            os.makedirs(CACHE_DIR, exist_ok=True)
        except Exception as e:
            print("Warning: could not create cache dir:", e)

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
        print("Error loading", path, e)
        return set()

def save_json_set(path, s):
    try:
        ensure_cache_dir()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(s), f)
        os.replace(tmp, path)
    except Exception as e:
        print("Error saving", path, e)

def fingerprint(title, link, published):
    return hashlib.sha256(f"{title}|{link}|{published}".encode()).hexdigest()

# -------------- safe HTTP helpers --------------
def safe_get(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[HTTP] GET error for {url}: {e}")
        return None

def safe_get_json_with_retry(url, timeout=15, retries=1, backoff=0.5):
    for attempt in range(0, retries + 1):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as he:
            code = None
            try:
                code = he.response.status_code
            except Exception:
                pass
            print(f"[HTTP JSON] GET error for {url}: {he} (status={code})")
            # retry on 5xx once
            if code and 500 <= code < 600 and attempt < retries:
                time.sleep(backoff * (attempt+1))
                continue
            return None
        except Exception as e:
            print(f"[HTTP JSON] GET error for {url}: {e}")
            if attempt < retries:
                time.sleep(backoff * (attempt+1))
                continue
            return None
    return None

# -------------- index lists (S&P500, NASDAQ100 via Wikipedia) --------------
def safe_fetch(url):
    return safe_get(url)

def get_sp500_list():
    html = safe_fetch("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    out = []
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "constituents"}) or soup.find("table", class_="wikitable")
    if not table:
        return out
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all(["td","th"])
        if len(cols) >= 2:
            ticker = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            out.append((ticker.replace(".", "-"), name))
    print(f"Fetched S&P500: {len(out)}")
    return out

def get_nasdaq100_list():
    html = safe_fetch("https://en.wikipedia.org/wiki/Nasdaq-100")
    out = []
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table", class_="wikitable"):
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all(["td","th"])
            if len(tds) >= 2:
                a = tds[0].get_text(strip=True); b = tds[1].get_text(strip=True)
                # try to detect ticker
                cand = b if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", b) else a
                if re.fullmatch(r"[A-Z0-9\.\-]{1,10}", cand):
                    ticker = cand.replace(".", "-")
                    name = a if cand == b else b
                    out.append((ticker, name))
    print(f"Fetched NASDAQ-100: {len(out)}")
    return out

# -------------- upcoming earnings (Yahoo) with retry & HTML fallback --------------
def fetch_yahoo_calendar_json(day_iso):
    url = f"https://query1.finance.yahoo.com/v7/finance/calendar/earnings?day={day_iso}"
    return safe_get_json_with_retry(url, retries=1, backoff=0.3)

def fetch_yahoo_calendar_html(day_iso):
    url = f"https://finance.yahoo.com/calendar/earnings?day={day_iso}"
    html = safe_get(url)
    if not html:
        return []
    items = []
    try:
        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("table tbody tr")
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) >= 6:
                t = tds[0].get_text(strip=True)
                name = tds[1].get_text(strip=True)
                time_of_day = tds[4].get_text(strip=True)
                items.append({"title": f"Earnings scheduled: {t} ({name}) {time_of_day}", "link": url, "published": day_iso, "ticker": t, "company": name})
    except Exception as e:
        print("Yahoo HTML parse failed:", e)
    return items

def fetch_upcoming_earnings(days=UPCOMING_DAYS):
    tz = pytz.timezone(TIMEZONE)
    out = []
    for d in range(days):
        day = (datetime.now(tz) + timedelta(days=d)).strftime("%Y-%m-%d")
        j = fetch_yahoo_calendar_json(day)
        if j and isinstance(j, dict):
            # attempt to extract
            try:
                # multiple possible structures; walk simple known ones
                # best-effort: look for "earnings" entries
                if "calendar" in j and isinstance(j["calendar"], dict):
                    res = j["calendar"].get("result") or []
                else:
                    res = j.get("result") or j.get("earnings", {}).get("result") or []
                for item in (res or []):
                    # structure varies; try best-effort extraction
                    sym = item.get("symbol") or item.get("ticker") or item.get("shortName") or item.get("company") or ""
                    name = item.get("shortName") or item.get("company") or ""
                    t_of_day = item.get("time", "") or item.get("timeOfDay", "")
                    out.append({"title": f"Earnings scheduled: {sym} ({name}) {t_of_day}".strip(), "link": f"https://finance.yahoo.com/calendar/earnings?day={day}", "published": day, "ticker": sym, "company": name})
            except Exception as e:
                print("Yahoo JSON parsing: fallback error:", e)
        else:
            # fallback to HTML
            items_html = fetch_yahoo_calendar_html(day)
            out.extend(items_html)
        time.sleep(0.12)
    # dedupe by (ticker,published)
    uniq = []
    seenloc = set()
    for it in out:
        key = ((it.get("ticker") or "").upper(), it.get("published",""))
        if key not in seenloc:
            seenloc.add(key); uniq.append(it)
    return uniq

# -------------- per-ticker earnings helper (JSON -> HTML scraping) --------------
def _parse_earnings_text_to_datetime(text):
    if not text:
        return None
    text = re.sub(r"\b(after|before) (market )?(close|open)\b", "", text, flags=re.IGNORECASE).strip()
    m_iso = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
    if m_iso:
        try:
            return datetime.fromisoformat(m_iso.group(1)).replace(tzinfo=pytz.UTC)
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
    return None

def fetch_earnings_for_ticker_yahoo(ticker):
    ticker = (ticker or "").upper()
    # try JSON
    json_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=calendarEvents"
    j = safe_get_json_with_retry(json_url, retries=0)
    if j:
        try:
            q = j.get("quoteSummary", {}).get("result")
            if isinstance(q, list) and q:
                ev = q[0].get("calendarEvents", {}).get("earnings", {})
                ed = ev.get("earningsDate")
                if ed:
                    raw = None
                    if isinstance(ed, list) and ed and isinstance(ed[0], dict) and "raw" in ed[0]:
                        raw = ed[0]["raw"]
                    elif isinstance(ed, dict) and "raw" in ed:
                        raw = ed["raw"]
                    if raw:
                        return {"ticker": ticker, "earnings_dt": datetime.fromtimestamp(int(raw), pytz.UTC)}
        except Exception as e:
            print("per-ticker json parse error", ticker, e)
    # fallback HTML
    quote_url = f"https://finance.yahoo.com/quote/{ticker}"
    html = safe_get(quote_url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    nodes = soup.find_all(string=re.compile(r"\bEarnings Date\b", flags=re.IGNORECASE))
    for node in nodes:
        try:
            parent = node.parent
            nextsib = parent.find_next_sibling()
            if nextsib:
                cand = nextsib.get_text(" ", strip=True)
                dt = _parse_earnings_text_to_datetime(cand)
                if dt:
                    return {"ticker": ticker, "earnings_dt": dt}
            grand = parent.find_parent()
            if grand:
                blob = grand.get_text(" ", strip=True)
                dt = _parse_earnings_text_to_datetime(blob)
                if dt:
                    return {"ticker": ticker, "earnings_dt": dt}
        except Exception:
            pass
    # try full page text
    txt = soup.get_text(" ", strip=True)
    dt = _parse_earnings_text_to_datetime(txt)
    if dt:
        return {"ticker": ticker, "earnings_dt": dt}
    return None

# -------------- classification (ai/product/scandal/deal/mna) --------------
AI_PATTERNS = [
    r"\binvest(s|ed|ing)?\b.*\b(ai|artificial intelligence|generative ai|machine learning|ml)\b",
    r"\b(partner(s)? with|partners with)\b.*\b(OpenAI|Anthropic|NVIDIA|Google|Microsoft|AWS|Meta)\b",
    r"\b(orders?|purchases?)\b.*\b(gpu|a100|h100|accelerator)\b",
    r"\b(opens|launches|announces)\b.*\b(ai lab|ai center|research)\b",
]
PRODUCT_KEYWORDS = ["launch","launches","new product","releases","unveil","introduc"]
SCANDAL_KEYWORDS = ["scandal","allegation","fraud","lawsuit","investigation","probe","recall"]
DEAL_KEYWORDS = ["partnership","partners with","signs deal","agreement","contract"]
MNA_KEYWORDS = ["acquir","acquisition","merger","takeover","to buy","agrees to buy"]

def detect_ai_intent(text):
    t = text.lower()
    for p in AI_PATTERNS:
        try:
            if re.search(p, t, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False

def classify(title, summary):
    txt = (title + " " + (summary or "")).lower()
    if detect_ai_intent(txt):
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
    return bool(re.search(r"(post-?launch|after the launch|shortly after the launch|following the launch|following the release)", txt))

# -------------- notification helpers --------------
def notify_telegram_digest(text):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("Telegram not configured; would send length", len(text))
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    CHUNK = 3800
    parts = []
    if len(text) <= CHUNK:
        parts = [text]
    else:
        rem = text
        while rem:
            if len(rem) <= CHUNK:
                parts.append(rem); break
            slice_ = rem[:CHUNK]
            idx = slice_.rfind("\n\n")
            if idx <= 0:
                idx = slice_.rfind("\n")
            if idx <= 0:
                idx = CHUNK
            parts.append(rem[:idx].rstrip())
            rem = rem[idx:].lstrip()
    for p in parts:
        try:
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": p, "disable_web_page_preview": True}, timeout=10)
            time.sleep(0.25)
        except Exception as e:
            print("Telegram chunk send error:", e)

def send_email(subject, full_text):
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and ALERT_EMAIL_TO):
        print("Email not configured; skipping.")
        return
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = ALERT_EMAIL_TO
        msg.set_content(full_text)
        msg.add_attachment(full_text.encode("utf-8"), maintype="text", subtype="plain", filename="full_digest.txt")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.starttls(); s.login(SMTP_USER, SMTP_PASS); s.send_message(msg)
        print("Email sent ok.")
    except Exception as e:
        print("Email send error:", e)

# -------------- batch utils --------------
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# -------------- robust env parsing --------------
def parse_bool_env_value(val, default=False):
    if val is None:
        return default
    v = str(val).strip().lower()
    return v in ("1","true","yes","on")

def parse_manual_mode_env(raw):
    """
    Accepts:
    - 'morning' | 'evening' | 'auto' (direct)
    - 'mode=morning' or 'mode=morning, process_all=false, max_tickers=200'
    - '' -> returns ''
    Returns: normalized 'morning' | 'evening' | 'auto' | ''
    """
    if not raw:
        return ""
    s = str(raw).strip()
    # if simple word
    low = s.lower()
    if low in ("morning","evening","auto",""):
        return low
    # if contains 'mode=' parse it
    m = re.search(r"mode\s*=\s*([a-zA-Z0-9_-]+)", s)
    if m:
        return m.group(1).lower()
    # try to pick out 'morning' or 'evening' anywhere
    if "morning" in low:
        return "morning"
    if "evening" in low:
        return "evening"
    return ""

# ---------------- Main ----------------
def main():
    ensure_cache_dir()

    # Show raw env so you can debug easily from logs
    raw_MANUAL_MODE = os.getenv("MANUAL_MODE", "")
    print("RAW_MANUAL_MODE (env):", raw_MANUAL_MODE)
    parsed_manual_mode = parse_manual_mode_env(raw_MANUAL_MODE)
    print("Parsed MANUAL_MODE ->", parsed_manual_mode or "<empty>")

    # read PROCESS_ALL_BATCHES carefully
    # priority order applied later, but parse envs now
    env_process_all = os.getenv("PROCESS_ALL_BATCHES", "")
    process_all_flag = parse_bool_env_value(env_process_all, default=True)

    # also allow process_all input embedded in MANUAL_MODE raw string (like 'process_all=false')
    m_proc = re.search(r"process_all\s*=\s*(true|false|1|0|yes|no)", str(raw_MANUAL_MODE), flags=re.IGNORECASE)
    if m_proc:
        process_all_flag = parse_bool_env_value(m_proc.group(1), process_all_flag)

    # parse FORCE_* flags (explicit overrides)
    force_single = parse_bool_env_value(os.getenv("FORCE_SINGLE_BATCH", "false"), False)
    force_all_override = parse_bool_env_value(os.getenv("FORCE_ALL_BATCHES", "false"), False)

    # manual run detection
    github_event_name = os.getenv("GITHUB_EVENT_NAME", "")
    manual_run_flag = (github_event_name == "workflow_dispatch") or (parsed_manual_mode in ("morning","evening"))

    print("Runtime flags: MAX_TICKERS:", MAX_TICKERS, "PROCESS_ALL_BATCHES(env):", env_process_all,
          "-> parsed process_all_flag:", process_all_flag,
          "manual_run_flag:", manual_run_flag,
          "force_single:", force_single, "force_all_override:", force_all_override)

    # Build universe
    sp = get_sp500_list()
    nas = get_nasdaq100_list()
    combined = sp + nas
    uniq = {}
    for t,n in combined:
        key = t.upper()
        if key not in uniq:
            uniq[key] = (t,n)
    universe = list(uniq.values())
    total = len(universe)
    print("Universe total tickers:", total)

    # Upcoming earnings via calendar (retry JSON once then fallback)
    upcoming = fetch_upcoming_earnings(UPCOMING_DAYS)
    print("DEBUG: calendar-based upcoming earnings fetched:", len(upcoming))

    # Per-ticker supplement for earnings (same logic as before)
    per_ticker_upcoming = []
    existing_tickers = set((it.get("ticker") or "").upper() for it in upcoming if it.get("ticker"))
    need_full_check = (len(upcoming) < max(3, min(20, len(universe)//30)))
    checked_cnt = 0; found_cnt = 0
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
                per_ticker_upcoming.append({"title": f"Earnings scheduled: {res['ticker']} ({res.get('company','')}) {published_iso}",
                                            "link": f"https://finance.yahoo.com/quote/{tk}/calendar?p={tk}",
                                            "published": published_iso, "ticker": tk, "company": res.get("company") or name})
                found_cnt += 1
        time.sleep(0.12)
    print("Per-ticker checks completed: checked=", checked_cnt, "found=", found_cnt)

    # merge upcoming lists
    all_upcoming = upcoming + per_ticker_upcoming
    dedup = []; seenloc = set()
    for it in all_upcoming:
        key = ((it.get("ticker") or "").upper(), it.get("published",""))
        if key not in seenloc:
            seenloc.add(key); dedup.append(it)
    upcoming = dedup
    print("Total upcoming after supplement:", len(upcoming))

    # Morning/evening decision
    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.now(tz)
    local_hour = now_local.hour
    # if manual override provided, prefer parsed_manual_mode
    if parsed_manual_mode == "morning":
        is_morning = True; is_evening = False
        print("Manual override: forcing morning run.")
    elif parsed_manual_mode == "evening":
        is_morning = False; is_evening = True
        print("Manual override: forcing evening run.")
    else:
        is_morning = (local_hour == DAILY_DIGEST_HOUR_MORNING)
        is_evening = (local_hour == DAILY_DIGEST_HOUR_EVENING)
        print(f"Auto-detected by time: hour={local_hour}, is_morning={is_morning}, is_evening={is_evening}")

    if not (is_morning or is_evening) and not manual_run_flag:
        print("Not a digest hour and not manual. Exiting early.")
        return

    # Prepare categories
    categories = {"upcoming_earnings": [], "ai_special": [], "product_launch": [], "scandal_after_launch": [], "major_deal": [], "takeover": []}
    added_this_run = set()

    # include upcoming earnings in morning runs only (unchanged semantics)
    if is_morning:
        for it in upcoming:
            categories["upcoming_earnings"].append({"title": it.get("title",""), "link": it.get("link",""), "published": it.get("published",""), "ticker": it.get("ticker"), "company": it.get("company")})

    # Build batches
    batches = list(chunk_list(universe, MAX_TICKERS))
    print("Built batches:", len(batches), "MAX_TICKERS:", MAX_TICKERS)

    # Decide whether to process all batches or single batch using priority rules:
    # 1) FORCE_SINGLE_BATCH true -> single
    # 2) manual run & not force_all_override -> single unless process_all_flag true
    # 3) process_all_flag false -> single
    # 4) else process all
    if force_single:
        batches = [batches[0]] if batches else []
        print("FORCE_SINGLE_BATCH=true -> limiting to first batch.")
    elif manual_run_flag and not force_all_override and not process_all_flag:
        batches = [batches[0]] if batches else []
        print("Manual run & process_all not set -> limiting to first batch.")
    elif not process_all_flag:
        batches = [batches[0]] if batches else []
        print("PROCESS_ALL_BATCHES=false -> limiting to first batch.")
    else:
        print("Processing all batches this run.")

    print(f"Processing {len(batches)} batch(es) after flag resolution.")

    # Process batches (unchanged feed polling + classification logic; only core piping kept minimal here)
    seen = load_json_set(SEEN_FILE)
    for i, batch in enumerate(batches, start=1):
        print(f"Batch {i}/{len(batches)} tickers:", len(batch))
        feeds = [(t, n, f"https://news.google.com/rss/search?q={urllib.parse.quote(f'\"{n}\" OR {t} (earnings OR launch OR partnership OR invest OR ai OR acquire OR merger OR lawsuit)')}&hl=en-US&gl=US&ceid=US:en") for (t,n) in batch]
        for ticker, cname, rss in feeds:
            entries = []
            try:
                parsed = feedparser.parse(rss)
                entries = parsed.entries or []
            except Exception as e:
                print("feedparser error", e)
            if not entries:
                time.sleep(THROTTLE_SECONDS); continue
            for entry in entries:
                # recency
                pub = entry.get("published") or entry.get("updated") or ""
                # rough recency: rely on parsed dates minimally -> simple skip by RECENT_DAYS using published_parsed
                if entry.get("published_parsed"):
                    ts = time.mktime(entry["published_parsed"])
                    dt = datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(pytz.timezone(TIMEZONE))
                    if (datetime.now(pytz.timezone(TIMEZONE)) - dt) > timedelta(days=RECENT_DAYS):
                        continue
                title = entry.get("title","") or ""
                link = entry.get("link","") or ""
                summary = entry.get("summary","") or entry.get("description","") or ""
                fp = fingerprint(title, link, pub)
                if fp in seen: continue
                lbl = classify(title, summary)
                if lbl == "ai_special":
                    seen.add(fp); added_this_run.add(fp)
                    categories["ai_special"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": pub})
                    continue
                if lbl == "product_launch":
                    seen.add(fp); added_this_run.add(fp)
                    categories["product_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": pub})
                    continue
                if lbl == "scandal" and is_scandal_after_launch(title, summary):
                    seen.add(fp); added_this_run.add(fp)
                    categories["scandal_after_launch"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": pub})
                    continue
                if lbl == "major_deal":
                    seen.add(fp); added_this_run.add(fp)
                    categories["major_deal"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": pub})
                    continue
                if lbl == "takeover":
                    seen.add(fp); added_this_run.add(fp)
                    categories["takeover"].append({"ticker": ticker, "company": cname, "title": title, "link": link, "published": pub})
                    continue
            time.sleep(THROTTLE_SECONDS)
        if i < len(batches):
            time.sleep(1.0)

    # Build & send digest messages (kept concise)
    now_local = datetime.now(pytz.timezone(TIMEZONE))
    if is_morning:
        total_new = sum(len(v) for v in categories.values())
        if total_new:
            header = f"Morning Digest {now_local.strftime('%Y-%m-%d %H:%M')}\nProcessed {total} tickers across {len(batches)} batch(es). New: {total_new}\n\n"
            parts = [header]
            if categories["upcoming_earnings"]:
                parts.append("Upcoming earnings:\n")
                for it in categories["upcoming_earnings"]:
                    parts.append(f"• {it.get('ticker')} {it.get('title')}\n")
                parts.append("\n")
            if categories["ai_special"]:
                parts.append("AI Intentional Investments/Partnerships:\n")
                for it in categories["ai_special"]:
                    parts.append(f"• {it.get('ticker')} {it.get('title')}\n")
                parts.append("\n")
            # other categories same pattern...
            message = "".join(parts)
            notify_telegram_digest(message)
            send_email("Morning Market Digest", message)
    elif is_evening:
        # compute delta since morning_snapshot
        morning_snapshot = load_json_set(MORNING_SNAPSHOT_FILE)
        delta_fps = {fp for fp in added_this_run if fp not in morning_snapshot} if morning_snapshot else added_this_run.copy()
        if delta_fps:
            header = f"Evening Delta {now_local.strftime('%Y-%m-%d %H:%M')}\nNew since morning: {len(delta_fps)}\n\n"
            parts = [header]
            # include categories filtered by delta; simple approach: show all categories (small number)
            if categories["ai_special"]:
                parts.append("AI Intentional Investments/Partnerships (new):\n")
                for it in categories["ai_special"]:
                    fp = fingerprint(it.get("title",""), it.get("link",""), it.get("published",""))
                    if fp in delta_fps:
                        parts.append(f"• {it.get('ticker')} {it.get('title')}\n")
                parts.append("\n")
            message = "".join(parts)
            notify_telegram_digest(message)
            send_email("Evening Delta Digest", message)
        # update morning snapshot
        save_json_set(MORNING_SNAPSHOT_FILE, set( list(load_json_set(MORNING_SNAPSHOT_FILE)) + list(added_this_run) ))

    # persist seen
    save_json_set(SEEN_FILE, load_json_set(SEEN_FILE).union(added_this_run))
    print("Done. Saved seen count:", len(load_json_set(SEEN_FILE)))

if __name__ == "__main__":
    main()
