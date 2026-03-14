# US Market Event Alerts (GitHub Actions, no-persist)

This repo runs on **GitHub Actions** every 30 minutes and watches major US stocks (S&P 500 + NASDAQ-100).  
It classifies market-moving events (earnings, takeovers/M&A, scandals, product launches, venture results, major deals) and sends instant alerts via **Telegram** (required). Optional duplication channels: Slack and Email.

This variant uses rotation to keep each Actions run short. You set `MAX_TICKERS` to control how many tickers are processed per run. You changed this to **200**.

---

## What this does

- Auto-fetches S&P-500 and NASDAQ-100 constituents from Wikipedia at runtime.
- Builds focused Google News RSS queries per ticker/company for event keywords.
- Polls a rotating subset of tickers each run (no persistence; dedupe only inside the run).
- Sends instant alerts to Telegram (required), optionally Slack and/or email.
- Sends a daily digest email at **08:00 Europe/Paris** if email is configured.
- Runs every 30 minutes via GitHub Actions (see `.github/workflows/poll-rss.yml`).

---

## Rotation behaviour (important)

With `MAX_TICKERS = 200` and ~600 unique tickers (S&P500 + NASDAQ100 minus overlap):

- The runner processes **200 tickers per run**.
- With the workflow running **every 30 minutes**, the entire universe is covered every **~1.5 hours** (200 → 200 → 200).
- Major stocks may be seen sooner if they appear in the selected slice for that run.
- You can increase `MAX_TICKERS` if you want faster full coverage (tradeoff: longer run time / more Actions minutes).

---

## Files you should have in the repo

- `main_improved_action.py` — the improved poller script (must be in repo root).
- `.github/workflows/poll-rss.yml` — GitHub Actions workflow (configured to call the script).
- `README.md` — (this file).

---

## Required GitHub repository secrets

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

- `TELEGRAM_BOT_TOKEN` — token from @BotFather (required). Example: `123456789:AAAbbbCCC...`
- `TELEGRAM_CHAT_ID` — your personal chat id or group id (required). Private chat ids are positive; groups are negative.

Optional (only if you want Slack/email duplication):
- `SLACK_WEBHOOK` — incoming Slack webhook URL (optional)
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `ALERT_EMAIL_TO` — SMTP config and recipient for email (optional)

Optional tuning:
- `MAX_TICKERS` — number of tickers processed each run. You set it to `200`.
- `THROTTLE_SECONDS` — pause between feed polls (default `0.4`) to be polite to sources.
- `TIMEZONE` / `DAILY_DIGEST_HOUR` — timezone (default `Europe/Paris`) and digest hour (default `8`).

---

## How to get Telegram Bot Token & Chat ID (quick)

1. In Telegram, open **@BotFather**, send `/newbot`, follow prompts. BotFather returns the **bot token**. Save it to `TELEGRAM_BOT_TOKEN`.
2. Start your bot by searching its username and press **Start** (or add it to a group).
3. To get your chat id:
   - Send a message to the bot.
   - Open in browser:
     ```
     https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
     ```
     Look for `"chat":{"id":...}` in the JSON. That is `TELEGRAM_CHAT_ID`.
   - If sending to a group, add the bot to the group and read `"chat":{"id":...}` (group ids are negative).

Keep both values private (store them as GitHub Secrets).

---

## Quick setup steps

1. Push this repo to GitHub (include `main_improved_action.py` and `.github/workflows/poll-rss.yml`).
2. Add repository secrets (see list above).
3. Confirm `.github/workflows/poll-rss.yml` contains `MAX_TICKERS: '200'` (or set as a secret/env).
4. Open **Actions → Poll US Market (S&P500 + NASDAQ100) & Alerts** and watch the first run logs.
5. Verify you receive Telegram test alerts printed in logs (script prints alerts as it finds them).

---

## Tuning & recommendations

- **If you want faster detection for top stocks**: consider a small tweak to always include a priority list of ~50 largest market-cap tickers every run (I can supply that change). That ensures most important news is usually detected within 30 minutes.
- **If you want full ~600 coverage every run**: set `MAX_TICKERS` to `600` (longer run, more Actions minutes).
- **If you see missing headlines**: the script uses Google News RSS and public scrapes (Wikipedia / Yahoo). Occasionally sources block scrapers — increasing `THROTTLE_SECONDS` or adding retries helps.
- **To reduce noise**: tweak the `KEYWORDS` and `classify()` rules in `main_improved_action.py`.

---

## Troubleshooting

- **No Telegram messages**: check the `getUpdates` endpoint with your bot token. Ensure your bot has been started and `TELEGRAM_CHAT_ID` is correct.
- **Workflow times out or runs long**: reduce `MAX_TICKERS`, or increase throttles to avoid bursts.
- **Yahoo or Wikipedia scraping fails**: the script logs errors and skips gracefully. Retry or increase run frequency later.

---

## Next steps I can help with (optional)

- Add a **priority list** that is always scanned every run (recommended).
- Add light persistence (Supabase/Postgres) to dedupe across runs.
- Provide a `gh` CLI snippet to create Secrets quickly.
- Lower noise further by tuning classifier keywords.

If you want any of those, tell me which and I’ll produce the code or commands.
