# Stock News Digest

A lightweight GitHub Actions workflow that scans major US-listed companies and sends a **twice-daily digest** of market-moving company news to **Telegram** and optionally **email**.

It is designed for:

- **US stocks only**
- **No manual watchlist**
- **Digest format**, not noisy real-time alerts
- Focus on events that may move a stock up or down:
  - Upcoming earnings
  - AI investments / partnerships
  - New product launches
  - Major deals / partnerships
  - M&A / takeovers
  - Scandals tied to launches or major events

---

## What it does

The script:

- builds a stock universe from the **S&P 500** and **Nasdaq-100**
- scans company-related Google News RSS feeds
- classifies relevant items into categories
- sends:
  - a **morning full digest**
  - an **evening delta digest** (only what is new since the morning run)
- stores state in a cache so already-sent news is not repeated day after day

---

## Categories included

### 1. Upcoming earnings
Upcoming earnings for the next `UPCOMING_DAYS` days.

### 2. AI intentional investments / partnerships
Examples:
- explicit AI investments
- AI partnerships
- GPU / accelerator purchases
- AI labs / AI initiatives
- major AI model integrations

### 3. Product launches
Major new product announcements or launches.

### 4. Major deals / partnerships
Significant commercial deals or partnerships likely to move sentiment.

### 5. M&A / takeovers
Acquisitions, mergers, takeovers, or similar strategic transactions.

### 6. Scandals after launches / events
Fraud, lawsuits, recalls, probes, or similar negative events tied to launches or major business moments.

---

## How it avoids repeated noise

The workflow uses a cached `.cache` directory that contains:

- `seen.json`  
  Keeps fingerprints of news already sent, so the same news is not sent again on later days.

- `morning_snapshot.json`  
  Used by the evening run to send **only the delta** since the morning digest.

### Important behavior
- **Normal news items** are deduplicated using `seen.json`.
- **Upcoming earnings are intentionally not treated the same way**, so they may show up again while they are still inside the upcoming window.

---

## How the cache works

Every run executes:

```bash
mkdir -p .cache
