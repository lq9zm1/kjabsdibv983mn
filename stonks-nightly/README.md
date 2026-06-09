# Stonks nightly pipeline

Fully automated EOD ingestion + rebuild. Runs in the cloud (GitHub Actions) every
night, even with your Mac off. Your curated universe (`tickers.txt`) is the source
of truth; the pipeline never edits it and never touches `stock_theme_map`.

## What runs, and when

```
~01:30 AM Central  (GitHub Actions, cron 06:30 UTC)
  1. directory sync   – NASDAQ Trader = truth for valid/active tickers
  2. pull prices      – yfinance, only curated tickers still listed (slow/batched)
  3. load price_history (WRITE_TRUNCATE, partitioned by date / clustered by ticker)
  4. rebuilds (sql/ in order):
       metrics_daily → rs_momentum → theme_stats → theme_map_stats
       → theme_rs_momentum → ma_crosses → stock_rs_history → theme_rs_history
  5. tickers metadata (new tickers only; keeps last-known if yfinance is flaky)
  6. write ticker_review  (Delisted / No-data-streak / New listings)

3:00 AM & 3:15 PM Central  (your existing Apps Script)
  refreshBigQuery  → bq_dashboard   (unchanged)
  refreshTickerReview → ticker_review tab   (new — paste the .gs)
```

`v_stock_dashboard` is a view, so it’s automatically fresh once the tables rebuild.

## Files
- `tickers.txt` – your curated universe (edit + push to add/remove names)
- `pull.py` – stability-patched yfinance puller
- `run_nightly.py` – the orchestrator
- `sql/01..08_*.sql` – your rebuild queries (pulled from BigQuery job history; edit here to tune)
- `.github/workflows/nightly.yml` – the 2 AM cron
- `apps_script_ticker_review.gs` – paste into your Apps Script
- `requirements.txt`

Auto-created on first run (no manual setup): `directory_symbols`, `ticker_review_log`, `ticker_review`.

---

## One-time setup (~20 min)

### 1. Create the GitHub repo
- New **private** repo (e.g. `stonks-nightly`).
- Upload every file here, keeping the folder layout (`sql/…`, `.github/workflows/…`).
  (Easiest: GitHub web UI → “Add file → Upload files”, or `git init && git add . && git commit && git push`.)

### 2. Create a BigQuery service account + key
- GCP Console → **IAM & Admin → Service Accounts → Create**. Name it `stonks-nightly`.
- Grant two roles: **BigQuery Job User** and **BigQuery Data Editor**.
- Open the new account → **Keys → Add key → Create new key → JSON** → download it.

### 3. Add the key as a GitHub secret
- Repo → **Settings → Secrets and variables → Actions → New repository secret**.
- Name: **`GCP_SA_KEY`** — Value: paste the entire contents of the JSON file.

### 4. (Optional) check the schedule
- `nightly.yml` runs at **06:30 UTC** (~1:30 AM Central). Change the `cron:` line if you move timezones.

### 5. Run it once manually to verify
- Repo → **Actions → stonks-nightly → Run workflow**.
- Watch the log. Expect: directory counts → `pulled N rows` → 8 rebuilds → review summary → `DONE.`

### 6. Wire up the sheet tab
- Open the sheet → **Extensions → Apps Script**.
- Paste the whole function from `apps_script_ticker_review.gs`.
- In `onOpen()` add: `.addItem('Refresh Ticker Review', 'refreshTickerReview')`
- In `refreshAll()` add a line: `refreshTickerReview();`
- Save, then run `refreshTickerReview` once to create the `ticker_review` tab.

### Done
After that it’s hands-off. To change the universe, edit `tickers.txt` and push.
To tune any metric, edit the matching file in `sql/` and push.

## The review tab (your guardrail)
- **🔴 Delisted** – left the NASDAQ directory; shows which theme(s) it’s in. Auto-skipped from the pull; you decide whether to remove it from `stock_theme_map`.
- **🟡 No data** – still listed but yfinance failed N+ nights (flaky vs dying).
- **🟢 New listings** – appeared in the directory; theme it if relevant.

Nothing in `stock_theme_map` changes without you. A missed review never breaks the
nightly job — rebuilds keep running regardless.
