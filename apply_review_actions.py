#!/usr/bin/env python3
"""
apply_review_actions.py — the nightly EXECUTOR for the ticker_review tab's Submit queue.

Reads `ticker_review_actions` (status='pending') and APPLIES each queued action:
  • action='theme'  -> MOVE the ticker into that sub_theme (cleared from any prior theme FIRST) in
                       theme_map, AND ensure it's in tickers.txt. Re-theming a name never double-maps it.
  • action='remove' -> strip the ticker from theme_map  AND  tickers.txt (archived, reversible)
Then rebuilds stock_theme_map ONCE, backfills full price history for any newly-ADDED tickers
(so they're usable immediately, not only after the Sunday full reset), and marks the processed
queue rows status='done' ('error' if a sub_theme didn't exist — never silently dropped).

BACKUPS: whenever a Submit actually changes tickers.txt, the PRE-change file is snapshotted into
tickers_backups/ and only the newest 3 are kept — so you always have the last 3 changed versions to
roll back to. Backups accrue on real changes only, never on a quiet nightly.

WIRED INTO run_nightly.py as STEP 0 — before the universe is read and prices are pulled — so a
Submit takes effect on the very next nightly: adds flow into that night's price pull + theme
rebuilds; removes drop out of the pull.

CONSISTENCY ORDER (matters): tickers.txt (git, source of truth) is edited + committed + PUSHED
FIRST. Only if that succeeds do we touch the BigQuery theme_map / stock_theme_map. So we can never
end up with a theme_map change whose tickers.txt half was lost — if the push fails, the DB is left
untouched, the queue stays 'pending', and the next nightly retries cleanly.

Idempotent + safe:
  • already-present adds are no-ops; a re-added-then-removed name won't duplicate.
  • a bad/unknown sub_theme marks THAT action 'error' and is skipped (others still apply).
  • nothing commits if nothing changed.

Auth: GCP_SA_KEY (BigQuery) + EODHD_API_KEY (backfill) + repo contents:write (git push).
Requires: google-cloud-bigquery, pandas, requests (+ pull.py for the backfill).
"""
import re
import subprocess
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TICKERS_FILE = "tickers.txt"
ARCHIVE_FILE = "tickers_removed_archive.txt"
BACKUP_DIR   = "tickers_backups"       # rolling pre-change snapshots of tickers.txt
KEEP_BACKUPS = 3                       # keep only the newest N (last 3 changed versions)
TR_ACTIONS = f"{PROJECT}.{DATASET}.ticker_review_actions"

bq = bigquery.Client(project=PROJECT)

_PRICE_SCHEMA = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
    bigquery.SchemaField("adj_close", "FLOAT64"),
    bigquery.SchemaField("volume", "INT64"),
]


def tbl(n):   return f"{PROJECT}.{DATASET}.{n}"
def norm(s):  return str(s).upper().strip().replace(".", "-").replace("/", "-")


# ---- read the queue ---------------------------------------------------------
def _ensure_actions_table():
    bq.query(
        f"CREATE TABLE IF NOT EXISTS `{TR_ACTIONS}` "
        "(ticker STRING, action STRING, sub_theme STRING, submitted_at TIMESTAMP, status STRING)"
    ).result()


def pending_actions():
    """Return list of dicts for pending rows, oldest first."""
    rows = list(bq.query(
        f"SELECT ticker, action, sub_theme, CAST(submitted_at AS STRING) AS ts "
        f"FROM `{TR_ACTIONS}` WHERE status = 'pending' ORDER BY submitted_at"
    ).result())
    return [{"ticker": (r.ticker or "").strip(), "action": (r.action or "").strip(),
             "sub_theme": (r.sub_theme or "").strip(), "ts": r.ts} for r in rows]


def subtheme_map():
    """UPPER(sub_theme) -> canonical sub_theme, from theme_map (the dropdown's source of truth)."""
    out = {}
    for r in bq.query(f"SELECT DISTINCT sub_theme FROM `{tbl('theme_map')}` WHERE sub_theme IS NOT NULL").result():
        out[r.sub_theme.upper().strip()] = r.sub_theme
    return out


# ---- tickers.txt edits (git source of truth) --------------------------------
def add_to_tickers_file(symbols):
    """Append bare symbols not already present. Returns the ones actually added."""
    if not symbols:
        return []
    path = Path(TICKERS_FILE)
    raw = path.read_text() if path.exists() else ""
    have = {norm(t) for t in re.split(r"[^A-Za-z0-9.\-]+", raw.upper()) if t}
    lines = raw.splitlines()
    added = []
    for s in symbols:
        s = s.upper().strip()
        if s and norm(s) not in have:
            lines.append(s)
            have.add(norm(s))
            added.append(s)
    if added:
        path.write_text("\n".join(lines) + "\n")
    return added


def _rolling_backup(pre_text):
    """Save the PRE-change tickers.txt into tickers_backups/, keeping only the newest KEEP_BACKUPS.
    Called ONLY when a Submit actually changed the file — so backups accrue on real changes, never on
    a quiet nightly. To roll back, just copy the wanted backup over tickers.txt and commit."""
    if pre_text is None:
        return
    d = Path(BACKUP_DIR)
    d.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = d / f"tickers_{ts}.txt"
    dst.write_text(pre_text)
    backups = sorted(d.glob("tickers_*.txt"))          # timestamped names sort chronologically
    for old in backups[:-KEEP_BACKUPS]:
        old.unlink()
    kept = [p.name for p in sorted(d.glob("tickers_*.txt"))]
    print(f"   backup: saved {dst.name} (pre-change) | keeping last {len(kept)}: {kept}")


def prune_tickers_file(remove_norm):
    """Remove matching symbols from tickers.txt (bare-token, punctuation-insensitive). Returns removed."""
    if not remove_norm or not Path(TICKERS_FILE).exists():
        return []
    lines = Path(TICKERS_FILE).read_text().splitlines()
    kept, removed = [], []
    for ln in lines:
        toks = [t for t in re.split(r"[^A-Za-z0-9.\-]+", ln) if t]
        keep = [t for t in toks if norm(t) not in remove_norm]
        removed += [t for t in toks if norm(t) in remove_norm]
        if not toks:
            kept.append(ln)                                   # preserve blank lines
        elif keep:
            kept.append(" ".join(keep) if len(toks) > 1 else keep[0])
        # else: line held only removed tokens -> drop it
    Path(TICKERS_FILE).write_text("\n".join(kept) + "\n")
    return sorted(set(removed))


def git_commit(added, removed):
    """Commit + PUSH tickers.txt (+archive). Raises on push failure so the DB step is skipped."""
    if not (added or removed):
        return
    subprocess.run(["git", "config", "user.name", "stonks-bot"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@users.noreply.github.com"], check=True)
    files = [TICKERS_FILE] + ([ARCHIVE_FILE] if Path(ARCHIVE_FILE).exists() else [])
    if Path(BACKUP_DIR).exists():
        files.append(BACKUP_DIR)                       # commit the rolling backups too
    subprocess.run(["git", "add", "-A", *files], check=True)   # -A also stages pruned old backups
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
        print("   (no tickers.txt change to commit)")
        return
    bits = []
    if added:   bits.append("add " + ", ".join(added))
    if removed: bits.append("remove " + ", ".join(removed))
    msg = "ticker_review actions: " + " | ".join(bits)
    subprocess.run(["git", "commit", "-m", msg[:2000]], check=True)
    subprocess.run(["git", "push"], check=True)               # raises CalledProcessError on failure


# ---- theme_map (BigQuery) edits ---------------------------------------------
def theme_map_add(ticker, canonical_sub):
    """Append ticker to that sub_theme's comma list, only if not already there. Returns rows affected."""
    tk = ticker.upper().strip()
    job = bq.query(
        f"""UPDATE `{tbl('theme_map')}`
            SET tickers = IF(TRIM(IFNULL(tickers, '')) = '', @tk, CONCAT(TRIM(tickers), ', ', @tk))
            WHERE sub_theme = @sub
              AND NOT EXISTS (SELECT 1 FROM UNNEST(SPLIT(IFNULL(tickers, ''), ',')) x
                             WHERE UPPER(TRIM(x)) = @tk)""",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("tk", "STRING", tk),
            bigquery.ScalarQueryParameter("sub", "STRING", canonical_sub),
        ]),
    )
    job.result()
    return job.num_dml_affected_rows or 0


def theme_map_strip(remove_list):
    """Remove the symbols from every theme_map.tickers list they appear in."""
    if not remove_list:
        return
    inlist = ",".join("'" + t.upper().replace("'", "") + "'" for t in remove_list)
    bq.query(f"""
        UPDATE `{tbl('theme_map')}`
        SET tickers = (SELECT STRING_AGG(TRIM(t), ', ')
                       FROM UNNEST(SPLIT(tickers, ',')) t
                       WHERE UPPER(TRIM(t)) NOT IN ({inlist}))
        WHERE EXISTS (SELECT 1 FROM UNNEST(SPLIT(tickers, ',')) t
                      WHERE UPPER(TRIM(t)) IN ({inlist}))
    """).result()


def rebuild_stock_theme_map():
    bq.query(f"""
        CREATE OR REPLACE TABLE `{tbl('stock_theme_map')}` AS
        SELECT TRIM(t) AS ticker, main_theme, sub_theme, etf
        FROM `{tbl('theme_map')}`, UNNEST(SPLIT(tickers, ',')) t
        WHERE TRIM(t) != ''
    """).result()


# ---- full-history backfill for newly-added names ----------------------------
def backfill_history(new_adds):
    """Pull FULL history for freshly-added tickers and upsert into price_history, so a name added
    mid-week isn't stuck with only 7 days until the Sunday full reset. Best-effort: any failure is
    logged and swallowed (the Sunday reset will still fill it)."""
    if not new_adds:
        return
    try:
        import pull as pullmod
        df, failed = pullmod.pull_prices(new_adds)
        if df is None or df.empty:
            print(f"   backfill: no data returned for {new_adds} (Sunday reset will fill).")
            return
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        min_date = df["date"].min().isoformat()
        stg = tbl("price_history_staging")
        bq.load_table_from_dataframe(
            df, stg, job_config=bigquery.LoadJobConfig(
                schema=_PRICE_SCHEMA, write_disposition="WRITE_TRUNCATE")).result()
        bq.query(f"""
            MERGE `{tbl('price_history')}` T
            USING `{stg}` S
            ON T.ticker = S.ticker AND T.date = S.date AND T.date >= DATE '{min_date}'
            WHEN MATCHED THEN UPDATE SET
              open = S.open, high = S.high, low = S.low, close = S.close,
              adj_close = S.adj_close, volume = S.volume
            WHEN NOT MATCHED THEN INSERT
              (ticker, date, open, high, low, close, adj_close, volume)
              VALUES (S.ticker, S.date, S.open, S.high, S.low, S.close, S.adj_close, S.volume)
        """).result()
        print(f"   backfilled full history: {df['ticker'].nunique()} ticker(s), {len(df):,} bars"
              + (f" | no data: {failed}" if failed else ""))
    except Exception as e:
        print(f"   (backfill skipped: {e})")


# ---- queue status writes ----------------------------------------------------
def _mark(status, ticker, action, ts):
    # match submitted_at by its exact string form (ts came from CAST(submitted_at AS STRING)),
    # avoiding any TIMESTAMP() re-parse ambiguity.
    bq.query(
        f"UPDATE `{TR_ACTIONS}` SET status = @st "
        f"WHERE status = 'pending' AND ticker = @tk AND action = @ac AND CAST(submitted_at AS STRING) = @ts",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("st", "STRING", status),
            bigquery.ScalarQueryParameter("tk", "STRING", ticker),
            bigquery.ScalarQueryParameter("ac", "STRING", action),
            bigquery.ScalarQueryParameter("ts", "STRING", ts),
        ]),
    ).result()


# ---- orchestration ----------------------------------------------------------
def reconcile():
    _ensure_actions_table()
    pend = pending_actions()
    if not pend:
        print("apply_review_actions: no pending Submits.")
        return
    print(f"apply_review_actions: {len(pend)} pending action(s).")

    submap = subtheme_map()
    theme_ok, theme_err, removes = [], [], []       # each entry keeps (ticker, action, ts[, canon])
    for a in pend:
        tk, act, ts = a["ticker"], a["action"], a["ts"]
        if not tk or tk == "-":
            theme_err.append((tk, act, ts)); continue
        if act == "theme":
            canon = submap.get(a["sub_theme"].upper())
            if not canon:
                print(f"   !! '{tk}' -> unknown sub_theme '{a['sub_theme']}' — marking error.")
                theme_err.append((tk, act, ts)); continue
            theme_ok.append((tk, act, ts, canon))
        elif act == "remove":
            removes.append((tk, act, ts))
        else:
            theme_err.append((tk, act, ts))

    add_syms = sorted({t[0].upper() for t in theme_ok})
    rem_syms = sorted({t[0].upper() for t in removes})

    # 1) tickers.txt FIRST (source of truth): snapshot the pre-change file, edit, commit + push.
    #    If push fails -> raise, DB untouched.
    pre_text = Path(TICKERS_FILE).read_text() if Path(TICKERS_FILE).exists() else None
    added = add_to_tickers_file(add_syms)
    removed_from_file = prune_tickers_file({norm(t) for t in rem_syms})
    if added or removed_from_file:
        _rolling_backup(pre_text)                    # rolling last-3 backup — only when the file truly changed
    if removed_from_file:
        with open(ARCHIVE_FILE, "a") as f:
            for t in removed_from_file:
                f.write(f"{date.today()}\t{t}\t(ticker_review remove)\n")
    git_commit(added, removed_from_file)             # push must succeed before we touch the DB
    print(f"   tickers.txt: +{len(added)} added {added or ''} | -{len(removed_from_file)} removed {removed_from_file or ''}")

    # 2) theme_map (BigQuery). A theme pick is a MOVE, not an append: clear the ticker from any
    #    prior sub-theme FIRST, then place it in the chosen one — so re-theming a name (you changed
    #    your mind on its group) never leaves it double-mapped. Then apply removes, then ONE rebuild.
    theme_tickers = sorted({t[0].upper() for t in theme_ok})
    if theme_tickers:
        theme_map_strip(theme_tickers)                    # remove from ALL current themes first (MOVE)
    for tk, act, ts, canon in theme_ok:
        theme_map_add(tk, canon)
        print(f"   theme_map: {tk} -> '{canon}' (moved)")
    if rem_syms:
        theme_map_strip(rem_syms)
    if theme_ok or removes:
        rebuild_stock_theme_map()
        print("   stock_theme_map rebuilt (theme SQL 03/06/15 refresh later in this nightly).")

    # 3) backfill history for genuinely-new names (added to the file this run).
    backfill_history(added)

    # 4) mark the queue: errors first, then everything else applied -> done.
    for tk, act, ts in theme_err:
        _mark("error", tk, act, ts)
    for tk, act, ts, canon in theme_ok:
        _mark("done", tk, act, ts)
    for tk, act, ts in removes:
        _mark("done", tk, act, ts)
    print(f"   queue: {len(theme_ok)} theme + {len(removes)} remove -> done"
          + (f" | {len(theme_err)} error" if theme_err else ""))


def main():
    reconcile()


if __name__ == "__main__":
    main()
