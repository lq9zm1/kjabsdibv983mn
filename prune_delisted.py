"""
prune_delisted.py — auto-prune CONFIRMED-delisted tickers from tickers.txt AND theme_map,
then commit the change back to the repo. Runs weekly via prune_delisted.yml.

WHY: ticker_review flags names that left the exchange directory >=3 nights ("CONFIRMED").
They are already not being pulled; this keeps tickers.txt + theme_map clean with no manual work,
so you never have to remember to do it.

SAFETY RAILS:
  • Prune ONLY section='delisted' rows whose detail says CONFIRMED (>=3 nights). NEVER auto-add.
  • Archive every pruned symbol to tickers_removed_archive.txt (dated) — fully reversible.
  • Also strip pruned tickers from theme_map + rebuild stock_theme_map in the SAME run, so nothing
    is ever left themed-but-unpulled. (Downstream theme SQL 03/06/15 refreshes on the next nightly.)
  • The commit message lists exactly what was removed.

Runs with GCP_SA_KEY (BigQuery) + repo contents:write (git push). Requires: google-cloud-bigquery.
"""
import re
import subprocess
from datetime import date
from pathlib import Path

from google.cloud import bigquery

PROJECT = "stonks-498420"
DATASET = "stonks_data"
TICKERS_FILE = "tickers.txt"
ARCHIVE_FILE = "tickers_removed_archive.txt"

bq = bigquery.Client(project=PROJECT)


def tbl(n):   return f"{PROJECT}.{DATASET}.{n}"
def norm(s):  return str(s).upper().strip().replace(".", "-").replace("/", "-")


def confirmed_delisted():
    """Tickers the nightly has flagged delisted AND confirmed (>=3 nights absent)."""
    q = f"""SELECT DISTINCT ticker FROM `{tbl('ticker_review')}`
            WHERE section = 'delisted' AND UPPER(detail) LIKE '%CONFIRMED%'
              AND ticker IS NOT NULL AND ticker != '-'"""
    return sorted({r.ticker for r in bq.query(q).result()})


def prune_tickers_file(remove_norm):
    """Remove matching symbols from tickers.txt (bare-token, punctuation-insensitive)."""
    if not Path(TICKERS_FILE).exists():
        return []
    lines = Path(TICKERS_FILE).read_text().splitlines()
    kept, removed = [], []
    for ln in lines:
        toks = [t for t in re.split(r"[^A-Za-z0-9.\-]+", ln) if t]
        keep = [t for t in toks if norm(t) not in remove_norm]
        removed += [t for t in toks if norm(t) in remove_norm]
        if not toks:
            kept.append(ln)                                  # preserve blank lines
        elif keep:
            kept.append(" ".join(keep) if len(toks) > 1 else keep[0])
        # else: line held only removed tokens -> drop it
    Path(TICKERS_FILE).write_text("\n".join(kept) + "\n")
    return sorted(set(removed))


def prune_theme_map(remove_list):
    """Strip the symbols from theme_map.tickers and rebuild the exploded stock_theme_map."""
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
    bq.query(f"""
        CREATE OR REPLACE TABLE `{tbl('stock_theme_map')}` AS
        SELECT TRIM(t) AS ticker, main_theme, sub_theme, etf
        FROM `{tbl('theme_map')}`, UNNEST(SPLIT(tickers, ',')) t
        WHERE TRIM(t) != ''
    """).result()


def git_commit(removed):
    if not removed:
        return
    subprocess.run(["git", "config", "user.name", "stonks-bot"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@users.noreply.github.com"], check=True)
    subprocess.run(["git", "add", TICKERS_FILE, ARCHIVE_FILE], check=True)
    # nothing staged (no net change) -> skip commit
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
        print("   (no file change to commit)"); return
    msg = "auto-prune delisted: " + ", ".join(sorted(removed))
    subprocess.run(["git", "commit", "-m", msg[:2000]], check=True)
    subprocess.run(["git", "push"], check=True)


def main():
    conf = confirmed_delisted()
    if not conf:
        print("No confirmed-delisted tickers — nothing to prune.")
        return
    print(f"{len(conf)} confirmed-delisted: {conf}")

    removed = prune_tickers_file({norm(t) for t in conf})
    if removed:
        with open(ARCHIVE_FILE, "a") as f:
            for t in removed:
                f.write(f"{date.today()}\t{t}\n")
    prune_theme_map(conf)                        # strips from theme_map even if not in tickers.txt

    print(f"Pruned from tickers.txt: {removed or '(none present)'}")
    print("theme_map + stock_theme_map rebuilt (03/06/15 refresh on next nightly).")
    git_commit(removed)


if __name__ == "__main__":
    main()
