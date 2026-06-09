/***** ADD TO YOUR EXISTING STONKS APPS SCRIPT *****
 * Pulls the nightly ticker_review table from BigQuery into a `ticker_review`
 * tab, grouped into sections (Delisted / No data / New listings).
 * Reuses your existing PROJECT_ID constant.
 *
 * SETUP (one time):
 *  1. Paste this whole function into your script.
 *  2. In onOpen(), add:   .addItem('Refresh Ticker Review', 'refreshTickerReview')
 *  3. In refreshAll(), add a line:   refreshTickerReview();
 ************************************************************/
function refreshTickerReview() {
  const ss = SpreadsheetApp.getActive();
  const TAB = 'ticker_review';
  const sql =
    "SELECT section, ticker, name, themes, detail " +
    "FROM `stonks-498420.stonks_data.ticker_review` " +
    "ORDER BY CASE section WHEN 'delisted' THEN 1 WHEN 'no_data' THEN 2 " +
    "WHEN 'new' THEN 3 ELSE 4 END, ticker";

  // ---- run query (same pattern as runQueryToTab) ----
  let job = BigQuery.Jobs.query({ query: sql, useLegacySql: false }, PROJECT_ID);
  const jobId = job.jobReference.jobId;
  while (!job.jobComplete) { Utilities.sleep(800); job = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId); }
  let rows = (job.rows || []).map(r => r.f.map(c => c.v));
  let pageToken = job.pageToken;
  while (pageToken) {
    const more = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId, { pageToken });
    rows = rows.concat((more.rows || []).map(r => r.f.map(c => c.v)));
    pageToken = more.pageToken;
  }

  // ---- build the sheet layout with section headers ----
  const SECTION_LABEL = {
    delisted: '🔴 DELISTED — review (still in stock_theme_map until you remove)',
    no_data:  '🟡 NO DATA (still listed, yfinance failing)',
    new:      '🟢 NEW LISTINGS (theme if relevant)',
    ok:       '✅ No changes',
  };
  const out = [['Ticker', 'Name', 'Theme(s)', 'Detail']];
  let lastSection = null;
  rows.forEach(r => {
    const [section, ticker, name, themes, detail] = r;
    if (section !== lastSection) {
      out.push(['']);
      out.push([SECTION_LABEL[section] || section, '', '', '']);
      lastSection = section;
    }
    out.push([ticker || '', name || '', themes || '', detail || '']);
  });

  // ---- write ----
  let sh = ss.getSheetByName(TAB);
  if (!sh) sh = ss.insertSheet(TAB);
  sh.clearContents();
  sh.getRange(1, 1, 1, 1).setValue('Ticker Review — last updated ' + new Date().toLocaleString());
  if (out.length) sh.getRange(3, 1, out.length, 4).setValues(out);
  sh.setFrozenRows(3);

  ss.toast('ticker_review refreshed (' + rows.length + ' flagged)', 'Stonks', 5);
}
