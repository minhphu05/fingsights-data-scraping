# Finsights data collection

Python source adapters and Dagster jobs for financial news, market data, and official issuer disclosures. Financial News uses HTML category pages or publisher sitemaps to discover articles, then fetches full article pages with BeautifulSoup. It does not use RSS. Five source backfill jobs and a separate `news_quality_backfill_job` run in isolated K3s pods.

## Data ownership

| Data | Destination | Purpose |
| --- | --- | --- |
| Dagster run and event history | PostgreSQL `dagster` | Dagster's own orchestration state |
| Source, run, item, fetch-attempt and checkpoint audit | MySQL Node 2 `finsights_scraping` | Scrape history and retry/reconciliation evidence only |
| Full article bodies, publishers, authors and security matches | MySQL Node 2 `finsights_news` | Canonical news product data |
| Securities, daily prices, index/quote snapshots and canonical aliases | MySQL Node 2 `finsights_market_data` | Canonical market facts and symbol/company matching |
| Exchange issuer filings, corporate actions, and regulator notices | MySQL Node 2 `finsights_disclosures` | HOSE/HNX issuer disclosures, VSDC rights events, and UBCKNN notices; source URLs and attachments are retained as metadata |
| Binary assets and object location metadata | OCI Object Storage; references in MySQL Node 2 | Article bodies stay in MySQL; binary media downloads remain disabled |

`finsights_scraping` records stable source IDs and the destination UID so a run can be audited without becoming a content database. Node 1 is reserved for AI-service-owned databases and is not written by these backend collectors.

## Layout

```text
configs/
  news/ticker-matching.yaml
  reference/security-aliases.yaml
  sources/news/cafef.yaml
  sources/news/{thanhnien,tuoitre,vnexpress,vneconomy,baomoi}.yaml
  sources/market/vietstock.yaml
  sources/market/vietstock-datafeed-mapping.example.yaml
  storage/object-storage.yaml
src/finsights_scraping/
  domain/                 # Date-window and normalized data contracts
  sources/news/rss.py     # Retained legacy adapter; not used by Financial News jobs
  sources/news/configured_html.py # Paginated full-HTML archive adapter
  sources/news/sitemap_html.py # Sitemap URL discovery + full-HTML article parsing
  sources/market/vietstock.py # Licensed historical DataFeed adapter
  persistence/            # Node 2 canonical data writes and collection audit writes
  storage/                # UUID asset identity and object-key rules
  orchestration/          # One Dagster job/schedule per source
legacy/previous-src/      # Archived pre-rewrite modules, retained for reference
```

## Official exchange, regulator, and depository sources

Section 2 sources in the planning sheet have independent Dagster jobs and date windows. They all write canonical data and scrape audit history to Node 2. The database writer maps symbols to `market_securities`/`market_companies`; records without a verified company/security link are retained as `regulatory_notices` rather than assigned a fabricated UID.

| Source key | Official collection method | Destination |
| --- | --- | --- |
| `hose` | Public HOSE news API; page and detail records, with linked file URLs retained | `issuer_disclosures` or `regulatory_notices` |
| `hnx` | HNX listed-stock and UPCoM disclosure boards; HTML table pagination (schedule disabled until the portal TLS certificate is repaired) | `issuer_disclosures` or `regulatory_notices` |
| `ubcknn` | Official SSC public notices/legal pages rendered through headless Chromium | `regulatory_notices` |
| `vsdc` | VSDC issuer rights-event board and detail pages; parses symbol, record/payment dates, cash, and share ratios | `issuer_disclosures` + `corporate_actions` |

HOSE, HNX, and UBCKNN download linked PDF/Excel/Word attachments and archive them in the OCI Tokyo bucket. The disclosure rows keep the stable asset UID, source URL, object key, MIME type, byte size, and checksum in MySQL `metadata_json`. Set `attachment_storage_mode: link_only` in a source config to retain links without mirroring files. VSDC stays metadata-only, as its planned data is corporate-action fields rather than documents.

Each job accepts inclusive Vietnam-local `start_date` and `end_date`. For a local fetch that writes JSON under `data/disclosures/` without touching MySQL:

```bash
python scripts/run_official_disclosures_local.py --source hose --start-date 2026-11-16 --end-date 2026-11-17
```

Use `--source all` for a serial local smoke run. The Dagster jobs are `hose_disclosures_job`, `hnx_disclosures_job`, `ubcknn_notices_job`, and `vsdc_corporate_actions_job`. They can be submitted together with `scripts/submit_dagster_job.py --source all_official --start-date YYYY-MM-DD --end-date YYYY-MM-DD`; this command skips any source whose config sets `collection_enabled: false`. The HNX schedule is disabled until its portal certificate is repaired. The VSDC and UBCKNN clients use headless Chromium for JavaScript/AJAX pagination; the Docker image installs its browser runtime. TLS verification stays enabled for every HTTP and browser request.

## Exact date windows

Manual news jobs accept inclusive `start_date` and `end_date` values in `YYYY-MM-DD` format, interpreted in `Asia/Ho_Chi_Minh`. Locally, use `scripts/run_local.py --source all --start-date ... --end-date ...`; output is grouped under a date-window directory.

The five Financial News publishers are CafeF, Tuổi Trẻ, Thanh Niên, VnExpress, and Báo Mới. VnEconomy remains available as a manually run adapter but is outside this five-source collection. All five publishers are configured for licensed full-text retention, source attribution, and `robots.txt` checks. Their HTML adapters reject missing article bodies instead of silently substituting an excerpt.

The backfill jobs discover dated pages from publisher HTML archives or XML sitemaps, fetch full article HTML, verify publication dates, and write publication-day batches. Tuổi Trẻ and Thanh Niên use five-day sitemap partitions; Báo Mới uses daily sitemap partitions; CafeF and VnExpress use paginated HTML archives. Each article write is idempotent by publisher ID and canonical URL. Binary media downloads remain disabled. A database quality gate labels every persisted article `financial`, `non_financial`, or `needs_review`, records evidence and ruleset version, and keeps excluded/review articles out of the financial feed.

Run a small publisher backfill locally before submitting a larger range to Dagster:

```bash
python scripts/test_financial_news_full_text.py
```

Dagster exposes `cafef_news_backfill_job`, `thanhnien_news_backfill_job`, `tuoitre_news_backfill_job`, and `vnexpress_news_backfill_job`, plus `baomoi_news_backfill_job`. Submit the five source jobs through `scripts/submit_dagster_job.py --source all_backfill`, or submit one `*_backfill` source. To classify existing rows without downloading articles again, run `news_quality_backfill_job` with this configuration:

```yaml
ops:
  classify_news_quality_backfill:
    config:
      start_date: "2026-01-01"
      end_date: "2026-11-23"
      source_keys: "cafef,thanhnien,tuoitre,vnexpress,baomoi"
```

The job writes its classification and evidence to `finsights_news.news_article_quality` and updates `news_articles.article_status`; articles outside finance remain available in the separate non-financial/review feed.

Set `collection_enabled: false` to pause a source. Licensed sources use `licensed_personal_project`, `internal_only: false`, and `redistribution_allowed: true`; the collector still applies attribution and robots.txt checks. The VnEconomy full-text backfill uses its published monthly sitemap partitions.

Source publication and quote timestamps are converted to UTC for `DATETIME` columns; `business_date` / `trade_date` remain Vietnam-local calendar dates for exact range filtering.

The public Vietstock board is a live/current view. The job therefore uses only a licensed DataFeed/API response with a provider trade date and observation time. Its required coverage includes all active stock symbols on HOSE, HNX, and UPCOM plus VN-Index, VN30-Index, HNX-Index, HNX30-Index, and UPCoM-Index. Optional extensions include VNAllshare, VNMidcap, VN100, VNSmallcap, HNX30TRI, and the UPCoM Large/Medium/Small indices; enable them only if the DataFeed contract supplies those fields. HOSE lists its full index family and HNX lists additional HNX/UPCoM benchmarks on their official pages: [HOSE index factsheet](https://staticfile.hsx.vn/Uploads/UploadDocuments/2478336/Form_Factsheet_MCIndices_VN_T07.2026.pdf) and [HNX stock/index page](https://www.hnx.vn/vi-vn/co-phieu.html). The current adapter rejects missing dates, rows outside the requested window, and a date with no quote on one of the three exchanges or a missing required index. That confirms exchange/index presence, not every active symbol; full-universe assurance still depends on the DataFeed contract guaranteeing its symbol coverage or a separate reconciliation against an official active-symbol roster. The example mapping is deliberately blank: fill it from the contract before enabling the scheduled job. No old date will be fabricated from today's live quote.

### CoPhieu68 market and financial data

`cophieu68_market_job` reads the current active ticker roster from Node 2 `finsights_market_data`, paginates `quote/history.php` through the requested start date, and stores daily OHLCV plus foreign buy/sell volumes in `market_prices_daily`. CoPhieu68 displays prices in thousands of VND, so prices are multiplied by 1,000 before persistence. The job also reads every quarter/year available in the source's income statement, balance sheet, and `financial_indexes` table and stores those values in Node 2 `finsights_fundamentals`; where the source page does not state an amount unit, the original number and raw label are retained without guessed scaling. CoPhieu68's detail page currently exposes income and balance tables; it does not expose a cash-flow table there. Scrape attempts and per-date/per-period audit items are recorded in Node 2 `finsights_scraping`.

The schedule runs on weekdays at 17:30 Vietnam time. Backfill with `python scripts/submit_dagster_job.py --source cophieu68 --start-date 2026-01-01 --end-date YYYY-MM-DD`.

## Financial relevance and ticker matching

CafeF discovers stories from its stock-market, company finance, banking, and macro-investment HTML archives, then applies financial relevance terms. Matching against a listed company uses `market_security_aliases` in Node 2 `finsights_market_data`: ticker, legal/short names, brand or former names, with effective dates. A short symbol must appear as a whole token and have financial context; company-name phrase matches retain the matched alias, evidence snippet, method, and confidence for review. This reduces false positives from ordinary words and abbreviations.

An exact ticker hit is matched as a token in financial context. Company aliases are stored in Node 2 `market_security_aliases`, where they can be reviewed and revised without changing scraper code. The sheet's 15 Financial News tickers and company names are seeded idempotently with `python scripts/sync_security_catalog.py`. After adding or correcting aliases, use `python scripts/reconcile_news_security_matches.py --start-date 2026-01-01 --end-date 2026-11-17` to apply them to already-stored articles. Both scripts use the Node 2 TLS CA and the scoped Node 2 app credentials.

The initial alias example is stored in `configs/reference/security-aliases.yaml` and can be loaded idempotently with `python scripts/sync_security_aliases.py` after SHB exists in `market_securities`. The official SHB name is “Ngân hàng TMCP Sài Gòn – Hà Nội”; the shorter phrase “Ngân hàng TMCP Sài Gòn” is not used as an SHB alias because it is ambiguous.

## Object storage

Keep a small number of buckets per environment and retention/access class, not one bucket per source, ticker, article, or date. The current layout is:

```text
raw/news/{publisher}/{yyyy}/{mm}/{dd}/{article_uid}/images/{asset_uid}.{ext}
raw/news/{publisher}/{yyyy}/{mm}/{dd}/{article_uid}/audio/{asset_uid}.{ext}
raw/news/{publisher}/{yyyy}/{mm}/{dd}/{article_uid}/tables/{asset_uid}.{ext}
raw/news/{publisher}/{yyyy}/{mm}/{dd}/{article_uid}/html/{asset_uid}.html
raw/market/{provider}/{yyyy}/{mm}/{dd}/{symbol}/{asset_uid}.{ext}
```

Every object has a UUID `asset_uid`; SHA-256 and size support content deduplication. The Financial News batch is link-only and does not upload images, audio, tables, or source HTML. The storage code remains available for other authorized collectors; no access keys or expiring URLs belong in the database.

## Schedules and collection frequency

| Job | Default schedule | Behavior |
| --- | --- | --- |
| `cafef_news_job` | Daily at 02:00 Vietnam time, starting 2026-11-24 | Collect the previous calendar day after it has elapsed |
| `vietstock_market_job` | Weekdays at 16:30 Vietnam time | EOD snapshot after the session; Dagster skips it until the licensed endpoint/token are configured |
| `thanhnien_news_job` | Daily at 02:00 Vietnam time, starting 2026-11-24 | Collect the previous calendar day |
| `tuoitre_news_job` | Daily at 02:00 Vietnam time, starting 2026-11-24 | Collect the previous calendar day |
| `vnexpress_news_job` | Daily at 02:00 Vietnam time, starting 2026-11-24 | Collect the previous calendar day |
| `baomoi_news_job` | Daily at 02:00 Vietnam time, starting 2026-11-24 | Collect the previous calendar day |
| `financial_news_job` | Manual batch | Run full HTML collection for the five Financial News publishers |
| Five `*_news_backfill_job` jobs | Manual backfill | Fetch dated HTML articles and persist full-text day batches to Node 2 |
| `news_quality_backfill_job` | Manual backfill | Reclassify existing articles for the selected date range |

For stock/index metrics, start with one EOD snapshot per trading day, then a second reconciliation around 16:30–17:00 if the provider revises closing data. If intraday analysis becomes necessary, add 15-minute snapshots during market sessions after confirming the licensed rate and storage limits; there is no need to collect every stock every five minutes at the start.

The scraper pods request 100m CPU/256 MiB and have limits of 500m CPU/1 GiB for HTML backfills and 500m CPU/512–768 MiB for lighter jobs. This keeps simultaneous source runs modest for the two 8 GB VPS nodes. Dagster's webserver/daemon and database consume separate capacity.

## Configuration and secrets

Set `NEWS_DATABASE_URL`, `MARKET_DATABASE_URL`, and `SCRAPING_DATABASE_URL` to Node 2 databases `finsights_news`, `finsights_market_data`, and `finsights_scraping`. `MARKET_DATABASE_URL` uses the market-data app account, whose password CoreDB provisions from `MYSQL_SCRAPING_APP_PASSWORD`. `ASSETS_DATABASE_URL` is optional until normalized asset rows are written. All MySQL writers use `MYSQL_NODE_2_TLS_CA_FILE` and refuse plaintext connections. These collectors write to Node 2 and do not write to Node 1 or the MarketService read-model database `finsights_market`. See `.env.example` for names/placeholders; do not commit populated values. `VIETSTOCK_API_TOKEN` is a secret.

Dagster's run launcher supplies the Node 2 database URLs, app-password secrets, and MySQL CA ConfigMap to isolated K3s run pods. The code-server image or its container-context config must roll out after configuration changes so new runs use the same Node 2 settings.

For local Dagster UI:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dagster dev -m src.finsights_scraping.orchestration.definitions
```

In Dagster Launchpad, select a source job and provide both required dates, for example:

```yaml
ops:
  collect_cafef_news:
    config:
      start_date: "2026-11-01"
      end_date: "2026-11-02"
```

For full-text HTML collection, select `financial_news_job` and provide both dates. For a single publisher backfill, select the matching `*_news_backfill_job` and use this op config:

```yaml
ops:
  collect_news_backfill:
    config:
      source_key: vneconomy
      start_date: "2026-01-01"
      end_date: "2026-11-08"
      storage_mode: link_only
```

Licensed full-text sources require the matching redistribution flag, attribution, and `robots.txt` checks. The backfill fails when archive/sitemap coverage cannot prove the requested boundary or an article has no body. The smoke script validates exactly one HTML article per active publisher and does not write to MySQL.
