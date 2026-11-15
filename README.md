# Finsights data collection

Python source adapters and Dagster jobs for financial news and market data. Each configured source has its own Dagster job, and `financial_news_job` runs the five Financial News RSS publishers in one date-window batch. With the K3s run launcher, each triggered job runs in an isolated pod.

## Data ownership

| Data | Destination | Purpose |
| --- | --- | --- |
| Dagster run and event history | PostgreSQL `dagster` | Dagster's own orchestration state |
| Source, run, item, fetch-attempt and checkpoint audit | MySQL Node 2 `finsights_scraping` | Scrape history and retry/reconciliation evidence only |
| Articles, RSS summaries, publishers, authors and security matches | MySQL Node 2 `finsights_news` | Canonical news product data |
| Securities, daily prices, index/quote snapshots and canonical aliases | MySQL Node 2 `finsights_market_data` | Canonical market facts and symbol/company matching |
| Binary assets and object location metadata | OCI Object Storage; references in MySQL Node 2 | Financial News RSS collection stores links and feed excerpts only; it does not download publisher media |

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
  sources/news/rss.py     # Shared RSS-only metadata collector
  sources/news/configured_html.py # Legacy/config-driven archive adapter
  sources/market/vietstock.py # Licensed historical DataFeed adapter
  persistence/            # Node 2 canonical data writes and collection audit writes
  storage/                # UUID asset identity and object-key rules
  orchestration/          # One Dagster job/schedule per source
legacy/previous-src/      # Archived pre-rewrite modules, retained for reference
```

## Exact date windows

Manual news jobs accept inclusive `start_date` and `end_date` values in `YYYY-MM-DD` format, interpreted in `Asia/Ho_Chi_Minh`. Locally, use `scripts/run_local.py --source all --start-date ... --end-date ...`; output is grouped under a date-window directory.

The Financial News group is CafeF, Tuổi Trẻ, Thanh Niên, VnExpress, and VnEconomy. It reads each publisher's RSS feeds only and stores title, feed summary, source link, publication time, author/category when present, and ticker matches. It does not fetch article pages or media. Publisher IDs (or a stable URL hash where the RSS feed has no ID) and canonical URLs make repeated runs idempotent. RSS entries are filtered to the requested inclusive date window and written in publication-day batches.

RSS endpoints generally expose a rolling set of recent items rather than a historical export. The run reports each feed's oldest/newest item and marks a requested window as `RSS_RETENTION_LIMITED` when its start predates the visible feed. A successful RSS request therefore does not claim that every article since `start_date` was available. For a deeper backfill, use a publisher-provided archive or a licensed historical feed after confirming its allowed use.

The five publisher configs require internal-only collection, attribution, no redistribution, and robots.txt checks. Set `collection_enabled: false` to pause a source. VnEconomy is the added finance-focused source; its RSS page lists finance, securities, latest, and business feeds and asks that publishers identify the source: https://vneconomy.vn/rss.html.

Source publication and quote timestamps are converted to UTC for `DATETIME` columns; `business_date` / `trade_date` remain Vietnam-local calendar dates for exact range filtering.

The public Vietstock board is a live/current view. The job therefore uses only a licensed DataFeed/API response with a provider trade date and observation time. Its required coverage includes all active stock symbols on HOSE, HNX, and UPCOM plus VN-Index, VN30-Index, HNX-Index, HNX30-Index, and UPCoM-Index. Optional extensions include VNAllshare, VNMidcap, VN100, VNSmallcap, HNX30TRI, and the UPCoM Large/Medium/Small indices; enable them only if the DataFeed contract supplies those fields. HOSE lists its full index family and HNX lists additional HNX/UPCoM benchmarks on their official pages: [HOSE index factsheet](https://staticfile.hsx.vn/Uploads/UploadDocuments/2478336/Form_Factsheet_MCIndices_VN_T07.2026.pdf) and [HNX stock/index page](https://www.hnx.vn/vi-vn/co-phieu.html). The current adapter rejects missing dates, rows outside the requested window, and a date with no quote on one of the three exchanges or a missing required index. That confirms exchange/index presence, not every active symbol; full-universe assurance still depends on the DataFeed contract guaranteeing its symbol coverage or a separate reconciliation against an official active-symbol roster. The example mapping is deliberately blank: fill it from the contract before enabling the scheduled job. No old date will be fabricated from today's live quote.

## Financial relevance and ticker matching

CafeF starts with stock-market, company finance, banking, and macro-investment RSS feeds, then applies financial relevance terms. Matching against a listed company uses `market_security_aliases` in Node 2 `finsights_market_data`: ticker, legal/short names, brand or former names, with effective dates. A short symbol must appear as a whole token and have financial context; company-name phrase matches retain the matched alias, evidence snippet, method, and confidence for review. This reduces false positives from ordinary words and abbreviations.

An exact `SHB` symbol hit is strong only as a token in financial context. Company aliases such as the legal bank name and known former/brand names should be maintained in `market_security_aliases`; the scraper does not hard-code company name guesses.

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
| `cafef_news_job` | Daily at 02:15 Vietnam time | Collect the previous calendar day after it has elapsed |
| `vietstock_market_job` | Weekdays at 16:30 Vietnam time | EOD snapshot after the session; Dagster skips it until the licensed endpoint/token are configured |
| `thanhnien_news_job` | Daily at 02:30 Vietnam time | Collect the previous calendar day |
| `tuoitre_news_job` | Daily at 02:45 Vietnam time | Collect the previous calendar day |
| `baomoi_news_job` | Daily at 03:00 Vietnam time | Collect the previous calendar day |
| `vnexpress_news_job` | Daily at 03:15 Vietnam time | Collect the previous calendar day |
| `vneconomy_news_job` | Daily at 04:00 Vietnam time | Collect the previous calendar day |
| `financial_news_job` | Manual batch | Run all five RSS publishers for the requested date range |

For stock/index metrics, start with one EOD snapshot per trading day, then a second reconciliation around 16:30–17:00 if the provider revises closing data. If intraday analysis becomes necessary, add 15-minute snapshots during market sessions after confirming the licensed rate and storage limits; there is no need to collect every stock every five minutes at the start.

The scraper pods request 100m CPU/256 MiB and have limits of 500m CPU/512–768 MiB. This keeps simultaneous source runs modest for the two 8 GB VPS nodes. Dagster's webserver/daemon and database consume separate capacity.

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

For one of the new sources, choose its job and pass `source_key` (`thanhnien`, `tuoitre`, `baomoi`, or `vnexpress`) with the same inclusive dates. The source job is enabled by its YAML. Archive pagination patterns and selectors still need a first collection to confirm against the publisher's current markup; any robots.txt denial or inability to prove the requested date boundary fails the run.

```yaml
ops:
  collect_configured_news:
    config:
      source_key: vnexpress
      start_date: "2026-11-01"
      end_date: "2026-11-02"
```

No database reconciliation, production deployment, or scraping run is performed by editing these source/config files.
