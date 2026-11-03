# Finsights data collection

Python source adapters and Dagster jobs for financial news and market data. Each configured source has its own Dagster job. With the K3s run launcher, one triggered job runs in one short-lived pod; the CafeF and Vietstock jobs do not share a scraping pod.

## Data ownership

| Data | Destination | Purpose |
| --- | --- | --- |
| Dagster run and event history | PostgreSQL `dagster` | Dagster's own orchestration state |
| Source, run, item, fetch-attempt and checkpoint audit | MySQL Node 2 `finsights_scraping` | Scrape history and retry/reconciliation evidence only |
| Articles, ordered text/media/table blocks, authors and security matches | MySQL Node 1 `finsights_news` | News product data |
| Securities, daily prices, index/quote snapshots and canonical aliases | MySQL Node 1 `finsights_market` | Market facts and symbol/company matching |
| Binary assets and object location metadata | OCI Object Storage; article/block metadata in MySQL Node 1 | News image/audio/table uploads are enabled per source config when OCI credentials are present |

Node 2 does not hold a second copy of news bodies or market facts. `finsights_scraping` records stable source IDs and the destination UID so a run can be audited without becoming a content database.

## Layout

```text
configs/
  news/ticker-matching.yaml
  reference/security-aliases.yaml
  sources/news/cafef.yaml
  sources/news/{thanhnien,tuoitre,baomoi,vnexpress}.yaml
  sources/market/vietstock.yaml
  sources/market/vietstock-datafeed-mapping.example.yaml
  storage/object-storage.yaml
src/finsights_scraping/
  domain/                 # Date-window and normalized data contracts
  sources/news/cafef.py   # CafeF RSS + archive + article detail adapter
  sources/news/configured_html.py # Shared config-driven adapter for other publishers
  sources/market/vietstock.py # Licensed historical DataFeed adapter
  persistence/            # Node 1 product writes and Node 2 audit writes
  storage/                # UUID asset identity and object-key rules
  orchestration/          # One Dagster job/schedule per source
legacy/previous-src/      # Archived pre-rewrite modules, retained for reference
```

## Exact date windows

All manual jobs require `start_date` and `end_date` as `YYYY-MM-DD`, inclusive in `Asia/Ho_Chi_Minh`.

CafeF uses RSS only as a recent-discovery overlap. For backfills and exact day collection, it paginates configured financial archives, fetches each article detail, reads a publisher or structured-data publication timestamp, and filters on that timestamp in Vietnam time. It stops only when a fully parsed archive page is older than `start_date`. If dates are absent, an archive is empty unexpectedly, or the page limit is reached before that boundary, the run fails so it cannot report a partial window as complete.

CafeF publisher IDs are extracted from `.chn` URLs and stored with the normalized article. Canonical URL and publisher ID are unique ingestion keys. Body content is stored as ordered blocks, preserving paragraph breaks and the order of headings, quotes, images, audio, video, lists, and tables. Reprints keep their source-origin URL separately when the page exposes it.

Thanh Niên, Tuổi Trẻ, Báo Mới, and VnExpress each have a source YAML and a separate Dagster job/pod. The shared adapter reads recent RSS where the publisher supplies it, uses a source archive for backfills, parses the article's publisher ID and publication timestamp, applies the inclusive Vietnam-local date window, and keeps the same ordered content-block format as CafeF. A job fails if it cannot reach an article older than `start_date`, or if an article has no reliable publication timestamp. RSS is only a recent overlap; it is never treated as historical coverage.

All four sources are enabled in `personal_academic_research` scope for this thesis. Their source configs set internal-only use, attribution required, redistribution disabled, and robots.txt checks on. Set `collection_enabled: false` to pause an individual source. The adapter uses publisher pages and documented RSS only; it does not call undocumented APIs. Báo Mới is configured as an excerpt/aggregator record and does not claim its text is the original publisher's complete article. Date-window guarantees still depend on the configured archive pagination and selectors matching the publisher's live pages; a run fails when it cannot prove the requested boundary.

Source publication and quote timestamps are converted to UTC for `DATETIME` columns; `business_date` / `trade_date` remain Vietnam-local calendar dates for exact range filtering.

The public Vietstock board is a live/current view. The job therefore uses only a licensed DataFeed/API response with a provider trade date and observation time. Its required coverage includes all active stock symbols on HOSE, HNX, and UPCOM plus VN-Index, VN30-Index, HNX-Index, HNX30-Index, and UPCoM-Index. Optional extensions include VNAllshare, VNMidcap, VN100, VNSmallcap, HNX30TRI, and the UPCoM Large/Medium/Small indices; enable them only if the DataFeed contract supplies those fields. HOSE lists its full index family and HNX lists additional HNX/UPCoM benchmarks on their official pages: [HOSE index factsheet](https://staticfile.hsx.vn/Uploads/UploadDocuments/2478336/Form_Factsheet_MCIndices_VN_T07.2026.pdf) and [HNX stock/index page](https://www.hnx.vn/vi-vn/co-phieu.html). The current adapter rejects missing dates, rows outside the requested window, and a date with no quote on one of the three exchanges or a missing required index. That confirms exchange/index presence, not every active symbol; full-universe assurance still depends on the DataFeed contract guaranteeing its symbol coverage or a separate reconciliation against an official active-symbol roster. The example mapping is deliberately blank: fill it from the contract before enabling the scheduled job. No old date will be fabricated from today's live quote.

## Financial relevance and ticker matching

CafeF starts with the stock-market, company finance, banking, and macro-investment feeds/archives, then applies financial relevance terms. Matching against a listed company uses `market_security_aliases` in Node 1: ticker, legal/short names, brand or former names, with effective dates. A short symbol must appear as a whole token and have financial context; company-name phrase matches retain the matched alias, evidence snippet, method, and confidence for review. This reduces false positives from ordinary words and abbreviations.

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

Every object has a UUID `asset_uid`; SHA-256 and size support content deduplication. The four new source configs enable image/audio/table uploads for internal thesis research; uploads require the matching Tokyo OCI credentials in the runtime environment. If credentials are absent, the uploader stays in dry-run mode. CafeF remains link-only until its config explicitly enables binary collection. Object keys and asset IDs are stored in news media/block metadata; a separate normalized registry write into `finsights_assets` is not implemented yet. No access keys or expiring URLs belong in the database.

## Schedules and collection frequency

| Job | Default schedule | Behavior |
| --- | --- | --- |
| `cafef_news_job` | Daily at 02:15 Vietnam time | Collect the previous calendar day after it has elapsed |
| `vietstock_market_job` | Weekdays at 16:30 Vietnam time | EOD snapshot after the session; Dagster skips it until the licensed endpoint/token are configured |
| `thanhnien_news_job` | Daily at 02:30 Vietnam time | Collect the previous calendar day |
| `tuoitre_news_job` | Daily at 02:45 Vietnam time | Collect the previous calendar day |
| `baomoi_news_job` | Daily at 03:00 Vietnam time | Collect the previous calendar day |
| `vnexpress_news_job` | Daily at 03:15 Vietnam time | Collect the previous calendar day |

For stock/index metrics, start with one EOD snapshot per trading day, then a second reconciliation around 16:30–17:00 if the provider revises closing data. If intraday analysis becomes necessary, add 15-minute snapshots during market sessions after confirming the licensed rate and storage limits; there is no need to collect every stock every five minutes at the start.

The scraper pods request 100m CPU/256 MiB and have limits of 500m CPU/512–768 MiB. This keeps simultaneous source runs modest for the two 8 GB VPS nodes. Dagster's webserver/daemon and database consume separate capacity.

## Configuration and secrets

Set `NEWS_DATABASE_URL`, `MARKET_DATABASE_URL`, and `SCRAPING_DATABASE_URL` as Kubernetes Secret-backed variables. They must use the dedicated Node 1/Node 2 app users. Mount the Node-specific CA files and set `MYSQL_NODE_1_TLS_CA_FILE` / `MYSQL_NODE_2_TLS_CA_FILE`; the SQLAlchemy engines refuse to connect without a CA and verify the trusted certificate chain. See `.env.example` for names and placeholders; do not commit populated values. `VIETSTOCK_API_TOKEN` is a secret. The DataFeed URL/field mapping is filled only from Vietstock's licensed contract.

For run pods, Dagster's `runK8sConfig` (or the equivalent run-level K8s configuration) must expose those variables and mount both CA files. Create keys for the three database URLs in the `finsights-scraper-runtime` Kubernetes Secret, plus the Vietstock API token in `vietstock-datafeed`; expose the non-secret endpoint/mapping path separately. The code location pod alone does not pass its process environment into K3s run pods automatically.

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
