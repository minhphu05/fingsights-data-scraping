import asyncio
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from src.database.connection import get_db_session
from src.database.repository import CrawlRepository
from src.scrapers.stocks.vietstock_indices.scraper import VietstockIndicesScraper
from src.scrapers.stocks.fireant.scraper import FireAntStockScraper


@asset(
    group_name="stock_scrapers",
    compute_kind="python",
    description="Crawls official Vietnam market indices (VNINDEX, VN30, HNX, UPCOM) from Vietstock",
)
def vietstock_indices_asset(context: AssetExecutionContext) -> Output[dict]:
    scraper = VietstockIndicesScraper()
    results = asyncio.run(scraper.run(max_items=10))

    with get_db_session() as session:
        repo = CrawlRepository(session)
        source = repo.get_or_create_source(
            source_code=scraper.source_code,
            name=scraper.name,
            category=scraper.category,
            base_url=scraper.base_url,
            rate_limit_rps=scraper.rate_limit_rps,
        )
        run = repo.start_run(
            source_id=source.id,
            dagster_run_id=context.run_id,
        )

        for item in results["items"]:
            repo.save_stock_quote(
                source_id=source.id,
                ticker=item["ticker"],
                trade_date=item["trade_date"],
                run_id=run.id,
                price_open=item.get("price_open"),
                price_high=item.get("price_high"),
                price_low=item.get("price_low"),
                price_close=item.get("price_close"),
                price_change=item.get("price_change"),
                pct_change=item.get("pct_change"),
                total_volume=item.get("total_volume"),
                total_value=item.get("total_value"),
                raw_payload=item.get("raw_payload"),
            )

        repo.finish_run(
            run_id=run.id,
            status="SUCCESS",
            discovered=results["total_discovered"],
            scraped=results["scraped_count"],
            skipped=0,
            failed=results["failed_count"],
        )

    context.log.info(f"Successfully updated {results['scraped_count']} market indices.")

    return Output(
        value=results,
        metadata={
            "indices_count": MetadataValue.int(results["scraped_count"]),
            "source": MetadataValue.text("vietstock_indices"),
        },
    )


@asset(
    group_name="stock_scrapers",
    compute_kind="python",
    description="Crawls real-time order matching and quotes for top VN30 stocks from FireAnt",
)
def fireant_stocks_asset(context: AssetExecutionContext) -> Output[dict]:
    scraper = FireAntStockScraper()
    results = asyncio.run(scraper.run(max_items=20))

    with get_db_session() as session:
        repo = CrawlRepository(session)
        source = repo.get_or_create_source(
            source_code=scraper.source_code,
            name=scraper.name,
            category=scraper.category,
            base_url=scraper.base_url,
            rate_limit_rps=scraper.rate_limit_rps,
        )
        run = repo.start_run(
            source_id=source.id,
            dagster_run_id=context.run_id,
        )

        for item in results["items"]:
            repo.save_stock_quote(
                source_id=source.id,
                ticker=item["ticker"],
                trade_date=item["trade_date"],
                run_id=run.id,
                price_open=item.get("price_open"),
                price_high=item.get("price_high"),
                price_low=item.get("price_low"),
                price_close=item.get("price_close"),
                price_change=item.get("price_change"),
                pct_change=item.get("pct_change"),
                total_volume=item.get("total_volume"),
                total_value=item.get("total_value"),
                raw_payload=item.get("raw_payload"),
            )

        repo.finish_run(
            run_id=run.id,
            status="SUCCESS",
            discovered=results["total_discovered"],
            scraped=results["scraped_count"],
            skipped=0,
            failed=results["failed_count"],
        )

    context.log.info(f"Successfully updated {results['scraped_count']} stock quotes.")

    return Output(
        value=results,
        metadata={
            "stocks_count": MetadataValue.int(results["scraped_count"]),
            "source": MetadataValue.text("fireant_stocks"),
        },
    )
