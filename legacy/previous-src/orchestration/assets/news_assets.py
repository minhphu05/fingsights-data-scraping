import asyncio
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from src.database.connection import get_db_session
from src.database.repository import CrawlRepository
from src.scrapers.news.cafef.scraper import CafeFNewsScraper
from src.scrapers.news.thanhnien.scraper import ThanhNienNewsScraper


@asset(
    group_name="news_scrapers",
    compute_kind="python",
    description="Crawls and extracts latest financial and business news from CafeF",
)
def cafef_articles_asset(context: AssetExecutionContext) -> Output[dict]:
    scraper = CafeFNewsScraper()
    results = asyncio.run(scraper.run(max_items=30))

    # Persist metadata and raw articles into MySQL Node 2
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
            repo.save_article(
                source_id=source.id,
                run_id=run.id,
                url=item["url"],
                title=item["title"],
                summary=item.get("summary"),
                content_raw=item.get("content_raw"),
                author=item.get("author"),
                published_at=item.get("published_at"),
                tickers=item.get("tickers"),
                metadata=item.get("metadata"),
            )

        repo.finish_run(
            run_id=run.id,
            status="SUCCESS",
            discovered=results["total_discovered"],
            scraped=results["scraped_count"],
            skipped=results["unseen_count"] - results["scraped_count"],
            failed=results["failed_count"],
        )

    context.log.info(f"Successfully scraped {results['scraped_count']} CafeF articles.")

    return Output(
        value=results,
        metadata={
            "total_discovered": MetadataValue.int(results["total_discovered"]),
            "scraped_count": MetadataValue.int(results["scraped_count"]),
            "failed_count": MetadataValue.int(results["failed_count"]),
            "source": MetadataValue.text("cafef_news"),
        },
    )


@asset(
    group_name="news_scrapers",
    compute_kind="python",
    description="Crawls and extracts financial and economy news from Báo Thanh Niên",
)
def thanhnien_articles_asset(context: AssetExecutionContext) -> Output[dict]:
    scraper = ThanhNienNewsScraper()
    results = asyncio.run(scraper.run(max_items=30))

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
            repo.save_article(
                source_id=source.id,
                run_id=run.id,
                url=item["url"],
                title=item["title"],
                summary=item.get("summary"),
                content_raw=item.get("content_raw"),
                author=item.get("author"),
                published_at=item.get("published_at"),
                tickers=item.get("tickers"),
                metadata=item.get("metadata"),
            )

        repo.finish_run(
            run_id=run.id,
            status="SUCCESS",
            discovered=results["total_discovered"],
            scraped=results["scraped_count"],
            skipped=results["unseen_count"] - results["scraped_count"],
            failed=results["failed_count"],
        )

    context.log.info(f"Successfully scraped {results['scraped_count']} Thanh Nien articles.")

    return Output(
        value=results,
        metadata={
            "total_discovered": MetadataValue.int(results["total_discovered"]),
            "scraped_count": MetadataValue.int(results["scraped_count"]),
            "failed_count": MetadataValue.int(results["failed_count"]),
            "source": MetadataValue.text("thanhnien_economy"),
        },
    )
