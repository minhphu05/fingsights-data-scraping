from src.orchestration.assets.news_assets import (
    cafef_articles_asset,
    thanhnien_articles_asset,
)
from src.orchestration.assets.stock_assets import (
    vietstock_indices_asset,
    fireant_stocks_asset,
)

news_assets = [cafef_articles_asset, thanhnien_articles_asset]
stock_assets = [vietstock_indices_asset, fireant_stocks_asset]
all_assets = news_assets + stock_assets

__all__ = [
    "cafef_articles_asset",
    "thanhnien_articles_asset",
    "vietstock_indices_asset",
    "fireant_stocks_asset",
    "news_assets",
    "stock_assets",
    "all_assets",
]
