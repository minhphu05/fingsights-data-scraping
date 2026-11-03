from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..domain.models import Article, ContentBlock
from ..storage import OciS3Uploader, build_asset_key, new_asset_uid
from .mysql_engine import create_tls_mysql_engine
from .security_matching import SecurityAliasMatcher

logger = logging.getLogger("finsights.persistence.news")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_naive(value):
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _plain_text(blocks: list[ContentBlock]) -> str:
    content = ""
    for block in blocks:
        if block.block_type == "line_break":
            content += "\n"
            continue
        if block.text:
            if content and not content.endswith("\n"):
                content += "\n\n"
            content += block.text
        elif block.block_type == "image" and block.media:
            caption = block.media.get("caption")
            if caption:
                if content and not content.endswith("\n"):
                    content += "\n\n"
                content += str(caption)
    return content


class Node1NewsWriter:
    """Idempotent writes to Node 1 news; binary assets remain link-only by default."""

    def __init__(
        self,
        news_engine: Engine | None = None,
        market_engine: Engine | None = None,
        assets_engine: Engine | None = None,
        storage_mode: str = "link_only",
    ):
        self.storage_mode = storage_mode.strip().lower()
        news_url = os.getenv("NEWS_DATABASE_URL", "").strip()
        market_url = os.getenv("MARKET_DATABASE_URL", "").strip()
        assets_url = os.getenv("ASSETS_DATABASE_URL", "").strip()
        if news_engine is None:
            if not news_url:
                raise RuntimeError("NEWS_DATABASE_URL is required for Node 1 news persistence")
            news_engine = create_tls_mysql_engine(news_url, os.getenv("MYSQL_NODE_1_TLS_CA_FILE", ""))
        if market_engine is None:
            if not market_url:
                raise RuntimeError("MARKET_DATABASE_URL is required for canonical ticker/company matching")
            market_engine = create_tls_mysql_engine(market_url, os.getenv("MYSQL_NODE_1_TLS_CA_FILE", ""))
        if assets_engine is None and assets_url:
            assets_engine = create_tls_mysql_engine(assets_url, os.getenv("MYSQL_NODE_1_TLS_CA_FILE", ""))
        self.news_engine = news_engine
        self.market_engine = market_engine
        self.assets_engine = assets_engine
        self.matcher = SecurityAliasMatcher(market_engine) if market_engine is not None else None
        self.uploader = OciS3Uploader()

    def persist_articles(self, articles: list[Article]) -> dict[str, str]:
        article_uids: dict[str, str] = {}
        for article in articles:
            article_uids[article.source_article_id] = self._persist_article(article)
        return article_uids

    def _persist_article(self, article: Article) -> str:
        canonical_hash = _sha256(article.canonical_url)
        plaintext = _plain_text(article.blocks)
        content_hash = _sha256(plaintext)
        content_fingerprint = str(article.metadata.get("content_fingerprint_sha256") or content_hash)
        article_uid = str(uuid.uuid4())
        published_at = _utc_naive(article.published_at)
        local_published_date = article.published_at.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
        local_day_start_utc = _utc_naive(datetime.combine(local_published_date, time.min, ZoneInfo("Asia/Ho_Chi_Minh")))
        local_day_end_utc = _utc_naive(datetime.combine(local_published_date + timedelta(days=1), time.min, ZoneInfo("Asia/Ho_Chi_Minh")))
        modified_at = article.metadata.get("modified_at")
        if modified_at is not None and hasattr(modified_at, "astimezone"):
            modified_at = _utc_naive(modified_at)
        metadata_json = json.dumps(article.metadata, ensure_ascii=False, default=str)

        with self.news_engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO news_publishers
                    (code, name, domain, homepage_url, language_code, attribution_name, content_rights, is_active)
                VALUES (:code, :name, :domain, :homepage_url, 'vi', :attribution_name, :content_rights, 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name), homepage_url=VALUES(homepage_url),
                    attribution_name=VALUES(attribution_name), content_rights=VALUES(content_rights), is_active=1
            """), {
                "code": article.publisher_code,
                "name": article.metadata.get("publisher_name", article.publisher_code),
                "domain": article.metadata.get("publisher_domain", "cafef.vn"),
                "homepage_url": article.metadata.get("homepage_url", "https://cafef.vn/"),
                "attribution_name": article.metadata.get("attribution_name", article.publisher_code),
                "content_rights": article.metadata.get("rights_status", "unknown"),
            })
            publisher_id = connection.execute(
                text("SELECT id FROM news_publishers WHERE code=:code"), {"code": article.publisher_code}
            ).scalar_one()

            existing = connection.execute(text("""
                SELECT id, article_uid FROM news_articles
                WHERE publisher_id=:publisher_id AND
                    (publisher_article_id=:source_id OR canonical_url_sha256=:canonical_hash)
                LIMIT 1
            """), {
                "publisher_id": publisher_id,
                "source_id": article.source_article_id,
                "canonical_hash": canonical_hash,
            }).mappings().first()
            write_canonical_content = True
            is_primary_publication = True
            if existing:
                database_article_id = int(existing["id"])
                article_uid = str(existing["article_uid"])
                connection.execute(text("""
                    UPDATE news_articles SET publisher_article_id=:source_id, canonical_url=:url,
                        canonical_url_sha256=:url_hash, title=:title, subtitle=:subtitle, summary=:summary,
                        original_section=:section, published_at=:published_at, modified_at=:modified_at,
                        cover_image_url=:cover_url, cover_image_alt=:cover_alt, content_rights=:content_rights,
                        metadata_json=:metadata,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=:article_id
                """), {
                    "source_id": article.source_article_id, "url": article.canonical_url,
                    "url_hash": canonical_hash, "title": article.title, "subtitle": article.subtitle,
                    "summary": article.summary, "section": article.section,
                    "published_at": published_at, "modified_at": modified_at,
                    "cover_url": article.cover_image_url, "cover_alt": article.cover_image_alt,
                    "content_rights": article.metadata.get("rights_status", "unknown"),
                    "metadata": metadata_json, "article_id": database_article_id,
                })
            else:
                duplicate = connection.execute(text("""
                    SELECT a.id, a.article_uid
                    FROM news_article_publications p
                    JOIN news_articles a ON a.id=p.article_id
                    WHERE p.publisher_id=:publisher_id
                      AND p.content_fingerprint_sha256=:content_fingerprint
                      AND p.published_at >= :day_start AND p.published_at < :day_end
                    ORDER BY p.is_primary DESC, p.id ASC
                    LIMIT 1
                """), {"publisher_id": publisher_id,
                "day_start": local_day_start_utc, "day_end": local_day_end_utc,
                "content_fingerprint": content_fingerprint}).mappings().first()
                if duplicate:
                    database_article_id = int(duplicate["id"])
                    article_uid = str(duplicate["article_uid"])
                    write_canonical_content = False
                    is_primary_publication = False
                else:
                    result = connection.execute(text("""
                        INSERT INTO news_articles
                            (article_uid, publisher_id, publisher_article_id, canonical_url, canonical_url_sha256,
                             title, subtitle, summary, original_section, language_code, published_at, modified_at,
                             cover_image_url, cover_image_alt, article_status, content_rights, metadata_json)
                        VALUES
                            (:article_uid, :publisher_id, :source_id, :url, :url_hash, :title, :subtitle, :summary,
                             :section, 'vi', :published_at, :modified_at, :cover_url, :cover_alt,
                             'published', :content_rights, :metadata)
                    """), {
                        "article_uid": article_uid, "publisher_id": publisher_id,
                        "source_id": article.source_article_id, "url": article.canonical_url,
                        "url_hash": canonical_hash, "title": article.title, "subtitle": article.subtitle,
                        "summary": article.summary, "section": article.section,
                        "published_at": published_at, "modified_at": modified_at,
                        "cover_url": article.cover_image_url, "cover_alt": article.cover_image_alt,
                        "content_rights": article.metadata.get("rights_status", "unknown"),
                        "metadata": metadata_json,
                    })
                    database_article_id = int(result.lastrowid)

            connection.execute(text("""
                INSERT INTO news_article_publications
                    (article_id, publisher_id, publisher_article_id, canonical_url, canonical_url_sha256,
                     original_source_url, published_at, title_override, summary_override,
                     content_fingerprint_sha256, is_primary)
                VALUES (:article_id, :publisher_id, :source_id, :url, :url_hash, :original_url,
                    :published_at, :title, :summary, :content_hash, :is_primary)
                ON DUPLICATE KEY UPDATE article_id=VALUES(article_id), original_source_url=VALUES(original_source_url),
                    published_at=VALUES(published_at), title_override=VALUES(title_override),
                    summary_override=VALUES(summary_override), content_fingerprint_sha256=VALUES(content_fingerprint_sha256),
                    is_primary=GREATEST(is_primary, VALUES(is_primary))
            """), {
                "article_id": database_article_id, "publisher_id": publisher_id,
                "source_id": article.source_article_id, "url": article.canonical_url,
                "url_hash": canonical_hash, "original_url": article.original_source_url,
                "published_at": published_at, "title": article.title, "summary": article.summary,
                "content_hash": content_fingerprint, "is_primary": is_primary_publication,
            })

            if write_canonical_content:
                connection.execute(text("""
                INSERT INTO news_article_contents
                    (article_id, body_plaintext, content_format, word_count, content_sha256, updated_at)
                VALUES (:article_id, :body, 'ordered_blocks', :word_count, :content_hash, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE body_plaintext=VALUES(body_plaintext), content_format=VALUES(content_format),
                    word_count=VALUES(word_count), content_sha256=VALUES(content_sha256), updated_at=CURRENT_TIMESTAMP
                """), {
                    "article_id": database_article_id, "body": plaintext,
                    "word_count": len(plaintext.split()), "content_hash": content_hash,
                })

            if write_canonical_content:
                # Replacing the ordered representation makes retries deterministic and duplicate-safe.
                connection.execute(text("DELETE FROM news_article_content_blocks WHERE article_id=:id"), {"id": database_article_id})
                connection.execute(text("DELETE FROM news_article_media WHERE article_id=:id"), {"id": database_article_id})
            target_storage = self.uploader.route_source_target(article.publisher_code)
            binary_upload_allowed = (
                self.storage_mode not in ("link_only", "links_only", "none")
                and (
                    article.metadata.get("binary_asset_collection_enabled") is True
                    or (
                        article.metadata.get("rights_status", "unknown") in set(
                            article.metadata.get("download_binary_assets_only_if_rights_status_in", [])
                        )
                    )
                )
            )
            for block in (article.blocks if write_canonical_content else []):
                media_id = None
                asset_uid = None
                if block.media:
                    media = block.media
                    asset_uid = media.get("asset_uid") or new_asset_uid()
                    ext = "jpg" if block.block_type == "image" else ("mp3" if block.block_type == "audio" else "bin")
                    media["asset_uid"] = asset_uid
                    if binary_upload_allowed:
                        object_key = build_asset_key(
                            domain="news",
                            source_code=article.publisher_code,
                            published_date=local_published_date,
                            article_uid=article_uid,
                            asset_uid=asset_uid,
                            asset_kind=block.block_type if block.block_type in ("image", "audio", "table", "html") else "image",
                            extension=ext,
                        )
                        media["object_key"] = object_key
                    else:
                        media["storage_state"] = "external_link_only_binary_collection_disabled"

                    # Download and upload only when explicitly enabled for this source.
                    upload_enabled = os.getenv("ENABLE_OCI_MEDIA_UPLOAD", "true").casefold() in ("true", "1", "yes")
                    if binary_upload_allowed and not upload_enabled:
                        media["storage_state"] = "upload_disabled_by_environment"
                    if binary_upload_allowed and upload_enabled:
                        try:
                            import urllib.request
                            req = urllib.request.Request(
                                media["original_url"],
                                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                            )
                            with urllib.request.urlopen(req, timeout=10) as resp:
                                media_bytes = resp.read()
                                mime = resp.headers.get("Content-Type", "image/jpeg" if block.block_type == "image" else "application/octet-stream")
                                upload_result = self.uploader.upload_bytes(
                                    data=media_bytes,
                                    object_key=object_key,
                                    mime_type=mime,
                                    target_name=target_storage,
                                )
                            media["storage_state"] = "uploaded" if upload_result["uploaded"] else "pending_credentials"
                            media["object_storage"] = upload_result
                        except Exception as exc:
                            logger.warning("Media upload failed for %s (%s)", asset_uid, type(exc).__name__)
                            media["storage_state"] = "upload_failed"
                            media["storage_error_type"] = type(exc).__name__

                    result = connection.execute(text("""
                        INSERT INTO news_article_media
                            (article_id, media_type, role, original_url, asset_uid, alt_text, caption, credit,
                             sort_order, metadata_json)
                        VALUES (:article_id, :media_type, 'inline', :url, :asset_uid, :alt, :caption, :credit, :sort_order, :metadata)
                    """), {
                        "article_id": database_article_id, "media_type": block.block_type,
                        "url": media["original_url"], "asset_uid": asset_uid, "alt": media.get("alt"),
                        "caption": media.get("caption"), "credit": media.get("credit"),
                        "sort_order": block.order,
                        "metadata": json.dumps(media, ensure_ascii=False, default=str),
                    })
                    media_id = int(result.lastrowid)

                table_json_str = None
                if block.table:
                    if block.metadata is None:
                        block.metadata = {}
                    table_json_str = json.dumps(block.table, ensure_ascii=False)
                    if binary_upload_allowed:
                        table_asset_uid = new_asset_uid()
                        table_key = build_asset_key(
                            domain="news",
                            source_code=article.publisher_code,
                            published_date=local_published_date,
                            article_uid=article_uid,
                            asset_uid=table_asset_uid,
                            asset_kind="table",
                            extension="json",
                        )
                        block.metadata["asset_uid"] = table_asset_uid
                        block.metadata["object_key"] = table_key
                        if os.getenv("ENABLE_OCI_MEDIA_UPLOAD", "true").casefold() in ("true", "1", "yes"):
                            try:
                                upload_result = self.uploader.upload_json(
                                    block.table, object_key=table_key, target_name=target_storage
                                )
                                block.metadata["storage_state"] = "uploaded" if upload_result["uploaded"] else "pending_credentials"
                                block.metadata["object_storage"] = upload_result
                            except Exception as exc:
                                logger.warning("Table upload failed for %s (%s)", table_asset_uid, type(exc).__name__)
                                block.metadata["storage_state"] = "upload_failed"
                                block.metadata["storage_error_type"] = type(exc).__name__
                        else:
                            block.metadata["storage_state"] = "upload_disabled_by_environment"

                connection.execute(text("""
                    INSERT INTO news_article_content_blocks
                        (article_id, block_order, block_type, text_content, media_id, table_payload_json, metadata_json)
                    VALUES (:article_id, :block_order, :block_type, :text_content, :media_id, :table_json, :metadata)
                """), {
                    "article_id": database_article_id, "block_order": block.order,
                    "block_type": block.block_type, "text_content": block.text, "media_id": media_id,
                    "table_json": table_json_str,
                    "metadata": json.dumps(block.metadata, ensure_ascii=False, default=str),
                })

            if article.author:
                author_key = _sha256(article.author.casefold())[:48]
                author_id = connection.execute(text("""
                    SELECT id FROM news_authors WHERE publisher_id=:publisher_id AND author_key=:author_key LIMIT 1
                """), {"publisher_id": publisher_id, "author_key": author_key}).scalar_one_or_none()
                if author_id is None:
                    result = connection.execute(text("""
                        INSERT INTO news_authors (publisher_id, display_name, author_key) VALUES (:publisher_id, :name, :key)
                    """), {"publisher_id": publisher_id, "name": article.author[:220], "key": author_key})
                    author_id = int(result.lastrowid)
                connection.execute(text("""
                    INSERT INTO news_article_authors (article_id, author_id, author_order)
                    VALUES (:article_id, :author_id, 1)
                    ON DUPLICATE KEY UPDATE author_order=VALUES(author_order)
                """), {"article_id": database_article_id, "author_id": author_id})

            if self.matcher:
                if write_canonical_content:
                    connection.execute(text("DELETE FROM news_article_securities WHERE article_id=:id"),
                                       {"id": database_article_id})
                table_text = [
                    " ".join(str(cell) for row in block.table.get("rows", []) for cell in row)
                    for block in article.blocks if block.table
                ]
                matching_text = "\n".join([
                    article.title, article.summary or "", *(b.text or "" for b in article.blocks), *table_text
                ])
                published_local_date = article.published_at.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
                for match in self.matcher.match(matching_text, published_local_date):
                    connection.execute(text("""
                        INSERT INTO news_article_securities
                            (article_id, security_uid, symbol_at_publication, relation_type, is_primary,
                             match_method, match_confidence, matched_alias, evidence_text, review_status)
                        VALUES (:article_id, :security_uid, :symbol, 'mentioned', 0, :method,
                                :confidence, :alias, :evidence, :review_status)
                        ON DUPLICATE KEY UPDATE match_method=VALUES(match_method),
                            match_confidence=VALUES(match_confidence), matched_alias=VALUES(matched_alias),
                            evidence_text=VALUES(evidence_text), review_status=VALUES(review_status)
                    """), {
                        "article_id": database_article_id, "security_uid": match.security_uid,
                        "symbol": match.symbol, "method": match.method, "confidence": match.confidence,
                        "alias": match.alias_text, "evidence": match.evidence,
                        "review_status": "auto" if match.confidence >= 0.95 and match.method == "ticker_boundary" else "pending",
                    })
        return article_uid

