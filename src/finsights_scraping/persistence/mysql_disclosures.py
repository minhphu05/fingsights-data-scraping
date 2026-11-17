from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import os
import re
import ssl
import time
import uuid
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path, PurePosixPath
from typing import Any
from unicodedata import normalize
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url

from ..domain.disclosures import DisclosureRecord
from ..storage import OciS3Uploader
from .mysql_engine import create_tls_mysql_engine

logger = logging.getLogger("finsights.persistence.disclosures")


def _utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value


def _canonical_hash(value: str | None) -> str | None:
    return hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None


def _name_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", normalized.casefold()).strip()


class Node2DisclosureWriter:
    """Idempotent persistence to Node 2 disclosure tables; no writes are made to Node 1."""

    def __init__(
        self, *, disclosures_engine: Engine | None = None, market_engine: Engine | None = None,
        attachment_storage_mode: str = "link_only", tls_extra_ca_file: str | None = None,
    ):
        self.attachment_storage_mode = attachment_storage_mode.strip().casefold()
        self.max_attachment_bytes = int(os.getenv("DISCLOSURE_MAX_ATTACHMENT_BYTES", str(50 * 1024 * 1024)))
        self.attachments_uploaded = 0
        self.attachments_pending = 0
        self.attachments_failed = 0
        self.attachment_attempts: list[dict[str, Any]] = []
        self.uploader = OciS3Uploader()
        verify: bool | ssl.SSLContext = True
        if tls_extra_ca_file:
            ca_path = Path(tls_extra_ca_file)
            if not ca_path.is_absolute():
                project_root = Path(__file__).resolve().parents[3]
                ca_path = (project_root / ca_path).resolve()
            if not ca_path.is_file():
                raise FileNotFoundError(f"Configured TLS CA certificate does not exist: {ca_path}")
            verify = ssl.create_default_context()
            verify.load_verify_locations(cafile=str(ca_path))
        self.document_client = httpx.Client(
            timeout=httpx.Timeout(60), follow_redirects=True, verify=verify,
            headers={"User-Agent": "FinsightsDataCollector/1.0 (+https://finsights.vn; public market information)"},
        )
        ca_file = os.getenv("MYSQL_NODE_2_TLS_CA_FILE", "")
        if disclosures_engine is None:
            explicit_url = os.getenv("DISCLOSURES_DATABASE_URL", "").strip()
            if explicit_url:
                disclosures_engine = create_tls_mysql_engine(
                    explicit_url,
                    ca_file,
                    password_env="MYSQL_SCRAPING_APP_PASSWORD",
                )
            else:
                market_url = os.getenv("MARKET_DATABASE_URL", "").strip()
                shared_password = os.getenv("MYSQL_SCRAPING_APP_PASSWORD", "")
                if not market_url or not shared_password:
                    raise RuntimeError(
                        "DISCLOSURES_DATABASE_URL or Node 2 MARKET_DATABASE_URL plus "
                        "MYSQL_SCRAPING_APP_PASSWORD is required"
                    )
                base = make_url(market_url)
                disclosure_url = base.set(
                    database="finsights_disclosures",
                    username="finsights_disclosures_app",
                    password=shared_password,
                )
                disclosures_engine = create_tls_mysql_engine(
                    str(disclosure_url),
                    ca_file,
                    password_env="MYSQL_SCRAPING_APP_PASSWORD",
                )
        if market_engine is None:
            market_url = os.getenv("MARKET_DATABASE_URL", "").strip()
            if not market_url:
                raise RuntimeError("MARKET_DATABASE_URL is required to match disclosure symbols to Node 2 securities")
            market_engine = create_tls_mysql_engine(
                market_url,
                ca_file,
                password_env="MYSQL_SCRAPING_APP_PASSWORD",
            )
        self.disclosures_engine = disclosures_engine
        self.market_engine = market_engine
        self.security_by_symbol: dict[str, dict[str, str]] = {}
        self.company_by_name: dict[str, str] = {}
        self._load_market_catalog()

    def close(self) -> None:
        self.document_client.close()
        self.disclosures_engine.dispose()
        self.market_engine.dispose()

    def _load_market_catalog(self) -> None:
        with self.market_engine.connect() as connection:
            rows = connection.execute(text("""
                SELECT s.symbol, s.security_uid, c.company_uid, c.legal_name, c.short_name
                FROM market_securities s
                JOIN market_companies c ON c.id=s.company_id
                WHERE s.symbol IS NOT NULL
            """)).mappings()
            for row in rows:
                symbol = str(row["symbol"]).strip().upper()
                if symbol:
                    self.security_by_symbol[symbol] = {
                        "security_uid": str(row["security_uid"]),
                        "company_uid": str(row["company_uid"]),
                    }
                for label in (row.get("legal_name"), row.get("short_name")):
                    key = _name_key(str(label) if label else None)
                    if key:
                        self.company_by_name[key] = str(row["company_uid"])

    def _identity(self, record: DisclosureRecord, target: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"finsights:{target}:{record.source_code}:{record.source_item_id}"))

    def _security(self, record: DisclosureRecord) -> dict[str, str] | None:
        if record.symbol:
            security = self.security_by_symbol.get(record.symbol.upper())
            if security:
                return security
        if record.company_name:
            company_uid = self.company_by_name.get(_name_key(record.company_name))
            if company_uid:
                return {"company_uid": company_uid, "security_uid": ""}
        return None

    def persist_records(self, records: list[DisclosureRecord]) -> dict[str, str]:
        uids: dict[str, str] = {}
        for record in records:
            self._archive_attachments(record)
            security = self._security(record)
            if record.item_type == "corporate_action" and security and security.get("security_uid"):
                disclosure_id, disclosure_uid = self._persist_issuer_disclosure(record, security)
                action_uid = self._persist_corporate_action(record, security, disclosure_id)
                uids[record.source_item_id] = action_uid or disclosure_uid
            elif record.item_type == "issuer_disclosure" and security:
                _disclosure_id, disclosure_uid = self._persist_issuer_disclosure(record, security)
                uids[record.source_item_id] = disclosure_uid
            else:
                uids[record.source_item_id] = self._persist_regulatory_notice(record, security)
        return uids

    def _archive_attachments(self, record: DisclosureRecord) -> None:
        record.metadata["attachment_storage_mode"] = self.attachment_storage_mode
        if self.attachment_storage_mode not in {"oci", "upload", "full"}:
            for attachment in record.attachments:
                attachment.setdefault("storage_state", "link_only")
            return

        local_date = (
            record.published_at.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
            if record.published_at else datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
        )
        item_root_uid = self._identity(record, "document-root")
        allowed_extensions = {"pdf", "xls", "xlsx", "doc", "docx", "ppt", "pptx", "zip", "rar", "csv"}
        for attachment in record.attachments:
            url = str(attachment.get("url") or "").strip()
            if not url:
                attachment["storage_state"] = "missing_source_url"
                continue
            if attachment.get("storage_state") == "uploaded" and attachment.get("asset_uid"):
                record.document_asset_uid = record.document_asset_uid or str(attachment["asset_uid"])
                continue

            requested_at = datetime.now(timezone.utc)
            started = time.monotonic()
            fetch_attempt: dict[str, Any] = {
                "resource_kind": "attachment",
                "url": url,
                "requested_at": requested_at,
                "attempt_number": 1,
            }
            try:
                response = self.document_client.get(url)
                response.raise_for_status()
                data = response.content
                fetch_attempt.update({
                    "completed_at": datetime.now(timezone.utc),
                    "http_status": response.status_code,
                    "content_type": response.headers.get("content-type"),
                    "size_bytes": len(data),
                    "response_sha256": hashlib.sha256(data).hexdigest(),
                    "duration_ms": round((time.monotonic() - started) * 1000),
                    "outcome": "SUCCESS",
                })
                self.attachment_attempts.append(fetch_attempt)
                if not data:
                    raise ValueError("The official source returned an empty attachment")
                if len(data) > self.max_attachment_bytes:
                    raise ValueError(
                        f"Attachment size {len(data)} exceeds configured limit {self.max_attachment_bytes}"
                    )

                disposition = Message()
                content_disposition = response.headers.get("content-disposition")
                if content_disposition:
                    disposition["content-disposition"] = content_disposition
                filename = str(
                    disposition.get_filename()
                    or attachment.get("label")
                    or PurePosixPath(urlparse(str(response.url)).path).name
                    or "document"
                )
                filename = PurePosixPath(filename.replace("\\", "/")).name
                extension = PurePosixPath(urlparse(url).path).suffix.lower().lstrip(".")
                if extension not in allowed_extensions:
                    extension = PurePosixPath(filename).suffix.lower().lstrip(".")
                if extension not in allowed_extensions:
                    extension = "bin"
                response_mime_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
                guessed_mime_type = mimetypes.guess_type(filename)[0]
                mime_type = (
                    guessed_mime_type if response_mime_type in {"application/octet-stream", "binary/octet-stream"}
                    else response_mime_type
                ) or str(attachment.get("mime_type") or "") or guessed_mime_type or "application/octet-stream"
                attachment_uid = str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"finsights:disclosure-asset:{record.source_code}:{record.source_item_id}:{url}",
                ))
                safe_source = re.sub(r"[^a-z0-9-]+", "-", record.source_code.casefold()).strip("-")
                object_key = (
                    f"raw/disclosures/{safe_source}/{local_date:%Y/%m/%d}/"
                    f"{item_root_uid}/documents/{attachment_uid}.{extension}"
                )
                upload = self.uploader.upload_bytes(
                    data=data,
                    object_key=object_key,
                    mime_type=mime_type,
                    target_name=self.uploader.route_source_target(record.source_code),
                )
                attachment.update({
                    "asset_uid": attachment_uid,
                    "storage_state": "uploaded" if upload.get("uploaded") else "pending_credentials",
                    "object_storage": upload,
                    "mime_type": mime_type,
                    "size_bytes": len(data),
                    "content_sha256": upload.get("content_sha256"),
                    "original_filename": filename[:500],
                })
                if upload.get("uploaded"):
                    record.document_asset_uid = record.document_asset_uid or attachment_uid
                    record.metadata.setdefault("primary_attachment_mime_type", mime_type)
                    self.attachments_uploaded += 1
                else:
                    self.attachments_pending += 1
                    logger.warning(
                        "Disclosure attachment was fetched but not uploaded: source=%s item=%s",
                        record.source_code, record.source_item_id,
                    )
            except Exception as exc:
                if fetch_attempt not in self.attachment_attempts:
                    fetch_attempt.update({
                        "completed_at": datetime.now(timezone.utc),
                        "duration_ms": round((time.monotonic() - started) * 1000),
                        "outcome": "FAILED",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:1000],
                    })
                    self.attachment_attempts.append(fetch_attempt)
                self.attachments_failed += 1
                attachment["storage_state"] = "upload_failed"
                attachment["storage_error_type"] = type(exc).__name__
                attachment["storage_error"] = str(exc)[:500]
                logger.warning(
                    "Could not archive disclosure attachment: source=%s item=%s error=%s",
                    record.source_code, record.source_item_id, type(exc).__name__,
                )

        record.metadata["stored_documents"] = [
            {
                key: attachment[key]
                for key in (
                    "asset_uid", "url", "label", "storage_state", "object_storage",
                    "mime_type", "size_bytes", "content_sha256",
                )
                if key in attachment
            }
            for attachment in record.attachments
        ]

    def _persist_issuer_disclosure(self, record: DisclosureRecord, security: dict[str, str]) -> tuple[int, str]:
        uid = self._identity(record, "issuer-disclosure")
        metadata = {**record.metadata, "symbol": record.symbol, "company_name": record.company_name, "attachments": record.attachments}
        with self.disclosures_engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO issuer_disclosures
                    (disclosure_uid,source_code,source_item_id,company_uid,security_uid,disclosure_kind,
                     title,summary,language_code,published_at,original_url,canonical_url_sha256,
                     document_asset_uid,mime_type,rights_status,record_status,metadata_json)
                VALUES
                    (:uid,:source_code,:source_item_id,:company_uid,:security_uid,:disclosure_kind,
                     :title,:summary,'vi',:published_at,:url,:url_hash,:document_asset_uid,:mime_type,'link_only','published',:metadata)
                ON DUPLICATE KEY UPDATE
                    company_uid=VALUES(company_uid), security_uid=VALUES(security_uid),
                    disclosure_kind=VALUES(disclosure_kind), title=VALUES(title), summary=VALUES(summary),
                    published_at=VALUES(published_at), original_url=VALUES(original_url),
                    document_asset_uid=VALUES(document_asset_uid),
                    metadata_json=VALUES(metadata_json), record_status='published', updated_at=CURRENT_TIMESTAMP(3)
            """), {
                "uid": uid, "source_code": record.source_code, "source_item_id": record.source_item_id,
                "company_uid": security["company_uid"], "security_uid": security.get("security_uid") or None,
                "disclosure_kind": record.disclosure_kind, "title": record.title[:600],
                "summary": record.summary, "published_at": _utc_naive(record.published_at),
                "url": record.original_url, "url_hash": _canonical_hash(record.original_url),
                "document_asset_uid": record.document_asset_uid,
                "mime_type": record.metadata.get("primary_attachment_mime_type"),
                "metadata": json.dumps(metadata, ensure_ascii=False, default=str),
            })
            row = connection.execute(text("""
                SELECT id, disclosure_uid FROM issuer_disclosures
                WHERE source_code=:source_code AND source_item_id=:source_item_id
            """), {"source_code": record.source_code, "source_item_id": record.source_item_id}).mappings().one()
            return int(row["id"]), str(row["disclosure_uid"])

    def _persist_corporate_action(self, record: DisclosureRecord, security: dict[str, str], disclosure_id: int) -> str:
        uid = self._identity(record, "corporate-action")
        action_type = str(record.metadata.get("action_type") or record.notice_kind or "other")[:36]
        announcement_date = record.published_at.date() if record.published_at else None
        metadata = {**record.metadata, "original_url": record.original_url, "attachments": record.attachments}
        with self.disclosures_engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO corporate_actions
                    (action_uid,source_code,source_item_id,company_uid,security_uid,action_type,
                     announcement_date,ex_date,record_date,payment_date,cash_per_share_vnd,
                     ratio_numerator,ratio_denominator,subscription_price_vnd,description,disclosure_id,metadata_json)
                VALUES
                    (:uid,:source_code,:source_item_id,:company_uid,:security_uid,:action_type,
                     :announcement_date,:ex_date,:record_date,:payment_date,:cash,:ratio_num,
                     :ratio_den,:subscription_price,:description,:disclosure_id,:metadata)
                ON DUPLICATE KEY UPDATE
                    company_uid=VALUES(company_uid), security_uid=VALUES(security_uid),
                    action_type=VALUES(action_type), announcement_date=VALUES(announcement_date),
                    ex_date=VALUES(ex_date), record_date=VALUES(record_date), payment_date=VALUES(payment_date),
                    cash_per_share_vnd=VALUES(cash_per_share_vnd), ratio_numerator=VALUES(ratio_numerator),
                    ratio_denominator=VALUES(ratio_denominator), subscription_price_vnd=VALUES(subscription_price_vnd),
                    description=VALUES(description), disclosure_id=VALUES(disclosure_id),
                    metadata_json=VALUES(metadata_json), updated_at=CURRENT_TIMESTAMP(3)
            """), {
                "uid": uid, "source_code": record.source_code, "source_item_id": record.source_item_id,
                "company_uid": security["company_uid"], "security_uid": security["security_uid"],
                "action_type": action_type, "announcement_date": announcement_date,
                "ex_date": record.ex_date, "record_date": record.record_date,
                "payment_date": record.payment_date, "cash": record.cash_per_share_vnd,
                "ratio_num": record.ratio_numerator, "ratio_den": record.ratio_denominator,
                "subscription_price": record.subscription_price_vnd, "description": record.summary,
                "disclosure_id": disclosure_id,
                "metadata": json.dumps(metadata, ensure_ascii=False, default=str),
            })
            return str(connection.execute(text("""
                SELECT action_uid FROM corporate_actions
                WHERE source_code=:source_code AND source_item_id=:source_item_id
            """), {"source_code": record.source_code, "source_item_id": record.source_item_id}).scalar_one())

    def _persist_regulatory_notice(self, record: DisclosureRecord, security: dict[str, str] | None) -> str:
        uid = self._identity(record, "regulatory-notice")
        metadata = {**record.metadata, "symbol": record.symbol, "company_name": record.company_name, "attachments": record.attachments}
        with self.disclosures_engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO regulatory_notices
                    (notice_uid,source_code,source_item_id,authority_name,notice_kind,title,summary,
                     company_uid,published_at,effective_date,original_url,canonical_url_sha256,
                     document_asset_uid,rights_status,metadata_json)
                VALUES (:uid,:source_code,:source_item_id,:authority,:notice_kind,:title,:summary,
                    :company_uid,:published_at,:effective_date,:url,:url_hash,:document_asset_uid,'link_only',:metadata)
                ON DUPLICATE KEY UPDATE
                    authority_name=VALUES(authority_name), notice_kind=VALUES(notice_kind),
                    title=VALUES(title), summary=VALUES(summary), company_uid=VALUES(company_uid),
                    published_at=VALUES(published_at), effective_date=VALUES(effective_date),
                    original_url=VALUES(original_url), document_asset_uid=VALUES(document_asset_uid),
                    metadata_json=VALUES(metadata_json)
            """), {
                "uid": uid, "source_code": record.source_code, "source_item_id": record.source_item_id,
                "authority": (record.authority_name or "Ủy ban Chứng khoán Nhà nước")[:180],
                "notice_kind": (record.notice_kind or "other")[:36], "title": record.title[:600],
                "summary": record.summary, "company_uid": security.get("company_uid") if security else None,
                "published_at": _utc_naive(record.published_at), "effective_date": record.effective_date,
                "url": record.original_url, "url_hash": _canonical_hash(record.original_url),
                "document_asset_uid": record.document_asset_uid,
                "metadata": json.dumps(metadata, ensure_ascii=False, default=str),
            })
            return str(connection.execute(text("""
                SELECT notice_uid FROM regulatory_notices
                WHERE source_code=:source_code AND source_item_id=:source_item_id
            """), {"source_code": record.source_code, "source_item_id": record.source_item_id}).scalar_one())
