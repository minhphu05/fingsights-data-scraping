from __future__ import annotations

import re
import unicodedata
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlparse


RULESET_VERSION = "financial-news-rules-2026-11-23-v1"

FINANCIAL_HEADLINE_TERMS = (
    "chứng khoán", "cổ phiếu", "vn-index", "vn30", "hose", "hnx", "upcom",
    "báo cáo tài chính", "cổ tức", "trái phiếu", "lãi suất", "tỷ giá",
    "khối ngoại", "thanh khoản", "niêm yết", "giao dịch chứng khoán",
    "thị trường tài chính", "tài chính", "kinh tế", "thị trường", "đầu tư",
    "doanh nghiệp", "lợi nhuận", "doanh thu", "tăng vốn", "cổ đông",
    "ngân hàng", "đầu tư chứng khoán", "phái sinh", "chứng quyền", "ipo",
    "bất động sản", "lạm phát", "tín dụng", "giá vàng", "giá dầu", "giá xăng",
    "bảo hiểm", "quỹ đầu tư", "ngoại hối",
)

SPORTS_TERMS = (
    "bóng đá", "world cup", "u17", "u20", "u23", "vff", "fifa",
    "trận đấu", "đội tuyển", "cầu thủ", "bàn thắng", "huấn luyện viên",
    "giải đấu", "sân cỏ", "thể thao",
)

FINANCIAL_SECTIONS = {
    "finance", "financial_news", "stock_market", "business_finance",
    "macro_economy", "banking", "chung_khoan", "tai_chinh", "thi_truong",
    "doanh_nghiep", "kinh_doanh",
}


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", value).strip()


def _matches(text: str, terms: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    for term in terms:
        candidate = normalize(term)
        if candidate and re.search(rf"(?<![\w]){re.escape(candidate)}(?![\w])", text):
            found.append(term)
    return found


def _host(url: str | None) -> str | None:
    if not url:
        return None
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    return host or None


@dataclass(frozen=True)
class NewsQualityAssessment:
    financial_relevance: str
    topic_label: str
    publication_decision: str
    confidence: float
    reason_code: str
    evidence: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class FinancialNewsQualityGate:
    """Conservative, explainable gate; ticker aliases alone never prove relevance."""

    ruleset_version = RULESET_VERSION

    def assess(
        self,
        *,
        title: str,
        summary: str | None,
        body: str,
        section: str | None,
        canonical_url: str,
        original_source_url: str | None,
        publisher_domain: str | None,
        matched_securities: list[dict[str, Any]] | None = None,
    ) -> NewsQualityAssessment:
        headline = normalize(" ".join(part for part in (title, summary or "") if part))
        full_text = normalize(" ".join(part for part in (title, summary or "", body) if part))
        finance_headline = _matches(headline, FINANCIAL_HEADLINE_TERMS)
        finance_body = _matches(full_text, FINANCIAL_HEADLINE_TERMS)
        sports_headline = _matches(headline, SPORTS_TERMS)
        sports_body = _matches(full_text, SPORTS_TERMS)

        section_key = normalize(section or "").replace(" ", "_")
        trusted_finance_section = section_key in FINANCIAL_SECTIONS
        expected_domain = (publisher_domain or "").lower().removeprefix("www.")
        canonical_host = _host(canonical_url)
        original_host = _host(original_source_url)
        domain_matches = bool(
            not expected_domain
            or canonical_host == expected_domain
            or (canonical_host and canonical_host.endswith("." + expected_domain))
        )

        evidence = {
            "publisher_domain": expected_domain or None,
            "canonical_host": canonical_host,
            "original_source_host": original_host,
            "canonical_domain_matches_publisher": domain_matches,
            "source_section": section,
            "financial_terms_in_headline": finance_headline,
            "financial_terms_in_article": finance_body,
            "sports_terms_in_headline": sports_headline,
            "sports_terms_in_article": sports_body,
            "matched_securities": matched_securities or [],
        }

        if not domain_matches:
            return NewsQualityAssessment(
                "needs_review", "source_mismatch", "review", 0.25,
                "canonical_domain_mismatch", evidence,
            )

        has_finance_evidence = bool(finance_headline or len(finance_body) >= 2)
        has_sports_headline = bool(sports_headline)
        if has_sports_headline and not has_finance_evidence:
            return NewsQualityAssessment(
                "non_financial", "sports", "exclude", 0.99,
                "sports_topic_without_financial_evidence", evidence,
            )

        if finance_headline and not has_sports_headline:
            return NewsQualityAssessment(
                "financial", "financial_news", "publish", 0.98,
                "financial_evidence_in_headline", evidence,
            )

        if has_finance_evidence and has_sports_headline:
            return NewsQualityAssessment(
                "needs_review", "mixed_topics", "review", 0.55,
                "conflicting_sports_and_financial_evidence", evidence,
            )

        if trusted_finance_section and (finance_body or not sports_body):
            return NewsQualityAssessment(
                "financial", "financial_news", "publish", 0.82,
                "trusted_financial_source_section", evidence,
            )

        if len(finance_body) >= 2:
            return NewsQualityAssessment(
                "financial", "financial_news", "publish", 0.84,
                "multiple_financial_article_signals", evidence,
            )

        if sports_body:
            return NewsQualityAssessment(
                "non_financial", "sports", "exclude", 0.9,
                "sports_topic_without_financial_evidence", evidence,
            )

        return NewsQualityAssessment(
            "needs_review", "unclassified", "review", 0.4,
            "insufficient_financial_evidence", evidence,
        )
