from .hose import HoseDisclosureClient
from .hnx import HnxDisclosureClient
from .ubcknn import UbcknnNoticeClient
from .vsdc import VsdcCorporateActionClient


def build_client(source_key: str, source_config: dict):
    clients = {
        "hose": HoseDisclosureClient,
        "hnx": HnxDisclosureClient,
        "ubcknn": UbcknnNoticeClient,
        "vsdc": VsdcCorporateActionClient,
    }
    try:
        client_type = clients[source_key]
    except KeyError as exc:
        raise ValueError(f"Unsupported official disclosure source: {source_key!r}") from exc
    return client_type(source_config)

__all__ = [
    "HoseDisclosureClient",
    "HnxDisclosureClient",
    "UbcknnNoticeClient",
    "VsdcCorporateActionClient",
    "build_client",
]
