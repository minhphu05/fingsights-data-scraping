from typing import Any
from src.config.settings import settings


def get_k8s_job_tags(source_name: str, cpu_limit: str = "500m", memory_limit: str = "1Gi") -> dict[str, Any]:
    """Generate Kubernetes pod specification tags for isolated execution on K3s."""
    return {
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": cpu_limit, "memory": memory_limit},
                },
                "env": [
                    {"name": "SOURCE_NAME", "value": source_name},
                ],
            },
            "pod_spec_config": {
                "node_selector": {
                    settings.k8s_node_selector_key: settings.k8s_node_selector_value,
                },
                "restart_policy": "Never",
            },
            "pod_template_spec_metadata": {
                "labels": {
                    "app.kubernetes.io/part-of": "finsights-platform",
                    "tier": "data-scraping",
                    "scraper-source": source_name,
                }
            },
        }
    }
