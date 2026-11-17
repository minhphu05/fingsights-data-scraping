# ==============================================================================
# Finsights Data Scraping Engine - Dockerfile
# ==============================================================================

FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
RUN python -m playwright install chromium

# Final runtime image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    libatomic1 \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed python dependencies from builder
COPY --from=builder /root/.local /root/.local
COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# ADF and AJAX-only official portals need a real headless browser to render public pages.
RUN apt-get update \
    && python -m playwright install-deps chromium \
    && rm -rf /var/lib/apt/lists/*

# Copy application source code
COPY . .

# Default port for Dagster gRPC server
EXPOSE 3030

# Default command starts Dagster gRPC code server for K3s user deployments
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-m", "src.finsights_scraping.orchestration.definitions"]
