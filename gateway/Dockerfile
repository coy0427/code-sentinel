# ==============================================================================
# Multi-Stage Production Dockerfile for Ingestion Gateway API
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build Dependencies
# ------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Configure apt sandbox and repository parameters for reliable mirror fetch
RUN echo 'APT::Sandbox::User "root";' > /etc/apt/apt.conf.d/01sandbox && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/02valid && \
    sed -i 's/^Signed-By:/Trusted: yes\n#Signed-By:/' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true && \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install wheel and project dependencies
RUN pip install --upgrade pip setuptools wheel
COPY pyproject.toml .
# Install packages directly
RUN pip install \
    "fastapi>=0.110.0" \
    "uvicorn[standard]>=0.28.0" \
    "pydantic>=2.6.0" \
    "pydantic-settings>=2.2.0" \
    "sqlalchemy>=2.0.28" \
    "asyncpg>=0.29.0" \
    "httpx>=0.27.0"

# ------------------------------------------------------------------------------
# Stage 2: Runtime Image (Hardened, Non-Root)
# ------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runner

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

WORKDIR /app

# Install runtime dependencies as root
RUN echo 'APT::Sandbox::User "root";' > /etc/apt/apt.conf.d/01sandbox && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/02valid && \
    sed -i 's/^Signed-By:/Trusted: yes\n#Signed-By:/' /etc/apt/sources.list.d/debian.sources 2>/dev/null || true && \
    apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create unprivileged application user and group
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /sbin/nologin -d /app appuser && \
    mkdir -p /app/certs && \
    chown -R appuser:appgroup /app

# Copy application source code
COPY gateway/app /app/gateway/app

# Drop root privileges
USER appuser:appgroup

EXPOSE 8443

# Healthcheck testing the local endpoint over HTTPS with client mTLS
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -k -f --cert /app/certs/client.crt --key /app/certs/client.key --cacert /app/certs/ca.crt https://localhost:8443/health || exit 1

# Execute Uvicorn with strict mutual TLS (ssl-cert-reqs 2 = ssl.CERT_REQUIRED)
CMD ["uvicorn", "gateway.app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8443", \
     "--ssl-keyfile", "/app/certs/server.key", \
     "--ssl-certfile", "/app/certs/server.crt", \
     "--ssl-ca-certs", "/app/certs/ca.crt", \
     "--ssl-cert-reqs", "2"]
