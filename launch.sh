#!/usr/bin/env bash
# ==============================================================================
# Single-Command Orchestrator & Lifecycle Manager
# Secure Edge IoT & Telemetry Ingestion Engine (project_1)
# ==============================================================================
# Handles pre-flight cleanup, PKI certificate verification, Docker service
# orchestration (TimescaleDB + Ingestion Gateway), health check polling,
# and concurrent Edge Producer Agent execution with clean signal teardown.
# ==============================================================================

set -euo pipefail

# ANSI Color Codes
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Structured Logging Tags
log_info()    { echo -e "${BLUE}[*]${NC} $1"; }
log_success() { echo -e "${GREEN}[+]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[!]${NC} $1"; }
log_error()   { echo -e "${RED}[-]${NC} $1"; }

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${PROJECT_DIR}"

GATEWAY_HOST="${GATEWAY_HOST:-localhost}"
GATEWAY_PORT="${GATEWAY_PORT:-8443}"
GATEWAY_URL="https://${GATEWAY_HOST}:${GATEWAY_PORT}"
HEALTH_ENDPOINT="${GATEWAY_URL}/health"
STATS_ENDPOINT="${GATEWAY_URL}/api/v1/telemetry/stats"

CERTS_DIR="${PROJECT_DIR}/certs"
CA_CERT="${CERTS_DIR}/ca.crt"
CLIENT_CERT="${CERTS_DIR}/client.crt"
CLIENT_KEY="${CERTS_DIR}/client.key"
SERVER_CERT="${CERTS_DIR}/server.crt"
SERVER_KEY="${CERTS_DIR}/server.key"

VENV_DIR="${PROJECT_DIR}/.venv"
PYTHON_BIN="${VENV_DIR}/bin/python3"
AGENT_PID=""

# ------------------------------------------------------------------------------
# Graceful Teardown Handler (Trap Signals)
# ------------------------------------------------------------------------------
cleanup() {
    trap - SIGINT SIGTERM SIGHUP EXIT
    echo ""
    log_info "Initiating graceful teardown of Secure Edge IoT Engine..."

    # Terminate background Edge Producer Agent
    if [[ -n "${AGENT_PID:-}" ]] && kill -0 "${AGENT_PID}" 2>/dev/null; then
        log_info "Stopping Edge Producer Agent (PID: ${AGENT_PID})..."
        kill -TERM "${AGENT_PID}" 2>/dev/null || true

        local wait_counter=0
        while kill -0 "${AGENT_PID}" 2>/dev/null && [ ${wait_counter} -lt 5 ]; do
            sleep 0.5
            ((wait_counter++))
        done

        if kill -0 "${AGENT_PID}" 2>/dev/null; then
            kill -KILL "${AGENT_PID}" 2>/dev/null || true
        fi
        log_success "Edge Producer Agent stopped."
    fi

    # Optional: Stop Docker Compose services if STOP_ON_EXIT is set (default: false)
    if [[ "${STOP_ON_EXIT:-0}" == "1" ]]; then
        log_info "Stopping Docker Compose services (TimescaleDB & Gateway)..."
        docker compose stop >/dev/null 2>&1 || true
        log_success "Docker containers stopped."
    else
        log_info "Docker services remain active in background. Run 'docker compose stop' to pause."
    fi

    log_success "Shutdown complete. Goodbye!"
    exit 0
}

trap cleanup SIGINT SIGTERM SIGHUP EXIT

echo -e "${BOLD}==================================================================${NC}"
echo -e "${BOLD}  Secure Edge IoT & Telemetry Ingestion Engine Orchestrator      ${NC}"
echo -e "${BOLD}==================================================================${NC}"

# ------------------------------------------------------------------------------
# 1. Pre-Flight Process Cleanup
# ------------------------------------------------------------------------------
log_info "Step 1/5: Checking for dangling edge agent processes..."

AGENT_PIDS=$(pgrep -f "edge_agent\.client" 2>/dev/null || true)
if [[ -n "${AGENT_PIDS}" ]]; then
    log_warn "Found lingering Edge Agent processes (PID: ${AGENT_PIDS}). Terminating..."
    for pid in ${AGENT_PIDS}; do
        kill -9 "${pid}" 2>/dev/null || true
    done
    sleep 0.5
    log_success "Dangling agent processes cleared."
else
    log_success "No conflicting agent processes detected."
fi

# ------------------------------------------------------------------------------
# 2. PKI Certificate Verification & Auto-Generation
# ------------------------------------------------------------------------------
log_info "Step 2/5: Verifying mTLS Cryptographic PKI Infrastructure..."

REQUIRED_CERTS=("${CA_CERT}" "${SERVER_CERT}" "${SERVER_KEY}" "${CLIENT_CERT}" "${CLIENT_KEY}")
CERTS_MISSING=false

for cert_file in "${REQUIRED_CERTS[@]}"; do
    if [[ ! -f "${cert_file}" ]]; then
        CERTS_MISSING=true
        break
    fi
done

if [ "${CERTS_MISSING}" = true ]; then
    log_warn "Missing required PKI certificates. Generating fresh mTLS certificates..."
    chmod +x "${CERTS_DIR}/generate_certs.sh"
    "${CERTS_DIR}/generate_certs.sh"
    log_success "mTLS PKI infrastructure successfully generated and verified."
else
    log_success "All required mTLS certificates and keys verified in ${CERTS_DIR}."
fi

# ------------------------------------------------------------------------------
# 3. Python Virtual Environment Verification
# ------------------------------------------------------------------------------
log_info "Step 3/5: Verifying Python virtual environment..."

if [[ ! -d "${VENV_DIR}" || ! -x "${PYTHON_BIN}" ]]; then
    log_warn "Virtual environment not found at ${VENV_DIR}. Provisioning..."
    python3 -m venv "${VENV_DIR}"
    "${VENV_DIR}/bin/pip" install --upgrade pip setuptools wheel --quiet
    "${VENV_DIR}/bin/pip" install -e ".[dev]" --quiet
    log_success "Virtual environment provisioned and packages installed."
else
    # Quick check for httpx
    if ! "${PYTHON_BIN}" -c "import httpx" >/dev/null 2>&1; then
        log_warn "Installing required dependencies into virtualenv..."
        "${VENV_DIR}/bin/pip" install -e ".[dev]" --quiet
    fi
    log_success "Virtual environment verified at ${VENV_DIR}."
fi

# ------------------------------------------------------------------------------
# 4. Docker Service Orchestration & Health Polling
# ------------------------------------------------------------------------------
log_info "Step 4/5: Starting Docker Compose services (TimescaleDB & Gateway)..."

if ! command -v docker >/dev/null 2>&1; then
    log_error "Docker is required but not installed or not in PATH."
    exit 1
fi

docker compose up -d

log_info "Polling Gateway mTLS health endpoint at ${HEALTH_ENDPOINT} (max 20s)..."

MAX_ATTEMPTS=40
POLL_INTERVAL=0.5
GATEWAY_READY=false

for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
    # Probe healthcheck with client mTLS
    HTTP_STATUS=$(curl -s -k \
        --cert "${CLIENT_CERT}" \
        --key "${CLIENT_KEY}" \
        --cacert "${CA_CERT}" \
        -o /dev/null \
        -w "%{http_code}" \
        "${HEALTH_ENDPOINT}" 2>/dev/null || true)

    if [[ "${HTTP_STATUS}" == "200" ]]; then
        GATEWAY_READY=true
        break
    fi

    sleep "${POLL_INTERVAL}"
done

if [ "${GATEWAY_READY}" = true ]; then
    HEALTH_BODY=$(curl -s -k \
        --cert "${CLIENT_CERT}" \
        --key "${CLIENT_KEY}" \
        --cacert "${CA_CERT}" \
        "${HEALTH_ENDPOINT}" 2>/dev/null || true)
    log_success "Gateway is healthy and operational (HTTP 200): ${HEALTH_BODY}"
else
    log_error "Gateway health check timed out after 20 seconds. Check logs with 'docker compose logs gateway'."
    exit 1
fi

# ------------------------------------------------------------------------------
# 5. Launch Edge Producer Agent & Stream Telemetry
# ------------------------------------------------------------------------------
log_info "Step 5/5: Launching Edge Producer Agent with mTLS..."
echo ""
echo -e "${GREEN}${BOLD}[+] Ingestion Gateway:  ${GATEWAY_URL}${NC}"
echo -e "${GREEN}${BOLD}[+] Health Endpoint:    ${HEALTH_ENDPOINT}${NC}"
echo -e "${GREEN}${BOLD}[+] Stats Endpoint:     ${STATS_ENDPOINT}${NC}"
echo ""
log_info "Streaming live sensor collection, SQLite spooling, and mTLS transmission..."
log_info "Press [Ctrl+C] at any time to gracefully stop the agent."
echo ""

# Export environment variables for the agent process
export GATEWAY_URL="${GATEWAY_URL}"
export SSL_CA_CERT="${CA_CERT}"
export CLIENT_CERT_PATH="${CLIENT_CERT}"
export CLIENT_KEY_PATH="${CLIENT_KEY}"
export SPOOL_DB_PATH="${PROJECT_DIR}/spool.db"

# Execute edge agent using the virtualenv interpreter
"${PYTHON_BIN}" -m edge_agent.client &
AGENT_PID=$!

# Wait for the agent process
wait "${AGENT_PID}"
