#!/usr/bin/env bash
# ==============================================================================
# Secure Edge IoT - PKI & Mutual TLS (mTLS) Certificate Generator
# ==============================================================================
# Generates a dedicated Root Certificate Authority (CA), Server Certificate
# (with SAN for localhost, 127.0.0.1, and Docker service names), and Client
# Certificates for industrial edge devices.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# ANSI Colors for logging
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

DAYS_VALID=3650
RSA_BITS=4096

log_info "Initializing PKI generation directory: ${SCRIPT_DIR}"

# ------------------------------------------------------------------------------
# 1. Clean up prior artifacts if any
# ------------------------------------------------------------------------------
rm -f *.key *.crt *.csr *.srl *.cnf

# ------------------------------------------------------------------------------
# 2. Generate Root Certificate Authority (CA)
# ------------------------------------------------------------------------------
log_info "Step 1/3: Generating Root CA private key and self-signed certificate..."

openssl genrsa -out ca.key "${RSA_BITS}"
chmod 600 ca.key

cat > ca.cnf <<EOF
[ req ]
default_bits        = ${RSA_BITS}
default_md          = sha256
prompt              = no
distinguished_name  = req_distinguished_name
x509_extensions     = v3_ca

[ req_distinguished_name ]
C   = US
ST  = Texas
L   = Austin
O   = SecureEdge Industrial Systems
OU  = Security Operations Center
CN  = SecureEdge Root CA

[ v3_ca ]
subjectKeyIdentifier   = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints       = critical, CA:true
keyUsage               = critical, digitalSignature, cRLSign, keyCertSign
EOF

openssl req -new -x509 -days "${DAYS_VALID}" -key ca.key -out ca.crt -config ca.cnf
chmod 644 ca.crt
rm -f ca.cnf
log_success "Root CA generated: ca.crt & ca.key"

# ------------------------------------------------------------------------------
# 3. Generate Ingestion Gateway Server Certificate
# ------------------------------------------------------------------------------
log_info "Step 2/3: Generating Gateway Server key, CSR, and SAN certificate..."

openssl genrsa -out server.key "${RSA_BITS}"
chmod 600 server.key

cat > server.cnf <<EOF
[ req ]
default_bits        = ${RSA_BITS}
default_md          = sha256
prompt              = no
distinguished_name  = req_distinguished_name
req_extensions      = v3_req

[ req_distinguished_name ]
C   = US
ST  = Texas
L   = Austin
O   = SecureEdge Industrial Systems
OU  = Ingestion Gateway Fleet
CN  = gateway.secureedge.local

[ v3_req ]
basicConstraints     = critical, CA:false
keyUsage             = critical, digitalSignature, keyEncipherment
extendedKeyUsage     = serverAuth
subjectAltName       = @alt_names

[ alt_names ]
DNS.1 = localhost
DNS.2 = gateway
DNS.3 = gateway.secureedge.local
IP.1  = 127.0.0.1
IP.2  = 0.0.0.0
EOF

openssl req -new -key server.key -out server.csr -config server.cnf

openssl x509 -req -days "${DAYS_VALID}" \
    -in server.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out server.crt \
    -extfile server.cnf \
    -extensions v3_req

chmod 644 server.crt
rm -f server.csr server.cnf
log_success "Server Certificate generated: server.crt & server.key (with SANs)"

# ------------------------------------------------------------------------------
# 4. Generate Edge Device Client Certificate
# ------------------------------------------------------------------------------
log_info "Step 3/3: Generating Edge Sensor Client key, CSR, and certificate..."

openssl genrsa -out client.key "${RSA_BITS}"
chmod 600 client.key

cat > client.cnf <<EOF
[ req ]
default_bits        = ${RSA_BITS}
default_md          = sha256
prompt              = no
distinguished_name  = req_distinguished_name
req_extensions      = v3_req

[ req_distinguished_name ]
C   = US
ST  = Texas
L   = Austin
O   = SecureEdge Industrial Systems
OU  = Edge Sensor Telemetry
CN  = edge-sensor-01

[ v3_req ]
basicConstraints     = critical, CA:false
keyUsage             = critical, digitalSignature, keyEncipherment
extendedKeyUsage     = clientAuth
EOF

openssl req -new -key client.key -out client.csr -config client.cnf

openssl x509 -req -days "${DAYS_VALID}" \
    -in client.csr \
    -CA ca.crt \
    -CAkey ca.key \
    -CAcreateserial \
    -out client.crt \
    -extfile client.cnf \
    -extensions v3_req

chmod 644 client.crt
rm -f client.csr client.cnf
log_success "Client Certificate generated: client.crt & client.key"

# ------------------------------------------------------------------------------
# 5. Cryptographic Chain Verification
# ------------------------------------------------------------------------------
log_info "Verifying certificate chains against Root CA..."

openssl verify -CAfile ca.crt server.crt
openssl verify -CAfile ca.crt client.crt

log_success "All certificates successfully generated and cryptographically verified!"
echo ""
echo "Summary of generated PKI files in ${SCRIPT_DIR}:"
ls -la *.crt *.key

