#!/usr/bin/env bash
# Generate self-signed CA + server + client certificates for mTLS testing.
# Usage: bash examples/certs/generate.sh
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
DAYS=365
SUBJ_CA="/CN=PHP-IoT Test CA"
SUBJ_SRV="/CN=localhost"
SUBJ_CLI="/CN=php-iot-client"

echo "=== Generating test certificates in $DIR ==="

# 1. CA key + cert
openssl genrsa -out "$DIR/ca.key" 2048 2>/dev/null
openssl req -x509 -new -nodes -key "$DIR/ca.key" \
    -sha256 -days $DAYS -out "$DIR/ca.pem" \
    -subj "$SUBJ_CA" 2>/dev/null
echo "[ok] CA certificate: ca.pem"

# 2. Server key + cert (signed by CA)
# The SAN is what matters: OpenSSL and PHP stopped honouring CN for hostname
# verification, so without it every client must disable verify_peer_name — which
# defeats the point of having a CA at all.
openssl genrsa -out "$DIR/server.key" 2048 2>/dev/null
openssl req -new -key "$DIR/server.key" \
    -out "$DIR/server.csr" -subj "$SUBJ_SRV" 2>/dev/null
cat > "$DIR/server.ext" <<'EOF'
subjectAltName = DNS:localhost, DNS:mosquitto, IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
openssl x509 -req -in "$DIR/server.csr" \
    -CA "$DIR/ca.pem" -CAkey "$DIR/ca.key" -CAcreateserial \
    -extfile "$DIR/server.ext" \
    -out "$DIR/server.pem" -days $DAYS -sha256 2>/dev/null
rm -f "$DIR/server.csr" "$DIR/server.ext"
echo "[ok] Server certificate: server.pem (SAN: localhost, mosquitto, 127.0.0.1)"

# 3. Client key + cert (signed by same CA)
openssl genrsa -out "$DIR/client.key" 2048 2>/dev/null
openssl req -new -key "$DIR/client.key" \
    -out "$DIR/client.csr" -subj "$SUBJ_CLI" 2>/dev/null
cat > "$DIR/client.ext" <<'EOF'
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in "$DIR/client.csr" \
    -CA "$DIR/ca.pem" -CAkey "$DIR/ca.key" -CAcreateserial \
    -extfile "$DIR/client.ext" \
    -out "$DIR/client.pem" -days $DAYS -sha256 2>/dev/null
rm -f "$DIR/client.csr" "$DIR/client.ext"
echo "[ok] Client certificate: client.pem"

# Keys hold private material — keep them off other accounts on this machine.
chmod 600 "$DIR"/*.key

# Cleanup serial file
rm -f "$DIR/ca.srl"

echo ""
echo "=== Done! Files generated: ==="
echo "  CA:     $DIR/ca.pem / ca.key"
echo "  Server: $DIR/server.pem / server.key"
echo "  Client: $DIR/client.pem / client.key"
echo ""
echo "Mosquitto config (add to mosquitto.conf):"
echo "  listener 8883"
echo "  cafile $DIR/ca.pem"
echo "  certfile $DIR/server.pem"
echo "  keyfile $DIR/server.key"
echo "  require_certificate true"
