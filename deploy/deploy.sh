#!/usr/bin/env bash
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────
PROJECT_NAME="pyspark-nexus"
DOMAIN="${PROJECT_NAME}.temujinlabs.com"
PROJECT_DIR="/opt/${PROJECT_NAME}"
REPO_URL="${1:?Usage: ./deploy.sh <git-repo-url>}"

echo "==> Deploying ${PROJECT_NAME} to ${DOMAIN}"

# ─── 1. Install dependencies if missing ─────────────────────────
echo "==> Checking dependencies..."
for cmd in docker nginx certbot; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "    Installing ${cmd}..."
        apt-get update -qq && apt-get install -y -qq "$cmd"
    fi
done

# Ensure docker compose plugin is available
if ! docker compose version &>/dev/null; then
    echo "ERROR: docker compose plugin not found. Install it first."
    exit 1
fi

# ─── 2. Clone / update project ──────────────────────────────────
echo "==> Setting up project at ${PROJECT_DIR}..."
if [ -d "${PROJECT_DIR}/.git" ]; then
    echo "    Repo exists, pulling latest..."
    cd "${PROJECT_DIR}"
    git pull
else
    echo "    Cloning repo..."
    mkdir -p "${PROJECT_DIR}"
    git clone "${REPO_URL}" "${PROJECT_DIR}"
    cd "${PROJECT_DIR}"
fi

# ─── 3. Check .env exists ───────────────────────────────────────
if [ ! -f "${PROJECT_DIR}/.env" ]; then
    echo ""
    echo "ERROR: No .env file found at ${PROJECT_DIR}/.env"
    echo "Create it with your production secrets before running this script:"
    echo ""
    echo "  cp .env.example .env"
    echo "  nano .env  # fill in GEMINI_API_KEY"
    echo ""
    exit 1
fi

# ─── 4. Start containers with production compose ────────────────
echo "==> Starting Docker containers..."
cd "${PROJECT_DIR}"
docker compose -f docker-compose.prod.yml up -d --build

echo "==> Waiting for backend health check..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:8050/health >/dev/null 2>&1; then
        echo "    Backend is healthy!"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "WARNING: Backend did not become healthy in 30s. Check logs:"
        echo "  docker compose -f docker-compose.prod.yml logs backend"
    fi
    sleep 1
done

# ─── 5. Configure Nginx ─────────────────────────────────────────
echo "==> Configuring Nginx..."
NGINX_CONF="/etc/nginx/sites-available/${DOMAIN}"
cp "${PROJECT_DIR}/deploy/nginx/${DOMAIN}" "${NGINX_CONF}"

if [ ! -L "/etc/nginx/sites-enabled/${DOMAIN}" ]; then
    ln -s "${NGINX_CONF}" "/etc/nginx/sites-enabled/${DOMAIN}"
fi

nginx -t
systemctl reload nginx
echo "    Nginx configured and reloaded."

# ─── 6. SSL via Certbot ─────────────────────────────────────────
echo "==> Setting up SSL..."
if [ ! -d "/etc/letsencrypt/live/${DOMAIN}" ]; then
    certbot --nginx -d "${DOMAIN}" --non-interactive --agree-tos --email admin@temujinlabs.com
else
    echo "    SSL certificate already exists."
fi

# ─── 7. Firewall ────────────────────────────────────────────────
echo "==> Configuring firewall..."
if command -v ufw &>/dev/null; then
    ufw allow 22/tcp   >/dev/null 2>&1 || true
    ufw allow 80/tcp   >/dev/null 2>&1 || true
    ufw allow 443/tcp  >/dev/null 2>&1 || true
    ufw --force enable >/dev/null 2>&1 || true
    echo "    UFW: allowing 22, 80, 443 only."
else
    echo "    ufw not found, skipping firewall setup."
fi

# ─── Done ────────────────────────────────────────────────────────
echo ""
echo "==> Deployment complete!"
echo "    URL:  https://${DOMAIN}"
echo ""
echo "    Useful commands:"
echo "      docker compose -f docker-compose.prod.yml logs -f"
echo "      docker compose -f docker-compose.prod.yml ps"
echo "      docker compose -f docker-compose.prod.yml restart"
