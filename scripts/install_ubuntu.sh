#!/usr/bin/env bash
set -euo pipefail

APP_NAME="marzban-panel-hub"
INSTALL_DIR="/opt/${APP_NAME}"
APP_USER="${APP_NAME}"
REPO_URL=""
BRANCH="main"

WEB_HOST="127.0.0.1"
WEB_PORT="8000"
TIMEZONE="Asia/Tehran"
POLL_INTERVAL_SECONDS="30"
SIGNUP_CODE=""

usage() {
  cat <<EOF
${APP_NAME} - Ubuntu installer (systemd)

Usage:
  sudo bash install_ubuntu.sh --repo-url https://github.com/USER/REPO.git [options]

Options:
  --repo-url URL            (required) Git repo URL
  --branch NAME             Git branch (default: ${BRANCH})
  --install-dir PATH        Install path (default: ${INSTALL_DIR})
  --user NAME               Linux user to run service (default: ${APP_USER})
  --host HOST               WEB_HOST (default: ${WEB_HOST})
  --port PORT               WEB_PORT (default: ${WEB_PORT})
  --timezone TZ             TIMEZONE (default: ${TIMEZONE})
  --poll-interval SECONDS   POLL_INTERVAL_SECONDS (default: ${POLL_INTERVAL_SECONDS})
  --signup-code CODE        SIGNUP_CODE (default: empty = open signup)
  -h, --help                Show help

One-liner example (after pushing to GitHub):
  wget -qO- https://raw.githubusercontent.com/USER/REPO/main/scripts/install_ubuntu.sh | \\
    sudo bash -s -- --repo-url https://github.com/USER/REPO.git --host 0.0.0.0 --port 8000
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-url) REPO_URL="${2:-}"; shift 2 ;;
    --branch) BRANCH="${2:-}"; shift 2 ;;
    --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
    --user) APP_USER="${2:-}"; shift 2 ;;
    --host) WEB_HOST="${2:-}"; shift 2 ;;
    --port) WEB_PORT="${2:-}"; shift 2 ;;
    --timezone) TIMEZONE="${2:-}"; shift 2 ;;
    --poll-interval) POLL_INTERVAL_SECONDS="${2:-}"; shift 2 ;;
    --signup-code) SIGNUP_CODE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1 (use --help)" ;;
  esac
done

[[ $EUID -eq 0 ]] || die "Run as root (sudo)."
[[ -n "${REPO_URL}" ]] || die "--repo-url is required (use --help)."

need_cmd apt-get
need_cmd systemctl

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  ca-certificates \
  git \
  python3 \
  python3-venv \
  python3-pip \
  wget

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "/var/lib/${APP_NAME}" --shell /usr/sbin/nologin "${APP_USER}"
fi

if [[ -d "${INSTALL_DIR}/.git" ]]; then
  git -C "${INSTALL_DIR}" fetch --all --prune
  git -C "${INSTALL_DIR}" checkout "${BRANCH}"
  git -C "${INSTALL_DIR}" pull --ff-only
else
  rm -rf "${INSTALL_DIR}"
  git clone --depth 1 --branch "${BRANCH}" "${REPO_URL}" "${INSTALL_DIR}"
fi

mkdir -p "${INSTALL_DIR}/data"
chown -R "${APP_USER}:${APP_USER}" "${INSTALL_DIR}/data"

python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip
"${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt"

ENV_FILE="/etc/${APP_NAME}.env"
cat > "${ENV_FILE}" <<EOF
WEB_HOST=${WEB_HOST}
WEB_PORT=${WEB_PORT}
TIMEZONE=${TIMEZONE}
POLL_INTERVAL_SECONDS=${POLL_INTERVAL_SECONDS}
SIGNUP_CODE=${SIGNUP_CODE}
DB_PATH=${INSTALL_DIR}/data/bot.sqlite3
APP_SECRET_KEY_FILE=${INSTALL_DIR}/data/app_secret.key
PYTHONUNBUFFERED=1
PYTHONDONTWRITEBYTECODE=1
PYTHONIOENCODING=utf-8
EOF
chmod 0640 "${ENV_FILE}"
chown root:"${APP_USER}" "${ENV_FILE}"

SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Marzban Panel Hub (Web + Telegram)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${INSTALL_DIR}/venv/bin/python web.py
Restart=on-failure
RestartSec=3

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=${INSTALL_DIR}/data

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${APP_NAME}"
systemctl restart "${APP_NAME}"

echo
echo "Installed: ${APP_NAME}"
echo "Service:   systemctl status ${APP_NAME} --no-pager"
echo "Logs:      journalctl -u ${APP_NAME} -f"
echo "URL:       http://${WEB_HOST}:${WEB_PORT}"

