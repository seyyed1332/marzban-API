#!/usr/bin/env bash
set -euo pipefail

APP_NAME="marzban-panel-hub"
INSTALL_DIR="/opt/${APP_NAME}"
BRANCH="main"

usage() {
  cat <<EOF
${APP_NAME} - Ubuntu updater

Usage:
  sudo bash update_ubuntu.sh [options]

Options:
  --install-dir PATH   Install path (default: ${INSTALL_DIR})
  --branch NAME        Git branch (default: ${BRANCH})
  -h, --help           Show help

One-liner example:
  wget -qO- https://raw.githubusercontent.com/USER/REPO/main/scripts/update_ubuntu.sh | sudo bash -s --
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
    --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
    --branch) BRANCH="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1 (use --help)" ;;
  esac
done

[[ $EUID -eq 0 ]] || die "Run as root (sudo)."

need_cmd git
need_cmd systemctl

[[ -d "${INSTALL_DIR}/.git" ]] || die "Not installed at ${INSTALL_DIR} (missing .git)."
[[ -x "${INSTALL_DIR}/venv/bin/pip" ]] || die "Missing venv at ${INSTALL_DIR}/venv."

git -C "${INSTALL_DIR}" fetch --all --prune
git -C "${INSTALL_DIR}" checkout "${BRANCH}"
git -C "${INSTALL_DIR}" pull --ff-only

"${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt"

systemctl restart "${APP_NAME}"
systemctl status "${APP_NAME}" --no-pager || true

echo
echo "Updated: ${APP_NAME}"

