#!/usr/bin/env bash
set -euo pipefail

# 1) Some networks block HTTP; switch Debian mirrors to HTTPS to avoid 403
sed -i 's|http://deb.debian.org|https://deb.debian.org|g; s|http://security.debian.org|https://security.debian.org|g' /etc/apt/sources.list || true

apt-get update
apt-get install -y --no-install-recommends curl gnupg2 ca-certificates apt-transport-https unixodbc unixodbc-dev

# 2) Correct Microsoft repo for Debian 12 (bookworm): use the official config list
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/microsoft.gpg
curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list -o /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17

# Show registered ODBC drivers
odbcinst -q -d
