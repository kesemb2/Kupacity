#!/usr/bin/env bash
# build.sh — Render build command for the cinema-api backend
# Set as the Build Command in Render:  ./build.sh
set -e

echo "==> Installing Python dependencies..."
pip install --no-cache-dir -r requirements.txt

echo "==> Installing chromedriver for SeleniumBase UC mode..."
sbase install chromedriver

echo "==> Build complete."
