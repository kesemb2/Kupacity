#!/usr/bin/env bash
# build.sh — Render build command for the cinema-api backend
#
# For non-Docker Render deploys, set as the Build Command:
#   cd backend && chmod +x build.sh && ./build.sh
#
# For Docker deploys (render.yaml runtime: docker), the Dockerfile
# handles everything — this script is not used.
set -e

echo "==> Installing Python dependencies..."
pip install --no-cache-dir -r requirements.txt

# undetected-chromedriver downloads a matching chromedriver automatically
# on first use, so no manual driver install step is needed.

echo "==> Build complete."
