FROM python:3.11-slim

WORKDIR /app

# ---- Install Chromium (works on both AMD64 and ARM64) --------------------
# Required by undetected-chromedriver (it patches the real chromedriver
# binary at runtime to bypass Cloudflare / bot detection).
# The scraper's _detect_chrome_major_version() already searches for the
# "chromium" binary, so no code changes are needed.
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2t64 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# ---- Install Python dependencies ----------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# undetected-chromedriver auto-downloads a matching chromedriver on first
# run, so no manual driver install step is needed.

# ---- Copy application code -----------------------------------------------
COPY . .

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
