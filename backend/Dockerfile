FROM python:3.11-slim

WORKDIR /app

# ---- Install Google Chrome stable ----------------------------------------
# Required by undetected-chromedriver (it patches the real chromedriver
# binary at runtime to bypass Cloudflare / bot detection).
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
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
