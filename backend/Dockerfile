FROM python:3.11-slim

WORKDIR /app

# ---- Install Python dependencies ----------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Install Playwright's bundled Chromium + system deps ----------------
# playwright install-deps installs all required system libraries (libnss3,
# libatk, fonts, etc.) and playwright install chromium downloads the
# matching Chromium binary.
RUN playwright install-deps chromium && playwright install chromium

# ---- Copy application code -----------------------------------------------
COPY . .

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
