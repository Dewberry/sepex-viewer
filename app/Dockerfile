# ---- Base image ----
FROM python:3.11-slim

# ---- System deps (for pandas, numpy, requests, pydantic, etc.) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ---- Create app directory ----
WORKDIR /app

# ---- Copy Python dependencies ----
COPY requirements.txt /app/

# ---- Install dependencies ----
RUN pip install --no-cache-dir -r requirements.txt

# ---- Copy the app code ----
COPY . /app

# ---- Expose Streamlit port ----
EXPOSE 8501

# ---- Streamlit config (prevents using browser auto-open, sets host) ----
ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ENABLEXSRSFPROTECTION=false \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501

# ---- Default command: run the SEPEX viewer ----
CMD ["streamlit", "run", "app/home.py"]
