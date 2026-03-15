# Stage 1: Build frontend
FROM node:20-slim AS frontend
WORKDIR /app/frontend
COPY src/universal_roster_v2/web/frontend/package*.json ./
RUN npm ci --silent
COPY src/universal_roster_v2/web/frontend/ ./
RUN npm run build

# Stage 2: Python runtime
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy everything first, then install
COPY pyproject.toml ./
COPY src/ ./src/
COPY knowledge_base/ ./knowledge_base/

# Install as non-editable (no -e flag)
RUN pip install --no-cache-dir ".[web]" google-cloud-bigquery>=3.20 google-cloud-secret-manager>=2.20 gunicorn>=22.0 google-genai>=1.0.0

# Copy built frontend from stage 1
COPY --from=frontend /app/static/dist/ ./src/universal_roster_v2/web/static/dist/

# Pre-compile Python bytecode
RUN python -m compileall -q src/

ENV PORT=8080 PYTHONPATH=/app/src PYTHONUNBUFFERED=1

CMD exec gunicorn universal_roster_v2.web.server:app \
    --bind 0.0.0.0:$PORT \
    --worker-class uvicorn.workers.UvicornWorker \
    --workers 2 --threads 4 --timeout 300 --keep-alive 30 --preload
