# Build from repo root: docker build -f backend/Dockerfile -t us-central1-docker.pkg.dev/hypeon-ai-prod/hypeon-analytics/backend .
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app
ENV PORT=8080

# Dependencies
COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt

# Application and config (repo root layout for config_loader and knowledge_base paths)
COPY backend/ ./backend/
COPY config/ ./config/
COPY rules_config.json ./
COPY bigquery_schema/ ./bigquery_schema/

EXPOSE 8080
CMD ["sh", "-c", "uvicorn backend.app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
