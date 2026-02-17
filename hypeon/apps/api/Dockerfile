# Stage 1: build frontend
FROM node:20-alpine AS web-builder
WORKDIR /app
COPY apps/web/package.json apps/web/package-lock.json* ./
RUN npm ci
COPY apps/web .
ENV VITE_API_URL=
RUN npm run build

# Stage 2: API + serve static
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e .
COPY --from=web-builder /app/dist apps/web/dist
ENV PYTHONPATH=/app
# Run as non-root
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
CMD ["uvicorn", "apps.api.src.app:app", "--host", "0.0.0.0", "--port", "8000"]
