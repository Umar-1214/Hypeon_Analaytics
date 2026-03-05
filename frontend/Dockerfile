# Build from repo root. Option A (host build, no npm in Docker): from repo root run:
#   cd frontend && npm install && npm run build && cd .. && docker build -f frontend/Dockerfile -t us-central1-docker.pkg.dev/hypeon-ai-prod/hypeon-analytics/frontend ./frontend
# Option B (build in Docker): ensure dist is in .dockerignore and uncomment the build stage below.
# Runtime config: set VITE_API_BASE, VITE_FIREBASE_* etc. as env vars on Cloud Run.
FROM node:20-alpine AS build
WORKDIR /app
COPY . .
# Pre-built dist must exist (run npm run build in frontend/ before docker build)

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8080
CMD ["/entrypoint.sh"]
