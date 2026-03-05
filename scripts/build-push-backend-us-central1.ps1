# Build and push backend image to us-central1 Artifact Registry
# Usage: from repo root: .\scripts\build-push-backend-us-central1.ps1
# Prereqs: gcloud CLI, docker; run once: gcloud auth login; gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

$ErrorActionPreference = "Stop"
$Registry = "us-central1-docker.pkg.dev/hypeon-ai-prod/hypeon-analytics"
$RepoRoot = $PSScriptRoot + "\.."

Set-Location $RepoRoot

Write-Host "Building backend..." -ForegroundColor Cyan
docker build -t "${Registry}/backend:latest" -f backend/Dockerfile .
if ($LASTEXITCODE -ne 0) { throw "Backend build failed" }

Write-Host "Pushing to ${Registry}/backend:latest..." -ForegroundColor Cyan
docker push "${Registry}/backend:latest"
if ($LASTEXITCODE -ne 0) { throw "Backend push failed" }

Write-Host "Done. Image: ${Registry}/backend:latest" -ForegroundColor Green
