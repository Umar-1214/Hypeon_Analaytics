# Build and push backend + frontend to Artifact Registry
# Prereq: gcloud auth login; gcloud auth application-default login
#         gcloud auth configure-docker us-central1-docker.pkg.dev --quiet
# Usage: .\docker-build-push.ps1 [backend|frontend]
#        No args = build and push both.
$Registry = "us-central1-docker.pkg.dev/hypeon-ai-prod/hypeon-analytics"
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Target = $args[0]

Set-Location $Root
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet 2>$null

$doBackend = (-not $Target) -or ($Target -eq "backend")
$doFrontend = (-not $Target) -or ($Target -eq "frontend")

if ($doBackend) {
    Write-Host "Building backend..."
    docker build -f backend/Dockerfile -t "${Registry}/backend:latest" .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Write-Host "Pushing backend..."
    docker push "${Registry}/backend:latest"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if ($doFrontend) {
    Write-Host "Building frontend (npm install + build on host, then docker)..."
    Push-Location frontend
    npm install
    if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
    npm run build
    if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
    Pop-Location
    docker build -f frontend/Dockerfile -t "${Registry}/frontend:latest" ./frontend
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Write-Host "Pushing frontend..."
    docker push "${Registry}/frontend:latest"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "Done. Images: ${Registry}/backend:latest, ${Registry}/frontend:latest"
