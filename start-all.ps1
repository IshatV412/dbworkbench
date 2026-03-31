$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$FrontendDir = Join-Path $RepoRoot "frontend"
$EnvFile = Join-Path $RepoRoot "django_backend\.env"

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found at $PythonExe. Run 'uv venv; uv sync' first."
}

if (-not (Test-Path $FrontendDir)) {
    throw "Frontend directory not found at $FrontendDir"
}

if (-not (Test-Path $EnvFile)) {
    throw ".env file not found at $EnvFile. Create it with SECRET_KEY, JWT_SECRET_KEY, FERNET_KEY"
}

# Load .env into current process so child processes inherit
Get-Content $EnvFile | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
    }
}

# Stop any existing instances
$existing = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and (
        $_.CommandLine -like "*django_backend*manage.py*runserver*8000*" -or
        $_.CommandLine -like "*uvicorn*fastapi_backend*8001*" -or
        $_.CommandLine -like "*npm*run dev*" -or
        $_.CommandLine -like "*vite*"
    )
}

if ($existing) {
    Write-Host "Stopping existing processes..."
    $existing | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 2
}

# Run Django migrations
Write-Host "Running Django migrations..."
& $PythonExe django_backend/manage.py migrate --run-syncdb 2>&1 | Out-Null

# Start Django (auth server)
$django = Start-Process -FilePath $PythonExe `
    -ArgumentList "django_backend/manage.py", "runserver", "127.0.0.1:8000" `
    -WorkingDirectory $RepoRoot -WindowStyle Hidden -PassThru

# Start FastAPI (main API)
$fastapi = Start-Process -FilePath $PythonExe `
    -ArgumentList "-m", "uvicorn", "fastapi_backend.app.main:app", "--host", "127.0.0.1", "--port", "8001", "--reload" `
    -WorkingDirectory $RepoRoot -WindowStyle Hidden -PassThru

# Start Frontend (Vite dev server)
$frontend = Start-Process -FilePath "npm.cmd" `
    -ArgumentList "run", "dev" `
    -WorkingDirectory $FrontendDir -WindowStyle Hidden -PassThru

Write-Host ""
Write-Host "=== WEAVE-DB Started ===" -ForegroundColor Green
Write-Host "  Django (auth):   http://127.0.0.1:8000" -ForegroundColor Cyan
Write-Host "  FastAPI (API):   http://127.0.0.1:8001" -ForegroundColor Cyan
Write-Host "  FastAPI docs:    http://127.0.0.1:8001/docs" -ForegroundColor Cyan
Write-Host "  Frontend (UI):   http://localhost:8080" -ForegroundColor Cyan
Write-Host ""
Write-Host "  PIDs: Django=$($django.Id), FastAPI=$($fastapi.Id), Frontend=$($frontend.Id)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Kafka: producer auto-connects if kafka/config.ini exists (graceful fallback)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "To stop all: Get-Process -Id $($django.Id),$($fastapi.Id),$($frontend.Id) | Stop-Process" -ForegroundColor Yellow
