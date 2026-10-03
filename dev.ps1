$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ui = Join-Path $root "bt-app\ui"

$vite = Start-Process -FilePath "cmd.exe" `
    -ArgumentList "/c", "npm run dev" `
    -WorkingDirectory $ui `
    -PassThru `
    -WindowStyle Minimized

try {
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        if ($vite.HasExited) {
            throw "the vite dev server exited immediately (is port 5183 taken?)"
        }
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:5183" -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        throw "the vite dev server did not start on http://localhost:5183"
    }
    Write-Host "vite dev server running on http://localhost:5183 (HMR enabled)"
    Write-Host "starting bt-app (debug build loads the dev server)..."
    cargo run -p bt-app
}
finally {
    if ($vite -and -not $vite.HasExited) {
        taskkill /T /F /PID $vite.Id | Out-Null
    }
}
