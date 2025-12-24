<#
.SYNOPSIS
    Installs LumaDB as a Windows Service
.DESCRIPTION
    Downloads, configures, and installs LumaDB as a Windows service.
.PARAMETER Version
    The version of LumaDB to install (default: latest)
.PARAMETER InstallPath
    Installation directory (default: C:\Program Files\LumaDB)
.PARAMETER DataPath
    Data directory (default: C:\ProgramData\LumaDB)
.EXAMPLE
    .\install.ps1
    .\install.ps1 -Version "0.1.0-beta.1"
    .\install.ps1 -InstallPath "D:\LumaDB" -DataPath "D:\LumaDB\Data"
#>

param(
    [string]$Version = "latest",
    [string]$InstallPath = "C:\Program Files\LumaDB",
    [string]$DataPath = "C:\ProgramData\LumaDB"
)

$ErrorActionPreference = "Stop"

Write-Host @"
╔══════════════════════════════════════════════════════════╗
║              LumaDB Windows Installer                     ║
║         Ultra-fast unified database platform              ║
╚══════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

# Check for admin privileges
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "ERROR: This script requires administrator privileges." -ForegroundColor Red
    Write-Host "Please run PowerShell as Administrator and try again." -ForegroundColor Yellow
    exit 1
}

Write-Host "`nInstalling LumaDB $Version..." -ForegroundColor Green

# Create directories
Write-Host "Creating directories..." -ForegroundColor Cyan
$directories = @(
    $InstallPath,
    "$DataPath\data",
    "$DataPath\logs",
    "$DataPath\config"
)
foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        Write-Host "  Created: $dir" -ForegroundColor Gray
    }
}

# Determine download URL
$arch = if ([Environment]::Is64BitOperatingSystem) { "amd64" } else { "386" }
$downloadUrl = if ($Version -eq "latest") {
    "https://github.com/abiolaogu/LumaDB/releases/latest/download/lumadb-windows-$arch.zip"
} else {
    "https://github.com/abiolaogu/LumaDB/releases/download/v$Version/lumadb-windows-$arch.zip"
}

# Download LumaDB
Write-Host "Downloading LumaDB from $downloadUrl..." -ForegroundColor Cyan
$zipPath = "$env:TEMP\lumadb.zip"
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -UseBasicParsing
} catch {
    Write-Host "ERROR: Failed to download LumaDB: $_" -ForegroundColor Red
    exit 1
}

# Extract
Write-Host "Extracting files..." -ForegroundColor Cyan
Expand-Archive -Path $zipPath -DestinationPath $InstallPath -Force
Remove-Item $zipPath -ErrorAction SilentlyContinue

# Create default configuration
$configPath = "$DataPath\config\lumadb.yaml"
if (-not (Test-Path $configPath)) {
    Write-Host "Creating default configuration..." -ForegroundColor Cyan
    $configContent = @"
# LumaDB Windows Configuration
server:
  node_id: 1
  data_dir: $($DataPath -replace '\\', '/')/data
  log_dir: $($DataPath -replace '\\', '/')/logs

api:
  rest:
    host: "0.0.0.0"
    port: 8080
  graphql:
    host: "0.0.0.0"
    port: 4000
  grpc:
    host: "0.0.0.0"
    port: 50051

kafka:
  host: "0.0.0.0"
  port: 9092

logging:
  level: info
  format: json

security:
  tls:
    enabled: false
  auth:
    enabled: false
"@
    $configContent | Out-File -FilePath $configPath -Encoding UTF8
}

# Install as Windows Service using sc.exe
Write-Host "Installing Windows Service..." -ForegroundColor Cyan

$serviceName = "LumaDB"
$serviceDisplayName = "LumaDB Database Server"
$serviceDescription = "LumaDB - Ultra-fast unified database platform with 100x faster streaming"
$binaryPath = "`"$InstallPath\lumadb.exe`" server --config `"$configPath`""

# Remove existing service if present
$existingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($existingService) {
    Write-Host "Removing existing service..." -ForegroundColor Yellow
    Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
    sc.exe delete $serviceName | Out-Null
    Start-Sleep -Seconds 2
}

# Create service
sc.exe create $serviceName binPath= $binaryPath start= auto DisplayName= $serviceDisplayName | Out-Null
sc.exe description $serviceName $serviceDescription | Out-Null
sc.exe failure $serviceName reset= 86400 actions= restart/5000/restart/10000/restart/30000 | Out-Null

# Configure firewall
Write-Host "Configuring Windows Firewall..." -ForegroundColor Cyan
$firewallRules = @(
    @{Name="LumaDB REST API"; Port=8080},
    @{Name="LumaDB Kafka"; Port=9092},
    @{Name="LumaDB GraphQL"; Port=4000},
    @{Name="LumaDB gRPC"; Port=50051}
)

foreach ($rule in $firewallRules) {
    $existing = Get-NetFirewallRule -DisplayName $rule.Name -ErrorAction SilentlyContinue
    if (-not $existing) {
        New-NetFirewallRule -DisplayName $rule.Name -Direction Inbound -Port $rule.Port -Protocol TCP -Action Allow | Out-Null
        Write-Host "  Created firewall rule: $($rule.Name)" -ForegroundColor Gray
    }
}

# Start service
Write-Host "Starting LumaDB service..." -ForegroundColor Cyan
Start-Service -Name $serviceName

# Wait and verify
Start-Sleep -Seconds 3
$service = Get-Service -Name $serviceName
if ($service.Status -eq 'Running') {
    Write-Host "`n" -NoNewline
    Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║           LumaDB installed successfully!                  ║" -ForegroundColor Green
    Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
} else {
    Write-Host "WARNING: Service may not be running. Check Event Viewer for details." -ForegroundColor Yellow
}

Write-Host "`nService Status:" -ForegroundColor Yellow
Get-Service $serviceName | Format-Table Name, Status, DisplayName -AutoSize

Write-Host "Endpoints:" -ForegroundColor Yellow
Write-Host "  REST API:  http://localhost:8080"
Write-Host "  Kafka:     localhost:9092"
Write-Host "  GraphQL:   http://localhost:4000"
Write-Host "  gRPC:      localhost:50051"

Write-Host "`nPaths:" -ForegroundColor Yellow
Write-Host "  Binary:        $InstallPath\lumadb.exe"
Write-Host "  Configuration: $configPath"
Write-Host "  Data:          $DataPath\data"
Write-Host "  Logs:          $DataPath\logs"

Write-Host "`nCommands:" -ForegroundColor Yellow
Write-Host "  Status:   Get-Service LumaDB"
Write-Host "  Stop:     Stop-Service LumaDB"
Write-Host "  Start:    Start-Service LumaDB"
Write-Host "  Restart:  Restart-Service LumaDB"
Write-Host "  Logs:     Get-Content '$DataPath\logs\lumadb.log' -Tail 50 -Wait"
