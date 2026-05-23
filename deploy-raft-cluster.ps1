# ============================================================================
# Raft Cluster Auto-Deploy Script
# Deploys and starts 3-node Raft cluster on: gengar-1, oddish-1, charmander-5
# ============================================================================

param(
    [string]$ProjectPath = "C:\Users\HP\OneDrive - Instituto Politécnico de Setúbal\Ambiente de Trabalho\algdistp2\AD-Project2-73492-73650-73789",
    [string]$Node1 = "gengar-1",
    [string]$Node2 = "oddish-1",
    [string]$Node3 = "charmander-5"
)

$ProjectBase = Join-Path $ProjectPath "distalg-project2-base"
$ConfigFile = Join-Path $ProjectBase "src\main\resources\babel_config.properties"
$Nodes = @($Node1, $Node2, $Node3)
$BabelPort = 34000
$ServerPort = 35000

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Raft Cluster Deployment Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Update babel_config.properties
Write-Host "[1/4] Updating babel_config.properties..." -ForegroundColor Yellow
$InitialMembership = ($Nodes | ForEach-Object { "$_:$BabelPort" }) -join ","
$ConfigContent = @"
#### Agreement
agreement_proto_id=100

#### StateMachine
babel.port=$BabelPort
initial_membership=$InitialMembership

#### App
server_port=$ServerPort

##### General
babel.interface=eth0
"@

Set-Content -Path $ConfigFile -Value $ConfigContent
Write-Host "✓ Config updated: initial_membership=$InitialMembership" -ForegroundColor Green
Write-Host ""

# Step 2: Build the JAR
Write-Host "[2/4] Building project (mvn clean package)..." -ForegroundColor Yellow
Push-Location $ProjectBase
mvn clean package -DskipTests
if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Build failed!" -ForegroundColor Red
    Pop-Location
    exit 1
}
Pop-Location
$JarPath = Join-Path $ProjectBase "target\DistAlg.jar"
Write-Host "✓ JAR built: $JarPath" -ForegroundColor Green
Write-Host ""

# Step 3: Deploy JAR to each node
Write-Host "[3/4] Deploying JAR to cluster nodes..." -ForegroundColor Yellow
foreach ($Node in $Nodes) {
    Write-Host "  Copying to $Node..." -ForegroundColor Cyan
    scp -r "$ProjectBase" "${Node}:~/DistAlg" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ $Node deployment complete" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Failed to deploy to $Node (SCP may not be available; ensure SSH is configured)" -ForegroundColor Red
    }
}
Write-Host ""

# Step 4: Start Raft instances on each node
Write-Host "[4/4] Starting Raft instances on each node..." -ForegroundColor Yellow
Write-Host ""

foreach ($i = 0; $i -lt $Nodes.Count; $i++) {
    $Node = $Nodes[$i]
    $LogFile = "raft_${Node}_$(Get-Date -Format yyyyMMdd_HHmmss).log"

    Write-Host "Starting on $Node (logging to /tmp/raft_${Node}.log)..." -ForegroundColor Cyan

    $RemoteCmd = "cd ~/DistAlg/distalg-project2-base && java -jar target/DistAlg.jar babel.address=$Node babel.port=$BabelPort initial_membership=$InitialMembership server_port=$ServerPort"

    # Start process on remote node (non-blocking via nohup)
    ssh -n $Node "nohup bash -c '$RemoteCmd' > /tmp/raft_${Node}.log 2>&1 &" 2>&1 | Out-Null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Started on $Node (PID logged remotely)" -ForegroundColor Green
    } else {
        Write-Host "✗ Failed to start on $Node" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "View logs on each node:" -ForegroundColor Yellow
foreach ($Node in $Nodes) {
    Write-Host "  ssh $Node 'tail -f /tmp/raft_${Node}.log'" -ForegroundColor Cyan
}
Write-Host ""
Write-Host "To stop all instances, run on each node:" -ForegroundColor Yellow
Write-Host "  ssh <node> 'pkill -f DistAlg.jar'" -ForegroundColor Cyan
Write-Host ""
Write-Host "Waiting 10 seconds, then showing first logs from each node..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

foreach ($Node in $Nodes) {
    Write-Host ""
    Write-Host "--- $Node logs (last 20 lines) ---" -ForegroundColor Cyan
    ssh $Node "tail -20 /tmp/raft_${Node}.log" 2>&1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "✓ Check logs above for 'Leader changed: null -> <node>' to confirm consensus!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

