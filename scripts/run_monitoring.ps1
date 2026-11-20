# Lance un run de surveillance complet : flux Spark Structured Streaming + producteur de micro-batches.
# Usage : powershell -File scripts\run_monitoring.ps1 -Run clean [-Source clean] [-Delay 0.5] [-WaitPid 1234]
param(
    [string]$Run = "clean",
    [string]$Source = "",
    [double]$Delay = 0.5,
    [int]$WaitPid = 0
)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($WaitPid -gt 0) { Wait-Process -Id $WaitPid -ErrorAction SilentlyContinue }
if (-not $Source) { $Source = $Run }
$py = Join-Path $root ".venv\Scripts\python.exe"
$n = (Get-ChildItem (Join-Path $root "data\batches\$Source") -Filter *.parquet).Count
$stream = Start-Process -FilePath $py -ArgumentList "-u","scripts\05_stream.py","--run",$Run,"--stop-after",$n `
    -WorkingDirectory $root -RedirectStandardOutput "logs\stream_$Run.out" -RedirectStandardError "logs\stream_$Run.err" `
    -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 25
Start-Process -FilePath $py -ArgumentList "-u","scripts\04_producer.py","--run",$Run,"--source",$Source,"--delay",$Delay `
    -WorkingDirectory $root -RedirectStandardOutput "logs\producer_$Run.out" -RedirectStandardError "logs\producer_$Run.err" `
    -WindowStyle Hidden | Out-Null
Wait-Process -Id $stream.Id
