# Lance toutes les expériences de performance (E1–E4) à la suite, en processus détaché.
# Usage : powershell -File scripts\run_benchmarks.ps1 [-WaitPid 1234] [-Repeats 3]
param([int]$WaitPid = 0, [int]$Repeats = 3)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($WaitPid -gt 0) { Wait-Process -Id $WaitPid -ErrorAction SilentlyContinue }
$py = Join-Path $root ".venv\Scripts\python.exe"
foreach ($exp in @("E1", "E3", "E2")) {
    & $py -u scripts\06_benchmark.py --exp $exp --repeats $Repeats 2>&1 | Out-File -Append -Encoding utf8 "logs\bench.out"
}
# E4 (streaming complet ~15 min par répétition) : une seule répétition
& $py -u scripts\06_benchmark.py --exp E4 --repeats 1 2>&1 | Out-File -Append -Encoding utf8 "logs\bench.out"
"BENCHMARKS TERMINES" | Out-File -Append -Encoding utf8 "logs\bench.out"
