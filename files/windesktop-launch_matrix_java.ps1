<#
  windesktop-launch_matrix_java.ps1

  Invoked by ExecuteStreamCommand on the WindowsDesktop (Java CEM) agent class's !matrix flow —
  Java-leg equivalent of windesktop-launch_matrix.py. Fixed action, no payload needed (same
  convention as agent-NvidiaNano-launch_matrix.py / gaming-pc-launch_matrix.py — "on" is the
  only mode). See windesktop-launch_stream_java.ps1's header for why ExecuteStreamCommand was
  used here instead of the newly-available Groovy ExecuteScript.
#>

$ErrorActionPreference = "Stop"

$ProfileDir = "C:\minifi-windesktop\chrome-profile-v2"
$RepositionScript = "C:\minifi-windesktop\reposition_chrome.ps1"
$MatrixHtml = "C:\minifi-windesktop\matrix-screensaver.html"
$TargetX = -1920
$TargetY = 0
$TargetW = 1280
$TargetH = 720
$ChromePaths = @(
    "C:\Program Files\Google\Chrome\Application\chrome.exe",
    "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
)

function Find-Chrome {
    foreach ($p in $ChromePaths) { if (Test-Path $p) { return $p } }
    return "chrome.exe"
}

function Write-Result($ok, $extra) {
    $result = @{ ok = $ok } + $extra
    $result | ConvertTo-Json -Compress | Write-Output
}

try {
    # Drain stdin even though matrix mode ignores its content — ExecuteStreamCommand still
    # pipes whatever the upstream ListenHTTP received, and leaving it unread has caused
    # blocked-pipe stalls with other stream-command scripts in this lab before.
    [Console]::In.ReadToEnd() | Out-Null

    $url = "file:///" + $MatrixHtml.Replace("\", "/")

    Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -like "*$ProfileDir*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 1000

    $chrome = Find-Chrome
    Start-Process -FilePath $chrome -ArgumentList @(
        "--new-window",
        "--user-data-dir=$ProfileDir",
        "--window-position=$TargetX,$TargetY",
        "--window-size=$TargetW,$TargetH",
        $url
    ) | Out-Null

    $repositionOut = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $RepositionScript `
        -X $TargetX -Y $TargetY -W $TargetW -H $TargetH `
        -ProfileDir $ProfileDir -TimeoutSeconds 10
    $repositionOut = ($repositionOut -join "`n").Trim()

    if ($repositionOut.StartsWith("OK")) {
        Write-Result $true @{ reposition = $repositionOut }
    } else {
        Write-Result $true @{ status = "RepositionFailed"; reposition = $repositionOut }
    }
} catch {
    Write-Result $false @{ error = $_.Exception.Message }
    exit 1
}
