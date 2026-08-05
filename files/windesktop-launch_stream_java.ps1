<#
  windesktop-launch_stream_java.ps1

  Invoked by ExecuteStreamCommand on the WindowsDesktop (Java CEM) agent class's !load flow —
  the Java-leg equivalent of windesktop-launch_stream.py's ExecuteScript Python Script Body.

  Java MiNiFi 2.24.08.0-19 (this box's staged CEM tarball) has no ExecuteScript in the *stock*
  binary (efm-windows-java-minifi.md confirmed this 2026-07-25, 114 processors). A same-day
  NAR drop-in (efm-windows-java-minifi.md, "SOLVED 2026-07-27") since added a working
  ExecuteScript (Groovy engine) to this exact agent's manifest (114 -> 122 processors) — so
  ExecuteScript *is* actually available on this box now, contradicting the plain "Java doesn't
  have it" assumption. This script still uses ExecuteStreamCommand as asked (simpler: no
  Groovy/JVM code to write, and ExecuteStreamCommand's stdin-piping is the correct way to hand
  ListenHTTP's per-request JSON to an external process — ExecuteProcess itself has no path to
  receive per-flowfile content at all, it only runs a fixed command on a timer). See
  efm-executescript.md and efm-windows-java-minifi.md for the full NAR-drop-in finding if a
  future session wants to redo this leg with ExecuteScript/Groovy instead.

  Reads the incoming FlowFile JSON ({"streamer": "..."}) from stdin, reuses
  windesktop-reposition_chrome.ps1 (same profile dir/monitor as the C++ leg, so the two never
  collide), and writes a JSON result to stdout — ExecuteStreamCommand captures stdout as the
  outgoing FlowFile content when "Output Destination Attribute" is unset.
#>

$ErrorActionPreference = "Stop"

$ProfileDir = "C:\minifi-windesktop\chrome-profile-v2"
$RepositionScript = "C:\minifi-windesktop\reposition_chrome.ps1"
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
    $stdinRaw = [Console]::In.ReadToEnd()
    $payload = $null
    if ($stdinRaw -and $stdinRaw.Trim() -ne "") {
        $payload = $stdinRaw | ConvertFrom-Json
    }
    $streamer = $payload.streamer
    if (-not $streamer) {
        Write-Result $false @{ error = "payload missing 'streamer' field" }
        exit 1
    }

    $url = "https://www.twitch.tv/$streamer"

    # Same scoped kill as the C++ leg — only chrome.exe processes running with OUR
    # -ProfileDir, so screen2's browser_launcher.py chrome (different --user-data-dir)
    # is never touched.
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

    # -ExecutionPolicy Bypass: the run-minifi.bat Java agent runs under the interactive
    # user, not LocalSystem, so this hasn't been observed to bite here the way it did on
    # the C++ Windows-service leg — kept for parity/safety regardless of how this agent
    # ends up being started in a future session.
    $repositionOut = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $RepositionScript `
        -X $TargetX -Y $TargetY -W $TargetW -H $TargetH `
        -ProfileDir $ProfileDir -SiteFullscreenKey "f" -TimeoutSeconds 10
    $repositionOut = ($repositionOut -join "`n").Trim()

    if ($repositionOut.StartsWith("OK")) {
        Write-Result $true @{ streamer = $streamer; reposition = $repositionOut }
    } else {
        # Chrome did launch even if reposition failed — report success with a status flag
        # rather than failing the flowfile, same convention as the C++ leg's script.
        Write-Result $true @{ streamer = $streamer; status = "RepositionFailed"; reposition = $repositionOut }
    }
} catch {
    Write-Result $false @{ error = $_.Exception.Message }
    exit 1
}
