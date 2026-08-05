<#
  windesktop-reposition_chrome.ps1

  Own copy for the WindowsDesktop/WindowsDesktopCpp EFM issue (#4) test rig — adapted from
  C:\minifi-manual\reposition_chrome.ps1 (screen2's browser_launcher.py companion script).
  Not a shared file: this one lives under C:\minifi-windesktop\ and must never be pointed at
  C:\minifi-manual\ or vice versa, so a change here can't affect the live screen2 listener.

  Difference from the original: window discovery is scoped to a chrome.exe process whose
  command line contains -ProfileDir, via Get-CimInstance Win32_Process, instead of "the first
  chrome.exe with a non-empty MainWindowTitle". The original's discovery is fine when it's the
  only chrome.exe on the box; on this box it is not always the only one (screen2's
  browser_launcher.py Scheduled Task can also spawn chrome.exe on :5901), so grabbing the wrong
  window and repositioning someone else's browser was a real risk worth closing here.

  -SiteFullscreenKey is optional: when set (e.g. "f" for Twitch's own player-fullscreen hotkey),
  the script also clicks screen-center and sends that key after F11, same as the Twitch flow.
  Leave it unset for the matrix-rain page — F11 alone is enough since there's no site chrome to
  hide on a local canvas page.
#>
param(
    [Parameter(Mandatory=$true)][int]$X,
    [Parameter(Mandatory=$true)][int]$Y,
    [Parameter(Mandatory=$true)][int]$W,
    [Parameter(Mandatory=$true)][int]$H,
    [Parameter(Mandatory=$true)][string]$ProfileDir,
    [string]$SiteFullscreenKey = "",
    [int]$TimeoutSeconds = 10
)

Add-Type @"
using System;
using System.Runtime.InteropServices;
public class Win32RepositionWinDesktop {
    [DllImport("user32.dll")]
    public static extern bool MoveWindow(IntPtr hWnd, int X, int Y, int nWidth, int nHeight, bool bRepaint);
    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr hWnd, out RECT lpRect);
    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")]
    public static extern void keybd_event(byte bVk, byte bScan, uint dwFlags, UIntPtr dwExtraInfo);
    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int X, int Y);
    [DllImport("user32.dll")]
    public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
    public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }
}
"@

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$hwnd = [IntPtr]::Zero
while ((Get-Date) -lt $deadline) {
    $matchingPids = (Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -like "*$ProfileDir*" }).ProcessId
    if ($matchingPids) {
        $proc = Get-Process -Id $matchingPids -ErrorAction SilentlyContinue |
            Where-Object { $_.MainWindowTitle -ne '' } | Select-Object -First 1
        if ($proc) {
            $hwnd = $proc.MainWindowHandle
            break
        }
    }
    Start-Sleep -Milliseconds 300
}

if ($hwnd -eq [IntPtr]::Zero) {
    Write-Host "FAIL: no chrome window with -ProfileDir=$ProfileDir appeared within timeout"
    exit 1
}

[Win32RepositionWinDesktop]::MoveWindow($hwnd, $X, $Y, $W, $H, $true) | Out-Null
Start-Sleep -Milliseconds 300

$rect = New-Object Win32RepositionWinDesktop+RECT
[Win32RepositionWinDesktop]::GetWindowRect($hwnd, [ref]$rect) | Out-Null

# Same monitor-affinity gotcha as the original: Chrome's own --kiosk fullscreen locks its
# rendered output to whichever monitor the cursor was on at launch, and MoveWindow afterward
# only moves the frame, not the composited pixels. Launch windowed, move first, F11 second.
[Win32RepositionWinDesktop]::SetForegroundWindow($hwnd) | Out-Null
Start-Sleep -Milliseconds 200
[Win32RepositionWinDesktop]::keybd_event(0x7A, 0, 0, [UIntPtr]::Zero) | Out-Null   # F11 down
Start-Sleep -Milliseconds 50
[Win32RepositionWinDesktop]::keybd_event(0x7A, 0, 2, [UIntPtr]::Zero) | Out-Null   # F11 up
Start-Sleep -Milliseconds 300

if ($SiteFullscreenKey -ne "") {
    # F11 only hides Chrome's own toolbar; a real site (e.g. Twitch) still renders its own
    # sidebar/chat/nav around the video. Simulate the real user action: click the page to
    # give it focus, then send the site's own fullscreen hotkey.
    Start-Sleep -Milliseconds 2500
    $clickX = [int]($X + $W / 2)
    $clickY = [int]($Y + $H / 2)
    [Win32RepositionWinDesktop]::SetCursorPos($clickX, $clickY) | Out-Null
    Start-Sleep -Milliseconds 100
    [Win32RepositionWinDesktop]::mouse_event(0x0002, 0, 0, 0, [UIntPtr]::Zero) | Out-Null  # left down
    Start-Sleep -Milliseconds 50
    [Win32RepositionWinDesktop]::mouse_event(0x0004, 0, 0, 0, [UIntPtr]::Zero) | Out-Null  # left up
    Start-Sleep -Milliseconds 300
    $vk = [byte][char]($SiteFullscreenKey.ToUpper()[0])
    [Win32RepositionWinDesktop]::keybd_event($vk, 0, 0, [UIntPtr]::Zero) | Out-Null
    Start-Sleep -Milliseconds 50
    [Win32RepositionWinDesktop]::keybd_event($vk, 0, 2, [UIntPtr]::Zero) | Out-Null
    Start-Sleep -Milliseconds 300
}

Write-Host "OK: L=$($rect.Left) T=$($rect.Top) R=$($rect.Right) B=$($rect.Bottom)"
