# Start the home no-AVX build detached on the desktop (survives the ssh session).
# usage (from HA): ssh pc "powershell -NoProfile -File C:\wsl\moltis-build\launch-build.ps1 -HomeRev home4"
# Progress: C:\wsl\moltis-build\build.log ("BUILD DONE" / "BUILD FAILED" at the end)
param(
  [Parameter(Mandatory = $true)][string]$HomeRev,
  [string]$Branch = 'home/20261023.01',
  [string]$Version = '20261023.01'
)
$running = wsl.exe -d moltis-build -u root --exec pgrep -f build.sh
if ($running) { "already running: $running"; exit 1 }
$cmd = "wsl.exe -d moltis-build -u root --exec /usr/bin/env HOME_REV=$HomeRev HOME_BRANCH=$Branch MOLTIS_VERSION=$Version /bin/bash /mnt/c/wsl/moltis-build/build.sh"
$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $cmd }
"started pid=$($r.ProcessId) rv=$($r.ReturnValue) rev=$HomeRev branch=$Branch version=$Version"
