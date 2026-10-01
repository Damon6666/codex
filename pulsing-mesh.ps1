$ErrorActionPreference = "Stop"
$codex = "D:\workspace\Pusling\Codex\codex-rs\target\debug\codex.exe"
$kick = "D:\workspace\Pusling\Codex\codex-rs\target\debug\codex-pulsing-e2e.exe"
$log = "D:\workspace\Pusling\Codex\mesh-log"
New-Item -ItemType Directory -Force -Path $log | Out-Null

function Test-Port([int]$Port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(500)) { return $false }
        $client.EndConnect($iar) | Out-Null
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Wait-Port([int]$Port, [int]$Seconds) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Port $Port) { return }
        Start-Sleep -Milliseconds 200
    }
    throw "port $Port did not listen"
}

$shops = @(
    @{ Name = "author"; Port = 8102; Role = "你只写代码。这封信就是任务，按信里的协议立刻写完。代码不要留在本对话，整段代码放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-scaler"; Port = 8103; Role = "你只查切分 train/test 之前就做标准化。这封信里的代码就是审查对象。结论用 CLEAN 或 NEEDS_FIX。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-target"; Port = 8104; Role = "你只查目标泄漏。这封信里的代码就是审查对象。结论用 CLEAN 或 NEEDS_FIX。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-duprow"; Port = 8105; Role = "没有 drop_duplicates 才写 NEEDS_FIX。只要调用了 drop_duplicates 就写 CLEAN。这封信里的代码就是审查对象。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-disjoint"; Port = 8106; Role = "你只查同一行是否同时出现在 train 和 test。这封信里的代码就是审查对象。结论用 CLEAN 或 NEEDS_FIX。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-featsel"; Port = 8107; Role = "你只查切分前的特征选择。这封信里的代码就是审查对象。结论用 CLEAN 或 NEEDS_FIX。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-impute"; Port = 8108; Role = "你只查切分前的缺失值填充。这封信里的代码就是审查对象。结论用 CLEAN 或 NEEDS_FIX。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-temporal"; Port = 8109; Role = "没有日期或时间就写 CLEAN。这封信里的代码就是审查对象。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" },
    @{ Name = "adv-group"; Port = 8110; Role = "没有分组列就写 CLEAN。这封信里的代码就是审查对象。答完不要留在本对话，结论放进 followup_task 的 message，target 填来信指定的店名。不要调用 spawn_agent，不要调用 wait_agent。" }
)

$task = "你是编排进程。只用 followup_task 把任务分给已经在网上的店，target 填店名。followup_task 返回后这一轮就结束，对方回信会另开一轮。不要调用 spawn_agent，不要调用 wait_agent。打回时在信里带上上一轮结论和新代码。收到答复后用中文告诉用户结果。让 author 写 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。用 followup_task 把协议寄给 author，target 填 author。信里写明：写完的代码只能放进 followup_task 的 message 寄回 codex-a，不能只写在自己的回复里。收到代码后，去掉作者是谁，只用 followup_task 把代码原文分给 adv-scaler、adv-target、adv-duprow、adv-disjoint、adv-featsel、adv-impute、adv-temporal、adv-group。有 NEEDS_FIX 就用 followup_task 打回 author，8 个都 CLEAN 再签字。签字之后关闭网格：向 author、adv-scaler、adv-target、adv-duprow、adv-disjoint、adv-featsel、adv-impute、adv-temporal、adv-group 逐个调用 mesh_shutdown，target 填店名。不要等待它们回信。这 9 个都返回 delivered 之后，再对 codex-a 调用 mesh_shutdown。"

foreach ($name in @("CODEX_PULSING_SEEDS", "CODEX_PULSING_ROSTER", "CODEX_PULSING_ROLE", "CODEX_PULSING_SPAWN", "CODEX_PULSING_WAIT", "CODEX_PULSING_READY", "TERM")) {
    Remove-Item "Env:$name" -ErrorAction SilentlyContinue
}
$env:CODEX_PULSING = "1"
$env:CODEX_PULSING_NAME = "codex-a"
$env:CODEX_PULSING_LISTEN = "127.0.0.1:8101"

if (Test-Port 8101) { throw "127.0.0.1:8101 is already in use" }

$script:streams = @()

function Start-Codex([string]$Quote, [bool]$Hidden, [string]$LogName) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $codex
    if ($Quote) {
        $psi.Arguments = '"' + $Quote.Replace('"', '\"') + '"'
    }
    $psi.UseShellExecute = $false
    $psi.WorkingDirectory = "D:\workspace"
    $psi.CreateNoWindow = $Hidden
    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    if ($Hidden) {
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        [void]$proc.Start()
        $out = [System.IO.File]::Create((Join-Path $log ($LogName + ".out")))
        $err = [System.IO.File]::Create((Join-Path $log ($LogName + ".err")))
        $script:streams += $out
        $script:streams += $err
        [void]$proc.StandardOutput.BaseStream.CopyToAsync($out)
        [void]$proc.StandardError.BaseStream.CopyToAsync($err)
    } else {
        [void]$proc.Start()
    }
    return $proc
}

$judge = Start-Codex "" $false ""
$kids = @()
try {
    Wait-Port 8101 180
    foreach ($shop in $shops) {
        $env:CODEX_PULSING_NAME = $shop.Name
        $env:CODEX_PULSING_LISTEN = "127.0.0.1:$($shop.Port)"
        $env:CODEX_PULSING_SEEDS = "127.0.0.1:8101"
        $env:CODEX_PULSING_ROLE = $shop.Role
        $kids += Start-Codex "" $true $shop.Name
    }
    foreach ($shop in $shops) { Wait-Port $shop.Port 180 }
    & $kick yell --name mesh-kick --listen 127.0.0.1:8111 --seeds 127.0.0.1:8101 --to codex-a --text $task
    if ($LASTEXITCODE -ne 0) { throw "failed to deliver the task to codex-a" }
    [void]$judge.WaitForExit()
    Start-Sleep -Seconds 2
} finally {
    if ($judge -and -not $judge.HasExited) { $judge.Kill() }
    foreach ($kid in $kids) {
        if ($kid -and -not $kid.HasExited) { $kid.Kill() }
    }
    foreach ($stream in $script:streams) { $stream.Dispose() }
}
