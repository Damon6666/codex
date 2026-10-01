# run-demo.ps1 — 模型链重试版 demo 启动器
# 用法: powershell -File run-demo.ps1
# 原理: 依次尝试模型链, 某模型满载/失败(exit != 0)自动换下一个, 成功即停

$models = @('gpt-5.6-luna', 'gpt-5.6-sol')   # ← 模型链, 按需增删

cd D:\workspace
$env:CODEX_PULSING_TEAM = 'pulsing-team.toml'
$env:CODEX_PULSING_LOG_DIR = 'D:\workspace\Pusling\demo'

$task = '写一个 sklearn 的 LogisticRegression 训练脚本，并且在切分 train/test 之前先对特征做标准化。有 NEEDS_FIX 就打回写代码的那个重写，全部 CLEAN 后用中文汇报。'

foreach ($m in $models) {
  Write-Host "=== 尝试模型 $m ==="
  & 'D:\workspace\Pusling\Codex\codex-rs\target\debug\codex.exe' -c "model=`"$m`"" $task *>> D:\workspace\Pusling\demo\judge.log
  if ($LASTEXITCODE -eq 0) { Write-Host "=== 完成 (模型 $m) ==="; exit 0 }
  Write-Host "模型 $m 失败 (exit $LASTEXITCODE), 换下一个..."
}
Write-Host "=== 模型链全部失败 ==="
exit 1
