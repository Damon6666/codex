#!/usr/bin/env python3
"""check_leakage.py — 机械验收脚本(场景数据, 由 [completeness].acceptance_command 调用).

用法: python3 check_leakage.py <script.py>
退出码: 0 = 通过(无数据泄漏), 1 = 未通过(输出具体问题)

检查项(与审查店职责一一对应, 确定性静态检查):
  1. scaler/imputer 的 fit/fit_transform 必须出现在 train_test_split 之后,
     且仅在 X_train 上(切分前对全量 fit = 数据泄漏)
  2. 必须调用 drop_duplicates(重复行跨 train/test = 行泄漏)
  3. fit 对象不得作用于 y 或在 split 前的 DataFrame
模型的 CLEAN/NEEDS_FIX 结论是建议; 本脚本是可机器复核的验收。
"""
import re
import sys


def main() -> int:
    path = sys.argv[1]
    src = open(path, encoding="utf-8", errors="replace").read()
    problems = []

    split_m = re.search(r"train_test_split\s*\(", src)
    split_pos = split_m.start() if split_m else None

    for m in re.finditer(
        r"(StandardScaler|SimpleImputer|MinMaxScaler|RobustScaler)\s*\(\s*\)\s*\.\s*fit(?:_transform)?\s*\(\s*([A-Za-z_][\w]*)",
        src,
    ):
        target = m.group(2)
        fit_pos = m.start()
        if split_pos is None:
            problems.append(
                f"泄漏: {m.group(1)} 在 train_test_split 之前对 {target} fit(无切分直接全量拟合)"
            )
        elif fit_pos < split_pos:
            problems.append(
                f"泄漏: {m.group(1)} 在 train_test_split 之前对 {target} fit(先拟合后切分)"
            )
        elif target not in ("X_train",):
            problems.append(
                f"泄漏: {m.group(1)} 对 {target} fit — 必须仅在 X_train 上拟合"
            )

    if not re.search(r"drop_duplicates\s*\(", src):
        problems.append("泄漏: 未调用 drop_duplicates(重复行可能同时落入 train 与 test)")

    if problems:
        print("; ".join(problems))
        return 1
    print("acceptance ok: 切分后仅 X_train 拟合, drop_duplicates 已调用")
    return 0


if __name__ == "__main__":
    sys.exit(main())
