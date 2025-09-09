# vagird 策略工程（PTRADE 实盘）

- 主策略文件：`vagird.py`（**PTRADE 仅需此文件**）
- 开发环境：Windows + VSCode + Git
- 运行产物：`logs/`、`state/`、`reports/`（已被 .gitignore 排除）

## 本地常用命令
```powershell
# 一次性格式化 & 排查
.\scripts\format.bat
.\scripts\lint.bat

# 冒烟测试（仅做语法编译检查，不运行策略）
python -m pytest -q .\tests\test_smoke.py
