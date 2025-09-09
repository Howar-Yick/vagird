@echo off
REM —— 使用 ruff 做快速静态检查（不强制修改策略逻辑） ——
python -m pip install -q ruff
python -m ruff check .
if %errorlevel% NEQ 0 (
  echo [WARN] Ruff 提示存在可改进处（不影响 PTRADE 运行）
) else (
  echo [OK] Ruff 检查通过
)
