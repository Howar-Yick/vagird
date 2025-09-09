@echo off
REM 用法：tag.bat v2025.09.09
if "%~1"=="" (
  echo 用法: %~nx0 TAG_NAME
  exit /b 1
)
git tag -a %1 -m "release: %1"
git push --tags 2>nul
echo [OK] 已创建 Tag %1（如未配置远端，push 会被忽略）
