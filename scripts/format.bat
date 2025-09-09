@echo off
REM —— 安装并运行 isort + black —— 
python -m pip install -q --upgrade pip
python -m pip install -q black isort
python -m isort .
python -m black .
echo [OK] 格式化完成
