@echo off
echo === 基金量化机器人 - 测试套件 ===
echo.

echo 📦 检查测试依赖...
python -c "import pytest" 2>nul
if errorlevel 1 (
    echo 安装测试依赖...
    pip install -r requirements_test.txt
)

echo.
echo 🧪 运行测试套件...
echo ================================

pytest tests/ -v --tb=short --cov=. --cov-report=term-missing --cov-report=html

echo.
echo ================================
echo ✅ 测试完成！
echo.
echo 📊 详细报告: htmlcov\index.html
echo 💡 运行单个测试: pytest tests\test_xxx.py -v

pause
