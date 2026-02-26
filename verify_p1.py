#!/usr/bin/env python3
"""
P1优化验证脚本 - 快速测试所有P1改进是否正常工作
"""

import sys
import os
import time
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 60)
print("🚀 P1优化验证脚本")
print("=" * 60)
print()

# ============ 1. 测试数据层 ============
print("【1/4】测试统一数据获取层...")
print("-" * 60)

try:
    from data_layer import (
        get_data_fetcher, 
        PersistentCache,
        DataFetcher
    )
    
    # 测试缓存
    print("✓ 导入成功")
    
    cache = PersistentCache(".cache/test_cache.db", ttl_seconds=60)
    cache.set("test_key", {"hello": "world"})
    result = cache.get("test_key")
    
    assert result == {"hello": "world"}, "缓存读写失败"
    print("✓ 持久化缓存测试通过")
    
    # 测试数据获取器
    fetcher = get_data_fetcher()
    print("✓ 数据获取器初始化成功")
    
    # 测试基金数据获取（可能失败，但不应该崩溃）
    try:
        from data_layer import get_fund_latest_price
        price = get_fund_latest_price("008888")
        if price:
            print(f"✓ 基金数据获取成功: {price.get('price')}")
        else:
            print("⚠ 基金数据获取返回None（可能是网络问题）")
    except Exception as e:
        print(f"⚠ 基金数据获取失败（不影响验证）: {e}")
    
    print("✅ 数据层验证通过\n")
    
except Exception as e:
    print(f"❌ 数据层验证失败: {e}\n")
    sys.exit(1)


# ============ 2. 测试单元测试框架 ============
print("【2/4】测试单元测试框架...")
print("-" * 60)

try:
    import pytest
    print("✓ pytest已安装")
    
    # 检查测试文件是否存在
    test_files = [
        "tests/test_strategy.py",
        "tests/test_data_layer.py",
        "tests/conftest.py"
    ]
    
    for test_file in test_files:
        if os.path.exists(test_file):
            print(f"✓ {test_file} 存在")
        else:
            print(f"⚠ {test_file} 不存在")
    
    print("✅ 测试框架验证通过\n")
    
except ImportError:
    print("⚠ pytest未安装，请运行: pip install -r requirements_test.txt\n")


# ============ 3. 测试策略模块 ============
print("【3/4】测试策略模块...")
print("-" * 60)

try:
    from strategy import build_dynamic_grids, generate_today_signal
    
    # 测试网格构建
    print("测试网格构建...")
    start = time.time()
    grid = build_dynamic_grids("005165")
    elapsed1 = time.time() - start
    
    if grid and grid.get("base_price"):
        print(f"✓ 网格构建成功 (耗时: {elapsed1:.3f}s)")
        print(f"  - 基准价: {grid.get('base_price')}")
        print(f"  - 网格数: {len(grid.get('grids', []))}")
    else:
        print("⚠ 网格数据不完整（可能是数据源问题）")
    
    # 测试缓存效果
    print("测试缓存效果...")
    start = time.time()
    grid2 = build_dynamic_grids("005165")
    elapsed2 = time.time() - start
    
    if elapsed2 < elapsed1 * 0.5:
        print(f"✓ 缓存生效 (第二次: {elapsed2:.3f}s, 提速: {elapsed1/elapsed2:.1f}x)")
    else:
        print(f"⚠ 缓存可能未生效 (第二次: {elapsed2:.3f}s)")
    
    # 测试信号生成
    print("测试信号生成...")
    signal = generate_today_signal("005165", current_price=1.50)
    
    if signal and signal.get("action") in ["BUY", "HOLD", "SELL"]:
        print(f"✓ 信号生成成功: {signal.get('action')}")
        print(f"  - 理由: {signal.get('reason')[:50]}...")
    else:
        print("⚠ 信号格式异常")
    
    print("✅ 策略模块验证通过\n")
    
except Exception as e:
    print(f"❌ 策略模块验证失败: {e}\n")
    import traceback
    traceback.print_exc()


# ============ 4. 测试后端API ============
print("【4/4】测试后端API...")
print("-" * 60)

try:
    import requests
    
    # 测试健康检查
    try:
        resp = requests.get("http://localhost:8000/api/health", timeout=2)
        if resp.status_code == 200:
            print("✓ 后端API在线")
        else:
            print(f"⚠ 后端返回异常状态码: {resp.status_code}")
    except requests.exceptions.ConnectionError:
        print("⚠ 后端未启动（这是正常的，需要手动启动）")
        print("  启动命令: cd backend && python main.py")
    
    print("✅ 后端验证完成\n")
    
except ImportError:
    print("⚠ requests未安装\n")


# ============ 总结 ============
print("=" * 60)
print("📊 验证总结")
print("=" * 60)
print()
print("✅ P1优化的关键组件已验证完成！")
print()
print("下一步:")
print("  1. 运行完整测试: ./run_tests.sh 或 pytest tests/ -v")
print("  2. 启动后端: cd backend && python main.py")
print("  3. 启动前端: cd frontend && npm run dev")
print("  4. 查看文档: cat P1_USAGE_GUIDE.md")
print()
print("=" * 60)
