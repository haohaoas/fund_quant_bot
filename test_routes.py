#!/usr/bin/env python3
"""
测试后端路由是否正常
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000"

print("=" * 60)
print("🧪 测试后端路由")
print("=" * 60)
print()

# 测试列表
tests = [
    {
        "name": "健康检查",
        "url": f"{BASE_URL}/api/health",
        "expected_keys": ["ok"],
        "timeout": 5
    },
    {
        "name": "板块资金流",
        "url": f"{BASE_URL}/api/sector_fund_flow?top_n=5",
        "expected_keys": ["items", "generated_at"],
        "timeout": 10
    },
    {
        "name": "推荐状态",
        "url": f"{BASE_URL}/api/recommendations/status",
        "expected_keys": ["cached", "computing"],
        "timeout": 5
    },
    {
        "name": "推荐（可能较慢，首次调用会触发计算）",
        "url": f"{BASE_URL}/api/recommendations",
        "expected_keys": ["actions", "summary", "market", "cached"],
        "timeout": 90,  # 首次调用可能需要60秒
        "note": "首次调用会执行完整分析，耗时较长"
    },
    {
        "name": "推荐（第二次，应该很快）",
        "url": f"{BASE_URL}/api/recommendations",
        "expected_keys": ["actions", "summary", "cached"],
        "timeout": 5,
        "check_cached": True
    },
    {
        "name": "持仓",
        "url": f"{BASE_URL}/api/portfolio",
        "expected_keys": ["cash", "positions"],
        "timeout": 10
    },
]

results = {"passed": 0, "failed": 0}

for test in tests:
    print(f"测试: {test['name']}")
    print(f"  URL: {test['url']}")
    if test.get('note'):
        print(f"  📝 {test['note']}")
    
    try:
        start = time.time()
        resp = requests.get(test['url'], timeout=test.get('timeout', 10))
        elapsed = time.time() - start
        
        if resp.status_code == 200:
            data = resp.json()
            
            # 检查预期字段
            missing = []
            for key in test['expected_keys']:
                if key not in data:
                    missing.append(key)
            
            if missing:
                print(f"  ⚠️  响应缺少字段: {missing}")
                print(f"  实际字段: {list(data.keys())}")
                results["failed"] += 1
            else:
                # 检查是否使用了缓存
                if test.get('check_cached') and data.get('cached') is False:
                    print(f"  ⚠️  预期使用缓存但没有")
                    results["failed"] += 1
                else:
                    print(f"  ✅ 通过 ({elapsed:.2f}秒)")
                    results["passed"] += 1
                
                # 显示部分数据
                if "actions" in data:
                    print(f"     - 基金数: {len(data.get('actions', []))}")
                if "items" in data:
                    print(f"     - 板块数: {len(data.get('items', []))}")
                if "cached" in data:
                    cached = data.get('cached')
                    age = data.get('cache_age_seconds')
                    print(f"     - 缓存: {'是' if cached else '否'}" + 
                          (f" (已缓存 {age}秒)" if age is not None else ""))
                if "computing" in data and data.get('computing'):
                    print(f"     - ⚠️  正在计算中，请等待后再次请求")
        else:
            print(f"  ❌ 失败: HTTP {resp.status_code}")
            print(f"     {resp.text[:200]}")
            results["failed"] += 1
            
    except requests.exceptions.Timeout:
        print(f"  ❌ 超时（>{test.get('timeout', 10)}秒）")
        if 'recommendations' in test['url']:
            print(f"     提示: 首次调用推荐接口会执行完整分析，需要30-90秒")
            print(f"     建议: 等待完成后再次请求，将使用缓存（<1秒）")
        results["failed"] += 1
    except requests.exceptions.ConnectionError:
        print(f"  ❌ 连接失败: 后端未启动")
        print(f"     请运行: cd backend && python -m uvicorn main:app --reload")
        results["failed"] += 1
    except Exception as e:
        print(f"  ❌ 错误: {e}")
        results["failed"] += 1
    
    print()

print("=" * 60)
print(f"📊 测试结果: {results['passed']} 通过 / {results['failed']} 失败")
print("=" * 60)

if results["failed"] == 0:
    print("✅ 所有路由正常！")
    print()
    print("💡 使用建议:")
    print("  - 推荐接口首次调用较慢（30-90秒），之后会使用缓存（<1秒）")
    print("  - 缓存有效期: 5分钟")
    print("  - 强制刷新: GET /api/recommendations?force_refresh=true")
    print("  - 查看状态: GET /api/recommendations/status")
else:
    print("⚠️  部分路由有问题，请检查后端日志")
