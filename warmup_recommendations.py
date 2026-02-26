#!/usr/bin/env python3
"""
预热推荐接口 - 提前计算并缓存数据
"""
import requests
import time

BASE_URL = "http://localhost:8000"

print("=" * 60)
print("🔥 预热推荐接口")
print("=" * 60)
print()

# 1. 检查后端是否在线
print("1️⃣ 检查后端状态...")
try:
    resp = requests.get(f"{BASE_URL}/api/health", timeout=2)
    if resp.status_code == 200:
        print("   ✅ 后端在线")
    else:
        print("   ❌ 后端响应异常")
        exit(1)
except Exception as e:
    print(f"   ❌ 后端未启动: {e}")
    print("   请先启动后端: cd backend && python -m uvicorn main:app --reload")
    exit(1)

print()

# 2. 检查缓存状态
print("2️⃣ 检查推荐缓存状态...")
try:
    resp = requests.get(f"{BASE_URL}/api/recommendations/status", timeout=5)
    if resp.status_code == 200:
        status = resp.json()
        print(f"   - 缓存: {'有效' if status.get('cached') else '无效/不存在'}")
        print(f"   - 计算中: {'是' if status.get('computing') else '否'}")
        if status.get('cache_age_seconds') is not None:
            print(f"   - 缓存年龄: {status['cache_age_seconds']}秒")
        
        if status.get('cached'):
            print()
            print("   ✅ 缓存已有效，无需预热")
            exit(0)
except Exception as e:
    print(f"   ⚠️ 无法获取状态: {e}")

print()

# 3. 触发后台刷新
print("3️⃣ 触发后台刷新（异步）...")
try:
    resp = requests.post(f"{BASE_URL}/api/recommendations/refresh", timeout=5)
    if resp.status_code == 200:
        result = resp.json()
        if result.get('ok'):
            print("   ✅ 后台刷新已启动")
            print("   ⏳ 预计需要 30-90 秒...")
        else:
            print(f"   ⚠️ {result.get('message')}")
except Exception as e:
    print(f"   ⚠️ 触发失败: {e}")
    print("   尝试直接调用接口...")

print()

# 4. 等待完成
print("4️⃣ 等待计算完成...")
max_wait = 120  # 最多等待2分钟
start = time.time()

while time.time() - start < max_wait:
    try:
        resp = requests.get(f"{BASE_URL}/api/recommendations/status", timeout=5)
        if resp.status_code == 200:
            status = resp.json()
            computing = status.get('computing', False)
            cached = status.get('cached', False)
            
            elapsed = int(time.time() - start)
            
            if cached and not computing:
                print(f"   ✅ 完成！耗时 {elapsed} 秒")
                print()
                
                # 验证数据
                resp = requests.get(f"{BASE_URL}/api/recommendations", timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    actions = len(data.get('actions', []))
                    print(f"   📊 数据已就绪:")
                    print(f"      - 基金数: {actions}")
                    print(f"      - 使用缓存: {data.get('cached')}")
                    print(f"      - 缓存年龄: {data.get('cache_age_seconds', 0)}秒")
                
                print()
                print("=" * 60)
                print("🎉 预热完成！前端现在可以快速访问推荐数据了")
                print("=" * 60)
                exit(0)
            else:
                # 显示进度
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"   ⏳ 计算中... ({elapsed}秒)")
        
        time.sleep(2)
        
    except Exception as e:
        print(f"   ⚠️ 检查状态失败: {e}")
        time.sleep(5)

print()
print("   ⚠️ 等待超时（2分钟）")
print("   建议检查后端日志查看详细错误")
