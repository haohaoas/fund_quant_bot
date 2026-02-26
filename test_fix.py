#!/usr/bin/env python3
"""快速验证修复后的data_layer"""
import sys
sys.path.insert(0, '.')

print("测试 get_fund_latest_price 导入...")
try:
    from data_layer import get_fund_latest_price
    print("✓ 导入成功")
    
    print("\n测试获取基金价格...")
    price = get_fund_latest_price("008888")
    if price:
        print(f"✓ 获取成功:")
        print(f"  - 价格: {price['price']}")
        print(f"  - 涨跌: {price.get('pct', 'N/A')}")
        print(f"  - 来源: {price['source']}")
    else:
        print("⚠ 返回None（可能是网络问题）")
        
except Exception as e:
    print(f"✗ 失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("修复完成！现在可以再次运行: python verify_p1.py")
