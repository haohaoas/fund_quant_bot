# sector_universe.py
"""
板块与基金、关键词映射：
- 这里定义：我们关注哪些“板块（sector）”
- 每个板块关联：基金列表 + 新闻搜索关键词
"""

from __future__ import annotations

from typing import Dict, List


SECTOR_FUND_MAP: Dict[str, Dict] = {
    "半导体芯片": {
        "funds": ["008888"],  # 可继续加其他半导体基金
        "keywords": ["半导体", "芯片", "集成电路", "光刻机", "GPU", "高端制造"],
        "index_symbol": "sh000941",  # 示例：中证半导体指数（自己按实际改）
    },
    "机器人": {
        "funds": ["014881", "018125"],  # 机器人 + 先进制造
        "keywords": ["机器人", "工业自动化", "先进制造", "智能制造", "人形机器人"],
        "index_symbol": "sz399276",  # 示例：智能制造相关指数
    },
    "通信设备": {
        "funds": ["013238"],
        "keywords": ["通信设备", "光模块", "CPO", "800G", "5G", "光通信"],
        "index_symbol": "sz399001",  # 这里先用大一点的指数占位
    },
    # 你以后可以继续加板块：
    # "创新药": {...}
}


def get_all_sectors() -> List[str]:
    return list(SECTOR_FUND_MAP.keys())


def get_sector_for_fund(code: str) -> str | None:
    for sector, info in SECTOR_FUND_MAP.items():
        if code in info.get("funds", []):
            return sector
    return None