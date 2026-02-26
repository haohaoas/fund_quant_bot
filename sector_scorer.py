import akshare as ak

# 看看有没有你现在用的这个名字
hasattr(ak, "stock_fund_flow_sector")

# 看看真正有的名字
hasattr(ak, "stock_sector_fund_flow_rank")

help(ak.stock_sector_fund_flow_rank)