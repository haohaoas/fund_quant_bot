# backend/ui/components.py
from __future__ import annotations


def html_page(*, title: str, css: str, content: str, scripts: str = "", topbar: str = "") -> str:
    """Compose a full HTML page."""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
{css}
  </style>
</head>
<body>
{topbar}
{content}
{scripts}
</body>
</html>"""


def nav_pills(*, active: str, show_strategy: bool) -> str:
    """Render the top navigation pills."""
    def pill(href: str, text: str, key: str) -> str:
        cls = "pill active" if key == active else "pill"
        return f'<a class="{cls}" href="{href}">{text}</a>'

    items = [
        pill("/ui", "市场看板", "dashboard"),
        pill("/ui/portfolio", "我的持仓", "portfolio"),
    ]
    if show_strategy:
        items.append(pill("/ui/strategy", "策略建议", "strategy"))
    items.append(pill("/ui/record", "添加/减少", "record"))
    return "\n".join(items)


# Dashboard theme (keeps the gradient look)
DASHBOARD_CSS = """
    :root{
      --bg:#0f172a; --card:#111827; --text:#e5e7eb; --muted:#9ca3af; --line:rgba(255,255,255,0.07);
      /* A 股习惯：红涨绿跌（这里用 down=红, up=绿） */
      --up:#22c55e; --down:#ef4444; --accent:#38bdf8;
    }
    *{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
    body{margin:0;background:linear-gradient(180deg,#020617,var(--bg));color:var(--text);padding:28px}
    a{color:inherit;text-decoration:none}
    .container{max-width:1200px;margin:0 auto}
    .topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:16px}
    h1{margin:0;font-size:28px}
    .meta{color:var(--muted);font-size:12px;margin-top:6px}
    .nav{display:flex;gap:10px;flex-wrap:wrap;justify-content:flex-end}
    .pill{border:1px solid rgba(255,255,255,0.12);background:rgba(17,24,39,0.75);padding:8px 12px;border-radius:999px;font-size:13px}
    .pill.active{border-color:rgba(56,189,248,0.55);background:rgba(56,189,248,0.14)}
    .card{background:rgba(17,24,39,0.95);border-radius:14px;padding:16px;border:1px solid rgba(255,255,255,0.05);
          box-shadow:0 10px 30px rgba(0,0,0,0.35)}
    table{width:100%;border-collapse:collapse}
    th,td{padding:10px 10px;border-bottom:1px solid var(--line);font-size:13px;text-align:left;vertical-align:middle}
    th{color:var(--muted);font-weight:600}
    .right{text-align:right}
    .up{color:var(--up)}
    .down{color:var(--down)}
    .warn{color:#fbbf24}
    .controls{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
    .controls label{color:var(--muted);font-size:12px}
    .select{padding:6px 10px;border-radius:10px;border:1px solid rgba(255,255,255,0.12);
            background:rgba(2,6,23,0.35);color:var(--text);font-weight:600}
    .btn{cursor:pointer;border:none;border-radius:10px;padding:7px 12px;background:rgba(56,189,248,0.18);color:var(--text);font-weight:800}
    .btn:hover{background:rgba(56,189,248,0.26)}
"""


# App theme (portfolio / strategy / record)
APP_CSS = """
    :root {
      --bg:#0b1220;
      --card:rgba(255,255,255,.05);
      --line:rgba(255,255,255,.08);
      --text:#e6edf3;
      --muted:rgba(230,237,243,.72);
      /* A 股习惯：红涨绿跌（这里 down=红, up=绿） */
      --up:#22c55e;
      --down:#ef4444;
      --accent:rgba(147,197,253,.16);
      --accentLine:rgba(147,197,253,.35);
    }
    *{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,'PingFang SC','Hiragino Sans GB','Microsoft YaHei',sans-serif}
    body{margin:0;background:var(--bg);color:var(--text)}
    a{color:#93c5fd;text-decoration:none}

    .wrap{max-width:1100px;margin:0 auto;padding:18px}
    .top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}
    .brand{display:flex;align-items:center;gap:10px;font-weight:900;font-size:22px}

    .nav{display:flex;gap:10px;align-items:center;flex-wrap:wrap;justify-content:flex-end}
    .pill{display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);padding:8px 12px;border-radius:999px;font-weight:800;font-size:13px}
    .pill.active{background:var(--accent);border-color:var(--accentLine)}

    .card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:14px}
    .label{opacity:.75;font-size:12px}
    .muted{color:var(--muted)}
    .up{color:var(--up)}
    .down{color:var(--down)}

    .btn{cursor:pointer;border:none;border-radius:12px;padding:9px 14px;background:rgba(255,255,255,.10);color:var(--text);font-weight:900}
    .btn:hover{background:rgba(255,255,255,.14)}

    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{padding:10px 10px;border-bottom:1px solid rgba(255,255,255,.07);font-size:13px;text-align:left;vertical-align:middle}
    th{color:var(--muted);font-weight:700}
    .right{text-align:right}
"""