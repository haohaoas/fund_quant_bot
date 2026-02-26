# backend/ui/portfolio.py
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from .components import html_page, nav_pills, APP_CSS

router = APIRouter()


@router.get("/ui/portfolio", response_class=HTMLResponse)
def ui_portfolio():
    content = f"""
  <div class="wrap">
    <div class="top">
      <div class="brand">💼 我的持仓</div>
      <div class="nav">
        {nav_pills(active="portfolio", show_strategy=True)}
        <span class="pill">
          <span id="updated" class="muted">更新中…</span>
          <button class="btn" onclick="load()">刷新</button>
        </span>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">账户资产（按净投入估算）</div>
        <div class="big" id="assets">--</div>
      </div>
      <div class="card">
        <div class="label">当日总收益（占位）</div>
        <div class="big" id="today">--</div>
      </div>
    </div>

    <div class="card">
      <div class="label">持仓列表（手机 App 风格）</div>
      <div class="list" id="list"><div class="muted">加载中…</div></div>
      <div class="muted" style="margin-top:10px">说明：当日涨幅/收益后续接入基金行情后自动计算（你不用手填）。</div>
    </div>

    <div class="footer">Fund Quant Bot</div>
  </div>
"""

    extra_css = """
    .grid{display:grid;grid-template-columns:1.2fr 1fr;gap:14px;margin:14px 0}
    @media (max-width:900px){.grid{grid-template-columns:1fr}}
    .big{font-size:28px;font-weight:900;margin-top:6px}
    .list{display:flex;flex-direction:column;gap:12px;margin-top:12px}
    .item{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06);border-radius:16px;padding:14px}
    .itemTop{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
    .name{font-weight:900;font-size:16px;line-height:1.2}
    .sub{margin-top:4px;color:var(--muted);font-size:12px}
    .tag{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);color:var(--muted);font-size:12px;font-weight:900}
    .kpis{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-top:12px}
    .kpi{padding:10px 10px;border-radius:14px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06)}
    .kpi .k{font-size:12px;color:var(--muted)}
    .kpi .v{margin-top:6px;font-size:16px;font-weight:900}
    .footer{opacity:.65;text-align:center;margin-top:14px;font-size:12px}
"""

    scripts = """
<script>
  async function fetchJSON(url){
    const r = await fetch(url, { cache:'no-store' });
    const text = await r.text();
    let data = {};
    try{ data = text ? JSON.parse(text) : {}; }catch(e){ data = { raw:text }; }
    if(!r.ok){
      const msg = data && data.detail ? data.detail : text;
      throw new Error(msg || ('HTTP ' + r.status));
    }
    return data;
  }

  function fmtMoney(x){
    if(x===null||x===undefined||x==='--') return '--';
    const n = Number(x);
    if(Number.isNaN(n)) return String(x);
    return n.toLocaleString('zh-CN', {minimumFractionDigits:2, maximumFractionDigits:2});
  }

  function clsBySigned(val){
    const n = Number(val);
    if(Number.isNaN(n)) return 'muted';
    if(n>0) return 'down';
    if(n<0) return 'up';
    return 'muted';
  }

  function clsByPctText(pctText){
    const s = String(pctText||'').replace('%','');
    const n = Number(s);
    if(Number.isNaN(n)) return 'muted';
    if(n>0) return 'down';
    if(n<0) return 'up';
    return 'muted';
  }

  const SECTOR_KEY_PREFIX = 'fund_sector_override:';

  function getSectorOverride(code){
    const c = String(code||'').trim();
    if(!c) return '';
    try{ return (localStorage.getItem(SECTOR_KEY_PREFIX + c) || '').trim(); }catch(e){ return ''; }
  }

  function setSectorOverride(code, sector){
    const c = String(code||'').trim();
    if(!c) return;
    const s = String(sector||'').trim();
    try{
      if(!s){ localStorage.removeItem(SECTOR_KEY_PREFIX + c); }
      else{ localStorage.setItem(SECTOR_KEY_PREFIX + c, s); }
    }catch(e){}
  }

  function resolveSector(code, fallback){
    return getSectorOverride(code) || (fallback || '未知板块');
  }

  function bindSectorEditors(){
    const els = document.querySelectorAll('.js-edit-sector');
    els.forEach(el => {
      if(el.dataset.bound === '1') return;
      el.dataset.bound = '1';
      el.addEventListener('click', (ev) => {
        ev.preventDefault();
        const code = (el.dataset.code || '').trim();
        if(!code) return;
        const current = getSectorOverride(code) || (el.textContent || '').trim();
        const next = prompt('手动设置板块（留空=清除覆盖）', current);
        if(next === null) return;
        setSectorOverride(code, next);
        load().catch(()=>{});
      });
    });
  }

  async function load(){
    const list = document.getElementById('list');
    const updated = document.getElementById('updated');
    list.innerHTML = '<div class="muted">加载中…</div>';

    try{
      let data;
      try {
        data = await fetchJSON('/api/portfolio');
      } catch (e0) {
        try { data = await fetchJSON('/api/investments/summary'); }
        catch (e1) { data = await fetchJSON('/api/investments'); }
      }

      if (data && (data.positions || data.b_mode)) {
        const nowStr = new Date().toLocaleString('zh-CN');
        const aPositions = Array.isArray(data.positions) ? data.positions : [];
        const bPositions = (data.b_mode && Array.isArray(data.b_mode.positions)) ? data.b_mode.positions : [];
        const useA = aPositions.length > 0;

        let totalAssets = 0;
        let totalTodayProfit = null;
        let items = [];

        if (useA) {
          items = aPositions.map(pos => {
            const code = pos.code || pos.symbol || pos.fund_code || '-';
            const name = pos.name || code;
            const sector = resolveSector(code, pos.sector || '未知板块');
            const shares = Number(pos.shares || 0);
            const cost = Number(pos.avg_cost ?? pos.cost ?? pos.cost_price ?? 0);
            const holdAmount = shares * cost;

            const dcp = (pos.daily_change_pct === null || pos.daily_change_pct === undefined) ? null : Number(pos.daily_change_pct);
            const todayChgPct = (dcp === null || Number.isNaN(dcp)) ? '--' : (dcp.toFixed(2) + '%');

            const dp = (pos.daily_profit === null || pos.daily_profit === undefined) ? null : Number(pos.daily_profit);
            const hp = (pos.holding_profit === null || pos.holding_profit === undefined) ? null : Number(pos.holding_profit);

            const mv = (pos.market_value === null || pos.market_value === undefined) ? null : Number(pos.market_value);
            totalAssets += (mv !== null && !Number.isNaN(mv)) ? mv : holdAmount;

            return { code, name, sector, hold_amount: holdAmount, today_chg_pct: todayChgPct,
              today_profit: (dp !== null && !Number.isNaN(dp)) ? dp : null,
              holding_profit: (hp !== null && !Number.isNaN(hp)) ? hp : null,
            };
          });

          if (data.account && data.account.daily_profit !== null && data.account.daily_profit !== undefined) {
            const t = Number(data.account.daily_profit);
            totalTodayProfit = Number.isNaN(t) ? null : t;
          } else {
            let s = 0, ok = false;
            for (const it of items) {
              if (it.today_profit !== null && it.today_profit !== undefined) { s += Number(it.today_profit); ok = true; }
            }
            totalTodayProfit = ok ? s : null;
          }
        } else {
          items = bPositions.map(p => {
            const code = p.code || '-';
            const name = p.name || code;
            const sector = resolveSector(code, p.sector || '未知板块');
            const holdAmount = Number(p.net_amount ?? p.amount ?? 0);
            totalAssets += holdAmount;

            const dcp = (p.daily_change_pct === null || p.daily_change_pct === undefined) ? null : Number(p.daily_change_pct);
            const todayChgPct = (dcp === null || Number.isNaN(dcp)) ? '--' : (dcp.toFixed(2) + '%');

            const dp = (p.daily_profit === null || p.daily_profit === undefined) ? null : Number(p.daily_profit);
            const hp = (p.holding_profit === null || p.holding_profit === undefined) ? null : Number(p.holding_profit);

            return { code, name, sector, hold_amount: holdAmount, today_chg_pct: todayChgPct,
              today_profit: (dp !== null && !Number.isNaN(dp)) ? dp : null,
              holding_profit: (hp !== null && !Number.isNaN(hp)) ? hp : null,
            };
          });

          if (data.account && data.account.daily_profit !== null && data.account.daily_profit !== undefined) {
            const t = Number(data.account.daily_profit);
            totalTodayProfit = Number.isNaN(t) ? null : t;
          }
        }

        if (data.account && data.account.total_asset !== null && data.account.total_asset !== undefined) {
          const ta = Number(data.account.total_asset);
          if (!Number.isNaN(ta)) totalAssets = ta;
        }

        data = { generated_at: data.generated_at || nowStr, total_assets: totalAssets, total_today_profit: totalTodayProfit, items };
      }

      updated.textContent = '更新：' + (data.generated_at || '--');
      document.getElementById('assets').textContent = fmtMoney(data.total_assets);

      const t = data.total_today_profit;
      const todayEl = document.getElementById('today');
      todayEl.textContent = (t===null || t===undefined) ? '--' : fmtMoney(t);
      todayEl.className = 'big ' + clsBySigned(t);

      const items = data.items || [];

      function pctToNum(p){
        const s = String(p||'').replace('%','').trim();
        const n = Number(s);
        return Number.isNaN(n) ? null : n;
      }

      items.sort((a,b)=>{
        const an = pctToNum(a.today_chg_pct);
        const bn = pctToNum(b.today_chg_pct);
        if(an===null && bn===null) return 0;
        if(an===null) return 1;
        if(bn===null) return -1;
        return bn - an;
      });

      if(!items.length){
        list.innerHTML = '<div class="muted">暂无持仓数据：请先用 /api/investments 或 /api/trades 录入</div>';
        return;
      }

      list.innerHTML = items.map(it => {
        const name = it.name || it.code || '-';
        const sector = it.sector || '未知板块';
        const holdAmount = it.hold_amount;
        const chg = it.today_chg_pct || '--';
        const todayProfit = it.today_profit;
        const holdingProfit = it.holding_profit;
        const chgCls = clsByPctText(chg);
        const tpCls = clsBySigned(todayProfit);
        const hpCls = clsBySigned(holdingProfit);

        return `
          <div class="item">
            <div class="itemTop">
              <div>
                <div class="name">${name}</div>
                <div class="sub">${it.code || ''} · <a href="#" class="js-edit-sector" data-code="${it.code || ''}">${sector}</a> · 净投入 ${fmtMoney(holdAmount)}</div>
              </div>
              <div class="tag ${chgCls}">${chg}</div>
            </div>

            <div class="kpis">
              <div class="kpi"><div class="k">当日涨幅</div><div class="v ${chgCls}">${chg}</div></div>
              <div class="kpi"><div class="k">当日收益</div><div class="v ${tpCls}">${(todayProfit===null||todayProfit===undefined) ? '--' : fmtMoney(todayProfit)}</div></div>
              <div class="kpi"><div class="k">持有收益</div><div class="v ${hpCls}">${(holdingProfit===null||holdingProfit===undefined) ? '--' : fmtMoney(holdingProfit)}</div></div>
            </div>
          </div>
        `;
      }).join('');

      bindSectorEditors();

    }catch(e){
      updated.textContent = '加载失败：' + e.message;
      list.innerHTML = '<div class="muted">暂无数据</div>';
    }
  }

  load();
</script>
"""

    return html_page(
        title="Fund Quant Bot · 我的持仓",
        css=APP_CSS + "\n" + extra_css,
        content=content,
        scripts=scripts,
    )