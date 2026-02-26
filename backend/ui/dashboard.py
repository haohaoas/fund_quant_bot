# backend/ui/dashboard.py
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse

from .components import html_page, nav_pills, DASHBOARD_CSS

router = APIRouter()


@router.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/ui")


@router.get("/ui", response_class=HTMLResponse)
def ui_dashboard():
    content = f"""
  <div class="container">
    <div class="topbar">
      <div>
        <h1>📊 市场看板</h1>
        <div class="meta" id="meta">加载中...</div>
      </div>
      <div class="nav">
        {nav_pills(active="dashboard", show_strategy=False)}
      </div>
    </div>

    <div class="card">
      <div class="controls">
        <label>周期</label>
        <select id="indicator" class="select">
          <option value="今日" selected>今日</option>
          <option value="5日">5日</option>
          <option value="10日">10日</option>
        </select>

        <label>板块</label>
        <select id="sectorType" class="select">
          <option value="行业资金流" selected>行业</option>
          <option value="概念资金流">概念</option>
          <option value="地域资金流">地域</option>
        </select>

        <button class="btn" onclick="load()">刷新</button>
      </div>

      <table>
        <thead>
          <tr>
            <th>板块</th>
            <th class="right">涨跌幅</th>
            <th class="right">主力流入(亿)</th>
            <th class="right">主力流出(亿)</th>
            <th class="right">主力净流入(亿)</th>
          </tr>
        </thead>
        <tbody id="tbody">
          <tr><td colspan="5" class="meta">加载中...</td></tr>
        </tbody>
      </table>
    </div>
  </div>
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

  function fmtNum(x,d){
    const n=Number(x);
    if(Number.isNaN(n)) return '-';
    return n.toFixed(d==null?4:d);
  }

  function pick(obj, keys){
    for(const k of keys){
      const v = obj ? obj[k] : undefined;
      if(v !== undefined && v !== null && String(v).trim() !== '') return v;
    }
    return undefined;
  }

  const indicatorEl = document.getElementById('indicator');
  const sectorTypeEl = document.getElementById('sectorType');

  function getQS(){
    try{ return new URLSearchParams(location.search); }catch(e){ return new URLSearchParams(); }
  }

  function initControlsFromQS(){
    const qs = getQS();
    const ind = qs.get('indicator');
    const st = qs.get('sector_type');
    const allowInd = new Set(['今日','5日','10日']);
    const allowSt = new Set(['行业资金流','概念资金流','地域资金流']);

    if(indicatorEl && ind && allowInd.has(ind)) indicatorEl.value = ind;
    if(sectorTypeEl && st && allowSt.has(st)) sectorTypeEl.value = st;
  }

  function syncQS(indicator, sectorType){
    const qs = getQS();
    qs.set('indicator', indicator);
    qs.set('sector_type', sectorType);
    const newUrl = location.pathname + '?' + qs.toString();
    try{ history.replaceState(null, '', newUrl); }catch(e){}
  }

  function buildDashboardUrl(indicator, sectorType, topN){
    return `/api/dashboard?indicator=${encodeURIComponent(indicator)}&sector_type=${encodeURIComponent(sectorType)}&top_n=${encodeURIComponent(topN)}`;
  }

  async function load(){
    const indicator = indicatorEl ? indicatorEl.value : '今日';
    const sectorType = sectorTypeEl ? sectorTypeEl.value : '行业资金流';
    const topN = 20;

    syncQS(indicator, sectorType);

    const data = await fetchJSON(buildDashboardUrl(indicator, sectorType, topN));
    const meta = document.getElementById('meta');
    const fetched = data.fetched_at ? ('抓取：' + data.fetched_at) : '未抓取';
    const stale = data.stale ? '（缓存/可能过期）' : '';
    const warn = data.warning ? (' | ' + data.warning) : '';
    meta.innerHTML = `生成：${data.generated_at} | ${fetched} <span class="warn">${stale}</span>${warn}`;

    const rows = (data.sectors || []).map(s=>{
      const rawName = pick(s, ['name','sector','sector_name','bk_name','板块','f14','title','concept','industry','region']);
      const code = pick(s, ['code','sector_code','bk_code','f12','id']);
      const name = (rawName && String(rawName).trim() !== '未知板块')
        ? String(rawName)
        : (code ? `未知板块（${code}）` : (rawName ? String(rawName) : '未知板块'));

      const rawChg = pick(s, ['chg_pct','chg','change','涨跌幅','f3']);
      const chg = (rawChg === undefined || rawChg === null || String(rawChg).trim()==='')
        ? '-'
        : (String(rawChg).includes('%') ? String(rawChg) : (String(rawChg)));

      const chgNum = parseFloat(String(chg).replace('%',''));
      // A 股习惯：涨为红、跌为绿
      const chgCls = (!Number.isNaN(chgNum) && chgNum >= 0) ? 'down' : 'up';

      const mainIn  = pick(s, ['main_inflow','inflow','main_in','f62']);
      const mainOut = pick(s, ['main_outflow','outflow','main_out','f66']);
      const mainNet = pick(s, ['main_net','net','main_amount','净流入','f72']);

      return `
        <tr>
          <td title="${name}">${name}</td>
          <td class="right ${chgCls}">${chg||'-'}</td>
          <td class="right down">${fmtNum(mainIn,4)}</td>
          <td class="right up">${fmtNum(mainOut,4)}</td>
          <td class="right ${Number(mainNet)>=0?'down':'up'}">${fmtNum(mainNet,4)}</td>
        </tr>
      `;
    }).join('');

    document.getElementById('tbody').innerHTML = rows || `<tr><td colspan="5" class="meta">暂无数据</td></tr>`;
  }

  initControlsFromQS();

  if(indicatorEl){ indicatorEl.addEventListener('change', ()=>load().catch(()=>{})); }
  if(sectorTypeEl){ sectorTypeEl.addEventListener('change', ()=>load().catch(()=>{})); }

  load().catch(e=>{
    document.getElementById('meta').innerText = '加载失败：' + e.message;
    document.getElementById('tbody').innerHTML = `<tr><td colspan="5" class="meta">暂无数据</td></tr>`;
  });
</script>
"""

    return html_page(
        title="Fund Quant Bot · 市场看板",
        css=DASHBOARD_CSS,
        content=content,
        scripts=scripts,
    )