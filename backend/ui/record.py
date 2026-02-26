# backend/ui/record.py
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from .components import html_page, nav_pills, APP_CSS

router = APIRouter()


@router.get("/ui/record", response_class=HTMLResponse)
def ui_record():
    extra_css = """
    .form{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-top:10px}
    .field{display:flex;flex-direction:column;gap:6px}
    .field label{color:var(--muted);font-size:12px}
    .input{padding:9px 10px;border-radius:12px;border:1px solid rgba(255,255,255,.10);
           background:rgba(255,255,255,.04);color:var(--text);min-width:160px}
    .btnPrimary{cursor:pointer;border:none;border-radius:12px;padding:9px 14px;background:rgba(34,197,94,0.18);color:var(--text);font-weight:900}
    .btnPrimary:hover{background:rgba(34,197,94,0.26)}
    .btnSecondary{cursor:pointer;border:none;border-radius:12px;padding:9px 14px;background:rgba(56,189,248,0.18);color:var(--text);font-weight:900}
    .btnSecondary:hover{background:rgba(56,189,248,0.26)}
    .toast{margin-top:10px;color:var(--muted);font-size:12px}
    .footer{opacity:.65;text-align:center;margin-top:14px;font-size:12px}
    """

    content = f"""
  <div class="wrap">
    <div class="top">
      <div class="brand">➕ 添加/减少</div>
      <div class="nav">
        {nav_pills(active="record", show_strategy=True)}
      </div>
    </div>

    <div class="card">
      <div class="label">录入一笔（B 模式：净投入流水）</div>
      <div class="muted" style="margin-top:6px">买入/卖出只记录金额，系统会在“我的持仓”按基金代码聚合净投入。金额输入为正数即可：SELL 会自动转为负数。</div>

      <div class="form">
        <div class="field">
          <label>基金代码</label>
          <input id="invCode" class="input" placeholder="例如 008888" />
        </div>
        <div class="field">
          <label>板块（可选，手动覆盖）</label>
          <input id="invSector" class="input" placeholder="例如 半导体 / AI（留空不覆盖）" />
        </div>

        <div class="field">
          <label>操作</label>
          <select id="invAction" class="input" style="min-width:140px;">
            <option value="BUY" selected>买入（BUY）</option>
            <option value="SELL">卖出（SELL）</option>
          </select>
        </div>

        <div class="field">
          <label>金额（元）</label>
          <input id="invAmount" class="input" placeholder="例如 1000" />
        </div>

        <button class="btnPrimary" onclick="submitInvestment()">提交</button>

        <div class="field" style="margin-left:auto;">
          <label>账户现金（可选）</label>
          <input id="cash" class="input" placeholder="例如 50000" />
        </div>
        <button class="btnSecondary" onclick="submitCash()">更新现金</button>
      </div>

      <div id="toast" class="toast"></div>
    </div>

    <div class="card" style="margin-top:12px;">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;">
        <div>
          <div class="label">最近流水（只做查看，后续可加撤销/删除）</div>
          <div class="muted" id="meta" style="margin-top:6px">加载中…</div>
        </div>
        <div>
          <button class="btnSecondary" onclick="loadRecords()">刷新</button>
          <a class="pill" href="/ui/portfolio" style="margin-left:8px">去看持仓 →</a>
        </div>
      </div>

      <table>
        <thead>
          <tr>
            <th>时间</th>
            <th>代码</th>
            <th>板块</th>
            <th class="right">方向</th>
            <th class="right">金额</th>
          </tr>
        </thead>
        <tbody id="tbody">
          <tr><td colspan="5" class="muted">加载中…</td></tr>
        </tbody>
      </table>
    </div>

    <div class="footer">Fund Quant Bot</div>
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

  async function postJSON(url, body){
    const r = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body||{}),
    });
    const text = await r.text();
    let data = {};
    try{ data = text ? JSON.parse(text) : {}; }catch(e){ data = { raw:text }; }
    if(!r.ok){
      const msg = (data && data.detail) ? JSON.stringify(data.detail) : (text || ('HTTP ' + r.status));
      throw new Error(msg);
    }
    return data;
  }

  function toast(msg){
    const el = document.getElementById('toast');
    if(el) el.textContent = msg || '';
  }

  // ===== 手动板块覆盖（本地 localStorage） =====
  const SECTOR_KEY_PREFIX = 'fund_sector_override:';
  function setSectorOverride(code, sector){
    const c = String(code||'').trim();
    if(!c) return;
    const s = String(sector||'').trim();
    try{
      if(!s){ localStorage.removeItem(SECTOR_KEY_PREFIX + c); }
      else{ localStorage.setItem(SECTOR_KEY_PREFIX + c, s); }
    }catch(e){}
  }

  function fmtMoney(x){
    const n = Number(x);
    if(Number.isNaN(n)) return String(x||'');
    return n.toLocaleString('zh-CN', {minimumFractionDigits:2, maximumFractionDigits:2});
  }

  async function submitInvestment(){
    try{
      const codeEl = document.getElementById('invCode');
      const actEl = document.getElementById('invAction');
      const amtEl = document.getElementById('invAmount');
      const secEl = document.getElementById('invSector');
      const sectorOverride = (secEl && secEl.value ? secEl.value : '').trim();

      const code = (codeEl && codeEl.value ? codeEl.value : '').trim();
      const action = actEl ? actEl.value : 'BUY';
      const amount = amtEl && amtEl.value ? Number(amtEl.value) : NaN;

      if(!code){ toast('请输入基金代码'); return; }
      if(Number.isNaN(amount) || amount <= 0){ toast('请输入正确的金额（>0）'); return; }

      await postJSON('/api/investments', { code, action, amount });
      if(sectorOverride){ setSectorOverride(code, sectorOverride); }

      toast('已提交：' + (action==='SELL'?'卖出':'买入') + ' ' + code + ' ¥' + amount);
      if(amtEl) amtEl.value = '';
      if(secEl) secEl.value = '';
      loadRecords().catch(()=>{});
    }catch(e){
      toast('提交失败：' + e.message);
    }
  }

  async function submitCash(){
    try{
      const cashEl = document.getElementById('cash');
      const cash = cashEl && cashEl.value ? Number(cashEl.value) : NaN;
      if(Number.isNaN(cash) || cash < 0){ toast('请输入正确的现金（>=0）'); return; }
      await postJSON('/api/account/cash', { cash });
      toast('现金已更新：¥' + cash);
    }catch(e){
      toast('更新现金失败：' + e.message);
    }
  }

  async function loadRecords(){
    const meta = document.getElementById('meta');
    const tbody = document.getElementById('tbody');
    meta.textContent = '加载中…';
    tbody.innerHTML = '<tr><td colspan="5" class="muted">加载中…</td></tr>';

    try{
      let data;
      try{ data = await fetchJSON('/api/investments?limit=50'); }
      catch(e1){ data = await fetchJSON('/api/investments'); }

      const items = Array.isArray(data.items) ? data.items : (Array.isArray(data) ? data : []);
      meta.textContent = '记录数：' + items.length + (data.generated_at ? (' | 生成：' + data.generated_at) : '');

      if(!items.length){
        tbody.innerHTML = '<tr><td colspan="5" class="muted">暂无流水，请先在上方提交一笔</td></tr>';
        return;
      }

      tbody.innerHTML = items.map(it=>{
        const ts = it.created_at || it.time || it.datetime || '';
        const code = it.code || '';
        const sector = it.sector || '未知板块';
        const amt = Number(it.amount ?? 0);
        const dir = amt < 0 ? 'SELL' : 'BUY';
        const dirCls = amt < 0 ? 'up' : 'down';

        return `
          <tr>
            <td>${ts}</td>
            <td>${code}</td>
            <td>${sector}</td>
            <td class="right ${dirCls}">${dir}</td>
            <td class="right ${dirCls}">${fmtMoney(amt)}</td>
          </tr>
        `;
      }).join('');

    }catch(e){
      meta.textContent = '加载失败：' + e.message;
      tbody.innerHTML = '<tr><td colspan="5" class="muted">暂无数据</td></tr>';
    }
  }

  loadRecords().catch(()=>{});
</script>
"""

    return html_page(
        title="Fund Quant Bot · 添加/减少",
        css=APP_CSS + "\n" + extra_css,
        content=content,
        scripts=scripts,
    )