# backend/ui/strategy.py
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from .components import html_page, nav_pills, APP_CSS

router = APIRouter()


@router.get("/ui/strategy", response_class=HTMLResponse)
def ui_strategy():
    extra_css = """
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}
    @media (max-width:900px){.grid{grid-template-columns:1fr}}
    .controls{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
    .field{display:flex;flex-direction:column;gap:6px}
    .field label{color:var(--muted);font-size:12px}
    .input{padding:9px 10px;border-radius:12px;border:1px solid rgba(255,255,255,.10);
           background:rgba(255,255,255,.04);color:var(--text);min-width:140px}
    .btn{cursor:pointer;border:none;border-radius:12px;padding:9px 14px;background:rgba(56,189,248,0.18);color:var(--text);font-weight:900}
    .btn:hover{background:rgba(56,189,248,0.26)}
    .tag{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);color:var(--muted);font-size:12px;font-weight:900}
    .tag.warn{color:#fbbf24}
    .tag.ok{color:#93c5fd}
    .footer{opacity:.65;text-align:center;margin-top:14px;font-size:12px}
    .json{white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
          font-size:12px;line-height:1.45;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);
          padding:10px 12px;border-radius:12px;margin-top:8px}
    .sig{margin-bottom:14px}
    .sig-title{display:flex;align-items:center;gap:8px}
    .sig-body{margin-top:8px}
    .kv{display:grid;grid-template-columns:180px 1fr;gap:8px 12px;margin-top:6px}
    @media (max-width:900px){.kv{grid-template-columns:1fr}}
    .kv-k{color:var(--muted);font-size:12px}
    .kv-v{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:12px;line-height:1.45}
    .mini-table{width:100%;border-collapse:separate;border-spacing:0 6px;margin-top:6px}
    .mini-table td{padding:6px 10px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10)}
    .mini-table td:first-child{border-radius:12px 0 0 12px}
    .mini-table td:last-child{border-radius:0 12px 12px 0;text-align:right}
    .bar{height:8px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden;margin-top:6px}
    .bar > i{display:block;height:100%;background:rgba(56,189,248,0.45)}
    details.raw{margin-top:10px}
    details.raw > summary{cursor:pointer;color:var(--muted);font-size:12px;user-select:none}
    """

    scripts = """
<script>
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

  function fmtMoney(x){
    const n = Number(x);
    if(Number.isNaN(n)) return String(x||'');
    return n.toLocaleString('zh-CN', {minimumFractionDigits:2, maximumFractionDigits:2});
  }

  function tagHTML(text, kind){
    const cls = kind === 'warn' ? 'tag warn' : (kind === 'ok' ? 'tag ok' : 'tag');
    return `<span class="${cls}">${text}</span>`;
  }

  async function load(){
    const indEl = document.getElementById('indicator');
    const stEl = document.getElementById('sectorType');
    const bEl = document.getElementById('budget');
    const msEl = document.getElementById('maxSingle');

    const indicator = indEl ? indEl.value : '5日';
    const sector_type = stEl ? stEl.value : '行业资金流';
    const budget_today = bEl ? Number(bEl.value||0) : 0;
    const max_single_trade = msEl ? Number(msEl.value||1000) : 1000;

    const payload = {
      indicator,
      sector_type,
      top_n: 20,
      budget_today: Number.isNaN(budget_today) ? 0 : budget_today,
      max_single_trade: Number.isNaN(max_single_trade) ? 1000 : max_single_trade,
      max_trades_per_day: 5,
      min_cash_ratio: 0.15
    };

    const data = await postJSON('/api/strategy/plan', payload);

    const updated = document.getElementById('updated');
    if(updated) updated.textContent = '生成：' + (data.generated_at || '--');

    const snap = data.portfolio_snapshot || {};
    const snapshotEl = document.getElementById('snapshot');
    if(snapshotEl){
      const ratio = (Number(snap.cash_ratio||0)*100).toFixed(2) + '%';
      snapshotEl.innerHTML = `总资产：${fmtMoney(snap.total_asset)} | 现金：${fmtMoney(snap.cash)} | 现金比例：${ratio}`;
    }

    const market = data.market || {};
    const marketEl = document.getElementById('market');
    if(marketEl){
      const stale = market.stale ? '（缓存/可能过期）' : '';
      const warn = market.warning ? (' | ' + market.warning) : '';
      marketEl.innerHTML = `抓取：${market.fetched_at||'--'} <span class="muted">${stale}</span>${warn}`;
    }

    const tagsEl = document.getElementById('tags');
    if(tagsEl){
      const tags = [];
      if(market.stale) tags.push(tagHTML('市场数据可能过期', 'warn'));
      if(market.warning) tags.push(tagHTML('降级模式', 'warn'));
      tags.push(tagHTML('偏进攻', 'ok'));
      tagsEl.innerHTML = tags.join(' ');
    }

    const plan = Array.isArray(data.plan) ? data.plan : [];
    const planMeta = document.getElementById('planMeta');
    if(planMeta){
      planMeta.textContent = `共 ${plan.length} 笔建议（BUY 优先，SELL 克制）`;
    }

    const tbody = document.getElementById('planTbody');
    if(tbody){
      if(!plan.length){
        tbody.innerHTML = `<tr><td colspan="5" class="muted">暂无建议（可能是预算为 0 或风控限制，或市场数据不可用）</td></tr>`;
      }else{
        tbody.innerHTML = plan.map(p=>{
          const dirCls = p.action === 'BUY' ? 'down' : 'up';
          return `
            <tr>
              <td>${p.priority||''}</td>
              <td>${p.code||''}</td>
              <td class="${dirCls}">${p.action||''}</td>
              <td class="right ${dirCls}">${fmtMoney(p.amount)}</td>
              <td>${p.reason||''}</td>
            </tr>
          `;
        }).join('');
      }
    }

    const sig = Array.isArray(data.signals) ? data.signals : [];
    const sigEl = document.getElementById('signals');
    if(sigEl){
      if(!sig.length){
        sigEl.textContent = '暂无 signals';
      }else{
        const KEY_LABELS = {
          base_targets: '基础目标权重',
          dynamic_targets: '动态目标权重',
          tilt_params: '倾斜参数',
          min_cash_ratio: '最低现金比例',
          max_position_per_fund: '单只基金最高仓位',
          max_trades_per_day: '每日最多交易次数',
          max_single_trade: '单笔交易上限',
          sell_only_if_over_by: '仅当超配超过',
          cash: '现金',
          cash_floor: '现金安全垫',
          current_cash_ratio: '当前现金比例'
        };

        function esc(x){
          return String(x ?? '')
            .replaceAll('&','&amp;')
            .replaceAll('<','&lt;')
            .replaceAll('>','&gt;')
            .replaceAll('"','&quot;')
            .replaceAll("'",'&#39;');
        }

        function fmtPct(x){
          const n = Number(x);
          if(Number.isNaN(n)) return String(x ?? '');
          return (n * 100).toFixed(2) + '%';
        }

        function labelOf(k){
          return KEY_LABELS[k] || k;
        }

        function valToText(k, v){
          if(v === null || v === undefined) return '';
          if(typeof v === 'number'){
            if(k.includes('ratio') || k.includes('over_by')) return fmtPct(v);
            if(k.includes('cash') || k.includes('floor') || k.includes('trade') || k.includes('amount')) return fmtMoney(v);
            return String(v);
          }
          if(typeof v === 'boolean') return v ? '是' : '否';
          if(typeof v === 'string') return v;
          if(Array.isArray(v)) return v.join('、');
          return JSON.stringify(v);
        }

        function isPlainObject(o){
          return o && typeof o === 'object' && !Array.isArray(o);
        }

        function renderKV(obj, allowNested=false){
          const keys = Object.keys(obj || {});
          if(!keys.length) return '';
          const rows = keys.map(k=>{
            const v = obj[k];
            if(isPlainObject(v) && allowNested){
              return `
                <div class="kv-k">${esc(labelOf(k))}</div>
                <div class="kv-v">${renderKV(v, false) || esc(JSON.stringify(v))}</div>
              `;
            }
            return `
              <div class="kv-k">${esc(labelOf(k))}</div>
              <div class="kv-v">${esc(valToText(k, v))}</div>
            `;
          }).join('');
          return `<div class="kv">${rows}</div>`;
        }

        function renderTargets(title, targets){
          if(!isPlainObject(targets)) return '';
          const entries = Object.entries(targets);
          if(!entries.length) return '';
          entries.sort((a,b)=>Number(b[1]||0)-Number(a[1]||0));
          const rows = entries.map(([name,w])=>{
            const pct = fmtPct(w);
            const width = Math.max(0, Math.min(100, Number(w||0)*100));
            return `
              <tr>
                <td>${esc(name)}</td>
                <td>
                  <div class="bar"><i style="width:${width}%"></i></div>
                </td>
                <td style="text-align:right">${esc(pct)}</td>
              </tr>
            `;
          }).join('');
          return `
            <div class="muted" style="margin-top:8px">${esc(title)}</div>
            <table class="mini-table"><tbody>${rows}</tbody></table>
          `;
        }

        function renderSignalBody(s){
          const d = s.detail;
          if(d === null || d === undefined) return '';

          if(isPlainObject(d) && (d.base_targets || d.dynamic_targets)){
            const parts = [];
            if(d.base_targets) parts.push(renderTargets('基础权重', d.base_targets));
            if(d.dynamic_targets) parts.push(renderTargets('动态权重', d.dynamic_targets));
            const rest = {...d};
            delete rest.base_targets; delete rest.dynamic_targets;
            if(Object.keys(rest).length){
              parts.push(renderKV(rest, true));
            }
            return `<div class="sig-body">${parts.join('')}</div>`;
          }

          if(isPlainObject(d)){
            return `<div class="sig-body">${renderKV(d, true)}</div>`;
          }

          return `<div class="sig-body"><div class="kv"><div class="kv-k">详情</div><div class="kv-v">${esc(String(d))}</div></div></div>`;
        }

        function rawJSON(d){
          if(d === null || d === undefined) return '';
          try{
            return `<details class="raw"><summary>查看原始 JSON</summary><pre class="json">${esc(JSON.stringify(d, null, 2))}</pre></details>`;
          }catch(e){
            return '';
          }
        }

        sigEl.innerHTML = sig.map(s=>{
          const lvl = String(s.level||'info');
          const t = s.title || s.id || '';
          const lvlTag = lvl === 'warn' ? tagHTML('提示', 'warn') : tagHTML('说明', 'ok');
          const body = renderSignalBody(s);
          const raw = rawJSON(s.detail);
          return `
            <div class="sig">
              <div class="sig-title">${lvlTag} <b>${esc(t)}</b></div>
              ${body}
              ${raw}
            </div>
          `;
        }).join('');
      }
    }
  }

  // expose for inline onclick
  window.load = load;

  document.addEventListener('DOMContentLoaded', ()=>{
    load().catch(e=>{
      const updated = document.getElementById('updated');
      if(updated) updated.textContent = '加载失败：' + e.message;
      const tbody = document.getElementById('planTbody');
      if(tbody) tbody.innerHTML = `<tr><td colspan="5" class="muted">加载失败：${e.message}</td></tr>`;
      const sigEl = document.getElementById('signals');
      if(sigEl) sigEl.textContent = '加载失败：' + e.message;
    });
  });
</script>
"""

    content = f"""
  <div class="wrap">
    <div class="top">
      <div class="brand">🧭 策略建议</div>
      <div class="nav">
        {nav_pills(active="strategy", show_strategy=True)}
        <span class="pill">
          <span id="updated" class="muted">更新中…</span>
          <button class="btn" onclick="load()">刷新</button>
        </span>
      </div>
    </div>

    <div class="card">
      <div class="label">参数（偏进攻）</div>
      <div class="controls" style="margin-top:10px">
        <div class="field">
          <label>周期</label>
          <select id="indicator" class="input">
            <option value="今日">今日</option>
            <option value="5日" selected>5日</option>
            <option value="10日">10日</option>
          </select>
        </div>
        <div class="field">
          <label>板块</label>
          <select id="sectorType" class="input">
            <option value="行业资金流" selected>行业</option>
            <option value="概念资金流">概念</option>
            <option value="地域资金流">地域</option>
          </select>
        </div>
        <div class="field">
          <label>预算（元）</label>
          <input id="budget" class="input" value="2000" />
        </div>
        <div class="field">
          <label>单笔上限（元）</label>
          <input id="maxSingle" class="input" value="1000" />
        </div>
        <button class="btn" onclick="load()">生成计划</button>
      </div>
      <div class="muted" style="margin-top:10px">说明：策略页从“我的持仓”进入，不占用市场看板。若资金流数据不可用，会显示降级提示。</div>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">我的现状</div>
        <div class="muted" id="snapshot" style="margin-top:8px">加载中…</div>
      </div>
      <div class="card">
        <div class="label">市场数据状态</div>
        <div class="muted" id="market" style="margin-top:8px">加载中…</div>
      </div>
    </div>

    <div class="card" style="margin-top:14px;">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;">
        <div>
          <div class="label">行动计划单</div>
          <div class="muted" id="planMeta" style="margin-top:6px">加载中…</div>
        </div>
        <div id="tags"></div>
      </div>

      <table>
        <thead>
          <tr>
            <th>优先级</th>
            <th>代码</th>
            <th>方向</th>
            <th class="right">金额</th>
            <th>理由</th>
          </tr>
        </thead>
        <tbody id="planTbody">
          <tr><td colspan="5" class="muted">加载中…</td></tr>
        </tbody>
      </table>

      <div class="muted" style="margin-top:10px">提示：后续可加“一键写入 trades”按钮，把计划直接转成你的交易流水。</div>
    </div>

    <div class="card" style="margin-top:14px;">
      <div class="label">Signals（策略解释）</div>
      <div id="signals" class="muted" style="margin-top:10px">加载中…</div>
    </div>

    <div class="footer">Fund Quant Bot</div>

    {scripts}
  </div>
"""

    return html_page(
        title="Fund Quant Bot · 策略建议",
        css=APP_CSS + "\n" + extra_css,
        content=content,
        scripts="",  # scripts already embedded into content
    )