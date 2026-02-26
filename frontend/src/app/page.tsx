'use client';

import { useState, useEffect } from 'react';

interface Fund {
  code: string;
  name: string;
  latest: {
    price: number;
    time: string;
  };
  signal: string;
  ai_decision: {
    action: string;
    reason: string;
  };
}

interface SectorFlow {
  name: string;
  main_net: number;
  main_inflow: number;
  main_outflow: number;
  chg_pct: string;
  unit: string;
}

export default function Home() {
  const [funds, setFunds] = useState<Fund[]>([]);
  const [sectors, setSectors] = useState<SectorFlow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<string>('');

  const API_BASE = process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000';
  const [sectorType, setSectorType] = useState<'行业资金流' | '概念资金流' | '地域资金流'>('行业资金流');
  const [indicator, setIndicator] = useState<'今日' | '5日' | '10日'>('今日');

  useEffect(() => {
    fetchData();
    // 每5分钟刷新一次
    const interval = setInterval(fetchData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    // 切换板块类型/周期后自动刷新
    fetchData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sectorType, indicator]);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);

      // 获取基金数据
      const fundsRes = await fetch(`${API_BASE}/api/recommendations`);
      if (!fundsRes.ok) throw new Error('Failed to fetch funds');
      const fundsData = await fundsRes.json();

      // 获取板块资金流
      const sectorsRes = await fetch(
        `${API_BASE}/api/sector_fund_flow?indicator=${encodeURIComponent(indicator)}&sector_type=${encodeURIComponent(sectorType)}&top_n=20`
      );
      if (!sectorsRes.ok) throw new Error('Failed to fetch sectors');
      const sectorsData = await sectorsRes.json();

      setFunds(fundsData.actions || []);
      setSectors(sectorsData.items || []);
      setLastUpdate(new Date().toLocaleString('zh-CN'));
    } catch (err) {
      setError(err instanceof Error ? err.message : '数据加载失败');
    } finally {
      setLoading(false);
    }
  };

  const getActionColor = (action: string) => {
    switch (action.toUpperCase()) {
      case 'BUY': return 'text-green-600 bg-green-50';
      case 'SELL': return 'text-red-600 bg-red-50';
      case 'HOLD': return 'text-yellow-600 bg-yellow-50';
      default: return 'text-gray-600 bg-gray-50';
    }
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat('zh-CN', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(num);
  };

  if (loading && funds.length === 0) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">加载中...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center">
            <h1 className="text-2xl font-bold text-gray-900">
              基金量化交易机器人
            </h1>
            <div className="text-sm text-gray-500">
              最后更新: {lastUpdate}
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-8 sm:px-6 lg:px-8">
        {error && (
          <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-red-800">⚠️ {error}</p>
          </div>
        )}

        {/* 板块资金流 */}
        <section className="mb-8">
          <div className="flex flex-col gap-3 sm:flex-row sm:justify-between sm:items-center mb-4">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-4">
              <h2 className="text-xl font-semibold text-gray-900">
                板块资金流 Top 20
              </h2>

              <div className="flex flex-wrap items-center gap-2">
                <label className="text-sm text-gray-600">类型</label>
                <select
                  className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white"
                  value={sectorType}
                  onChange={(e) => setSectorType(e.target.value as any)}
                  disabled={loading}
                >
                  <option value="行业资金流">行业资金流</option>
                  <option value="概念资金流">概念资金流</option>
                  <option value="地域资金流">地域资金流</option>
                </select>

                <label className="text-sm text-gray-600 ml-2">周期</label>
                <select
                  className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white"
                  value={indicator}
                  onChange={(e) => setIndicator(e.target.value as any)}
                  disabled={loading}
                >
                  <option value="今日">今日</option>
                  <option value="5日">5日</option>
                  <option value="10日">10日</option>
                </select>

                <button
                  onClick={fetchData}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                  disabled={loading}
                >
                  {loading ? '刷新中...' : '刷新数据'}
                </button>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      排名
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      板块名称
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      主力净流入
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      涨跌幅
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {sectors.map((sector, index) => (
                    <tr key={sector.name} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {index + 1}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {sector.name}
                      </td>
                      <td className={`px-6 py-4 whitespace-nowrap text-sm text-right font-medium ${
                        sector.main_net > 0 ? 'text-green-600' : 'text-red-600'
                      }`}>
                        {sector.main_net > 0 ? '+' : ''}{formatNumber(sector.main_net)} {sector.unit}
                      </td>
                      <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${
                        parseFloat(sector.chg_pct) > 0 ? 'text-green-600' : 'text-red-600'
                      }`}>
                        {sector.chg_pct}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>

        {/* 基金持仓与建议 */}
        <section>
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            我的基金池
          </h2>

          {funds.length === 0 ? (
            <div className="bg-white rounded-lg shadow p-8 text-center text-gray-500">
              暂无基金数据，请配置 WATCH_FUNDS
            </div>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {funds.map((fund) => (
                <div
                  key={fund.code}
                  className="bg-white rounded-lg shadow p-6 hover:shadow-lg transition-shadow"
                >
                  <div className="flex justify-between items-start mb-4">
                    <div>
                      <h3 className="font-semibold text-gray-900">{fund.name}</h3>
                      <p className="text-sm text-gray-500">{fund.code}</p>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                      getActionColor(fund.ai_decision.action)
                    }`}>
                      {fund.ai_decision.action}
                    </span>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">最新价格:</span>
                      <span className="font-medium">{fund.latest.price.toFixed(4)}</span>
                    </div>
                    
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">量化信号:</span>
                      <span className={`font-medium ${getActionColor(fund.signal)}`}>
                        {fund.signal}
                      </span>
                    </div>

                    <div className="pt-3 border-t border-gray-100">
                      <p className="text-xs text-gray-600 leading-relaxed">
                        {fund.ai_decision.reason}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-gray-200 mt-12">
        <div className="max-w-7xl mx-auto px-4 py-6 sm:px-6 lg:px-8">
          <p className="text-center text-sm text-gray-500">
            Fund Quant Bot - AI驱动的基金量化交易系统
          </p>
        </div>
      </footer>
    </div>
  );
}
