import asyncio
import time
import random

from contextlib import asynccontextmanager
from threading import RLock

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse


from . import gateway, health, proxy_pool_db, runtime
from .gateway import handle_http_gateway, handle_mixed_gateway
from .health import health_check_worker
from .runtime import (
    ACTIVE_PORTS,
    API_PORT,
    GATEWAY_FAILURE_COOLDOWN_BASE,
    GATEWAY_FAILURE_COOLDOWN_FACTOR,
    GATEWAY_FAILURE_COOLDOWN_MAX,
    GATEWAY_PORT,
    GATEWAY_NODE_FAILURES,
    HEALTH_CHECK_EVENT,
    NODE_ACTIVITY,
    POOL_STATE,
    SHUTDOWN_TIMEOUT,
    SOCKS5_PORT,
    STATE_LOCK,
    TEST_INTERVAL,
    _get_valid_nodes_snapshot,
    _log,
    _loop_exception_handler,
    selector_retry_worker,
    session_cleanup_worker,
    hot_sync_from_sqlite,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_loop_exception_handler)

    async def hot_sync_worker():
        while True:
            try:
                await asyncio.sleep(15)
                await asyncio.to_thread(hot_sync_from_sqlite)
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    health_task = asyncio.create_task(health.health_check_worker())
    cleanup_task = asyncio.create_task(session_cleanup_worker())
    selector_task = asyncio.create_task(selector_retry_worker())
    sync_task = asyncio.create_task(hot_sync_worker())
    servers = []

    http_server = await asyncio.start_server(gateway.handle_http_gateway, "0.0.0.0", GATEWAY_PORT, limit=1024 * 1024)
    servers.append(http_server)
    _log("OK", f"HTTP 旋转代理网关已启动 -> http://127.0.0.1:{GATEWAY_PORT}")

    if SOCKS5_PORT:
        mixed_server = await asyncio.start_server(gateway.handle_mixed_gateway, "0.0.0.0", SOCKS5_PORT, limit=1024 * 1024)
        servers.append(mixed_server)
        _log("OK", f"Mixed 双协议网关已启动 -> http/socks5://127.0.0.1:{SOCKS5_PORT}")

    try:
        yield
    finally:
        _log("INFO", "正在关闭代理池，请稍候...")
        for task in (health_task, cleanup_task, selector_task, sync_task):
            task.cancel()
        await asyncio.gather(health_task, cleanup_task, selector_task, sync_task, return_exceptions=True)
        for server in servers:
            server.close()
        try:
            await asyncio.wait_for(
                asyncio.gather(*(server.wait_closed() for server in servers), return_exceptions=True),
                timeout=SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            pass


app = FastAPI(title="Proxy Pool API Engine", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Proxy Pool Dashboard (Async)</title>
    <style>
        :root {
            --bg-color: #0f172a; --card-bg: rgba(30, 41, 59, 0.7); --primary: #38bdf8;
            --success: #22c55e; --warn: #f59e0b; --danger: #ef4444; --text-main: #f8fafc; --text-dim: #94a3b8;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Inter', -apple-system, sans-serif; background: var(--bg-color); color: var(--text-main); line-height: 1.6; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }
        h1 { font-size: 1.8rem; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .schedule-banner { display: inline-flex; align-items: center; gap: 8px; font-size: 0.85rem; color: #cbd5e1; background: rgba(56, 189, 248, 0.08); border: 1px solid rgba(56, 189, 248, 0.2); padding: 4px 12px; border-radius: 999px; margin-top: 6px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 16px; margin-bottom: 25px; }
        .stat-card { background: var(--card-bg); padding: 18px; border-radius: 16px; border: 1px solid rgba(255,255,255,0.1); backdrop-filter: blur(10px); text-align: center; position: relative; overflow: hidden; }
        .stat-val { font-size: 1.8rem; font-weight: bold; color: var(--primary); margin: 4px 0; }
        .stat-label { font-size: 0.85rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; }
        .stat-sub { font-size: 0.88rem; color: var(--text-dim); margin-top: 4px; }
        .status-badge { display: inline-flex; align-items: center; gap: 6px; padding: 6px 14px; border-radius: 20px; font-size: 0.85rem; font-weight: 600; }
        .status-testing { background: rgba(56, 189, 248, 0.2); color: var(--primary); animation: pulse 1.6s infinite; border: 1px solid rgba(56, 189, 248, 0.4); }
        .status-idle { background: rgba(34, 197, 94, 0.15); color: var(--success); border: 1px solid rgba(34, 197, 94, 0.3); }
        @keyframes pulse { 0% { opacity: 1; transform: scale(1); } 50% { opacity: 0.6; transform: scale(0.98); } 100% { opacity: 1; transform: scale(1); } }
        .card { background: var(--card-bg); border-radius: 16px; border: 1px solid rgba(255,255,255,0.08); overflow: hidden; box-shadow: 0 24px 80px rgba(2, 6, 23, 0.35); }
        .table-header { padding: 20px; border-bottom: 1px solid rgba(255,255,255,0.05); display: flex; justify-content: space-between; align-items: center; }
        table { width: 100%; border-collapse: collapse; text-align: left; }
        th { padding: 14px 18px; color: var(--text-dim); font-weight: 500; font-size: 0.85rem; border-bottom: 1px solid rgba(255,255,255,0.05); }
        th.sortable-th { cursor: pointer; user-select: none; }
        th.sortable-th:hover { color: var(--primary); }
        td { padding: 12px 18px; border-bottom: 1px solid rgba(255,255,255,0.02); font-size: 0.9rem; }
        tr:hover { background: rgba(255,255,255,0.02); }
        .delay-tag { padding: 2px 8px; border-radius: 4px; font-weight: 600; font-family: monospace; }
        .delay-low { color: var(--success); } .delay-mid { color: var(--warn); } .delay-high { color: var(--danger); }
        .node-status { display: inline-flex; align-items: center; gap: 7px; padding: 3px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
        .node-status.in-use { background: rgba(56, 189, 248, 0.15); color: var(--primary); box-shadow: 0 0 16px rgba(56, 189, 248, 0.15); }
        .node-status.idle { background: rgba(148, 163, 184, 0.1); color: var(--text-dim); }
        .node-status.unhealthy { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.25); }
        .node-status.cooldown { background: rgba(245, 158, 11, 0.15); color: var(--warn); border: 1px solid rgba(245, 158, 11, 0.25); }
        .breath-dot { width: 8px; height: 8px; border-radius: 999px; background: var(--text-dim); display: inline-block; }
        .node-status.in-use .breath-dot { background: var(--primary); box-shadow: 0 0 0 rgba(56, 189, 248, 0.8); animation: breathe 1.6s infinite; }
        .node-status.unhealthy .breath-dot { background: var(--danger); }
        .node-status.cooldown .breath-dot { background: var(--warn); animation: breathe-warn 2s infinite; }
        @keyframes breathe { 0% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.65); } 70% { transform: scale(1.2); box-shadow: 0 0 0 8px rgba(56, 189, 248, 0); } 100% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(56, 189, 248, 0); } }
        @keyframes breathe-warn { 0% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.5); } 70% { transform: scale(1.2); box-shadow: 0 0 0 6px rgba(245, 158, 11, 0); } 100% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); } }
        .port-link { color: var(--primary); text-decoration: none; font-family: monospace; }
        .pagination { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; padding: 20px; gap: 15px; border-top: 1px solid rgba(255,255,255,0.05); }
        .pagination-left, .pagination-right { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
        .pagination button { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); color: var(--text-main); padding: 8px 20px; border-radius: 8px; cursor: pointer; transition: all 0.2s; font-size: 0.9rem; }
        .pagination button:hover:not(:disabled) { background: var(--primary); border-color: var(--primary); color: var(--bg-color); font-weight: 600; }
        .pagination button:disabled { opacity: 0.3; cursor: not-allowed; }
        .page-size-label { color: var(--text-dim); font-size: 0.9rem; }
        .page-size-select { background: #1e293b; color: var(--text-main); border: 1px solid rgba(56, 189, 248, 0.45); border-radius: 8px; padding: 8px 34px 8px 12px; outline: none; font-weight: 700; box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.08); cursor: pointer; }
        .page-size-select:hover, .page-size-select:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.18); }
        .page-size-select option { background: #0f172a; color: #f8fafc; font-weight: 700; }
        .page-info { color: var(--text-main); font-size: 0.9rem; min-width: 180px; text-align: center; }
        .control-bar { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; padding: 16px 20px; border-bottom: 1px solid rgba(255,255,255,0.05); }
        .control-group { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
        .control-input { background: #1e293b; color: var(--text-main); border: 1px solid rgba(56, 189, 248, 0.45); border-radius: 8px; padding: 8px 12px; outline: none; font-weight: 600; }
        .control-input::placeholder { color: #cbd5e1; opacity: 0.9; }
        .control-input:hover, .control-input:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.18); }
        .control-toggle { display: inline-flex; align-items: center; gap: 8px; color: var(--text-main); font-size: 0.9rem; }
        .control-toggle input { accent-color: var(--primary); }
        .control-label { color: var(--text-dim); font-size: 0.9rem; }
        .refresh-chip { background: rgba(56,189,248,0.12); border: 1px solid rgba(56,189,248,0.35); color: var(--primary); border-radius: 999px; padding: 6px 12px; font-size: 0.85rem; font-weight: 700; }
        .action-btn { background: linear-gradient(135deg, #0284c7, #38bdf8); color: #0f172a; border: none; border-radius: 8px; padding: 8px 16px; font-weight: 700; cursor: pointer; transition: all 0.2s; font-size: 0.88rem; }
        .action-btn:hover { filter: brightness(1.15); transform: translateY(-1px); }
        .action-btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
        .update-highlight { animation: highlight-flash 1.5s ease-out; }
        @keyframes highlight-flash { 0% { background: rgba(34, 197, 94, 0.35); } 100% { background: var(--card-bg); } }
        footer { margin-top: 40px; text-align: center; color: var(--text-dim); font-size: 0.8rem; }
        .airport-item { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 10px; padding: 12px; }
        .airport-item.down { border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.08); }
        .airport-name { font-weight: 600; font-size: 0.88rem; margin-bottom: 6px; word-break: break-all; }
        .airport-item.down .airport-name { color: var(--danger); }
        .airport-stats { font-size: 0.8rem; color: var(--text-dim); }
        .airport-badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
        .airport-badge.down { background: rgba(239, 68, 68, 0.2); color: var(--danger); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>Proxy Pool 管理后台</h1>
                <div class="schedule-banner">
                    <span>⚡ 启动 3 秒自动执行首次更新</span>
                    <span>·</span>
                    <span id="banner-interval">🔄 自动轮询更新检测</span>
                    <span>·</span>
                    <span>🕒 实时同步节点状态</span>
                </div>
            </div>
            <div id="status-container"></div>
        </header>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">全池总节点数</div>
                <div class="stat-val" id="total_nodes">-</div>
                <div class="stat-sub" id="total_nodes_sub">所有订阅解析总量</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">全池有效节点</div>
                <div class="stat-val" id="valid_nodes" style="color: var(--success);">-</div>
                <div class="stat-sub" id="valid_nodes_sub">全部健康可用数</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">端口映射数量</div>
                <div class="stat-val" id="mapped_count" style="color: var(--primary);">-</div>
                <div class="stat-sub" id="mapped_range">-</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">全池可用率</div>
                <div class="stat-val" id="availability" style="color: var(--warn);">-</div>
                <div class="stat-sub" id="port_availability">端口池连通率: 100%</div>
            </div>
            <div id="last-update-card" class="stat-card">
                <div class="stat-label">最近一次更新时间</div>
                <div class="stat-val" id="last_update" style="font-size: 1.05rem; line-height: 1.3; margin-top: 6px; color: #22c55e;">-</div>
                <div class="stat-sub" id="last_update_relative" style="color: #38bdf8; font-weight: 600;">正在获取...</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">下次自动更新倒计时</div>
                <div class="stat-val" id="countdown_timer" style="font-size: 1.6rem; color: #38bdf8; font-family: monospace;">-</div>
                <div class="stat-sub" id="schedule-sub">定时自动检测</div>
            </div>
        </div>
        <div class="card" id="airport-card" style="margin-bottom: 25px;">
            <div class="table-header">
                <div>
                    <div style="font-weight: 600; font-size: 1rem;">🏢 机场健康度</div>
                    <div style="color: var(--text-dim); font-size: 0.82rem; margin-top: 2px;">疑似整体死亡 = 节点 ≥5 且有效为 0（其他机场仍有存活）</div>
                </div>
            </div>
            <div id="airport-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 12px; padding: 18px 20px;">
                <div style="color: var(--text-dim);">加载中...</div>
            </div>
        </div>
        <div class="card">
            <div class="control-bar">
                <div class="control-group">
                    <input id="search-input" class="control-input" type="text" placeholder="搜索节点名/端口">
                    <select id="delay-filter" class="page-size-select">
                        <option value="">全部延迟</option>
                        <option value="200">≤ 200 ms</option>
                        <option value="500">≤ 500 ms</option>
                        <option value="1000">≤ 1000 ms</option>
                        <option value="2000">≤ 2000 ms</option>
                    </select>
                    <label class="control-toggle"><input id="in-use-only" type="checkbox"> 只看使用中</label>
                </div>
                <div class="control-group">
                    <span class="control-label">自动刷新</span>
                    <label class="control-toggle"><input id="auto-refresh-toggle" type="checkbox" checked> 开启</label>
                    <span class="control-label">间隔</span>
                    <select id="refresh-interval" class="page-size-select">
                        <option value="3000">3 秒</option>
                        <option value="5000">5 秒</option>
                        <option value="10000">10 秒</option>
                        <option value="30000">30 秒</option>
                    </select>
                    <span id="refresh-state" class="refresh-chip">自动刷新中</span>
                    <button id="test-now-btn" class="action-btn" title="立即通知后台发起全节点可用性测速">⚡ 立即测速</button>
                </div>
                <div class="control-group">
                    <span class="control-label">⏱ 测速周期</span>
                    <select id="setting-test-interval" class="page-size-select">
                        <option value="300">5 分钟 (300s)</option>
                        <option value="900">15 分钟 (900s)</option>
                        <option value="1800" selected>30 分钟 (1800s)</option>
                        <option value="3600">1 个小时 (1h)</option>
                        <option value="21600">6 个小时 (6h)</option>
                        <option value="43200">12 个小时 (12h)</option>
                        <option value="86400">24 个小时 (24h)</option>
                        <option value="custom">自定义秒数...</option>
                    </select>
                    <button id="save-interval-btn" class="action-btn" style="background: linear-gradient(135deg, #10b981, #34d399); font-size: 0.82rem; padding: 6px 12px;" title="保存修改后台测速检测周期">保存周期</button>
                </div>
            </div>
            <table>
                <thead>
                    <tr><th class="sortable-th" data-sort="default" data-label="#">#</th><th class="sortable-th" data-sort="name" data-label="节点名称">节点名称</th><th class="sortable-th" data-sort="port" data-label="本地端口">本地端口</th><th class="sortable-th" data-sort="delay" data-label="延迟 (Latency)">延迟 (Latency)</th><th class="sortable-th" data-sort="source" data-label="来源钱包/任务">来源钱包/任务</th><th class="sortable-th" data-sort="target" data-label="目标链接">目标链接</th><th class="sortable-th" data-sort="status" data-label="状态">状态</th></tr>
                </thead>
                <tbody id="node-list"></tbody>
            </table>
            <div class="pagination">
                <button id="first-page" disabled>首页</button>
                <button id="prev-page" disabled>上一页</button>
                <span id="page-info" class="page-info">第 1 页</span>
                <button id="next-page" disabled>下一页</button>
                <button id="last-page" disabled>末页</button>
            </div>
        </div>
        <footer>HTTP: http://127.0.0.1:""" + str(GATEWAY_PORT) + """ | Mixed: http://127.0.0.1:""" + str(SOCKS5_PORT) + """ / socks5://127.0.0.1:""" + str(SOCKS5_PORT) + """</footer>
    </div>
    <script>
        let currentPage = 1, currentPerPage = 50, currentTotalPages = 1;
        let sortBy = 'default', sortOrder = 'asc';
        let globalLastUpdateTs = 0;
        let globalLastUpdateStr = '';
        let globalTestInterval = 300;
        let globalIsTesting = false;
        let globalHasLiveUpdated = false;

        function initPaginationControls() {
            document.querySelector('.pagination').innerHTML = `
                <div class="pagination-left">
                    <span class="page-size-label">每页展示</span>
                    <select id="per-page-select" class="page-size-select">
                        <option value="25">25</option>
                        <option value="50" selected>50</option>
                        <option value="100">100</option>
                        <option value="200">200</option>
                        <option value="500">500</option>
                    </select>
                    <span class="page-size-label">条</span>
                </div>
                <div class="pagination-right">
                    <button id="first-page" disabled>首页</button>
                    <button id="prev-page" disabled>上一页</button>
                    <span id="page-info" class="page-info">第 1 页</span>
                    <button id="next-page" disabled>下一页</button>
                    <button id="last-page" disabled>末页</button>
                </div>`;
        }

        function formatDurationZh(sec) {
            sec = Math.max(0, Math.floor(sec || 0));
            const h = Math.floor(sec / 3600);
            const m = Math.floor((sec % 3600) / 60);
            const s = sec % 60;
            const parts = [];
            if (h > 0) parts.push(`${h} 小时`);
            if (m > 0) parts.push(`${m} 分钟`);
            if (s > 0 || parts.length === 0) parts.push(`${s} 秒`);
            return parts.join(' ');
        }

        function formatCountdown(remain) {
            remain = Math.max(0, Math.floor(remain || 0));
            const h = Math.floor(remain / 3600);
            const m = Math.floor((remain % 3600) / 60);
            const s = remain % 60;
            const mm = String(m).padStart(2, '0');
            const ss = String(s).padStart(2, '0');
            if (h > 0) {
                return `${String(h).padStart(2, '0')}:${mm}:${ss}`;
            }
            return `${mm}:${ss}`;
        }

        function updateRelativeAndCountdown() {
            const nowSec = Date.now() / 1000;
            const cdEl = document.getElementById('countdown_timer');
            const relEl = document.getElementById('last_update_relative');
            const schEl = document.getElementById('schedule-sub');
            const bannerInt = document.getElementById('banner-interval');

            if (globalTestInterval) {
                const intLabel = formatDurationZh(globalTestInterval);
                if (schEl) schEl.innerText = `每 ${intLabel} 自动检测`;
                if (bannerInt) bannerInt.innerText = `🔄 每 ${intLabel} (${globalTestInterval}s) 自动轮询更新`;
            }

            if (globalIsTesting) {
                if (cdEl) {
                    cdEl.innerText = '测速更新中...';
                    cdEl.style.color = 'var(--warn)';
                }
                if (relEl) relEl.innerText = '正在测速更新有效节点...';
                return;
            }

            if (globalLastUpdateTs > 0 || globalLastUpdateStr) {
                const diff = Math.max(0, Math.floor(nowSec - (globalLastUpdateTs || nowSec)));
                if (relEl) {
                    if (diff < 5) {
                        relEl.innerText = '刚刚完成实时测速更新 ✨';
                    } else if (diff < 60) {
                        relEl.innerText = `${diff} 秒前已完成实时更新`;
                    } else {
                        const h = Math.floor(diff / 3600);
                        const m = Math.floor((diff % 3600) / 60);
                        const s = diff % 60;
                        const timeParts = [];
                        if (h > 0) timeParts.push(`${h} 小时`);
                        if (m > 0) timeParts.push(`${m} 分`);
                        if (s > 0 || timeParts.length === 0) timeParts.push(`${s} 秒`);
                        relEl.innerText = `${timeParts.join(' ')}前已更新`;
                    }
                }

                const remain = Math.max(0, Math.floor(globalTestInterval - diff));
                if (cdEl) {
                    cdEl.innerText = formatCountdown(remain);
                    cdEl.title = `剩余 ${formatDurationZh(remain)}`;
                    cdEl.style.color = remain <= 10 ? 'var(--warn)' : '#38bdf8';
                }
            } else {
                if (cdEl) {
                    cdEl.innerText = '等待首次更新...';
                    cdEl.style.color = '#38bdf8';
                }
                if (relEl) relEl.innerText = '启动 3 秒后执行首次更新';
            }
        }

        setInterval(updateRelativeAndCountdown, 1000);

        async function fetchStatus(page = null) {
            if (page !== null) currentPage = page;
            try {
                const search = document.getElementById('search-input').value.trim();
                const maxMs = document.getElementById('delay-filter').value;
                const inUseOnly = document.getElementById('in-use-only').checked;
                const query = new URLSearchParams({
                    page: currentPage,
                    per_page: currentPerPage,
                    search: search,
                    in_use_only: inUseOnly,
                    sort_by: sortBy,
                    sort_order: sortOrder
                });
                if (maxMs) query.set('max_ms', maxMs);
                const res = await fetch(`/api/proxy/all?${query.toString()}`);
                const data = await res.json();
                const statusRes = await fetch('/api/status');
                const status = await statusRes.json();

                document.getElementById('total_nodes').innerText = status.total_nodes;
                document.getElementById('valid_nodes').innerText = status.valid_nodes;
                document.getElementById('mapped_count').innerText = status.mapped_count;
                document.getElementById('mapped_range').innerText = status.mapping_range || '-';

                // 全池可用率 = 全池有效节点 / 全池总节点
                const poolAvail = status.total_nodes > 0 ? (status.valid_nodes / status.total_nodes * 100).toFixed(1) + '%' : '0%';
                document.getElementById('availability').innerText = poolAvail;

                // 端口池连通率 = 已映射端口数 / 规划端口总数（500个）
                const targetPorts = status.mapped_count > 0 ? status.mapped_count : 500;
                const portAvailEl = document.getElementById('port_availability');
                if (portAvailEl && status.mapped_count > 0) {
                    portAvailEl.innerText = `端口池: ${status.mapped_count} 个连通`;
                }

                const liveUpdate = status.last_update || '';
                const cachedUpdate = status.cached_last_update || '';
                const displayUpdate = liveUpdate || cachedUpdate;
                const updateEl = document.getElementById('last_update');
                globalHasLiveUpdated = Boolean(status.has_live_updated || liveUpdate);

                if (globalLastUpdateStr && globalLastUpdateStr !== displayUpdate) {
                    const card = document.getElementById('last-update-card');
                    if (card) {
                        card.classList.remove('update-highlight');
                        void card.offsetWidth;
                        card.classList.add('update-highlight');
                    }
                }
                globalLastUpdateStr = displayUpdate;
                globalLastUpdateTs = Number(status.last_update_ts || 0);
                globalTestInterval = Number(status.test_interval || 1800);
                globalIsTesting = Boolean(status.is_testing);

                const intSelect = document.getElementById('setting-test-interval');
                if (intSelect && !intSelect.matches(':focus')) {
                    const matchOpt = Array.from(intSelect.options).find(opt => opt.value === String(globalTestInterval));
                    if (matchOpt) {
                        intSelect.value = String(globalTestInterval);
                    }
                }

                if (liveUpdate) {
                    updateEl.innerText = liveUpdate;
                    updateEl.style.color = '#22c55e';
                } else if (cachedUpdate) {
                    updateEl.innerText = cachedUpdate + ' (缓存)';
                    updateEl.style.color = '#38bdf8';
                } else {
                    updateEl.innerText = '⚡ 首次测速中 (启动3s)...';
                    updateEl.style.color = 'var(--warn)';
                }

                const sCont = document.getElementById('status-container');
                sCont.innerHTML = status.is_testing ? '<span class="status-badge status-testing">⚡ 正在测速更新节点中...</span>' : (globalHasLiveUpdated ? '<span class="status-badge status-idle">✔ 节点运行正常 (已实时测速)</span>' : '<span class="status-badge status-testing">⚡ 正在执行首次全节点测速...</span>');

                updateRelativeAndCountdown();

                currentPage = data.current_page;
                currentPerPage = data.per_page;
                currentTotalPages = data.total_pages;
                const isFirstPage = data.current_page <= 1;
                const isLastPage = data.current_page >= data.total_pages;
                document.getElementById('first-page').disabled = isFirstPage;
                document.getElementById('prev-page').disabled = isFirstPage;
                document.getElementById('next-page').disabled = isLastPage;
                document.getElementById('last-page').disabled = isLastPage;
                document.getElementById('page-info').innerText = `第 ${data.current_page} 页 / 共 ${data.total_pages} 页 · 共 ${data.total_count} 个`;

                const list = document.getElementById('node-list');
                list.innerHTML = '';
                if (!data.nodes || !data.nodes.length) {
                    list.innerHTML = '<tr><td class="empty-row" colspan="7">暂无符合条件的节点</td></tr>';
                    return;
                }
                data.nodes.forEach((node, index) => {
                    const globalIndex = (data.current_page - 1) * data.per_page + index + 1;
                    const dClass = node.delay_ms < 500 ? 'delay-low' : (node.delay_ms < 1500 ? 'delay-mid' : 'delay-high');
                    let statusClass, statusText;
                    if (node.health_status === 'unhealthy') {
                        statusClass = 'unhealthy'; statusText = '异常';
                    } else if (node.health_status === 'cooldown') {
                        statusClass = 'cooldown'; statusText = '冷却中';
                    } else if (node.in_use) {
                        statusClass = 'in-use'; statusText = '使用中';
                    } else {
                        statusClass = 'idle'; statusText = '闲置中';
                    }
                    const rowBg = node.health_status === 'unhealthy' ? 'background: rgba(239, 68, 68, 0.04);'
                        : node.health_status === 'cooldown' ? 'background: rgba(245, 158, 11, 0.04);'
                        : node.in_use ? 'background: rgba(56, 189, 248, 0.05);' : '';
                    const sourceLabel = node.source_kind ? `${node.source_kind}${node.source_id ? ' · ' + node.source_id : ''}` : '-';
                    const targetLabel = node.target_url || '-';
                    list.innerHTML += `
                        <tr style="${rowBg}">
                            <td>${globalIndex}</td>
                            <td style="font-weight: 600;">${node.name}</td>
                            <td><a class="port-link" href="http://127.0.0.1:${node.port}" target="_blank">127.0.0.1:${node.port}</a></td>
                            <td><span class="delay-tag ${dClass}">${node.delay_ms} ms</span></td>
                            <td style="color: var(--text-dim);">${sourceLabel}</td>
                            <td style="color: var(--text-dim);">${targetLabel}</td>
                            <td><span class="node-status ${statusClass}"><span class="breath-dot"></span>${statusText}</span></td>
                        </tr>`;
                });
            } catch (e) {
                console.error("Fetch status error:", e);
            }
        }
        function updateSortHeaders() {
            document.querySelectorAll('th[data-sort]').forEach(th => {
                const base = th.dataset.label;
                th.innerHTML = base + (th.dataset.sort === sortBy ? (sortOrder === 'asc' ? ' ▲' : ' ▼') : '');
            });
        }
        document.querySelectorAll('th[data-sort]').forEach(th => {
            th.addEventListener('dblclick', () => {
                const key = th.dataset.sort;
                if (sortBy === key) {
                    sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
                } else {
                    sortBy = key;
                    sortOrder = 'asc';
                }
                updateSortHeaders();
                fetchStatus(1);
            });
        });
        initPaginationControls();
        document.getElementById('per-page-select').onchange = (e) => {
            currentPerPage = parseInt(e.target.value);
            fetchStatus(1);
        };
        updateSortHeaders();
        document.getElementById('first-page').onclick = () => fetchStatus(1);
        document.getElementById('prev-page').onclick = () => fetchStatus(currentPage - 1);
        document.getElementById('next-page').onclick = () => fetchStatus(currentPage + 1);
        document.getElementById('last-page').onclick = () => fetchStatus(currentTotalPages);
        document.getElementById('search-input').oninput = () => fetchStatus(1);
        document.getElementById('delay-filter').onchange = () => fetchStatus(1);
        document.getElementById('in-use-only').onchange = () => fetchStatus(1);
        const refreshToggle = document.getElementById('auto-refresh-toggle');
        const refreshInterval = document.getElementById('refresh-interval');
        const refreshState = document.getElementById('refresh-state');
        let refreshTimer = null;
        function applyRefreshTimer() {
            if (refreshTimer) clearInterval(refreshTimer);
            if (refreshToggle.checked) {
                const interval = Number(refreshInterval.value);
                refreshState.innerText = `自动刷新中 / ${interval / 1000}s`;
                refreshTimer = setInterval(() => fetchStatus(), interval);
            } else {
                refreshState.innerText = '自动刷新已停用';
            }
        }
        refreshToggle.onchange = applyRefreshTimer;
        refreshInterval.onchange = applyRefreshTimer;
        const testBtn = document.getElementById('test-now-btn');
        if (testBtn) {
            testBtn.onclick = async () => {
                testBtn.disabled = true;
                testBtn.innerText = '⚡ 测速任务已触发...';
                try {
                    await fetch('/api/health/check?wait=false', { method: 'POST' });
                    setTimeout(() => fetchStatus(), 1000);
                } catch (e) {
                    console.error('Trigger Test Error', e);
                }
                setTimeout(() => {
                    testBtn.disabled = false;
                    testBtn.innerText = '⚡ 立即测速';
                }, 4000);
            };
        }
        const saveIntBtn = document.getElementById('save-interval-btn');
        const intSelect = document.getElementById('setting-test-interval');
        async function applyIntervalChange() {
            let val = intSelect.value;
            if (val === 'custom') {
                const customVal = prompt('请输入新的自动测速检测周期 (秒, 如 1800 为 30分钟):', globalTestInterval || 1800);
                if (!customVal || isNaN(customVal) || Number(customVal) < 10) {
                    alert('请输入有效的秒数 (大于等于 10 秒)');
                    return;
                }
                val = String(parseInt(customVal));
            }
            try {
                const res = await fetch(`/api/settings/interval?interval=${val}`, { method: 'POST' });
                const json = await res.json();
                if (json.status === 'ok') {
                    globalTestInterval = Number(json.test_interval);
                    saveIntBtn.innerText = '✔ 已保存';
                    setTimeout(() => { saveIntBtn.innerText = '保存周期'; }, 2000);
                    updateRelativeAndCountdown();
                    fetchStatus();
                }
            } catch (e) {
                alert('设置周期失败: ' + e);
            }
        }
        if (saveIntBtn) saveIntBtn.onclick = applyIntervalChange;
        if (intSelect) intSelect.onchange = () => { if (intSelect.value !== 'custom') applyIntervalChange(); else saveIntBtn.click(); };

        async function loadAirports() {
            try {
                const res = await fetch('/api/observability');
                const data = await res.json();
                const grid = document.getElementById('airport-grid');
                if (!data.airports || !data.airports.length) {
                    grid.innerHTML = '<div style="color: var(--text-dim);">暂无机场数据</div>';
                    return;
                }
                grid.innerHTML = data.airports.map(a => `
                    <div class="airport-item ${a.suspected_down ? 'down' : ''}">
                        <div class="airport-name">${a.source_file} ${a.suspected_down ? '<span class="airport-badge down">疑似整体死亡</span>' : ''}</div>
                        <div class="airport-stats">总 ${a.total} · 健康 ${a.healthy} · 可疑 ${a.suspect} · 死亡 ${a.dead}</div>
                    </div>`).join('');
            } catch (e) {
                console.error('Load airports error:', e);
            }
        }

        applyRefreshTimer();
        fetchStatus(1);
        loadAirports();
        setInterval(loadAirports, 15000);
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_template)

@app.get("/api/status")
async def get_status():
    hot_sync_from_sqlite()
    with STATE_LOCK:
        total_nodes = len(POOL_STATE["all_nodes"])
        all_valid_nodes = len(POOL_STATE["all_valid_nodes"])
        mapped_nodes = len(POOL_STATE["valid_nodes"])
        mapping_ports = list(POOL_STATE["mapping_ports"])
        last_update = POOL_STATE["last_update"]
        last_update_ts = POOL_STATE.get("last_update_ts", 0)
        has_live_updated = POOL_STATE.get("has_live_updated", False)
        cached_last_update = POOL_STATE.get("cached_last_update", "")
        is_testing = POOL_STATE["is_testing"]
    now = time.time()
    current_interval = getattr(runtime, "TEST_INTERVAL", 1800)
    next_update_seconds = max(0, int(current_interval - (now - last_update_ts))) if last_update_ts > 0 else 0
    with STATE_LOCK:
        gateway_failures = {
            port: dict(state) for port, state in GATEWAY_NODE_FAILURES.items()
            if state.get("until", 0) > now
        }
    statistics = proxy_pool_db.runtime_statistics()
    valid_nodes = all_valid_nodes if all_valid_nodes else mapped_nodes
    if mapping_ports:
        mapping_start = min(mapping_ports)
        mapping_end = max(mapping_ports)
        mapping_range = f"{mapping_start} ~ {mapping_end}"
    else:
        mapping_start = None
        mapping_end = None
        mapping_range = "-"
    return {
        "status": "ok",
        "total_nodes": total_nodes,
        "valid_nodes": valid_nodes,
        "mapped_count": mapped_nodes,
        "mapping_start": mapping_start,
        "mapping_end": mapping_end,
        "mapping_range": mapping_range,
        "last_update": last_update,
        "last_update_ts": last_update_ts,
        "has_live_updated": has_live_updated,
        "cached_last_update": cached_last_update,
        "test_interval": current_interval,
        "next_update_seconds": next_update_seconds,
        "is_testing": is_testing,
        "catalog_nodes": statistics["catalog_nodes"],
        "sources": statistics["sources"],
        "persisted_bindings": statistics["persisted_bindings"],
        "persisted_healthy": statistics["persisted_healthy"],
        "persisted_suspect": statistics["persisted_suspect"],
        "gateway_circuit_open": len(gateway_failures),
        "api_port": API_PORT,
        "gateway_port": GATEWAY_PORT,
        "socks5_port": SOCKS5_PORT
    }

@app.get("/api/proxy/random")
async def get_random_proxy():
    valid = _get_valid_nodes_snapshot()
    if not valid:
        return JSONResponse({"status": "error", "message": "当前暂无有效代理"}, status_code=503)
    weights = [1 / max(1, n.get("delay", 100)) for n in valid]
    selected = random.choices(valid, weights=weights, k=1)[0]
    return {
        "status": "success", "proxy_url": selected["local_url"],
        "port": selected["port"], "name": selected["name"], "delay_ms": selected["delay"]
    }

@app.get("/api/proxy/best")
async def get_best_proxy():
    valid = _get_valid_nodes_snapshot()
    if not valid:
        return JSONResponse({"status": "error", "message": "当前暂无有效代理"}, status_code=503)
    best = min(valid, key=lambda item: item["delay"])
    return {
        "status": "success", "proxy_url": best["local_url"],
        "port": best["port"], "name": best["name"], "delay_ms": best["delay"]
    }

@app.get("/api/proxy/all")
async def get_all_proxies(
    page: int = Query(1),
    per_page: int = Query(50),
    max_ms: int = Query(None),
    search: str = Query(""),
    in_use_only: bool = Query(False),
    sort_by: str = Query("default"),
    sort_order: str = Query("asc")
):
    now = time.time()
    with STATE_LOCK:
        valid = list(POOL_STATE["valid_nodes"])
        active_snapshot = set(ACTIVE_PORTS)
        activity_snapshot = dict(NODE_ACTIVITY)
        gw_failures = dict(GATEWAY_NODE_FAILURES)

    def _health_status(n):
        if n.get("consecutive_failures"):
            return "unhealthy"
        gw = gw_failures.get(n.get("port"), {})
        if gw.get("until", 0) > now:
            return "cooldown"
        return "healthy"

    for n in valid:
        n["_health"] = _health_status(n)

    if max_ms is not None:
        valid = [n for n in valid if n["delay"] <= max_ms]
    if search:
        search_value = search.strip().lower()
        valid = [n for n in valid if search_value in (str(n["name"]) + " " + str(n["port"])).lower()]
    if in_use_only:
        valid = [n for n in valid if n["port"] in active_snapshot]

    sort_by = sort_by.lower()
    reverse = sort_order.lower() == "desc"

    def _activity_value(port, key):
        return activity_snapshot.get(port, {}).get(key, "")

    _status_order = {"unhealthy": 0, "cooldown": 1, "healthy": 2}

    if sort_by == "name":
        valid.sort(key=lambda n: str(n["name"]).lower(), reverse=reverse)
    elif sort_by == "port":
        valid.sort(key=lambda n: n["port"], reverse=reverse)
    elif sort_by == "delay":
        valid.sort(key=lambda n: n["delay"], reverse=reverse)
    elif sort_by == "source":
        valid.sort(key=lambda n: (str(_activity_value(n["port"], "source_kind")) + " " + str(_activity_value(n["port"], "source_id"))).lower(), reverse=reverse)
    elif sort_by == "target":
        valid.sort(key=lambda n: str(_activity_value(n["port"], "target_url")).lower(), reverse=reverse)
    elif sort_by == "status":
        valid.sort(key=lambda n: (
            _status_order.get(n["_health"], 2),
            0 if n["port"] in active_snapshot else 1
        ), reverse=reverse)
    else:
        valid.sort(key=lambda n: (n["port"] not in active_snapshot, n["delay"]), reverse=reverse)

    total_count = len(valid)
    allowed_page_sizes = {25, 50, 100, 200, 500}
    if per_page not in allowed_page_sizes: per_page = 50
    if page < 1: page = 1
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    if page > total_pages and total_pages > 0: page = total_pages

    start = (page - 1) * per_page
    paged = valid[start:start + per_page]

    return {
        "status": "success",
        "total_count": total_count,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page,
        "count": len(paged),
        "nodes": [{
            "proxy_url": n["local_url"], "port": n["port"], "name": n["name"],
            "delay_ms": n["delay"], "in_use": n["port"] in active_snapshot,
            "health_status": n.get("_health", "healthy"),
            "source_kind": activity_snapshot.get(n["port"], {}).get("source_kind", ""),
            "source_id": activity_snapshot.get(n["port"], {}).get("source_id", ""),
            "target_url": activity_snapshot.get(n["port"], {}).get("target_url", "")
        } for n in paged]
    }

@app.get("/api/observability")
async def get_observability():
    """可观测性面板：机场健康度 + 节点明细（机场归属/失败次数/时间戳/数据新鲜度）。"""
    nodes = proxy_pool_db.load_observability_data()
    now = time.time()

    # 机场健康度聚合
    airport_agg = {}
    total_valid = 0
    for n in nodes:
        src = n.get("source_file") or "unknown"
        item = airport_agg.setdefault(src, {"total": 0, "healthy": 0, "suspect": 0, "dead": 0})
        item["total"] += 1
        delay = n.get("delay_ms")
        if delay is None:
            item["dead"] += 1
        elif (n.get("consecutive_failures") or 0) > 0:
            item["suspect"] += 1
        else:
            item["healthy"] += 1
            total_valid += 1
    airports = []
    for src, item in airport_agg.items():
        item["source_file"] = src
        item["suspected_down"] = (
            item["total"] >= 5 and item["healthy"] == 0 and total_valid > 0
        )
        airports.append(item)
    airports.sort(key=lambda x: (-x["total"], x["source_file"]))

    # 节点明细
    node_list = []
    for n in nodes:
        delay = n.get("delay_ms")
        if delay is not None:
            status = "suspect" if (n.get("consecutive_failures") or 0) > 0 else "healthy"
        else:
            status = "dead"
        node_list.append({
            "name": n["name"],
            "port": n.get("port"),
            "source_file": n.get("source_file"),
            "delay_ms": delay,
            "consecutive_failures": n.get("consecutive_failures") or 0,
            "last_ok_at": n.get("last_ok_at"),
            "last_fail_at": n.get("last_fail_at"),
            "updated_at": n.get("updated_at"),
            "status": status,
        })

    statistics = proxy_pool_db.runtime_statistics()
    return {
        "status": "ok",
        "statistics": statistics,
        "airports": airports,
        "nodes": node_list,
        "fetched_at": int(now),
    }


@app.get("/api/proxy/top")
async def get_top_proxies(n: int = Query(5)):
    valid = sorted(_get_valid_nodes_snapshot(), key=lambda item: item["delay"])[:n]
    if not valid:
        return JSONResponse({"status": "error", "message": "当前暂无有效代理"}, status_code=503)
    return {
        "status": "success", "count": len(valid),
        "nodes": [{"proxy_url": x["local_url"], "port": x["port"], "name": x["name"], "delay_ms": x["delay"]} for x in valid]
    }

@app.get("/api/proxy/text")
@app.get("/api/proxies/text")
@app.get("/proxy.txt")
@app.get("/static_ips.txt")
async def get_proxies_plain_text(
    count: int = Query(None, description="提取数量，默认全部"),
    max_ms: int = Query(None, description="最大延迟过滤(ms)"),
    protocol: str = Query("raw", description="输出格式: raw (127.0.0.1:58001), http, socks5")
):
    """纯文本输出代理 IP 列表，方便爬虫和指纹浏览器直接导入提取"""
    valid = _get_valid_nodes_snapshot()
    if max_ms is not None:
        valid = [n for n in valid if n["delay"] <= max_ms]
    if count is not None and count > 0:
        valid = valid[:count]
    
    lines = []
    for n in valid:
        port = n["port"]
        if protocol.lower() == "http":
            lines.append(f"http://127.0.0.1:{port}")
        elif protocol.lower() == "socks5":
            lines.append(f"socks5://127.0.0.1:{port}")
        else:
            lines.append(f"127.0.0.1:{port}")
            
    return PlainTextResponse("\n".join(lines), media_type="text/plain; charset=utf-8")

@app.get("/api/proxies")
@app.get("/api/proxy/list")
async def get_proxies_list(count: int = Query(None), max_ms: int = Query(None)):
    """标准 JSON 列表输出"""
    valid = _get_valid_nodes_snapshot()
    if max_ms is not None:
        valid = [n for n in valid if n["delay"] <= max_ms]
    if count is not None and count > 0:
        valid = valid[:count]
    return {
        "status": "success",
        "count": len(valid),
        "proxies": [
            {
                "ip": "127.0.0.1",
                "port": n["port"],
                "http": f"http://127.0.0.1:{n['port']}",
                "socks5": f"socks5://127.0.0.1:{n['port']}",
                "delay_ms": n["delay"],
                "name": n["name"]
            }
            for n in valid
        ]
    }

@app.get("/get")
@app.get("/random")
@app.get("/pop")
async def get_single_proxy_compat():
    """兼容常用开源代理池的 /get /random 接口"""
    valid = _get_valid_nodes_snapshot()
    if not valid:
        return JSONResponse({"status": "error", "message": "当前暂无有效代理", "proxy": None}, status_code=503)
    best = min(valid, key=lambda item: item["delay"])
    return {
        "proxy": f"127.0.0.1:{best['port']}",
        "http": f"http://127.0.0.1:{best['port']}",
        "socks5": f"socks5://127.0.0.1:{best['port']}",
        "delay": best["delay"],
        "name": best["name"]
    }

@app.get("/all")
async def get_all_compat():
    """兼容开源代理池 /all 接口"""
    valid = _get_valid_nodes_snapshot()
    return [{"proxy": f"127.0.0.1:{n['port']}", "delay": n["delay"], "name": n["name"]} for n in valid]

@app.get("/api/runtime/cooldowns")
async def get_runtime_cooldowns():
    now = time.time()
    with STATE_LOCK:
        states = [(port, dict(state)) for port, state in GATEWAY_NODE_FAILURES.items()]
    cooldowns = []
    for port, state in sorted(states):
        until = float(state.get("until") or 0)
        if until > now:
            cooldowns.append({
                **state,
                "until_epoch": until,
                "remaining_seconds": round(until - now, 1),
            })
    return {
        "status": "success",
        "active_count": len(cooldowns),
        "tracked_count": len(states),
        "threshold": 2,
        "cooldown_base_seconds": GATEWAY_FAILURE_COOLDOWN_BASE,
        "cooldown_factor_seconds": GATEWAY_FAILURE_COOLDOWN_FACTOR,
        "cooldown_max_seconds": GATEWAY_FAILURE_COOLDOWN_MAX,
        "cooldowns": cooldowns,
    }

@app.post("/api/runtime/cooldowns/clear")
async def clear_runtime_cooldowns():
    with STATE_LOCK:
        cleared = len(GATEWAY_NODE_FAILURES)
        GATEWAY_NODE_FAILURES.clear()
    _log("INFO", f"已手动清空 {cleared} 个网关节点熔断状态")
    return {"status": "success", "cleared": cleared}

@app.post("/api/settings/interval")
async def set_test_interval(interval: int = Query(..., ge=10, le=86400, description="测速间隔秒数")):
    from .runtime import apply_runtime_settings, _log
    apply_runtime_settings({"test_interval": interval})
    h = interval // 3600
    m = (interval % 3600) // 60
    s = interval % 60
    parts = []
    if h > 0: parts.append(f"{h}小时")
    if m > 0: parts.append(f"{m}分钟")
    if s > 0 or not parts: parts.append(f"{s}秒")
    desc = "".join(parts)
    _log("OK", f"后台节点检测周期已动态修改为: {desc} ({interval} 秒)")
    return {
        "status": "ok",
        "test_interval": interval,
        "message": f"测速检测周期已成功更新为 {interval} 秒"
    }

@app.post("/api/runtime/sync")
async def trigger_runtime_sync(force: bool = Query(True)):
    synced = await asyncio.to_thread(hot_sync_from_sqlite, force)
    with STATE_LOCK:
        last_update = POOL_STATE.get("last_update", "")
        valid_count = len(POOL_STATE.get("valid_nodes", []))
        mapped_count = len(POOL_STATE.get("mapping_ports", []))
    return {
        "status": "success",
        "synced": synced,
        "last_update": last_update,
        "valid_nodes": valid_count,
        "mapped_count": mapped_count
    }

@app.post("/api/health/check")
async def trigger_health_check(wait: bool = Query(True), timeout: int = Query(300)):
    with STATE_LOCK:
        already_testing = bool(POOL_STATE["is_testing"])
    if not already_testing:
        HEALTH_CHECK_EVENT.set()
    if not wait:
        return {"status": "accepted", "already_running": already_testing}

    deadline = time.monotonic() + max(5, min(timeout, 900))
    while time.monotonic() < deadline:
        await asyncio.sleep(0.25)
        with STATE_LOCK:
            testing = bool(POOL_STATE["is_testing"])
            last_update = POOL_STATE["last_update"]
            valid_count = len(POOL_STATE["valid_nodes"])
            catalog_count = len(POOL_STATE["all_valid_nodes"])
        if already_testing:
            return {"status": "already_running", "last_update": last_update}
        if not testing:
            return {
                "status": "completed",
                "last_update": last_update,
                "mapped_valid_nodes": valid_count,
                "catalog_valid_nodes": catalog_count,
            }
    return JSONResponse({"status": "timeout", "message": "健康检查仍在执行"}, status_code=202)

