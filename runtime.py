import asyncio
import base64
import glob
import ipaddress
import json
import os
import random
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, OrderedDict, deque
from contextlib import asynccontextmanager
from statistics import median
from threading import RLock

import aiohttp
import yaml

from . import proxy_pool_db

# ================= 配置区 =================
PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = PACKAGE_DIR


def _load_dotenv():
    """极简 .env 加载：从项目根目录读取 KEY=VALUE，写入环境变量（不覆盖已有值）。"""
    env_path = os.path.join(BASE_DIR, ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


_load_dotenv()

SUBSCRIPTION_DIR = os.path.join(BASE_DIR, "订阅yaml文件")  # 订阅 YAML 文件存放目录
OUTPUT_CONFIG = os.path.join(BASE_DIR, "proxy_pool_v3_final.yaml")
GENERATED_CONFIG_FILENAMES = {
    "pool_config_auth.yaml",
    "proxy_pool_v3_final.yaml",
    os.path.basename(OUTPUT_CONFIG),
}

START_PORT    = 58001                                         # 默认起始端口改为 58001
END_PORT      = 58500                                         # 默认结束端口改为 58500
MAPPING_PORTS_FILE = os.path.join(BASE_DIR, "proxy_ports.txt")  # 端口文件，支持单端口、host:port 和 58001-58500
PRIORITY_FILES = [
    "0-abwg.yaml",
    "0-zhouheiyavlmess.yaml",
    "clash-USA_2_1_.yaml",
    "clash-USA_2_2_.yaml",
]
CLASH_API_URL = "http://127.0.0.1:29090"                      # 内置独立 Mihomo 外部控制端口
CLASH_SECRET  = "123456789"                                   # 内置独立 Mihomo 控制器内部通信密钥 (Python内部使用，无需在外部脚本中配置)
TEST_INTERVAL = 1800                                          # 后台测速轮询间隔（秒, 默认1800秒=30分钟）
API_PORT      = 6001                                          # 本地 REST API 监听端口
GATEWAY_PORT  = 17890                                         # HTTP/HTTPS 旋转代理网关端口 (避开 Clash Verge 的 7890)
TEST_URLS = [                                                  # 多目标测速站点：节点需全部通过才算有效
    "https://www.google.com/generate_204",                     #   主目标：延迟以它为准 (HTTPS 避免某些机场劫持)
    "https://cp.cloudflare.com/generate_204",                  #   Cloudflare 路径，校验非 Google 线路
    "https://www.apple.com/library/test/success.html",         #   Apple CDN，第三条独立网络路径
]
TEST_URL      = TEST_URLS[0]                                   # 兼容别名：主测速地址
NODE_TIMEOUT  = 4000                                          # 节点测速超时（毫秒）
MAX_DELAY     = 3000                                          # 超过该延迟(ms)视为不可用，0 表示不限制
HEALTH_TEST_CONCURRENCY = 8                                   # 主动测速并发；过高会挤占线路并人为抬高延迟
HEALTH_TEST_BATCH_SIZE = 40                                   # 分批提交测速任务，降低瞬时内存和控制器压力
NODE_DELAY_SAMPLE_WINDOW = 3                                  # 延迟取最近若干次成功结果的中位数
NODE_FAILURE_CONFIRMATIONS = 3                                 # 节点连续失败几次后确认为不可用
DEFER_ACTIVE_PORT_SWITCH = True                               # 端口有网关连接时暂不换绑
HEALTH_CHECK_INITIAL_DELAY = 3                                # 已确认导入后，尽快开始第一轮主动测速
HEALTH_CHECK_RETRY_SHORT = 15                                 # 状态为空/未就绪时短间隔重试
HEALTH_MATCH_MIN_COVERAGE = 0.98                              # Clash 返回节点匹配率下限
HEALTH_DROP_RATIO = 0.75                                      # 有效节点骤降保护比例
HEALTH_DROP_CONFIRMATIONS = 3                                 # 异常骤降连续确认次数
HOT_SWITCH_PORT_BINDINGS = True                               # 通过 selector API 更新端口节点，避免重载配置
PORT_SELECTOR_PREFIX = "PORT_"
AUTO_GROUP_NAME = "自动"
SELECTOR_RETRY_INTERVAL = 5                                   # 手动导入后重试 selector 初始化

# ---- 网关性能优化 ----
GATEWAY_MAX_CONNECTIONS = 4096                                # 最大并发连接数
GATEWAY_MAX_ATTEMPTS = 3                                      # CONNECT 失败时同一次请求最多尝试节点数
GATEWAY_FAILURE_THRESHOLD = 2                                 # 连续失败进入短时熔断的阈值
GATEWAY_FAILURE_COOLDOWN_BASE = 30                             # 首次熔断基础冷却时间（秒）
GATEWAY_FAILURE_COOLDOWN_FACTOR = 60                           # 每次失败追加冷却时间（秒）
GATEWAY_FAILURE_COOLDOWN_MAX = 900                             # 冷却时间上限（秒）
RELAY_TIMEOUT = 300                                           # WebSocket/长连接超时保活秒数
SHUTDOWN_TIMEOUT = 1.0                                        # Ctrl+C 后等待连接关闭的最长秒数

DATABASE_FILE = os.path.join(BASE_DIR, "proxy_pool.sqlite3")  # SQLite 状态库
CACHE_FILE    = os.path.join(BASE_DIR, "pool_cache.json")     # 兼容保留的旧缓存文件
CACHE_ENABLED = True                                          # False 时不读取、不写入节点缓存
SUBSCRIBE_FILE = os.path.join(SUBSCRIPTION_DIR, "subscribe.txt")  # 订阅链接文本文件（位于订阅yaml文件目录）

# ---- 动态网关与静态端口认证 (对应 proxy_list.txt / 指纹浏览器所用账号密码) ----
# 敏感凭据通过环境变量注入，避免硬编码泄露（本仓库公开）。
# 可用环境变量：PROXY_POOL_GATEWAY_USERNAME / PROXY_POOL_GATEWAY_PASSWORD
# 未设置时回退到占位符，需在启动向导或环境变量中配置真实值。
GATEWAY_AUTH_ENABLED = True                     # 是否开启账号密码认证
GATEWAY_USERNAME     = os.environ.get("PROXY_POOL_GATEWAY_USERNAME", "nisuiyi_${sessionName}")    # 用户名模板 (导出到 proxy_list.txt 的用户名)
GATEWAY_PASSWORD     = os.environ.get("PROXY_POOL_GATEWAY_PASSWORD", "CHANGE_ME")    # 代理访问密码 (指纹浏览器/脚本/proxy_list.txt 所用的代理连接密码)

# ---- 延时 IP 轮换 ----
GATEWAY_ROTATION_MODE = "delay"                 # 轮换模式: per_request | per_session | delay
GATEWAY_ROTATION_INTERVAL = 300                 # 同一 IP 保持时长（秒）

# ---- SOCKS5 / Mixed 代理 ----
SOCKS5_PORT           = 17891                    # Mixed 双协议代理端口 (避开 Clash Verge 的 7891)，0 表示禁用
SOCKS5_AUTH_ENABLED   = True                     # SOCKS5 是否开启认证

# ---- IP 白名单免认证 ----
IP_WHITELIST = ["127.0.0.1", "::1"]            # 白名单 IP 无需认证即可使用

USERNAME_PARSE_ENABLED = True                    # 是否启用 Nstproxy 风格用户名参数解析
SUBSCRIPTIONS = [] 

CLASH_EXTERNAL_CONTROLLER = "127.0.0.1:29090"  
CLASH_MAIN_PORT           = 0
CLASH_SOCKS_PORT          = 0
CLASH_MIXED_PORT          = 27890
CLASH_TUN_ENABLE          = False
INVALID_KEYWORDS = ['到期', '流量', '官网', '重置', 'Expire', '剩余', '套餐']
CLASH_PROFILE_NAME = os.path.basename(OUTPUT_CONFIG)
CLASH_VERGE_HOME = os.path.join(os.environ.get("APPDATA", ""), "io.github.clash-verge-rev.clash-verge-rev")
CLASH_VERGE_PROFILES_YAML = os.path.join(CLASH_VERGE_HOME, "profiles.yaml")
CLASH_VERGE_PROFILE_DIR = os.path.join(CLASH_VERGE_HOME, "profiles")
AUTO_SYNC_CLASH_PROFILE = False                # 是否自动同步配置到 Clash Verge

# ================= 全局状态与内存池 =================
POOL_STATE = {
    "all_nodes": [],
    "all_valid_nodes": [],
    "valid_nodes": [],
    "mapping_ports": [],
    "last_update": "",
    "last_update_ts": 0,
    "has_live_updated": False,
    "cached_last_update": "",
    "is_testing": False
}

ACTIVE_PORTS = Counter()
ACTIVE_GATEWAY_CONNECTIONS = 0
SESSION_MAP = OrderedDict()
NODE_ACTIVITY = {}
NODE_ACTIVITY_TTL = 900
SESSION_MAP_MAX = 5000
STATE_LOCK = RLock()
SELECTOR_SWITCH_LOCK = RLock()
LAST_COMPACT_SIGNATURE = ""
PORT_BINDINGS = {}
PENDING_SELECTOR_BINDINGS = {}
PORT_SELECTOR_CANDIDATES = {}
HEALTH_SUSPECT_DROP_COUNT = 0
AUTO_SELECTED_NODE = ""
AUTO_MANUAL_NODE = ""
NODE_HEALTH_STATE = {}
GATEWAY_NODE_FAILURES = {}
HEALTH_CHECK_EVENT = asyncio.Event()

rotation_index = 0
delay_index = 0

def _loop_exception_handler(loop, context):
    exc = context.get("exception")
    message = str(context.get("message", ""))
    if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
        if getattr(exc, "winerror", None) == 10054 or "_call_connection_lost" in message:
            return
    if isinstance(exc, OSError) and getattr(exc, "winerror", None) == 10054:
        return


# ================= 工具函数 =================
def _log(level: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    icons = {"INFO": "[*]", "OK": "[+]", "WARN": "[!]", "FAIL": "[-]", "ERR": "[x]"}
    print(f"{ts} {icons.get(level, '[?]')} {msg}")

def _atomic_write_text(path: str, content: str):
    temp_path = f"{path}.tmp"
    try:
        with open(temp_path, 'w', encoding='utf-8', newline='') as file_obj:
            file_obj.write(content)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        raise

def _atomic_write_yaml(path: str, data):
    temp_path = f"{path}.tmp"
    try:
        with open(temp_path, 'w', encoding='utf-8', newline='') as file_obj:
            yaml.dump(data, file_obj, allow_unicode=True, sort_keys=False)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        raise

def _is_ip_whitelisted(client_ip: str) -> bool:
    if not IP_WHITELIST: return False
    try:
        addr = ipaddress.ip_address(client_ip)
        for entry in IP_WHITELIST:
            try:
                if '/' in entry:
                    if addr in ipaddress.ip_network(entry, strict=False): return True
                elif addr == ipaddress.ip_address(entry): return True
            except ValueError: continue
    except ValueError: pass
    return False

def _is_unresolved_template_value(value: str) -> bool:
    return not value or "$" in value or "{" in value or "}" in value

def _extract_param_value(username: str, key: str):
    pattern = rf"(?:^|_){re.escape(key)}-(.*?)(?=_(?:customer|sessid|ttl|country)-|$)"
    match = re.search(pattern, username)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None

def _parse_username_params(username: str):
    params = {"username": username}
    if not USERNAME_PARSE_ENABLED:
        params["session_key"] = username
        return params
    
    customer = _extract_param_value(username, "customer")
    if customer: params["customer"] = customer
    sessid = _extract_param_value(username, "sessid")
    if sessid: params["sessid"] = sessid
    ttl = _extract_param_value(username, "ttl")
    if ttl and ttl.isdigit(): params["ttl"] = int(ttl)
    country = _extract_param_value(username, "country")
    if country: params["country"] = country.upper()

    if _is_unresolved_template_value(params.get("sessid", "")):
        params.pop("sessid", None)
    
    if params.get("customer") and params.get("sessid"):
        params["session_key"] = f"{params['customer']}:{params['sessid']}"
    else:
        params["session_key"] = username
    return params

def _build_auth_session_params(username: str, client_port: int = 0):
    params = _parse_username_params(username)
    if not params.get("sessid"):
        base_key = params.get("session_key", username)
        params["session_key"] = f"{base_key}:{client_port}" if client_port else f"{base_key}:{int(time.time() * 1000)}"
    return params

def _build_proxy_url(port: int) -> str:
    if GATEWAY_AUTH_ENABLED:
        user = urllib.parse.quote(GATEWAY_USERNAME, safe="${}")
        return f"http://{user}:{GATEWAY_PASSWORD}@127.0.0.1:{port}"
    return f"http://127.0.0.1:{port}"

def _get_proxy_list_username() -> str:
    raw_user = GATEWAY_USERNAME.strip()
    if not raw_user:
        return "sunkoo"
    cleaned = raw_user.split("_", 1)[0].strip()
    return cleaned or "sunkoo"

def _store_session(session_key: str, node: tuple[str, int], expires_at: float):
    with STATE_LOCK:
        SESSION_MAP[session_key] = {"node": node, "expires_at": expires_at}
        SESSION_MAP.move_to_end(session_key)
        while len(SESSION_MAP) > SESSION_MAP_MAX:
            SESSION_MAP.popitem(last=False)

def _get_valid_nodes_snapshot():
    with STATE_LOCK:
        nodes = list(POOL_STATE["valid_nodes"])
    healthy_nodes = [node for node in nodes if not node.get("consecutive_failures")]
    if healthy_nodes:
        now = time.time()
        available = [
            node for node in healthy_nodes
            if GATEWAY_NODE_FAILURES.get(node.get("port"), {}).get("until", 0) <= now
        ]
        if available:
            return available
    return healthy_nodes or nodes

def report_gateway_success(port):
    with STATE_LOCK:
        GATEWAY_NODE_FAILURES.pop(port, None)

def report_gateway_failure(port, error=""):
    now = time.time()
    with STATE_LOCK:
        state = GATEWAY_NODE_FAILURES.setdefault(port, {"count": 0, "until": 0, "last_error": ""})
        state["count"] += 1
        state["total_failures"] = int(state.get("total_failures", 0)) + 1
        state["last_failed_at"] = now
        state["last_error"] = error if isinstance(error, str) else state.get("last_error", "")
        if state["count"] >= GATEWAY_FAILURE_THRESHOLD:
            failures = max(1, int(state.get("total_failures", 0)) + 1)
            cooldown = min(
                GATEWAY_FAILURE_COOLDOWN_MAX,
                GATEWAY_FAILURE_COOLDOWN_BASE + failures * GATEWAY_FAILURE_COOLDOWN_FACTOR,
            )
            state["until"] = now + cooldown
            state["count"] = 0

def _mark_port_active(port: int, delta: int):
    with STATE_LOCK:
        ACTIVE_PORTS[port] += delta
        if ACTIVE_PORTS[port] <= 0:
            ACTIVE_PORTS.pop(port, None)

def _try_acquire_gateway_slot() -> bool:
    global ACTIVE_GATEWAY_CONNECTIONS
    with STATE_LOCK:
        if ACTIVE_GATEWAY_CONNECTIONS >= GATEWAY_MAX_CONNECTIONS:
            return False
        ACTIVE_GATEWAY_CONNECTIONS += 1
        return True

def _release_gateway_slot():
    global ACTIVE_GATEWAY_CONNECTIONS
    with STATE_LOCK:
        ACTIVE_GATEWAY_CONNECTIONS = max(0, ACTIVE_GATEWAY_CONNECTIONS - 1)

def _export_proxy_files(listeners=None):
    proxy_list_user = _get_proxy_list_username()
    proxy_list_path = os.path.join(BASE_DIR, "proxy_list.txt")
    static_proxy_path = os.path.join(BASE_DIR, "静态代理ip.txt")

    if listeners:
        sorted_ports = sorted([l["port"] for l in listeners if isinstance(l, dict) and "port" in l])
    else:
        sorted_ports = _load_mapping_ports()

    # 1. 导出 proxy_list.txt (带协议与认证信息)
    lines = [
        f"socks5://{proxy_list_user}:{GATEWAY_PASSWORD}@127.0.0.1:{port}\n"
        for port in sorted_ports
    ]
    _atomic_write_text(proxy_list_path, "".join(lines))

    # 2. 导出 静态代理ip.txt (纯 127.0.0.1:port 格式)
    static_lines = [
        f"127.0.0.1:{port}\n"
        for port in sorted_ports
    ]
    _atomic_write_text(static_proxy_path, "".join(static_lines))

def _validate_generated_config(config):
    if not isinstance(config, dict):
        return False, "配置内容不是字典"
    if not config.get("proxies"):
        return False, "缺少代理节点"
    if not config.get("listeners"):
        return False, "缺少静态端口监听"
    if not config.get("proxy-groups"):
        return False, "缺少策略组"
    if not config.get("rules"):
        return False, "缺少规则"
    if CLASH_MIXED_PORT and config.get("mixed-port") != CLASH_MIXED_PORT:
        return False, f"mixed-port 不是 {CLASH_MIXED_PORT}"
    if CLASH_TUN_ENABLE:
        tun = config.get("tun")
        dns = config.get("dns")
        if not isinstance(tun, dict) or not tun.get("enable"):
            return False, "TUN 未启用"
        if not isinstance(dns, dict) or not dns.get("enable"):
            return False, "DNS 未启用"
    return True, ""

def _sync_clash_verge_profile() -> bool:
    """仅当 Clash Verge 当前 local profile 就是生成文件时，把最新配置复制进去并重载。
    不主动切换用户当前配置，避免影响用户手动选择的其它 profile。
    """
    if not AUTO_SYNC_CLASH_PROFILE:
        return False
    if not os.path.exists(OUTPUT_CONFIG):
        return False
    try:
        with open(OUTPUT_CONFIG, 'r', encoding='utf-8') as f:
            generated_config = yaml.safe_load(f) or {}
        valid, reason = _validate_generated_config(generated_config)
        if not valid:
            _log("WARN", f"生成配置校验失败，已取消 Clash 重载: {reason}")
            return False

        with open(CLASH_VERGE_PROFILES_YAML, 'r', encoding='utf-8') as f:
            profiles_data = yaml.safe_load(f) or {}
        current_uid = profiles_data.get("current")
        items = profiles_data.get("items") or []
        target_item = next((it for it in items if isinstance(it, dict) and it.get("uid") == current_uid), None)
        if not target_item:
            _log("WARN", "未找到 Clash Verge 当前 profile，跳过同步。请手动加载: " + OUTPUT_CONFIG)
            return False
        if target_item.get("type") != "local" or target_item.get("name") != CLASH_PROFILE_NAME:
            _log("INFO", f"当前 Clash Verge profile 不是 {CLASH_PROFILE_NAME}，已跳过自动同步")
            return False

        profile_file = target_item.get("file") or f"{current_uid}.yaml"
        target_path = os.path.join(CLASH_VERGE_PROFILE_DIR, profile_file)
        os.makedirs(CLASH_VERGE_PROFILE_DIR, exist_ok=True)
        with open(OUTPUT_CONFIG, 'rb') as f:
            generated_bytes = f.read()
        previous_bytes = None
        if os.path.exists(target_path):
            with open(target_path, 'rb') as f:
                previous_bytes = f.read()
            if previous_bytes == generated_bytes:
                _log("INFO", f"Clash Verge profile 内容未变化，跳过重载: {profile_file}")
                return True

        shutil.copyfile(OUTPUT_CONFIG, target_path)

        reload_url = f"{CLASH_API_URL}/configs?force=true"
        headers = {"Authorization": f"Bearer {CLASH_SECRET}", "Content-Type": "application/json"} if CLASH_SECRET else {"Content-Type": "application/json"}
        body = json.dumps({"path": target_path}).encode("utf-8")
        req = urllib.request.Request(reload_url, data=body, headers=headers, method="PUT")
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 204 or resp.status == 200:
                _log("OK", f"已同步并重载 Clash Verge profile: {profile_file}")
                return True
            _log("WARN", f"Clash 配置重载返回状态: {resp.status}")
            return False
    except Exception as e:
        try:
            if 'target_path' in locals() and 'previous_bytes' in locals() and previous_bytes is not None:
                with open(target_path, 'wb') as f:
                    f.write(previous_bytes)
                _log("WARN", "Clash 重载失败，已恢复原 profile 文件")
        except Exception as restore_error:
            _log("WARN", f"恢复原 profile 失败: {restore_error}")
        _log("WARN", f"Clash Verge profile 同步失败: {e}")
        return False

def _load_mapping_ports():
    source_path = MAPPING_PORTS_FILE
    if not os.path.exists(source_path):
        return list(range(START_PORT, END_PORT + 1))

    ports = []
    reserved_ports = {API_PORT, GATEWAY_PORT, SOCKS5_PORT, CLASH_MIXED_PORT}
    try:
        with open(source_path, 'r', encoding='utf-8') as f:
            for raw in f:
                line = raw.split("#", 1)[0].strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    line = line.rsplit(":", 1)[-1].strip()
                values = []
                range_match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", line)
                if range_match:
                    start, end = map(int, range_match.groups())
                    if start > end:
                        start, end = end, start
                    values = range(start, end + 1)
                elif line.isdigit():
                    values = [int(line)]
                for port in values:
                    if 1 <= port <= 65535 and port not in reserved_ports and port not in ports:
                        ports.append(port)
    except Exception as e:
        _log("WARN", f"读取端口映射文件失败: {e}")
    return sorted(ports)

def _port_selector_name(port: int) -> str:
    return f"{PORT_SELECTOR_PREFIX}{port}"

def _load_cached_port_bindings(proxy_names, configured_ports):
    if not CACHE_ENABLED:
        return {}
    try:
        return proxy_pool_db.load_port_bindings(proxy_names, configured_ports)
    except Exception as e:
        _log("WARN", f"读取 SQLite 端口映射失败，将按节点顺序生成: {e}")
    return {}

def _load_clash_port_bindings(proxy_names, configured_ports):
    available_names = set(proxy_names)
    bindings = {}
    headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
    request = urllib.request.Request(f"{CLASH_API_URL}/proxies", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            data = json.load(response)
        proxies = data.get("proxies", {}) if isinstance(data, dict) else {}
        used_names = set()
        for port in configured_ports:
            selector = proxies.get(_port_selector_name(port), {})
            name = selector.get("now") if isinstance(selector, dict) else None
            if name in available_names and name not in used_names:
                bindings[port] = name
                used_names.add(name)
    except Exception:
        return {}
    return bindings

def _prepare_port_selector_candidates(proxy_names, configured_ports):
    cached_bindings = _load_cached_port_bindings(proxy_names, configured_ports)
    minimum_complete = max(1, int(len(configured_ports) * 0.80))
    if len(cached_bindings) >= minimum_complete:
        bindings = cached_bindings
        _log("INFO", f"端口初始映射来源: 缓存 ({len(bindings)} 个)")
    else:
        clash_bindings = _load_clash_port_bindings(proxy_names, configured_ports)
        bindings = clash_bindings if len(clash_bindings) > len(cached_bindings) else cached_bindings
        if bindings:
            source = "Clash 当前选择器" if bindings is clash_bindings else "缓存"
            _log("INFO", f"端口初始映射来源: {source} ({len(bindings)} 个)")

    used_names = set(bindings.values())
    remaining_names = [name for name in proxy_names if name not in used_names]

    for port in configured_ports:
        if port in bindings or not remaining_names:
            continue
        name = remaining_names.pop(0)
        bindings[port] = name
        used_names.add(name)

    shared_candidates = [name for name in proxy_names if name not in used_names]
    selector_candidates = {}
    selector_groups = []
    for port in configured_ports:
        assigned_name = bindings.get(port)
        candidates = ([assigned_name] if assigned_name else []) + [
            name for name in proxy_names if name != assigned_name
        ]
        if not candidates and proxy_names:
            candidates = [proxy_names[0]]
        selector_candidates[port] = set(candidates)
        selector_groups.append({
            "name": _port_selector_name(port),
            "type": "select",
            "include-all": True,
            "hidden": True,
        })
    return bindings, selector_candidates, selector_groups, shared_candidates

def _take_port_candidate(unassigned, port):
    allowed_names = PORT_SELECTOR_CANDIDATES.get(port)
    for index, node in enumerate(unassigned):
        if not allowed_names or node.get("name") in allowed_names:
            return unassigned.pop(index)
    return None

def _switch_clash_selector(selector_name, node_name, timeout=3):
    headers = {"Content-Type": "application/json"}
    if CLASH_SECRET:
        headers["Authorization"] = f"Bearer {CLASH_SECRET}"
    url = f"{CLASH_API_URL}/proxies/{urllib.parse.quote(selector_name, safe='')}"
    body = json.dumps({"name": node_name}, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method="PUT")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status in (200, 204), ""
    except urllib.error.HTTPError as error:
        return False, f"HTTP {error.code}"
    except Exception as error:
        return False, str(error)

def _get_clash_selector_current(selector_name, timeout=3):
    headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
    url = f"{CLASH_API_URL}/proxies/{urllib.parse.quote(selector_name, safe='')}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.load(response)
        return data.get("now") if isinstance(data, dict) else None
    except Exception:
        return None

def _switch_port_selectors_unlocked(bindings):
    if not HOT_SWITCH_PORT_BINDINGS or not bindings:
        return {}, {}

    success_bindings = {}
    failed_bindings = {}
    error_examples = []
    binding_items = sorted(bindings.items())
    for item_index, (port, node_name) in enumerate(binding_items):
        selector_name = _port_selector_name(port)
        success, error_text = _switch_clash_selector(selector_name, node_name)
        if success:
            success_bindings[port] = node_name
        else:
            failed_bindings[port] = node_name
            if len(error_examples) < 3:
                error_examples.append(f"{port}: {error_text}")
            if error_text == "HTTP 404":
                _log("WARN", "Clash 中尚未加载端口选择器，停止本轮热切换")
                for remaining_port, remaining_name in binding_items[item_index + 1:]:
                    failed_bindings[remaining_port] = remaining_name
                break

    if success_bindings:
        _log("OK", f"端口节点热切换完成: 成功 {len(success_bindings)}，失败 {len(failed_bindings)}")
    elif failed_bindings:
        _log("WARN", f"端口节点热切换失败: {len(failed_bindings)} 个")
    if error_examples:
        _log("WARN", f"热切换失败示例: {'; '.join(error_examples)}")
    return success_bindings, failed_bindings

def _switch_port_selectors(bindings):
    with SELECTOR_SWITCH_LOCK:
        return _switch_port_selectors_unlocked(bindings)

def _switch_auto_selector(valid_nodes):
    global AUTO_SELECTED_NODE, AUTO_MANUAL_NODE
    if not valid_nodes:
        return False
    healthy_nodes = [node for node in valid_nodes if not node.get("consecutive_failures")]
    candidates = healthy_nodes or valid_nodes
    valid_names = {node["name"] for node in valid_nodes}
    current_name = _get_clash_selector_current(AUTO_GROUP_NAME)

    if AUTO_MANUAL_NODE:
        if AUTO_MANUAL_NODE not in valid_names:
            _log("WARN", f"自动策略手动节点已失效: {AUTO_MANUAL_NODE}，恢复自动择优")
            AUTO_MANUAL_NODE = ""
        elif current_name in valid_names:
            if current_name != AUTO_MANUAL_NODE:
                AUTO_MANUAL_NODE = current_name
                _log("INFO", f"自动策略已更新手动选择: {current_name}")
            return True
        else:
            return True

    if not AUTO_SELECTED_NODE and current_name in valid_names:
        with STATE_LOCK:
            all_nodes = list(POOL_STATE.get("all_nodes", []))
        initial_name = all_nodes[0].get("name") if all_nodes else None
        if initial_name and current_name != initial_name:
            AUTO_MANUAL_NODE = current_name
            _log("INFO", f"检测到自动策略的手动选择，将保持节点: {current_name}")
            return True

    if AUTO_SELECTED_NODE and current_name in valid_names and current_name != AUTO_SELECTED_NODE:
        AUTO_MANUAL_NODE = current_name
        AUTO_SELECTED_NODE = ""
        _log("INFO", f"检测到自动策略的手动选择，将保持节点: {current_name}")
        return True

    best_node = min(candidates, key=lambda node: node.get("delay", 999999))
    best_name = best_node["name"]
    if best_name == AUTO_SELECTED_NODE and current_name == best_name:
        return True
    success, error_text = _switch_clash_selector(AUTO_GROUP_NAME, best_name)
    if success:
        AUTO_SELECTED_NODE = best_name
        _log("OK", f"自动策略已切换到最低延迟节点: {best_name} ({best_node['delay']}ms)")
        return True
    _log("WARN", f"自动策略切换失败: {error_text}")
    return False

def _materialize_compact_mapping(valid_nodes, bindings=None):
    valid_by_name = {node["name"]: node for node in valid_nodes}
    selected_bindings = dict(PORT_BINDINGS if bindings is None else bindings)
    compact_nodes, listeners = [], []
    for port in sorted(selected_bindings):
        name = selected_bindings[port]
        node = valid_by_name.get(name)
        if node is None:
            continue
        compact_node = dict(node)
        compact_node["port"] = port
        compact_node["local_url"] = _build_proxy_url(port)
        compact_nodes.append(compact_node)
        listeners.append({
            "name": f"pool_{port - START_PORT:04d}",
            "type": "mixed",
            "listen": "127.0.0.1",
            "port": port,
            "proxy": name,
        })
    return compact_nodes, listeners

async def selector_retry_worker():
    global PORT_BINDINGS
    while True:
        await asyncio.sleep(SELECTOR_RETRY_INTERVAL)
        with STATE_LOCK:
            pending = dict(PENDING_SELECTOR_BINDINGS)
            active_ports = set(ACTIVE_PORTS)
            valid_names = {
                node.get("name") for node in POOL_STATE.get("all_valid_nodes", [])
                if isinstance(node, dict)
            }
            stale_ports = [port for port, name in pending.items() if name not in valid_names]
            for port in stale_ports:
                PENDING_SELECTOR_BINDINGS.pop(port, None)
        pending = {
            port: name for port, name in pending.items()
            if name in valid_names and (not DEFER_ACTIVE_PORT_SWITCH or port not in active_ports)
        }
        if not pending:
            continue
        success_bindings, _ = await asyncio.to_thread(_switch_port_selectors, pending)
        if success_bindings:
            with STATE_LOCK:
                for port, name in success_bindings.items():
                    if PENDING_SELECTOR_BINDINGS.get(port) == name:
                        PORT_BINDINGS[port] = name
                        PENDING_SELECTOR_BINDINGS.pop(port, None)
                all_valid_nodes = list(POOL_STATE.get("all_valid_nodes", []))
            compact_nodes, listeners = _materialize_compact_mapping(all_valid_nodes)
            with STATE_LOCK:
                POOL_STATE["valid_nodes"] = compact_nodes
                POOL_STATE["mapping_ports"] = [node["port"] for node in compact_nodes]
            _export_proxy_files(listeners)

def _apply_compact_valid_mapping(valid_nodes, rewrite_config=True):
    global LAST_COMPACT_SIGNATURE, PORT_BINDINGS, PENDING_SELECTOR_BINDINGS
    previous_bindings = dict(PORT_BINDINGS)
    priority_set = set(PRIORITY_FILES)
    valid_nodes = sorted(valid_nodes, key=lambda n: (
        1 if n.get("consecutive_failures") else 0,
        n.get("delay", 999999),
        0 if n.get("source_file") in priority_set else 1,
    ))
    valid_by_name = {node["name"]: node for node in valid_nodes}
    valid_names = set(valid_by_name)
    with STATE_LOCK:
        pending_snapshot = dict(PENDING_SELECTOR_BINDINGS)

    mapping_ports = _load_mapping_ports()
    if mapping_ports:
        desired_bindings = {}
        used_names = set()
        free_ports = []
        cancelled_pending_ports = set()

        for port in mapping_ports:
            current_name = PORT_BINDINGS.get(port)
            pending_name = pending_snapshot.get(port)
            selected_name = None

            if (
                current_name in valid_names
                and current_name not in used_names
                and current_name in PORT_SELECTOR_CANDIDATES.get(port, {current_name})
            ):
                selected_name = current_name
                if pending_name:
                    cancelled_pending_ports.add(port)
            elif (
                pending_name in valid_names
                and pending_name not in used_names
                and pending_name in PORT_SELECTOR_CANDIDATES.get(port, {pending_name})
            ):
                selected_name = pending_name

            if selected_name:
                desired_bindings[port] = selected_name
                used_names.add(selected_name)
            else:
                free_ports.append(port)

        reserve_nodes = [node for node in valid_nodes if node["name"] not in used_names]
        for port in free_ports:
            node = _take_port_candidate(reserve_nodes, port)
            if node is None:
                continue
            desired_bindings[port] = node["name"]
            used_names.add(node["name"])

        if cancelled_pending_ports:
            with STATE_LOCK:
                for port in cancelled_pending_ports:
                    PENDING_SELECTOR_BINDINGS.pop(port, None)

        if len(valid_nodes) >= len(mapping_ports) and len(desired_bindings) < len(mapping_ports):
            _log(
                "WARN",
                f"候选池映射不足: 有效 {len(valid_nodes)}，目标端口 {len(mapping_ports)}，实际映射 {len(desired_bindings)}",
            )
    else:
        desired_bindings = {
            START_PORT + index: node["name"]
            for index, node in enumerate(valid_nodes)
        }

    changed_bindings = {
        port: name for port, name in desired_bindings.items()
        if previous_bindings.get(port) != name
    }
    actual_bindings = dict(desired_bindings)

    if HOT_SWITCH_PORT_BINDINGS and changed_bindings:
        success_bindings, failed_bindings = _switch_port_selectors(changed_bindings)
        for port in failed_bindings:
            if port in previous_bindings:
                actual_bindings[port] = previous_bindings[port]
            else:
                actual_bindings.pop(port, None)
        with STATE_LOCK:
            for port in success_bindings:
                PENDING_SELECTOR_BINDINGS.pop(port, None)
            PENDING_SELECTOR_BINDINGS.update(failed_bindings)

    PORT_BINDINGS = actual_bindings
    with STATE_LOCK:
        POOL_STATE["mapping_ports"] = sorted(PORT_BINDINGS)

    compact_nodes, listeners = _materialize_compact_mapping(valid_nodes)

    if rewrite_config and not HOT_SWITCH_PORT_BINDINGS and os.path.exists(OUTPUT_CONFIG):
        try:
            with open(OUTPUT_CONFIG, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
            config["listeners"] = listeners
            _atomic_write_yaml(OUTPUT_CONFIG, config)
        except Exception as e:
            _log("WARN", f"更新紧凑端口配置失败: {e}")

    _export_proxy_files(listeners)

    signature = "|".join(f"{port}:{PORT_BINDINGS[port]}" for port in sorted(PORT_BINDINGS))
    if signature != LAST_COMPACT_SIGNATURE:
        LAST_COMPACT_SIGNATURE = signature
        if compact_nodes:
            _log("OK", f"有效节点端口已稳定映射: 共 {len(compact_nodes)} 个端口")
        else:
            _log("WARN", "当前暂无有效节点，已清空静态代理端口导出")
        if not HOT_SWITCH_PORT_BINDINGS:
            _sync_clash_verge_profile()

    return compact_nodes

def load_subscriptions_from_file():
    subs = []
    if not os.path.exists(SUBSCRIBE_FILE): return subs
    try:
        with open(SUBSCRIBE_FILE, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
        for i, line in enumerate(lines):
            if ',' in line:
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 2: subs.append((parts[0], parts[1]))
            else:
                subs.append((line, f"sub_{i+1:02d}.yaml"))
    except Exception as e:
        _log("WARN", f"读取 {SUBSCRIBE_FILE} 失败: {e}")
    return subs

async def update_subscriptions(custom_subs=None):
    subs_to_update = custom_subs if custom_subs is not None else SUBSCRIPTIONS
    if not subs_to_update: return
    _log("INFO", f"正在检查 {len(subs_to_update)} 个远程订阅更新...")
    os.makedirs(SUBSCRIPTION_DIR, exist_ok=True)

    # 机场订阅服务器通常会检测 User-Agent，只响应 Clash 系客户端
    ua_list = [
        "clash-verge/v2.0.2",
        "ClashMetaForAndroid/2.11.1.Meta",
        "clash.meta",
        "ClashX Pro/1.116.1 (com.west2online.ClashXPro; build:1.116.1; macOS 14.0.0) Alamofire/5.8.0",
    ]
    timeout = aiohttp.ClientTimeout(total=30, connect=15, sock_read=20)

    for url, filename in subs_to_update:
        saved = False
        last_error = ""
        for attempt, ua in enumerate(ua_list, 1):
            headers = {
                "User-Agent": ua,
                "Accept": "text/html,application/x-yaml,*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
            try:
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            text_preview = content[:200].decode("utf-8", errors="ignore").strip()
                            if not text_preview:
                                last_error = "返回内容为空"
                                _log("WARN", f"[{filename}] UA#{attempt} 返回空内容，换 UA 重试...")
                                continue
                            with open(os.path.join(SUBSCRIPTION_DIR, filename), 'wb') as f:
                                f.write(content)
                            _log("OK", f"已更新订阅 {filename} ({len(content)} bytes, UA#{attempt})")
                            saved = True
                            break
                        elif resp.status in (403, 451):
                            last_error = f"HTTP {resp.status} (UA 被拒)"
                            _log("WARN", f"[{filename}] UA#{attempt} 被拒 (HTTP {resp.status})，换 UA 重试...")
                        elif resp.status == 404:
                            last_error = f"HTTP 404 (订阅链接无效)"
                            _log("FAIL", f"[{filename}] HTTP 404，订阅链接可能已失效")
                            break
                        else:
                            last_error = f"HTTP {resp.status}"
                            _log("WARN", f"[{filename}] UA#{attempt} 返回 HTTP {resp.status}")
            except asyncio.TimeoutError:
                last_error = "连接超时"
                _log("WARN", f"[{filename}] UA#{attempt} 超时，换 UA 重试...")
            except aiohttp.ClientError as e:
                last_error = str(e)
                _log("WARN", f"[{filename}] UA#{attempt} 网络错误: {e}")
            except Exception as e:
                last_error = str(e)
                _log("WARN", f"[{filename}] UA#{attempt} 异常: {e}")
        if not saved:
            _log("FAIL", f"订阅下载失败 {filename}: {last_error} (已尝试 {len(ua_list)} 种 UA)")

# ================= 后台清理任务 =================
async def session_cleanup_worker():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        with STATE_LOCK:
            expired = [k for k, v in SESSION_MAP.items() if v["expires_at"] <= now]
            for k in expired:
                SESSION_MAP.pop(k, None)
            stale_ports = [port for port, info in NODE_ACTIVITY.items() if info.get("expires_at", 0) <= now]
            for port in stale_ports:
                NODE_ACTIVITY.pop(port, None)
        if expired:
            _log("INFO", f"Session 清理: 移除 {len(expired)} 个过期会话 (剩余 {len(SESSION_MAP)})")
        if stale_ports:
            _log("INFO", f"Node 清理: 移除 {len(stale_ports)} 个过期回调记录")


# ================= 缓存持久化 =================
def save_cache(valid_nodes, all_valid_nodes=None):
    if not CACHE_ENABLED:
        return
    try:
        nodes = all_valid_nodes if all_valid_nodes is not None else valid_nodes
        with STATE_LOCK:
            bindings = dict(PORT_BINDINGS)
        proxy_pool_db.init_database()
        proxy_pool_db.save_health_results(nodes, time.strftime("%Y-%m-%d %H:%M:%S"))
        proxy_pool_db.save_port_bindings(bindings)
    except Exception as e:
        _log("WARN", f"SQLite 状态保存失败: {e}")

def load_cache():
    if not CACHE_ENABLED:
        _log("INFO", "节点缓存已关闭，等待 Clash 返回实时状态后再建立映射")
        return []
    proxy_pool_db.init_database()
    try:
        with STATE_LOCK:
            current_names = {node.get("name") for node in POOL_STATE.get("all_nodes", [])}
            bindings = dict(PORT_BINDINGS)
        all_valid_nodes = proxy_pool_db.load_health_results(current_names)
        bound_by_name = {name: port for port, name in bindings.items()}
        nodes = []
        for node in all_valid_nodes:
            if node["name"] in bound_by_name:
                mapped_node = dict(node)
                mapped_node["port"] = bound_by_name[node["name"]]
                mapped_node["local_url"] = _build_proxy_url(mapped_node["port"])
                nodes.append(mapped_node)
        health_state = {}
        for node in all_valid_nodes:
            health_state[node["name"]] = {
                "failures": int(node.get("consecutive_failures") or 0),
                "delays": deque(node.get("_health_samples") or [], maxlen=NODE_DELAY_SAMPLE_WINDOW),
            }
        NODE_HEALTH_STATE.clear()
        NODE_HEALTH_STATE.update(health_state)
        if all_valid_nodes:
            with STATE_LOCK:
                POOL_STATE["all_valid_nodes"] = all_valid_nodes
            last_update = proxy_pool_db.get_meta("last_update", "未知时间")
            _log(
                "INFO",
                f"从 SQLite 加载了 {len(all_valid_nodes)} 个有效节点 / "
                f"{len(nodes)} 个已绑定端口 (上次更新: {last_update})",
            )
            return nodes
        return []
    except Exception as e:
        _log("WARN", f"SQLite 状态加载失败: {e}")
        return []

def _is_cache_mapping_safe(cached_nodes):
    mapping_ports = _load_mapping_ports()
    if not mapping_ports:
        return bool(cached_nodes)
    cached_ports = {node.get("port") for node in cached_nodes}
    cached_names = {node.get("name") for node in cached_nodes}
    cached_bindings = {node.get("port"): node.get("name") for node in cached_nodes}
    return (
        len(cached_nodes) >= len(mapping_ports)
        and len(cached_names) >= len(mapping_ports)
        and set(mapping_ports).issubset(cached_ports)
        and all(cached_bindings.get(port) == PORT_BINDINGS.get(port) for port in mapping_ports)
    )

def repair_bindings_from_cached_health():
    """Use known-healthy reserve nodes to repair missing mappings before first check."""
    global PORT_BINDINGS, LAST_COMPACT_SIGNATURE
    with STATE_LOCK:
        all_valid_nodes = list(POOL_STATE.get("all_valid_nodes", []))
        previous_bindings = dict(PORT_BINDINGS)
        pending_snapshot = dict(PENDING_SELECTOR_BINDINGS)

    if not all_valid_nodes:
        return []

    priority_set = set(PRIORITY_FILES)
    valid_by_name = {node["name"]: node for node in all_valid_nodes}
    mapping_ports = _load_mapping_ports()
    if not mapping_ports:
        return _materialize_compact_mapping(all_valid_nodes)

    desired_bindings = {}
    used_names = set()
    free_ports = []
    for port in mapping_ports:
        current_name = previous_bindings.get(port)
        if current_name in valid_by_name and current_name not in used_names:
            desired_bindings[port] = current_name
            used_names.add(current_name)
        else:
            free_ports.append(port)

    reserve_nodes = sorted(
        (node for node in all_valid_nodes if node["name"] not in used_names),
        key=lambda node: (
            1 if node.get("consecutive_failures") else 0,
            node.get("delay", 999999),
            0 if node.get("source_file") in priority_set else 1,
        ),
    )
    changed_bindings = {}
    repaired = 0
    for port in free_ports:
        if not reserve_nodes:
            break
        node = reserve_nodes.pop(0)
        if node["name"] in used_names:
            continue
        desired_bindings[port] = node["name"]
        used_names.add(node["name"])
        if previous_bindings.get(port) != node["name"]:
            changed_bindings[port] = node["name"]
        repaired += 1

    if changed_bindings:
        success_bindings, failed_bindings = _switch_port_selectors(changed_bindings)
        for port in failed_bindings:
            desired_bindings.pop(port, None)
            used_names.discard(changed_bindings[port])
            repaired -= 1
        with STATE_LOCK:
            for port in success_bindings:
                PENDING_SELECTOR_BINDINGS.pop(port, None)

    PORT_BINDINGS = {port: name for port, name in desired_bindings.items()}
    proxy_pool_db.save_port_bindings(PORT_BINDINGS)
    compact_nodes, listeners = _materialize_compact_mapping(all_valid_nodes)
    _export_proxy_files(listeners)
    with STATE_LOCK:
        POOL_STATE["mapping_ports"] = sorted(PORT_BINDINGS)
        POOL_STATE["valid_nodes"] = compact_nodes
        cached_meta_update = proxy_pool_db.get_meta("last_update", "")
        if cached_meta_update:
            POOL_STATE["cached_last_update"] = cached_meta_update
    if repaired:
        _log("OK", f"启动前已用缓存备用节点修复端口映射: {repaired} 个")
    return compact_nodes


def hot_sync_from_sqlite(force: bool = False) -> bool:
    """
    零断网热同步：检测 SQLite 数据库是否有更新的时间戳或节点。
    通过 Clash 选择器进行端口无感热切换，将长驻内存状态与 SQLite 数据库完全统一，保证网络 100% 不中断。
    """
    db_update = proxy_pool_db.get_meta("last_update", "")
    if not db_update:
        return False
    with STATE_LOCK:
        mem_update = POOL_STATE.get("last_update", "")
        if not force and mem_update and db_update <= mem_update:
            return False

    _log("INFO", f"检测到 SQLite 数据库有较新记录 ({db_update} > {mem_update or '无'})，正在执行无感热同步...")
    cached_nodes = repair_bindings_from_cached_health()
    if cached_nodes:
        with STATE_LOCK:
            POOL_STATE["last_update"] = db_update
            POOL_STATE["cached_last_update"] = db_update
            POOL_STATE["has_live_updated"] = True
            try:
                ts = time.mktime(time.strptime(db_update, "%Y-%m-%d %H:%M:%S"))
                POOL_STATE["last_update_ts"] = ts
            except Exception:
                pass
        _log("OK", f"零断网热同步完成，内存与数据库已完全统一: {db_update}")
        return True
    return False


def _sync_runtime_bindings_from_loaded_config(proxies):
    global PORT_BINDINGS, PENDING_SELECTOR_BINDINGS
    available_order = [
        node.get("name") for node in POOL_STATE.get("all_nodes", [])
        if isinstance(node, dict) and node.get("name")
    ]
    available_names = set(available_order)
    configured_ports = _load_mapping_ports()
    expected_bindings = dict(PORT_BINDINGS)
    selected_by_port = {}
    for port in configured_ports:
        selector = proxies.get(_port_selector_name(port))
        selected_by_port[port] = selector.get("now") if isinstance(selector, dict) else None

    loaded_bindings = {}
    used_names = set()

    # Keep selectors that already match the freshly generated one-to-one map.
    for port in configured_ports:
        selected_name = selected_by_port.get(port)
        if selected_name != expected_bindings.get(port) or selected_name not in available_names:
            continue
        loaded_bindings[port] = selected_name
        used_names.add(selected_name)

    # Preserve other unique selections restored by Clash Verge. Duplicate or
    # invalid selections are repaired below without requiring another import.
    for port in configured_ports:
        if port in loaded_bindings:
            continue
        selected_name = selected_by_port.get(port)
        if selected_name not in available_names or selected_name in used_names:
            continue
        loaded_bindings[port] = selected_name
        used_names.add(selected_name)

    repair_bindings = {}
    reserve_names = [name for name in available_order if name not in used_names]
    for port in configured_ports:
        if port in loaded_bindings:
            continue
        preferred_name = expected_bindings.get(port)
        if preferred_name in available_names and preferred_name not in used_names:
            selected_name = preferred_name
        elif reserve_names:
            selected_name = reserve_names.pop(0)
        else:
            break
        repair_bindings[port] = selected_name
        used_names.add(selected_name)
        while reserve_names and reserve_names[0] in used_names:
            reserve_names.pop(0)

    if repair_bindings:
        _log("WARN", f"Clash Verge 恢复了重复/无效的端口选择，正在自动修复 {len(repair_bindings)} 个 selector")
        success_bindings, failed_bindings = _switch_port_selectors(repair_bindings)
        loaded_bindings.update(success_bindings)
        if failed_bindings:
            _log("WARN", f"端口 selector 自动修复失败 {len(failed_bindings)} 个，稍后可直接再按回车重试")

    if len(loaded_bindings) != len(configured_ports) or len(set(loaded_bindings.values())) != len(configured_ports):
        return False
    PORT_BINDINGS = loaded_bindings
    proxy_pool_db.save_port_bindings(PORT_BINDINGS)
    with STATE_LOCK:
        PENDING_SELECTOR_BINDINGS.clear()
        PENDING_SELECTOR_BINDINGS.clear()
        POOL_STATE["mapping_ports"] = sorted(loaded_bindings)
    _log("OK", f"已从 Clash 当前加载的新 YAML 同步 {len(loaded_bindings)} 个端口节点映射")
    return True


def apply_runtime_settings(settings: dict):
    """动态更新运行时配置"""
    global START_PORT, END_PORT, GATEWAY_PORT, SOCKS5_PORT, API_PORT
    global GATEWAY_AUTH_ENABLED, GATEWAY_PASSWORD, GATEWAY_USERNAME, GATEWAY_ROTATION_INTERVAL
    global TEST_INTERVAL, MAX_DELAY, HEALTH_TEST_CONCURRENCY

    if "start_port" in settings and "end_port" in settings:
        START_PORT = int(settings["start_port"])
        END_PORT = int(settings["end_port"])
    if "gateway_port" in settings:
        GATEWAY_PORT = int(settings["gateway_port"])
    if "socks5_port" in settings:
        SOCKS5_PORT = int(settings["socks5_port"])
    if "api_port" in settings:
        API_PORT = int(settings["api_port"])
    if "auth_enabled" in settings:
        GATEWAY_AUTH_ENABLED = bool(settings["auth_enabled"])
    if "gateway_password" in settings:
        GATEWAY_PASSWORD = str(settings["gateway_password"])
    if "gateway_username" in settings:
        GATEWAY_USERNAME = str(settings["gateway_username"])
    if "rotation_interval" in settings:
        GATEWAY_ROTATION_INTERVAL = int(settings["rotation_interval"])
    if "test_interval" in settings:
        TEST_INTERVAL = int(settings["test_interval"])
    if "max_delay" in settings:
        MAX_DELAY = int(settings["max_delay"])
    if "concurrency" in settings:
        HEALTH_TEST_CONCURRENCY = int(settings["concurrency"])

    if "start_port" in settings or "end_port" in settings:
        _export_proxy_files()
