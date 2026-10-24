import asyncio
import ipaddress
import json
import glob
import os
import yaml

import aiohttp
from . import runtime
from . import proxy_pool_db

from .runtime import (
    AUTO_GROUP_NAME,
    BASE_DIR,
    CACHE_FILE,
    CLASH_EXTERNAL_CONTROLLER,
    CLASH_MAIN_PORT,
    CLASH_MIXED_PORT,
    CLASH_SECRET,
    CLASH_SOCKS_PORT,
    CLASH_TUN_ENABLE,
    GENERATED_CONFIG_FILENAMES,
    INVALID_KEYWORDS,
    NODE_HEALTH_STATE,
    OUTPUT_CONFIG,
    POOL_STATE,
    PRIORITY_FILES,
    START_PORT,
    STATE_LOCK,
    SUBSCRIBE_FILE,
    SUBSCRIPTION_DIR,
    SUBSCRIPTIONS,
    _atomic_write_yaml,
    _build_proxy_url,
    _export_proxy_files,
    _load_mapping_ports,
    _log,
    _port_selector_name,
    _prepare_port_selector_candidates,
)

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
                            # 基本校验：内容非空且看起来像 YAML/Base64
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
                            break  # 404 不需要重试
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



def _build_simple_proxy_groups(proxy_names):
    selectable_nodes = list(proxy_names)
    return [
        {
            "name": AUTO_GROUP_NAME,
            "type": "select",
            "proxies": selectable_nodes,
        },
        {"name": "国内", "type": "select", "proxies": ["DIRECT"] + selectable_nodes},
        {"name": "国外", "type": "select", "proxies": [AUTO_GROUP_NAME] + selectable_nodes + ["DIRECT"]},
    ]

def _build_simple_rules():
    return [
        "GEOSITE,private,国内",
        "GEOIP,private,国内,no-resolve",
        "GEOSITE,cn,国内",
        "GEOIP,CN,国内,no-resolve",
        "MATCH,国外",
    ]

def _build_base_config():
    config = {
        "port": CLASH_MAIN_PORT,
        "socks-port": CLASH_SOCKS_PORT,
        "mixed-port": CLASH_MIXED_PORT,
        "external-controller": CLASH_EXTERNAL_CONTROLLER,
        "secret": CLASH_SECRET,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "warning",
        "ipv6": False,
        "unified-delay": True,
        "tcp-concurrent": True,
        "profile": {"store-selected": False, "store-fake-ip": True},
        "dns": {
            "enable": True,
            "ipv6": False,
            "enhanced-mode": "fake-ip",
            "fake-ip-range": "198.18.0.1/16",
            "default-nameserver": ["223.5.5.5", "119.29.29.29"],
            "nameserver": ["https://doh.pub/dns-query", "https://dns.alidns.com/dns-query"],
            "proxy-server-nameserver": ["https://doh.pub/dns-query", "https://dns.alidns.com/dns-query"],
        },
        "sniffer": {
            "enable": True,
            "force-dns-mapping": True,
            "parse-pure-ip": True,
            "override-destination": True,
            "sniff": {
                "HTTP": {
                    "ports": [80, 8080, "8000-9000"],
                    "override-destination": True,
                },
                "TLS": {
                    "ports": [443, 8443],
                },
                "QUIC": {
                    "ports": [443, 8443],
                },
            },
        },
    }
    if CLASH_TUN_ENABLE:
        config["tun"] = {
            "enable": True,
            "device": "Mihomo",
            "stack": "mixed",
            "dns-hijack": ["any:53"],
            "auto-route": True,
            "auto-detect-interface": True,
        }
    return config

def _proxy_identity(proxy):
    comparable = {key: value for key, value in proxy.items() if key != "name"}
    return json.dumps(comparable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def _is_routable_proxy_server(value, allow_ipv6=False):
    server = str(value or "").strip().strip("[]").rstrip(".").lower()
    if not server or server == "localhost" or server.endswith(".localhost"):
        return False
    try:
        ip = ipaddress.ip_address(server)
        if ip.version == 6 and not allow_ipv6:
            return False
        return ip.is_global
    except ValueError:
        return True

def _unique_proxy_name(original_name, source_name, seen_names):
    if original_name not in seen_names:
        return original_name
    source_stem = os.path.splitext(source_name)[0]
    candidate = f"{original_name} [{source_stem}]"
    suffix = 2
    while candidate in seen_names:
        candidate = f"{original_name} [{source_stem}-{suffix}]"
        suffix += 1
    return candidate

def build_clash_config(priority_files_first=True):
    global PORT_BINDINGS, PORT_SELECTOR_CANDIDATES
    global PENDING_SELECTOR_BINDINGS, LAST_COMPACT_SIGNATURE, AUTO_SELECTED_NODE, AUTO_MANUAL_NODE, NODE_HEALTH_STATE
    output_name = os.path.basename(OUTPUT_CONFIG)
    # 从 订阅yaml文件 子目录读取订阅 YAML，合并后保存到 proxy_pool 目录
    os.makedirs(SUBSCRIPTION_DIR, exist_ok=True)
    yaml_files = sorted(
        f for f in glob.glob(os.path.join(SUBSCRIPTION_DIR, "*.yaml")) + glob.glob(os.path.join(SUBSCRIPTION_DIR, "*.yml"))
        if os.path.basename(f) not in GENERATED_CONFIG_FILENAMES
    )

    if not yaml_files:
        _log("ERR", f"在 [{SUBSCRIPTION_DIR}] 下未找到任何 YAML 订阅文件")
        return False

    _log("INFO", f"找到 {len(yaml_files)} 个订阅文件，开始合并...")
    all_raw_proxies, seen_names, seen_identities, all_sources = [], set(), set(), []
    identities_by_name = {}

    for file_path in yaml_files:
        source_name = os.path.basename(file_path)
        try:
            with open(file_path, 'rb') as f:
                raw_bytes = f.read()
            text = None
            for enc in ('utf-8', 'utf-8-sig', 'gb18030', 'big5'):
                try:
                    text = raw_bytes.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            if text is None:
                text = raw_bytes.decode('utf-8', errors='ignore')
            try:
                data = yaml.safe_load(text)
            except yaml.YAMLError:
                # 订阅文件里常见乱码问号（原 emoji 丢失），未加引号时会破坏 YAML 流式语法，剔除后重试
                data = yaml.safe_load(text.replace('?', ''))
                _log("INFO", f"[{source_name}] 已修复乱码字符后重新解析")
            proxies = data.get("proxies", []) if isinstance(data, dict) else []
            valid_count = 0
            for p in proxies:
                if not isinstance(p, dict): continue
                original_name = str(p.get('name', '')).strip()
                if not original_name or any(kw in original_name for kw in INVALID_KEYWORDS): continue
                if not p.get('server') or not p.get('port'): continue
                if not _is_routable_proxy_server(p.get("server")):
                    continue
                identity = _proxy_identity(p)
                if identity in seen_identities:
                    continue
                proxy_copy = dict(p)
                name = _unique_proxy_name(original_name, source_name, seen_names)
                proxy_copy["name"] = name
                all_raw_proxies.append(proxy_copy)
                all_sources.append(source_name)
                seen_names.add(name)
                seen_identities.add(identity)
                identities_by_name[name] = identity
                valid_count += 1
            _log("OK", f"[{os.path.basename(file_path)}] 提取到 {valid_count} 个有效节点")
        except Exception as e:
            _log("FAIL", f"[{os.path.basename(file_path)}] 解析失败: {e}")

    if not all_raw_proxies:
        _log("ERR", "未从任何文件提取到有效节点")
        return False

    if priority_files_first:
        priority_set = set(PRIORITY_FILES)
        ordered_indices = []
        for priority_name in PRIORITY_FILES:
            ordered_indices.extend(i for i, source_name in enumerate(all_sources) if source_name == priority_name)
        ordered_indices.extend(i for i in range(len(all_raw_proxies)) if i not in ordered_indices)
        if len(ordered_indices) == len(all_raw_proxies):
            all_raw_proxies = [all_raw_proxies[i] for i in ordered_indices]
            all_sources = [all_sources[i] for i in ordered_indices]
            _log("INFO", "已按优先级文件顺序调整节点，指定文件节点已放在前面")

    configured_ports = _load_mapping_ports()
    if not configured_ports:
        configured_ports = [START_PORT + index for index in range(len(all_raw_proxies))]

    proxy_pool_db.init_database()
    proxy_pool_db.save_catalog([
        dict(proxy, _source_file=all_sources[i], _identity=identities_by_name[proxy["name"]])
        for i, proxy in enumerate(all_raw_proxies)
    ])
    if proxy_pool_db.migrate_legacy_cache(CACHE_FILE, set(configured_ports)):
        _log("INFO", "已将旧版 pool_cache.json 迁移到 SQLite")

    new_all_nodes, proxy_names = [], []
    for i, proxy in enumerate(all_raw_proxies):
        port = START_PORT + i
        new_all_nodes.append({"name": proxy["name"], "port": port, "local_url": _build_proxy_url(port), "source_file": all_sources[i]})
        proxy_names.append(proxy["name"])

    listeners = [
        {
            "name": f"pool_{port - START_PORT:04d}",
            "type": "mixed",
            "listen": "127.0.0.1",
            "port": port,
            "proxy": _port_selector_name(port),
        }
        for port in configured_ports
    ]
    initial_bindings, selector_candidates, port_selector_groups, shared_candidates = _prepare_port_selector_candidates(
        proxy_names,
        configured_ports,
    )
    runtime.PORT_BINDINGS = dict(initial_bindings)
    proxy_pool_db.save_port_bindings(runtime.PORT_BINDINGS)
    runtime.PORT_SELECTOR_CANDIDATES = selector_candidates
    runtime.PENDING_SELECTOR_BINDINGS.clear()
    runtime.LAST_COMPACT_SIGNATURE = ""
    runtime.AUTO_SELECTED_NODE = ""
    runtime.AUTO_MANUAL_NODE = ""
    NODE_HEALTH_STATE.clear()

    final_groups = _build_simple_proxy_groups(proxy_names) + port_selector_groups
    final_rules = _build_simple_rules()
    config = _build_base_config()
    config.update({
        "proxies": all_raw_proxies,
        "listeners": listeners,
        "proxy-groups": final_groups,
        "rules": final_rules,
    })
    _log("INFO", "基础网络参数、策略组和规则均使用脚本内置独立配置，不继承订阅文件")

    _atomic_write_yaml(OUTPUT_CONFIG, config)

    _export_proxy_files(listeners)

    with STATE_LOCK:
        POOL_STATE["all_nodes"] = new_all_nodes
        POOL_STATE["all_valid_nodes"] = []
        POOL_STATE["valid_nodes"] = []
        POOL_STATE["mapping_ports"] = []
        POOL_STATE["last_update"] = ""

    _log("OK", f"已整合 {len(all_raw_proxies)} 个节点 → [{output_name}]")
    if configured_ports:
        _log("INFO", f"本地映射端口: {configured_ports[0]} ~ {configured_ports[-1]} (共 {len(configured_ports)} 个)")
        candidate_count = len(proxy_names)
        _log(
            "INFO",
            f"端口选择器已启用完整候选池: 每端口 {candidate_count} 个候选，初始未绑定 {len(shared_candidates)} 个",
        )
    return True
