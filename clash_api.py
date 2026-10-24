import json
import time
import urllib.request
from . import runtime

from .runtime import (
    AUTO_GROUP_NAME,
    CLASH_API_URL,
    CLASH_SECRET,
    HEALTH_MATCH_MIN_COVERAGE,
    OUTPUT_CONFIG,
    POOL_STATE,
    STATE_LOCK,
    _load_mapping_ports,
    _log,
    _port_selector_name,
    _sync_runtime_bindings_from_loaded_config,
)

async def _fetch_clash_proxies_status(session):
    """读取已加载配置，用于确认用户确实启用了本脚本生成的 profile。"""
    url = f"{CLASH_API_URL}/proxies"
    headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
    async with session.get(url, headers=headers, timeout=10) as resp:
        if resp.status != 200:
            _log("WARN", f"读取 Clash 节点状态失败，HTTP {resp.status}")
            return None
        data = await resp.json()
        proxies = data.get("proxies", {}) if isinstance(data, dict) else {}
        return proxies or None

def _loaded_pool_config_status(nodes_to_test, proxies):
    node_names = {node.get("name") for node in nodes_to_test}
    matched_count = sum(1 for name in node_names if name in proxies)
    configured_ports = _load_mapping_ports()
    selector_count = sum(1 for port in configured_ports if _port_selector_name(port) in proxies)
    auto_info = proxies.get(AUTO_GROUP_NAME)
    auto_ready = isinstance(auto_info, dict) and auto_info.get("type") == "Selector"
    ready = (
        matched_count >= max(1, int(len(node_names) * HEALTH_MATCH_MIN_COVERAGE))
        and selector_count == len(configured_ports)
        and auto_ready
    )
    return ready, matched_count, selector_count, len(configured_ports)

def _read_clash_proxies_sync(timeout=5):
    headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
    request = urllib.request.Request(f"{CLASH_API_URL}/proxies", headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = json.load(response)
    return data.get("proxies", {}) if isinstance(data, dict) else {}


def _wait_for_manual_clash_import():
    print("\n" + "=" * 68)
    print("  已生成全新 Clash 配置，请只在这里手动导入并启用一次：")
    print(f"  {OUTPUT_CONFIG}")
    print("  脚本会校验节点和 PORT_xxx 选择器，未加载正确配置不会继续。")
    print("=" * 68)
    while True:
        answer = input("\n是否已在 Clash Verge 中导入并启用新 YAML？[Y/q]（默认 Y，直接回车继续）: ").strip().lower()
        if answer in ("q", "quit", "exit", "退出"):
            return False
        last_status = (0, 0, len(_load_mapping_ports()))
        last_error = ""
        for _ in range(8):
            try:
                proxies = _read_clash_proxies_sync(timeout=3)
                with STATE_LOCK:
                    nodes_to_test = list(POOL_STATE["all_nodes"])
                ready, matched, selectors, selector_total = _loaded_pool_config_status(nodes_to_test, proxies)
                last_status = (matched, selectors, selector_total)
                if ready:
                    if not _sync_runtime_bindings_from_loaded_config(proxies):
                        _log("WARN", "新 YAML 中的端口 selector 映射不完整或存在重复节点，请重新导入后确认")
                        break
                    _log("OK", f"Clash 配置校验通过: 节点 {matched}/{len(nodes_to_test)}，端口选择器 {selectors}/{selector_total}")
                    return True
            except Exception as error:
                last_error = str(error)
            time.sleep(1)
        matched, selectors, selector_total = last_status
        if last_error and not matched:
            _log("WARN", f"暂时无法连接 Clash API ({CLASH_API_URL}): {last_error}")
        else:
            _log(
                "WARN",
                f"当前仍不是新配置: 节点匹配 {matched}/{len(POOL_STATE['all_nodes'])}，"
                f"端口选择器 {selectors}/{selector_total}。请确认已导入并启用后再按回车。",
            )
