import asyncio
import os
import re
import sys

# 兼容 Windows GBK/UTF-8 编码
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import uvicorn
import time

from . import proxy_pool_db
from .mihomo_runner import start_local_mihomo, stop_local_mihomo
from .config_builder import build_clash_config, load_subscriptions_from_file, update_subscriptions
from .clash_api import _read_clash_proxies_sync
from .runtime import (
    API_PORT,
    BASE_DIR,
    DATABASE_FILE,
    GATEWAY_PORT,
    OUTPUT_CONFIG,
    SHUTDOWN_TIMEOUT,
    SOCKS5_PORT,
    START_PORT,
    END_PORT,
    GATEWAY_AUTH_ENABLED,
    GATEWAY_USERNAME,
    GATEWAY_PASSWORD,
    GATEWAY_ROTATION_INTERVAL,
    TEST_INTERVAL,
    MAX_DELAY,
    HEALTH_TEST_CONCURRENCY,
    SUBSCRIBE_FILE,
    SUBSCRIPTION_DIR,
    SUBSCRIPTIONS,
    STATE_LOCK,
    POOL_STATE,
    MAPPING_PORTS_FILE,
    _apply_compact_valid_mapping,
    _load_mapping_ports,
    _is_cache_mapping_safe,
    _sync_runtime_bindings_from_loaded_config,
    apply_runtime_settings,
    repair_bindings_from_cached_health,
    load_cache,
    _log,
)


def prompt_with_default(prompt_text, default_val, val_type=str, validator=None):
    """带默认值的交互提示输入，直接回车或非交互环境下自动使用默认值"""
    while True:
        try:
            raw = input(f">> {prompt_text} [直接回车默认: {default_val}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print(f">> {prompt_text} [自动采用默认值: {default_val}]")
            return default_val
        if not raw:
            return default_val
        try:
            val = val_type(raw)
            if validator and not validator(val):
                print("  [!] 输入值不符合要求，请重新输入！")
                continue
            return val
        except Exception:
            print(f"  [!] 输入格式有误，请输入有效的 {val_type.__name__} 类型！")


def prompt_port_range(default_start=58001, default_end=58500):
    """专门的端口范围交互函数：起始端口默认 58001，终止端口可手动输入"""
    print("\n[本地静态端口映射设置]")
    start_p = prompt_with_default(
        "请输入起始端口 (Start Port)", 
        default_start, 
        int, 
        lambda x: 1024 <= x <= 65535
    )
    
    suggested_end = start_p + (default_end - default_start)
    if suggested_end > 65535: suggested_end = 65535

    end_p = prompt_with_default(
        f"请输入终止端口 (End Port, 默认生成 {suggested_end - start_p + 1} 个端口)", 
        suggested_end, 
        int, 
        lambda x: start_p <= x <= 65535
    )

    try:
        with open(MAPPING_PORTS_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# 本地静态端口映射配置\n{start_p}-{end_p}\n")
    except Exception as e:
        _log("WARN", f"写入 {MAPPING_PORTS_FILE} 失败: {e}")

    apply_runtime_settings({"start_port": start_p, "end_port": end_p})
    total = end_p - start_p + 1
    print(f"  [+] 端口范围已设定为: {start_p} ~ {end_p} (共 {total} 个端口)")
    return start_p, end_p


def run_interactive_wizard():
    """全参数深度交互配置向导"""
    print("\n" + "=" * 60)
    print("  [*] Proxy Pool 全参数配置向导 (所有选项均带默认值，回车跳过)")
    print("=" * 60)

    start_p, end_p = prompt_port_range(START_PORT, END_PORT)

    print("\n[2/6] 动态旋转代理网关端口:")
    gw_port = prompt_with_default("HTTP 动态网关端口", GATEWAY_PORT, int, lambda x: 1 <= x <= 65535)
    s5_port = prompt_with_default("Mixed (HTTP/SOCKS5) 双协议网关端口", SOCKS5_PORT, int, lambda x: 0 <= x <= 65535)

    print("\n[3/6] 动态网关与静态端口认证 (对应 proxy_list.txt / 指纹浏览器所用密码):")
    auth_choice = prompt_with_default("是否启用网关认证？(y/n)", "y" if GATEWAY_AUTH_ENABLED else "n").lower()
    auth_enabled = (auth_choice in ("y", "yes", "true", "1"))

    gw_user = GATEWAY_USERNAME
    gw_pass = GATEWAY_PASSWORD
    if auth_enabled:
        gw_user = prompt_with_default("网关用户名模板 (导出到 proxy_list.txt 的用户名, 支持 ${sessionName})", GATEWAY_USERNAME)
        gw_pass = prompt_with_default("网关访问密码 (指纹浏览器/脚本/proxy_list.txt 所用的代理连接密码)", GATEWAY_PASSWORD)

    print("\n[4/6] IP 会话与轮换策略:")
    rot_interval = prompt_with_default("同会话 IP 保持时长 TTL (秒)", GATEWAY_ROTATION_INTERVAL, int, lambda x: x > 0)

    print("\n[5/6] 节点健康测速与质量过滤:")
    test_inter = prompt_with_default("后台定期测速间隔 (秒)", TEST_INTERVAL, int, lambda x: x >= 30)
    max_d = prompt_with_default("最大允许延迟 (ms, 超过视为不可用，0为不限)", MAX_DELAY, int, lambda x: x >= 0)
    concur = prompt_with_default("测速并发线程数 (推荐 4~16)", HEALTH_TEST_CONCURRENCY, int, lambda x: 1 <= x <= 64)

    print("\n[6/6] Web 可视化管理仪表盘:")
    web_p = prompt_with_default("Web 后台监听端口", API_PORT, int, lambda x: 1 <= x <= 65535)

    apply_runtime_settings({
        "start_port": start_p,
        "end_port": end_p,
        "gateway_port": gw_port,
        "socks5_port": s5_port,
        "api_port": web_p,
        "auth_enabled": auth_enabled,
        "gateway_username": gw_user,
        "gateway_password": gw_pass,
        "rotation_interval": rot_interval,
        "test_interval": test_inter,
        "max_delay": max_d,
        "concurrency": concur
    })

    print("\n" + "=" * 60)
    print("  [+] 全套配置已保存并应用！当前参数摘要:")
    print(f"  · 静态端口映射: {start_p} ~ {end_p} (共 {end_p - start_p + 1} 个)")
    print(f"  · HTTP 动态网关: 127.0.0.1:{gw_port}")
    print(f"  · Mixed 动态网关: 127.0.0.1:{s5_port}")
    print(f"  · Web 管理后台: 127.0.0.1:{web_p}")
    print(f"  · 网关认证: {'已开启 (密码: ' + gw_pass + ')' if auth_enabled else '未开启 (免密)'}")
    print(f"  · 测速参数: 间隔 {test_inter}s | 最大延迟 {max_d}ms | 并发 {concur}")
    print("=" * 60 + "\n")


def run():
    try:
        print("=" * 60)
        print("   Proxy Pool v3.0 Final - 智能多链动态代理池")
        print("   内置独立内核 · 隔离 Clash Verge · 端口 58001 起始交互")
        print("=" * 60)
        print(f"  运行目录: {BASE_DIR}")
        print(f"  订阅目录: {SUBSCRIPTION_DIR}")
        print(f"  合并配置: {OUTPUT_CONFIG}")
        print(f"  状态数据库: {DATABASE_FILE}")

        print("\n[启动选项]")
        print("  [1] 极速启动 (交互确认 58001 起始与终止端口，其他使用默认值)")
        print("  [2] 全参数配置向导 (逐项交互设置端口、网关、密码、测速等)")
        print("  [3] 订阅模式更新 (从 subscribe.txt 更新远程订阅链接)")
        
        choice = prompt_with_default("请选择模式 (1:极速模式 / 2:全参数向导 / 3:订阅更新)", "1")

        if choice == "2":
            run_interactive_wizard()
        elif choice == "3":
            while True:
                file_subs = load_subscriptions_from_file()
                if file_subs:
                    asyncio.run(update_subscriptions(file_subs))
                    break
                else:
                    print(f"\n  [!] 提示: {os.path.basename(SUBSCRIBE_FILE)} 为空或不存在")
                    if not os.path.exists(SUBSCRIBE_FILE):
                        with open(SUBSCRIBE_FILE, 'w', encoding='utf-8') as f:
                            f.write("# 粘贴 Clash 订阅链接，格式：https://example.com/clash.yaml\n")
                    if sys.platform.startswith('win'):
                        os.system(f'notepad "{SUBSCRIBE_FILE}"')
                    retry = prompt_with_default("已填好并保存了吗？继续请回车 [退出输入 q]", "y").lower()
                    if retry in ('q', 'quit', 'exit'): break
            prompt_port_range(START_PORT, END_PORT)
        else:
            prompt_port_range(START_PORT, END_PORT)
            test_inter = prompt_with_default(
                "请输入节点自动测速轮询间隔 (秒, 默认 1800 = 30分钟, 如输入 3600 为 1小时)",
                TEST_INTERVAL,
                int,
                lambda x: x >= 10,
            )
        print("\n[阶段 1] 扫描 YAML 订阅并生成最新内核配置...")
        if not build_clash_config():
            print("\n[致命] 启动失败：无法生成配置文件，程序退出。")
            sys.exit(1)

        print("\n[阶段 2] 启动本地独立 Mihomo 内核...")
        if not start_local_mihomo():
            print("\n[致命] 启动失败：无法拉起内置 Mihomo 内核，程序退出。")
            sys.exit(1)

        try:
            proxies = _read_clash_proxies_sync(timeout=5)
            _sync_runtime_bindings_from_loaded_config(proxies)
        except Exception as e:
            _log("WARN", f"初始化读取选择器状态失败: {e}")

        with STATE_LOCK:
            POOL_STATE["all_valid_nodes"] = []
            POOL_STATE["valid_nodes"] = []
            POOL_STATE["mapping_ports"] = []

        cached_nodes = load_cache()
        if cached_nodes:
            cached_nodes = repair_bindings_from_cached_health()
            with STATE_LOCK:
                POOL_STATE["valid_nodes"] = cached_nodes
                POOL_STATE["cached_last_update"] = proxy_pool_db.get_meta("last_update", "")
                POOL_STATE["last_update"] = ""
                POOL_STATE["has_live_updated"] = False
            if not _is_cache_mapping_safe(cached_nodes):
                _log(
                    "WARN",
                    f"缓存映射不完整: {len(cached_nodes)}/{len(_load_mapping_ports())}，已尽量补齐并继续启动",
                )
        else:
            with STATE_LOCK:
                POOL_STATE["all_valid_nodes"] = []
                POOL_STATE["valid_nodes"] = []
                POOL_STATE["mapping_ports"] = []
            _log("WARN", "SQLite 中暂无可用健康缓存，将等待第一轮实时检测")

        mapping_list = _load_mapping_ports()
        port_desc = f"{mapping_list[0]} ~ {mapping_list[-1]}" if mapping_list else f"{START_PORT} ~ {END_PORT}"

        print("\n" + "=" * 65)
        print(f"  [+] 静态映射端口: {port_desc} (共 {len(mapping_list)} 个端口)")
        print(f"  [+] HTTP 动态网关: http://127.0.0.1:{GATEWAY_PORT}")
        print(f"  [+] Mixed 双协议: http/socks5://127.0.0.1:{SOCKS5_PORT}")
        print(f"  [+] Web 管理后台: http://127.0.0.1:{API_PORT}")
        print(f"  [+] 代理导出文件: proxy_list.txt / 静态代理ip.txt")
        print("  [*] 内置独立内核托管运行中，与系统 Clash Verge 彻底隔离！")
        print("=" * 65 + "\n")

        from .web_api import app
        uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="warning", timeout_graceful_shutdown=SHUTDOWN_TIMEOUT)

    finally:
        _log("INFO", "正在关闭内置独立 Mihomo 内核...")
        stop_local_mihomo()
