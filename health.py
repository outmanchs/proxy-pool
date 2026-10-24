import asyncio
import time
import urllib.parse
from collections import deque
from statistics import median

import aiohttp
from . import runtime
from . import proxy_pool_db
from .clash_api import _fetch_clash_proxies_status, _loaded_pool_config_status

from .runtime import (
    CLASH_API_URL,
    CLASH_SECRET,
    HEALTH_CHECK_INITIAL_DELAY,
    HEALTH_CHECK_RETRY_SHORT,
    HEALTH_DROP_CONFIRMATIONS,
    HEALTH_DROP_RATIO,
    HEALTH_SUSPECT_DROP_COUNT,
    HEALTH_TEST_CONCURRENCY,
    HEALTH_TEST_BATCH_SIZE,
    MAX_DELAY,
    NODE_DELAY_SAMPLE_WINDOW,
    NODE_FAILURE_CONFIRMATIONS,
    NODE_HEALTH_STATE,
    NODE_TIMEOUT,
    PENDING_SELECTOR_BINDINGS,
    POOL_STATE,
    PORT_BINDINGS,
    PRIORITY_FILES,
    STATE_LOCK,
    TEST_INTERVAL,
    TEST_URL,
    TEST_URLS,
    HEALTH_CHECK_EVENT,
    _apply_compact_valid_mapping,
    _load_mapping_ports,
    _log,
    _switch_auto_selector,
    _switch_port_selectors,
    save_cache,
    hot_sync_from_sqlite,
)

async def _test_one_proxy_delay(session, semaphore, node, test_url=None):
    name = node.get("name")
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"{CLASH_API_URL}/proxies/{encoded_name}/delay"
    headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
    params = {"timeout": str(NODE_TIMEOUT), "url": test_url or TEST_URL}
    timeout = aiohttp.ClientTimeout(total=NODE_TIMEOUT / 1000 + 3)
    try:
        async with semaphore:
            async with session.get(url, headers=headers, params=params, timeout=timeout) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
        delay = data.get("delay") if isinstance(data, dict) else None
        if not isinstance(delay, (int, float)) or delay <= 0:
            return None
        if MAX_DELAY and delay > MAX_DELAY:
            return None
        node_info = node.copy()
        node_info["delay"] = int(delay)
        return node_info
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return None

async def _test_node_multi_url(session, semaphore, node):
    """多目标测速：节点需通过全部 TEST_URLS 才算有效，延迟取主目标 (TEST_URLS[0]) 的结果。"""
    results = await asyncio.gather(*(
        _test_one_proxy_delay(session, semaphore, node, test_url) for test_url in TEST_URLS
    ))
    if any(res is None for res in results):
        return None
    return results[0]

async def _collect_active_health_results(nodes_to_test):
    semaphore = asyncio.Semaphore(max(1, HEALTH_TEST_CONCURRENCY))
    connector = aiohttp.TCPConnector(limit=max(1, HEALTH_TEST_CONCURRENCY), enable_cleanup_closed=True)
    batch_size = max(1, HEALTH_TEST_BATCH_SIZE)
    measured = []
    total = len(nodes_to_test)
    completed_count = 0
    start_ts = time.time()
    async with aiohttp.ClientSession(connector=connector) as session:
        for offset in range(0, total, batch_size):
            batch = nodes_to_test[offset:offset + batch_size]
            results = await asyncio.gather(*(
                _test_node_multi_url(session, semaphore, node) for node in batch
            ))
            batch_success = [node for node in results if node is not None]
            measured.extend(batch_success)
            completed_count += len(batch)
            percent = (completed_count / total) * 100
            elapsed = time.time() - start_ts
            avg_d = int(sum(n['delay'] for n in measured) / len(measured)) if measured else 0
            _log(
                "INFO",
                f"  ⏳ [测速进度 {completed_count}/{total} ({percent:.1f}%)] "
                f"实时可用: {len(measured)} 个 | 本批成功: {len(batch_success)}/{len(batch)} | "
                f"当前平均延迟: {avg_d}ms | 已耗时: {elapsed:.1f}s",
            )
    return measured

async def _find_sequential_valid_node(session, exclude_names, max_candidates=30, batch_size=6):
    semaphore = asyncio.Semaphore(batch_size)
    candidates = proxy_pool_db.load_ordered_candidates(exclude_names)
    if max_candidates:
        candidates = candidates[:max_candidates]
    for offset in range(0, len(candidates), batch_size):
        batch = candidates[offset:offset + batch_size]
        results = await asyncio.gather(*(
            _test_node_multi_url(session, semaphore, candidate) for candidate in batch
        ))
        for res in results:
            if res is not None:
                return res
    return None

def _stabilize_health_results(nodes_to_test, measured_valid_nodes):
    measured_by_name = {node["name"]: node for node in measured_valid_nodes}
    with STATE_LOCK:
        previous_valid = {
            node["name"]: node
            for node in POOL_STATE.get("all_valid_nodes", [])
            if isinstance(node, dict) and node.get("name")
        }

    stable_nodes = []
    transient_failures = 0
    confirmed_failures = 0
    current_names = set()

    for source_node in nodes_to_test:
        name = source_node.get("name")
        if not name:
            continue
        current_names.add(name)
        state = NODE_HEALTH_STATE.setdefault(name, {
            "failures": 0,
            "delays": deque(maxlen=NODE_DELAY_SAMPLE_WINDOW),
        })
        delays = state.get("delays")
        if not isinstance(delays, deque) or delays.maxlen != NODE_DELAY_SAMPLE_WINDOW:
            delays = deque(delays or [], maxlen=NODE_DELAY_SAMPLE_WINDOW)
            state["delays"] = delays

        measured = measured_by_name.get(name)
        previous = previous_valid.get(name)
        if measured is not None:
            if not delays and previous and isinstance(previous.get("delay"), (int, float)):
                delays.append(int(previous["delay"]))
            delays.append(int(measured["delay"]))
            state["failures"] = 0
            stable = dict(measured)
            stable["delay"] = int(round(median(delays)))
            stable_nodes.append(stable)
            continue

        state["failures"] = int(state.get("failures", 0)) + 1
        if previous is not None and state["failures"] < NODE_FAILURE_CONFIRMATIONS:
            stable = dict(previous)
            stable["consecutive_failures"] = state["failures"]
            stable_nodes.append(stable)
            transient_failures += 1
        else:
            confirmed_failures += 1

    for stale_name in set(NODE_HEALTH_STATE) - current_names:
        NODE_HEALTH_STATE.pop(stale_name, None)

    return stable_nodes, transient_failures, confirmed_failures

def _health_result_requires_confirmation(new_valid_count, mapping_target):
    global HEALTH_SUSPECT_DROP_COUNT
    with STATE_LOCK:
        previous_valid_count = len(POOL_STATE.get("all_valid_nodes", []))
        if previous_valid_count <= 0:
            previous_valid_count = len(POOL_STATE.get("valid_nodes", []))

    # 仅当已有稳定池且有效节点暴跌超过 25% (即低于 75%) 时才触发异常骤降保护
    suspicious_drop = (
        previous_valid_count >= 100
        and new_valid_count < previous_valid_count * HEALTH_DROP_RATIO
        and new_valid_count < mapping_target * HEALTH_DROP_RATIO
    )
    if not suspicious_drop:
        HEALTH_SUSPECT_DROP_COUNT = 0
        return False, previous_valid_count, 0

    HEALTH_SUSPECT_DROP_COUNT += 1
    if HEALTH_SUSPECT_DROP_COUNT >= HEALTH_DROP_CONFIRMATIONS:
        confirmations = HEALTH_SUSPECT_DROP_COUNT
        HEALTH_SUSPECT_DROP_COUNT = 0
        return False, previous_valid_count, confirmations
    return True, previous_valid_count, HEALTH_SUSPECT_DROP_COUNT


async def health_check_worker():
    initial_delay = 3
    is_first_scan = True
    _log(
        "INFO",
        f"节点检测系统已就绪，将在 {initial_delay} 秒后发起首次全量节点扫描...",
    )
    await asyncio.sleep(initial_delay)
    while True:
        hot_sync_from_sqlite()
        with STATE_LOCK:
            bound_names = list(PORT_BINDINGS.values())
            all_nodes = list(POOL_STATE.get("all_nodes", []))
            all_nodes_by_name = {
                node.get("name"): node for node in all_nodes
                if isinstance(node, dict) and node.get("name")
            }

            # 始终对全部订阅节点进行全量健康检测，获取全池真实有效节点总数
            nodes_to_test = all_nodes if all_nodes else [
                all_nodes_by_name[name] for name in dict.fromkeys(bound_names)
                if name in all_nodes_by_name
            ]
            scan_mode_desc = "全池全量节点扫描" if is_first_scan else "定期全池节点检测"

        if not nodes_to_test:
            await asyncio.sleep(3)
            continue

        with STATE_LOCK:
            POOL_STATE["is_testing"] = True
        start_time = time.time()
        total = len(nodes_to_test)
        wait_seconds = TEST_INTERVAL
        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
        _log("INFO", f"开始主动检测节点 [{scan_mode_desc}] (共 {total} 个节点，并发 {HEALTH_TEST_CONCURRENCY}，多目标测速 {len(TEST_URLS)} 站全过才算有效)...")
        try:
            async with aiohttp.ClientSession() as session:
                proxies = await _fetch_clash_proxies_status(session)
                if not proxies:
                    _log("WARN", "Clash API 未返回配置状态，保留当前映射，15秒后重试")
                    wait_seconds = HEALTH_CHECK_RETRY_SHORT
                else:
                    ready, matched_count, selector_count, selector_total = _loaded_pool_config_status(
                        nodes_to_test,
                        proxies,
                    )
                    if not ready:
                        with STATE_LOCK:
                            existing_count = len(POOL_STATE.get("valid_nodes", []))
                        _log(
                            "WARN",
                            f"Clash 尚未启用本次生成的配置: 节点匹配 {matched_count}/{total}, "
                            f"端口选择器 {selector_count}/{selector_total}；保留现有 {existing_count} 个映射",
                        )
                        wait_seconds = HEALTH_CHECK_RETRY_SHORT
                    else:
                        measured_valid_list = await _collect_active_health_results(nodes_to_test)
                        now_str = time.strftime("%Y-%m-%d %H:%M:%S")
                        finish_ts = time.time()
                        if not measured_valid_list:
                            with STATE_LOCK:
                                existing_count = len(POOL_STATE.get("valid_nodes", []))
                                POOL_STATE["last_update"] = now_str
                                POOL_STATE["last_update_ts"] = finish_ts
                            _log("WARN", f"本轮没有测得有效节点，保留现有 {existing_count} 个映射 (更新时间: {now_str})")
                            wait_seconds = HEALTH_CHECK_RETRY_SHORT
                        else:
                            valid_list, transient_failures, confirmed_failures = _stabilize_health_results(
                                nodes_to_test,
                                measured_valid_list,
                            )

                            with STATE_LOCK:
                                current_bound_items = list(PORT_BINDINGS.items())
                            stable_names = {node["name"] for node in valid_list}
                            measured_valid_by_name = {node["name"]: node for node in valid_list}

                            # 规则2：对于已经映射到端口的节点，没失效的不动，失效的就进行替换
                            failed_bindings = {
                                port: name for port, name in current_bound_items
                                if name not in stable_names
                            }
                            kept_count = len(current_bound_items) - len(failed_bindings)
                            replacement_count = 0
                            # 已经被正常未失效端口占用的节点集合
                            used_candidate_names = {
                                name for port, name in current_bound_items
                                if port not in failed_bindings
                            }

                            # 优先从本轮测得有效且未分配到任何端口的健康备用节点中选择替补（按健康、低延迟排序）
                            priority_set = set(PRIORITY_FILES)
                            available_fresh_candidates = sorted(
                                [node for node in valid_list if node["name"] not in used_candidate_names],
                                key=lambda n: (
                                    1 if n.get("consecutive_failures") else 0,
                                    n.get("delay", 999999),
                                    0 if n.get("source_file") in priority_set else 1,
                                )
                            )

                            replacement_map = {}
                            for port, failed_name in sorted(failed_bindings.items()):
                                replacement = None
                                while available_fresh_candidates:
                                    cand = available_fresh_candidates.pop(0)
                                    if cand["name"] not in used_candidate_names:
                                        replacement = cand
                                        used_candidate_names.add(cand["name"])
                                        break

                                if replacement is None:
                                    replacement = await _find_sequential_valid_node(
                                        session,
                                        used_candidate_names,
                                    )
                                    if replacement:
                                        used_candidate_names.add(replacement["name"])

                                if replacement is None:
                                    _log("WARN", f"端口 {port} 节点已失效，备用池中暂无可用健康节点")
                                    continue

                                replacement_map[port] = (failed_name, replacement)
                                used_candidate_names.add(replacement["name"])

                            if replacement_map:
                                switch_payload = {port: rep["name"] for port, (_, rep) in replacement_map.items()}
                                success_bindings, failed_switch = await asyncio.to_thread(
                                    _switch_port_selectors,
                                    switch_payload,
                                )

                                for port, (failed_name, replacement) in replacement_map.items():
                                    if port in success_bindings:
                                        with STATE_LOCK:
                                            PORT_BINDINGS[port] = replacement["name"]
                                            PENDING_SELECTOR_BINDINGS.pop(port, None)
                                        if replacement["name"] not in stable_names:
                                            valid_list.append(replacement)
                                            stable_names.add(replacement["name"])
                                        replacement_count += 1
                                        delay_text = f" ({replacement.get('delay', 0)}ms)"
                                        _log(
                                            "OK",
                                            f"端口 {port} 节点已失效，自动替换: "
                                            f"{failed_name} → {replacement['name']}{delay_text}",
                                        )
                                    else:
                                        _log("WARN", f"端口 {port} 换绑到 {replacement['name']} 失败")

                                if success_bindings:
                                    proxy_pool_db.save_port_bindings(success_bindings)

                            mapping_target = len(_load_mapping_ports()) or len(valid_list)
                            needs_confirmation, previous_count, confirmation_count = _health_result_requires_confirmation(
                                len(valid_list),
                                mapping_target,
                            )
                            if needs_confirmation:
                                with STATE_LOCK:
                                    existing_count = len(POOL_STATE.get("valid_nodes", []))
                                    POOL_STATE["last_update"] = now_str
                                    POOL_STATE["last_update_ts"] = finish_ts
                                _log(
                                    "WARN",
                                    f"检测到有效节点异常骤降 {previous_count} → {len(valid_list)}，"
                                    f"确认 {confirmation_count}/{HEALTH_DROP_CONFIRMATIONS}；保留现有 {existing_count} 个映射 (更新时间: {now_str})",
                                )
                                wait_seconds = HEALTH_CHECK_RETRY_SHORT
                            else:
                                priority_set = set(PRIORITY_FILES)
                                all_valid_nodes = sorted(valid_list, key=lambda n: (
                                    1 if n.get("consecutive_failures") else 0,
                                    n.get("delay", 999999),
                                    0 if n.get("source_file") in priority_set else 1,
                                ))
                                compact_valid_list = await asyncio.to_thread(
                                    _apply_compact_valid_mapping,
                                    all_valid_nodes,
                                )
                                await asyncio.to_thread(_switch_auto_selector, all_valid_nodes)

                                with STATE_LOCK:
                                    POOL_STATE["all_valid_nodes"] = all_valid_nodes
                                    POOL_STATE["valid_nodes"] = compact_valid_list
                                    POOL_STATE["mapping_ports"] = [node["port"] for node in compact_valid_list]
                                    POOL_STATE["last_update"] = now_str
                                    POOL_STATE["last_update_ts"] = finish_ts
                                    POOL_STATE["has_live_updated"] = True

                                proxy_pool_db.set_meta("last_update", now_str)
                                save_cache(compact_valid_list, all_valid_nodes)

                                elapsed_sec = time.time() - start_time
                                valid_count = len(all_valid_nodes)
                                fail_count = total - valid_count
                                avail_rate = (valid_count / total * 100) if total else 0.0
                                finish_time_cn = time.strftime("%Y年%m月%d日 %H:%M:%S", time.localtime(finish_ts))
                                next_run_ts = finish_ts + TEST_INTERVAL
                                next_time_cn = time.strftime("%Y年%m月%d日 %H:%M:%S", time.localtime(next_run_ts))
                                h_val = TEST_INTERVAL // 3600
                                m_val = (TEST_INTERVAL % 3600) // 60
                                s_val = TEST_INTERVAL % 60
                                parts = []
                                if h_val > 0: parts.append(f"{h_val} 小时")
                                if m_val > 0: parts.append(f"{m_val} 分钟")
                                if s_val > 0 or not parts: parts.append(f"{s_val} 秒")
                                next_interval_desc = " ".join(parts)
                                detail_desc = f" (直接响应 {len(measured_valid_list)} 个, 容灾缓冲 {transient_failures} 个)" if transient_failures else ""

                                print("=" * 70)
                                _log("OK", f"【全池节点检测完成摘要】({scan_mode_desc})")
                                _log(
                                    "OK",
                                    f"  · 全池节点统计: 共检测 {total} 个 | 全池有效 {valid_count} 个{detail_desc} | 失败 {fail_count} 个 "
                                    f"(全池可用率: {avail_rate:.1f}%, 成功: {valid_count}/{total})",
                                )
                                _log(
                                    "OK",
                                    f"  · 端口映射维护: 保持有效 {kept_count} 个 | 自动替换失效 {replacement_count} 个 | 端口池 {len(PORT_BINDINGS)} 个映射全部连通",
                                )
                                _log("OK", f"  · 本轮检测耗时: {elapsed_sec:.1f} 秒")
                                _log("OK", f"  · 本次完成时间: {finish_time_cn}")
                                _log("OK", f"  · 下次检测时间: {next_time_cn} (将在 {next_interval_desc} 后开始)")
                                print("=" * 70)

                                is_first_scan = False
                                wait_seconds = TEST_INTERVAL
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            _log("WARN", "本轮健康检测耗时超过上限(180s)，触发超时保护，保留现有映射进入下一轮")
            wait_seconds = HEALTH_CHECK_RETRY_SHORT
        except Exception as e:
            _log("WARN", f"读取 Clash 节点状态异常: {e}")
            wait_seconds = HEALTH_CHECK_RETRY_SHORT
        finally:
            with STATE_LOCK:
                POOL_STATE["is_testing"] = False
        if wait_seconds > 0:
            try:
                await asyncio.wait_for(HEALTH_CHECK_EVENT.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
        HEALTH_CHECK_EVENT.clear()
