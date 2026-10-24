from pathlib import Path
import json
import sys
import urllib.request

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def check_running_instance():
    try:
        req = urllib.request.Request("http://127.0.0.1:6001/api/status")
        with urllib.request.urlopen(req, timeout=1.5) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if data.get("status") == "ok":
                    return data
    except Exception:
        pass
    return None


def delegate_to_running_instance(status):
    print("\n" + "=" * 65)
    print("  [+] 提示: 代理池主服务已在后台健康运行中 (http://127.0.0.1:6001)")
    print(f"  [+] 本地端口: {status.get('mapping_range')} | 动态有效节点: {status.get('valid_nodes')} 个")
    print("  [*] 为保证现有业务和网络连接 100% 不断网，避免重复拉起内核。")
    print("  [*] 正在通知主服务执行零断网热同步...")
    try:
        try:
            req = urllib.request.Request("http://127.0.0.1:6001/api/runtime/sync?force=true", data=b"", method="POST")
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                sync_data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                print(f"  [+] 零断网热同步成功！最新统一时间: {sync_data.get('last_update', '已同步')}")
        except urllib.error.HTTPError as err:
            if err.code == 404:
                req = urllib.request.Request("http://127.0.0.1:6001/api/health/check?wait=false", data=b"", method="POST")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    print("  [+] 已向主服务发送健康检查与热刷新指令！")
            else:
                raise
    except Exception as e:
        print(f"  [!] 提示: 已向主服务发送同步请求 ({e})")
    print("=" * 65 + "\n")
    sys.exit(0)


if __name__ == "__main__":
    existing = check_running_instance()
    if existing:
        delegate_to_running_instance(existing)

    if __package__ in (None, ""):
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from proxy_pool.main import run
    else:
        from .main import run

    run()
