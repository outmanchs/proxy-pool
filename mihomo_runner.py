import atexit
import os
import signal
import subprocess
import sys
import time
import urllib.request
import json

from .runtime import (
    BASE_DIR,
    CLASH_API_URL,
    CLASH_SECRET,
    OUTPUT_CONFIG,
    _log,
)

import platform

# 默认内核查找候选映射
def _get_platform_candidates() -> list[str]:
    system = platform.system().lower()
    machine = platform.machine().lower()
    is_arm = machine in ("arm64", "aarch64", "armv8")

    if system == "windows":
        return [
            "mihomo.exe",
            "mihomo-windows-amd64.exe",
            "mihomo-windows-amd64-compatible.exe",
            "mihomo-windows-arm64.exe",
            "clash-meta.exe",
            "clash.exe",
        ]
    elif system == "darwin":  # macOS
        if is_arm:
            return [
                "mihomo-darwin-arm64",
                "mihomo",
                "clash-meta-darwin-arm64",
                "mihomo-darwin-amd64",  # Rosetta 2 转译兜底
            ]
        else:
            return [
                "mihomo-darwin-amd64",
                "mihomo",
                "clash-meta-darwin-amd64",
            ]
    else:  # Linux 及其他
        if is_arm:
            return ["mihomo-linux-arm64", "mihomo", "clash-meta-linux-arm64"]
        else:
            return ["mihomo-linux-amd64", "mihomo", "clash-meta-linux-amd64"]


_mihomo_process = None
_mihomo_log_file = None


def find_mihomo_executable() -> str | None:
    """自动根据当前操作系统与 CPU 架构智能查找并准备可执行文件"""
    candidates = _get_platform_candidates()
    # 无论哪个系统，最后都把通用名称 "mihomo" 作为通用备选
    if "mihomo" not in candidates:
        candidates.append("mihomo")

    for name in candidates:
        exe_path = os.path.join(BASE_DIR, name)
        if os.path.isfile(exe_path):
            # macOS / Linux 下自动赋予执行权限并清理隔离属性
            if sys.platform != "win32":
                try:
                    os.chmod(exe_path, 0o755)
                except Exception:
                    pass
                if sys.platform == "darwin":
                    try:
                        subprocess.run(
                            ["xattr", "-d", "com.apple.quarantine", exe_path],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    except Exception:
                        pass
            return exe_path
    return None


def is_mihomo_api_ready(timeout: float = 2.0) -> bool:
    """测试本地独立 Mihomo 的外部控制 API 是否就绪"""
    try:
        url = f"{CLASH_API_URL}/version"
        headers = {"Authorization": f"Bearer {CLASH_SECRET}"} if CLASH_SECRET else {}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode('utf-8', errors='ignore'))
                version = data.get("version", "unknown")
                return True
    except Exception:
        pass
    return False


def kill_orphan_mihomo_processes(exe_path: str):
    """清理属于本目录或残留的孤儿 mihomo 进程，防止端口冲突"""
    if sys.platform in ("darwin", "linux"):
        try:
            exe_name = os.path.basename(exe_path)
            subprocess.run(["pkill", "-f", exe_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
        return

    if sys.platform != "win32":
        return
    try:
        norm_target = os.path.normcase(os.path.abspath(exe_path))
        cmd = ["wmic", "process", "where", "name='mihomo.exe'", "get", "ProcessId,ExecutablePath"]
        output = subprocess.check_output(cmd, creationflags=subprocess.CREATE_NO_WINDOW, text=True, stderr=subprocess.DEVNULL)
        for line in output.strip().splitlines():
            line = line.strip()
            if not line or "ExecutablePath" in line:
                continue
            parts = line.rsplit(None, 1)
            if len(parts) == 2:
                proc_path, pid_str = parts[0].strip(), parts[1].strip()
                if os.path.normcase(os.path.abspath(proc_path)) == norm_target and pid_str.isdigit():
                    try:
                        subprocess.run(["taskkill", "/F", "/PID", pid_str], creationflags=subprocess.CREATE_NO_WINDOW, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        time.sleep(0.3)
                    except Exception:
                        pass
    except Exception:
        pass


import ctypes
from ctypes import wintypes

_job_handle = None

def _init_windows_job_object():
    """初始化 Windows Job Object，确保父进程无论如何退出，子进程都会被 Windows 内核强制杀死"""
    global _job_handle
    if sys.platform != "win32" or _job_handle is not None:
        return
    try:
        kernel32 = ctypes.windll.kernel32
        _job_handle = kernel32.CreateJobObjectW(None, None)
        if not _job_handle:
            return

        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
        JobObjectExtendedLimitInformation = 9

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoCounters", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryLimit", ctypes.c_size_t),
                ("PeakJobMemoryLimit", ctypes.c_size_t),
            ]

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        kernel32.SetInformationJobObject(
            _job_handle,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
    except Exception:
        pass


def _assign_process_to_job(proc_pid):
    """将子进程绑定到 Windows Job Object"""
    global _job_handle
    if sys.platform != "win32" or not _job_handle:
        return
    try:
        kernel32 = ctypes.windll.kernel32
        PROCESS_ALL_ACCESS = 0x1F0FFF
        h_proc = kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, proc_pid)
        if h_proc:
            kernel32.AssignProcessToJobObject(_job_handle, h_proc)
            kernel32.CloseHandle(h_proc)
    except Exception:
        pass


# Windows 控制台关闭事件处理器 (当用户直接点击CMD黑窗口右上角的 X 时触发)
_console_ctrl_handler = None
if sys.platform == "win32":
    try:
        PHANDLER_ROUTINE = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)
        def _win_console_handler(dwCtrlType):
            # 0: CTRL_C_EVENT, 1: CTRL_BREAK_EVENT, 2: CTRL_CLOSE_EVENT, 5: CTRL_LOGOFF_EVENT, 6: CTRL_SHUTDOWN_EVENT
            stop_local_mihomo()
            return True
        _console_ctrl_handler = PHANDLER_ROUTINE(_win_console_handler)
        ctypes.windll.kernel32.SetConsoleCtrlHandler(_console_ctrl_handler, True)
    except Exception:
        pass


def start_local_mihomo() -> bool:
    """启动同目录下的独立 Mihomo 内核进程"""
    global _mihomo_process, _mihomo_log_file

    exe_path = find_mihomo_executable()
    if not exe_path:
        print("\n" + "=" * 68)
        print("  ❌ [错误] 未在当前目录下找到 mihomo.exe 内核！")
        print(f"  当前运行目录: {BASE_DIR}")
        print("  👉 请将 mihomo.exe 放置在上述目录下后重新运行。")
        print("=" * 68 + "\n")
        return False

    if not os.path.isfile(OUTPUT_CONFIG):
        _log("FAIL", f"未找到生成的配置文件: {OUTPUT_CONFIG}")
        return False

    # 启动前彻底清理属于本目录的历史孤儿进程
    kill_orphan_mihomo_processes(exe_path)

    # 初始化 Windows 作业对象 (绑定退出联动)
    _init_windows_job_object()

    log_path = os.path.join(BASE_DIR, "mihomo.log")
    try:
        _mihomo_log_file = open(log_path, "w", encoding="utf-8")
    except Exception:
        _mihomo_log_file = subprocess.DEVNULL

    cmd = [
        exe_path,
        "-f", OUTPUT_CONFIG,
        "-d", BASE_DIR,
    ]

    _log("INFO", f"正在启动内置独立 Mihomo 内核: {os.path.basename(exe_path)}")

    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NO_WINDOW  # 静默后台运行，不弹黑框

    try:
        _mihomo_process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=_mihomo_log_file,
            stderr=_mihomo_log_file,
            creationflags=creation_flags
        )
        # 将子进程绑定到 Windows Job Object
        _assign_process_to_job(_mihomo_process.pid)
    except Exception as e:
        _log("FAIL", f"启动内置 Mihomo 失败: {e}")
        return False

    # 注册退出清理钩子 (atexit + 信号)
    atexit.register(stop_local_mihomo)
    for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
        if sig is not None:
            try:
                signal.signal(sig, lambda *args: stop_local_mihomo())
            except Exception:
                pass

    # 等待 API 就绪
    print("[*] 等待内置 Mihomo 内核加载配置与启动网络服务...", end="", flush=True)
    ready = False
    for _ in range(30):
        if _mihomo_process.poll() is not None:
            print(" [FAIL]")
            _log("FAIL", f"Mihomo 异常退出 (Exit Code: {_mihomo_process.returncode})，详情请查看: {log_path}")
            return False
        if is_mihomo_api_ready(timeout=1.5):
            ready = True
            break
        print(".", end="", flush=True)
        time.sleep(1)

    if ready:
        print(" [OK]")
        _log("OK", f"内置独立 Mihomo 内核启动成功 (PID: {_mihomo_process.pid} | API: {CLASH_API_URL})")
        return True
    else:
        print(" [TIMEOUT]")
        _log("FAIL", f"Mihomo 在 30 秒内未响应外部控制端口 {CLASH_API_URL}，请检查: {log_path}")
        stop_local_mihomo()
        return False


def stop_local_mihomo():
    """安全停止内置 Mihomo 进程并彻底清理全部孤儿进程"""
    global _mihomo_process, _mihomo_log_file
    if _mihomo_process is not None:
        try:
            if _mihomo_process.poll() is None:
                _mihomo_process.terminate()
                try:
                    _mihomo_process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    _mihomo_process.kill()
        except Exception:
            pass
        _mihomo_process = None

    # 二次兜底：清理同目录路径下的任何残留进程
    exe_path = find_mihomo_executable()
    if exe_path:
        kill_orphan_mihomo_processes(exe_path)

    if _mihomo_log_file and _mihomo_log_file not in (subprocess.DEVNULL, None):
        try:
            _mihomo_log_file.close()
        except Exception:
            pass
        _mihomo_log_file = None
