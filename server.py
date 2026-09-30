#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NInfer Console（NInfer 监控操作台）
===================================
纯 stdlib 单文件服务：状态聚合 + 一键操作 + 日志查看。
零第三方依赖：python3 (3.9+) 即可运行。

数据源（全部只读，操作只走 systemctl --user，所有路径/地址可配置）：
  - systemctl --user show <unit>          服务状态/PID/重启计数/RSS/启动时间
  - <engine_url>/health                   健康（毒化检测：进程在听但全 503）
  - <engine_url>/v1/models                模型 + max_model_len
  - nvidia-smi（裸 Linux 直接调）或 Windows 侧 nvidia-smi via powershell（WSL2）
                                          显存/利用率/温度/功耗（5s 缓存，自动探测）
  - /proc/meminfo + /proc/stat            主机内存/CPU
  - serve 日志（serve_log）               capacity / context cache / throughput / req#N / host KV pinned
                                           （throughput 曲线由服务端 5s 独立采样，与页面轮询解耦）
  - 结构化请求日志（request_jsonl）        每请求 tokens/缓存命中/TTFT/MTP（增量解析）
  - OpenClaw agent sqlite（可选，sessions_source=openclaw）单会话 input/output/cacheRead
  - 看门狗状态文件（watchdog_*）           看门狗状态

配置: config.toml（与脚本同目录，模板见 config.toml.example）> 环境变量 NINFER_DASH_<KEY> > 内置默认
启动: python3 server.py   （或 restart.sh / systemd user 单元 ninfer-dashboard.service.example）
访问: http://127.0.0.1:8090
"""

import concurrent.futures
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from hashlib import md5

# ---------------------------------------------------------------- 配置
# 优先级: 环境变量 NINFER_DASH_<KEY> > config.toml（与脚本同目录）> 内置默认
# 全部可配项见 config.toml.example

_DEFAULTS = {
    # 控制台自身
    "dashboard_port": 8090,
    "dashboard_bind": "127.0.0.1",
    # 引擎（NInfer serve / 任意 OpenAI 兼容 HTTP 端点）
    "engine_url": "http://127.0.0.1:8081",
    # 服务管理（systemd user 单元名；不用 systemd 时一键操作降级为报错）
    "service_unit": "ninfer",
    # 进程识别（/proc 扫描的 argv[0] 后缀，需与引擎二进制名一致）
    "process_name": "ninfer-serve",
    # 数据文件路径（全部只读）
    "serve_log": "/tmp/ninfer-serve.log",
    "request_jsonl": "/tmp/ninfer-req.jsonl",
    "watchdog_failcount": "/tmp/ninfer-health-watchdog.failcount",
    "watchdog_log": "/tmp/ninfer-health-watchdog.log",
    "prefix_dump": "/tmp/ninfer-prefix-dump.bin",
    "keep_stopped": "/tmp/ninfer-keep-stopped",
    "energy_file": "/tmp/ninfer-dashboard-energy.json",
    # GPU 采样: auto | nvidia-smi | windows-powershell
    # auto: 优先 PATH 里的 nvidia-smi（裸 Linux）；否则 WSL 环境走 Windows 侧 powershell
    "gpu_mode": "auto",
    "powershell_path": "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
    # 单会话命中率数据源: openclaw | off
    "sessions_source": "openclaw",
    "openclaw_sqlite": "~/.openclaw/agents/main/agent/openclaw-agent.sqlite",
    "session_providers": ("ninfer", "ninfer-orcarouter"),
    # 一键探针（POST /api/probe）
    "probe_model_fallback": "",  # 空 = 用 /v1/models 的第一个模型
    "probe_chat_template_kwargs": {},  # 如 {"enable_thinking": false}（Qwen 系需要）
}

_ENV_PREFIX = "NINFER_DASH_"


def _parse_toml_scalar(s):
    """最小 TOML 标量解析（Python < 3.11 无 tomllib 时的 fallback）。"""
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        return s[1:-1]
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [_parse_toml_scalar(x) for x in inner.split(",")]
    if s.startswith("{") and s.endswith("}"):
        inner = s[1:-1].strip()
        d = {}
        if inner:
            for part in inner.split(","):
                if "=" in part:
                    k, v = part.split("=", 1)
                    d[k.strip().strip("\"'")] = _parse_toml_scalar(v)
        return d
    if s in ("true", "false"):
        return s == "true"
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        return s


def _load_config_file(path):
    """读 config.toml：优先 tomllib（3.11+）；老版本走最小解析器（仅支持扁平 key=value）。"""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return {}
    try:
        import tomllib  # Python 3.11+
        loaded = tomllib.loads(text)
        # 顶层 [section] 表与内联表 {..} 都是 dict：保留内联表，忽略 section 表（键名不含点）
        return {k: v for k, v in loaded.items() if k in _DEFAULTS or not isinstance(v, dict)}
    except ImportError:
        cfg = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("[") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip()
            if v and v[0] not in "\"'[{":
                v = v.split("#", 1)[0].strip()
            cfg[k.strip()] = _parse_toml_scalar(v)
        return cfg


def _build_config():
    cfg = dict(_DEFAULTS)
    here = os.path.dirname(os.path.abspath(__file__))
    cfg.update(_load_config_file(os.path.join(here, "config.toml")))
    for key in _DEFAULTS:
        env = os.environ.get(_ENV_PREFIX + key.upper())
        if env is None:
            continue
        if isinstance(_DEFAULTS[key], bool):
            cfg[key] = env.strip().lower() in ("1", "true", "yes", "on")
        elif isinstance(_DEFAULTS[key], int):
            try:
                cfg[key] = int(env)
            except ValueError:
                pass
        elif isinstance(_DEFAULTS[key], (list, tuple)):
            cfg[key] = [x for x in env.split(",") if x]
        elif isinstance(_DEFAULTS[key], dict):
            try:
                cfg[key] = json.loads(env)
            except json.JSONDecodeError:
                pass
        else:
            cfg[key] = env
    cfg["openclaw_sqlite"] = os.path.expanduser(cfg["openclaw_sqlite"])
    return cfg


CONFIG = _build_config()

PORT = int(CONFIG["dashboard_port"])
BIND = CONFIG["dashboard_bind"]
NINFER_HOST = CONFIG["engine_url"].rstrip("/")
KEEP_STOPPED_FILE = CONFIG["keep_stopped"]  # 外部组件可约定读此标记：存在则请求到达也不自动拉起
SERVICE = CONFIG["service_unit"]
LOG_FILE = CONFIG["serve_log"]
REQ_JSONL = CONFIG["request_jsonl"]
WATCHDOG_FAILCOUNT = CONFIG["watchdog_failcount"]
WATCHDOG_LOG = CONFIG["watchdog_log"]
AGENT_SQLITE = CONFIG["openclaw_sqlite"]
PS = CONFIG["powershell_path"]
NINFER_PROVIDERS = tuple(CONFIG["session_providers"])

# Windows 侧组合查询：nvidia-smi（WSL 内 torch 读数不可信）+ 物理内存
GPU_QUERY = (
    "nvidia-smi --query-gpu=memory.used,memory.total,utilization.gpu,"
    "temperature.gpu,power.draw --format=csv,noheader; "
    "$os = Get-CimInstance Win32_OperatingSystem; "
    "$cs = Get-CimInstance Win32_ComputerSystem; "
    '"WINMEM " + $cs.TotalPhysicalMemory + " " + ($os.TotalVisibleMemorySize*1024) + " " + ($os.FreePhysicalMemory*1024)'
)


def _is_wsl():
    try:
        with open("/proc/version", encoding="utf-8", errors="replace") as f:
            if "microsoft" in f.read().lower():
                return True
    except OSError:
        pass
    return bool(os.environ.get("WSL_DISTRO_NAME"))


PLATFORM = "wsl" if _is_wsl() else "linux"


def _select_gpu_backend(mode=None):
    mode = mode or CONFIG.get("gpu_mode", "auto")
    if mode in ("nvidia-smi", "windows-powershell"):
        return mode
    # auto: 裸 Linux 优先 nvidia-smi；WSL2 走 Windows 侧 powershell（机内读数不可信）
    if shutil.which("nvidia-smi"):
        return "nvidia-smi"
    if _is_wsl() and os.path.exists(PS):
        return "windows-powershell"
    return "nvidia-smi"  # 兜底：直接试 nvidia-smi，失败时 UI 明示错误


GPU_BACKEND = _select_gpu_backend()

# ---------------------------------------------------------------- 小工具


def _http_get(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def _now_str():
    return datetime.now().strftime("%H:%M:%S")


def _fmt_uptime(seconds):
    """运行时长 → '41h 33m 47s'（顶部 subline 运行时间，2026-09-30 用户要求 h/min/s 形式）"""
    if seconds is None:
        return None
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def _human(n, unit="B"):
    """bytes → 人类可读"""
    if n is None:
        return None
    n = float(n)
    for u in (unit, "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or u == "TiB":
            return f"{n:.1f} {u}" if u != unit else f"{int(n)} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"


def _pct(a, b):
    if not b:
        return None
    return round(100.0 * a / b, 1)


# ---------------------------------------------------------------- 采集器

_GPU = {"data": None, "ts": 0, "error": None, "series": [],
        "winmem": None, "wslboot": None}

_ENERGY_FILE = CONFIG["energy_file"]


def _energy_load():
    try:
        with open(_ENERGY_FILE) as f:
            d = json.load(f)
            return {
                "service_epoch": int(d.get("service_epoch", 0)),
                "energy_j": float(d.get("energy_j", 0.0)),
            }
    except Exception:
        return {"service_epoch": 0, "energy_j": 0.0}


def _energy_save():
    try:
        with open(_ENERGY_FILE, "w") as f:
            json.dump(_ENERGY, f)
    except Exception:
        pass


_ENERGY = _energy_load()
_GPU_LOCK = threading.Lock()
_ACTION_LOCK = threading.Lock()
_TP_TAIL = {"offset": 0, "backfilled": False}
_TP_TAIL_LOCK = threading.Lock()
_HOST_SERIES = []
_HOST_LOCK = threading.Lock()
_CPU_LAST = None
_REQ_STATE = {
    "offset": 0,
    "seen": 0,
    "prompt": 0,
    "completion": 0,
    "cache_hit": 0,
    "drafted": 0,
    "accepted": 0,
    "errors": 0,
    "recent": [],          # 最近 30 条 request_done（新→旧）
    "startup_memory": None,  # server_start 事件的内存账本
    "error_rows": [],      # 最近 20 条 rejected/error
    "hourly": {},          # "HH" → {prompt, completion, cache_hit}
    "host_kv_occupied": None,
    "host_kv_capacity": None,
    "series_throughput": [],  # 服务端环形缓冲
    "req_ctx": {},  # req# → 完成时 ctxcache 快照（缓存未命中深度归因用）
    "req_probe": {},  # req# → 探针画像（GATE 差值/STALE/PEAK/ENTRY_CLR）
    "_gate_baseline": None,  # 上次见过的 GATE_SUMMARY 累计值（跨调用基线）
    "_probe_backfilled": False,  # 启动时全量回填过一次
    "_probe_tail_marker": None,  # 回填时刻日志最后一行（增量只处理其后的新行）
    "_probe_backfill_size": 0,  # 回填时刻日志大小（截断检测）
    "_capacity_cache": None,  # capacity 行只在服务启动输出一次，日志增长后滚出尾窗 → 缓存
    "_hostkv_cache": None,  # host KV pinned 行同理
    "_ctxcache_cache": None,  # context cache 行同理
}
_STATE_LOCK = threading.Lock()


_ENERGY_LAST_T = 0.0


def _gpu_sample():
    """单次 GPU 采样，返回 (data, winmem, error)；data=None 表示失败。

    windows-powershell：WSL2 调 Windows 侧 nvidia-smi（机内 torch 读数不可信），
    顺带采样 Windows 物理内存（winmem，供整机内存堆积条）；
    nvidia-smi：裸 Linux 直接调。
    """
    if GPU_BACKEND == "windows-powershell":
        r = subprocess.run(
            [PS, "-NoProfile", "-Command", GPU_QUERY],
            capture_output=True, text=True, timeout=25,
        )
        out = r.stdout
        lines = [l.strip() for l in out.splitlines() if l.strip()]
        parts = [p.strip() for p in (lines[0].split(",") if lines else [])]
        if len(parts) < 4:
            return None, None, (
                r.stderr.strip()[:160]
                or "nvidia-smi 无输出（驱动/Windows 侧异常）"
            )
        data = {
            "mem_used_mib": int(parts[0].split()[0]),
            "mem_total_mib": int(parts[1].split()[0]),
            "util_pct": int(parts[2].replace("%", "")),
            "temp_c": int(parts[3]),
            "power_w": float(parts[4].split()[0]) if len(parts) > 4 else None,
        }
        winmem = None
        for ln in lines:
            if ln.startswith("WINMEM "):
                nums = ln.split()
                if len(nums) >= 4:
                    winmem = {
                        "total": int(nums[1]),
                        "visible": int(nums[2]),
                        "free": int(nums[3]),
                    }
        return data, winmem, None
    r = subprocess.run(
        ["nvidia-smi",
         "--query-gpu=memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw",
         "--format=csv,noheader"],
        capture_output=True, text=True, timeout=15,
    )
    if r.returncode != 0:
        return None, None, (r.stderr.strip()[:160] or "nvidia-smi 执行失败（驱动未装？）")
    lines = [l.strip() for l in r.stdout.splitlines() if l.strip()]
    if not lines:
        return None, None, "nvidia-smi 无输出"
    parts = [p.strip() for p in lines[0].split(",")]
    if len(parts) < 4:
        return None, None, f"nvidia-smi 输出格式异常: {lines[0][:80]}"
    data = {
        "mem_used_mib": int(parts[0].split()[0]),
        "mem_total_mib": int(parts[1].split()[0]),
        "util_pct": int(parts[2].replace("%", "")),
        "temp_c": int(parts[3]),
        "power_w": float(parts[4].split()[0]) if len(parts) > 4 else None,
    }
    return data, None, None


def _gpu_loop():
    global _ENERGY_LAST_T
    while True:
        try:
            data, winmem, err = _gpu_sample()
            if data is not None:
                with _GPU_LOCK:
                    _GPU["data"], _GPU["ts"], _GPU["error"] = data, time.time(), None
                    _GPU["series"].append({"t": time.time(), **data})
                    if len(_GPU["series"]) > 720:
                        del _GPU["series"][: len(_GPU["series"]) - 720]
                    if winmem is not None:
                        _GPU["winmem"] = winmem
                # 累计耗电量：真实采样间隔×功率积分（自服务启动，服务重启清零，dashboard 重启不丢）
                try:
                    r2 = subprocess.run(
                        ["systemctl", "--user", "show", SERVICE, "-p", "ExecMainStartTimestamp"],
                        capture_output=True, text=True, timeout=5,
                    )
                    m2 = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", r2.stdout)
                    epoch = int(datetime.strptime(m2.group(1), "%Y-%m-%d %H:%M:%S").timestamp()) if m2 else 0
                except Exception:
                    epoch = 0
                if epoch and epoch != _ENERGY["service_epoch"]:
                    _ENERGY["service_epoch"] = epoch
                    _ENERGY["energy_j"] = 0.0
                    _ENERGY_LAST_T = time.time()
                    _energy_save()
                elif data.get("power_w") is not None:
                    now = time.time()
                    dt = now - _ENERGY_LAST_T if _ENERGY_LAST_T else 5.0
                    _ENERGY["energy_j"] += data["power_w"] * min(30.0, max(1.0, dt))
                    _ENERGY_LAST_T = now
                    _energy_save()
            else:
                # 采样失败 → 明示错误，不静默留旧值
                with _GPU_LOCK:
                    _GPU["error"] = err
        except Exception as e:  # noqa: BLE001
            with _GPU_LOCK:
                _GPU["error"] = f"{type(e).__name__}: {e}"
        time.sleep(5)


def _tp_loop():
    """服务端 5s 扫 serve 日志新行 → 吞吐环形缓冲（与页面轮询解耦）。

    旧实现只在浏览器页面轮询 /api/status 时追加曲线点，且每次只记尾窗
    最后一行：标签页后台时 Chrome 把页面 1s setInterval 节流成 1 次/分钟
    （intensive timer throttling），曲线退化成 1 点/分钟，引擎每 5s 的
    12 条上报只留 1 条，尖峰全丢（2026-09-29 用户反馈"吞吐采样不准确、
    曲线 1min 采样一次"）。现服务端独立采样；点时间戳用引擎 line_ts
    （真实打行时刻），不是 dashboard 发现时刻。"""
    st = _REQ_STATE
    while True:
        try:
            size = os.path.getsize(LOG_FILE)
        except OSError:
            size = 0
        with _TP_TAIL_LOCK:
            if not _TP_TAIL["backfilled"]:
                # 启动时回填当前尾窗，恢复 dashboard 重启前的历史曲线
                lines = _tail_lines(LOG_FILE, 512 * 1024, 3000)
                _TP_TAIL["offset"] = size
                _TP_TAIL["backfilled"] = True
            elif size < _TP_TAIL["offset"]:
                # 日志被截断/重开（引擎/OS 重启）→ 重扫尾窗
                lines = _tail_lines(LOG_FILE, 512 * 1024, 3000)
                _TP_TAIL["offset"] = size
            elif size == _TP_TAIL["offset"]:
                lines = []
            else:
                try:
                    with open(LOG_FILE, "rb") as f:
                        f.seek(_TP_TAIL["offset"])
                        chunk = f.read()
                    _TP_TAIL["offset"] = size
                    lines = chunk.decode("utf-8", "replace").splitlines()
                    if lines and not _RE_LOGTS.match(lines[0]):
                        lines = lines[1:]  # 首行是半行碎片（seek 点在行中间）
                except OSError:
                    lines = []
        new_pts = []
        for line in lines:
            d = _parse_throughput_line(line)
            if not d:
                continue
            mts = _RE_LOGTS.match(line)
            if mts:
                try:
                    d["t"] = int(datetime.strptime(
                        mts.group(1), "%Y-%m-%d %H:%M:%S").timestamp())
                except ValueError:
                    d["t"] = int(time.time())
            else:
                d["t"] = int(time.time())
            d["line_ts"] = d["t"]
            new_pts.append(d)
        if new_pts:
            with _STATE_LOCK:
                st["series_throughput"].extend(new_pts)
                if len(st["series_throughput"]) > 720:
                    st["series_throughput"] = st["series_throughput"][-720:]
        time.sleep(5)


def _port_listening(port=8081):
    s = socket.socket()
    s.settimeout(0.5)
    try:
        return s.connect_ex(("127.0.0.1", port)) == 0
    except OSError:
        return False
    finally:
        s.close()


def _find_ninfer_pid():
    """/proc 扫描引擎进程（不依赖 D-Bus）。
    只匹配 argv[0]（可执行文件名），避免 tail/vim 等以日志文件名含
    进程名的进程误报（2026-09-27：tail -f 残留导致停止后状态卡 starting）。"""
    name = CONFIG["process_name"]
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            raw = open(f"/proc/{pid}/cmdline", "rb").read()
        except OSError:
            continue
        argv0 = raw.split(b"\0", 1)[0].decode("utf-8", "replace")
        if argv0.endswith(name):
            return int(pid)
    return None


_SERVICE_START_EPOCH = 0  # collect_service 缓存（未命中归因：冷启动判定）


def collect_service():
    """主判据：端口 + /health + 进程扫描（与看门狗同判据，不依赖 D-Bus）；
    systemctl 只作辅助信息（RSS/重启计数/自启），失败时降级不阻塞。"""
    out = {}
    port = _port_listening()
    health = collect_health()
    pid = _find_ninfer_pid()
    if port and health["ok"]:
        state = "active"
    elif port and not health["ok"]:
        state = "poisoned"  # 进程在听但全 503（09-20 毒化事故形态）
    elif pid:
        state = "starting"  # 进程在但端口未就绪（启动中/停止中）
    else:
        state = "stopped"
    out["state"] = state
    out["pid"] = pid
    out["port_listening"] = port
    out["health"] = health
    global _SERVICE_START_EPOCH
    # 辅助：systemctl（D-Bus 不可用时只降级，不影响主判据）
    try:
        r = subprocess.run(
            ["systemctl", "--user", "show", SERVICE,
             "-p", "MainPID", "-p", "NRestarts", "-p", "MemoryCurrent",
             "-p", "ExecMainStartTimestamp", "-p", "FragmentPath"],
            capture_output=True, text=True, timeout=8,
        )
        for line in r.stdout.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k] = v
        r = subprocess.run(["systemctl", "--user", "is-enabled", SERVICE],
                           capture_output=True, text=True, timeout=8)
        out["is_enabled"] = r.stdout.strip() or "unknown"
    except Exception as e:  # noqa: BLE001
        out["systemd_error"] = f"{type(e).__name__}: {str(e)[:80]}"
        out["is_enabled"] = out.get("is_enabled", "unknown")
    ts = out.get("ExecMainStartTimestamp", "")
    if ts:
        m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", ts)
        if m:
            try:
                out["start_epoch"] = int(
                    datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
                )
            except ValueError:
                pass
    _SERVICE_START_EPOCH = out.get("start_epoch", 0)
    out["uptime_s"] = (
        int(time.time() - out["start_epoch"])
        if out.get("start_epoch") and state in ("active", "poisoned")
        else None
    )
    return out


def collect_health():
    try:
        code, body = _http_get(NINFER_HOST + "/health", timeout=3)
        return {"ok": code == 200, "code": code, "body": body[:120]}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "code": None, "error": str(e)[:120]}


def collect_model():
    try:
        code, body = _http_get(NINFER_HOST + "/v1/models", timeout=3)
        d = json.loads(body)
        m = d["data"][0] if d.get("data") else {}
        return {"id": m.get("id"), "max_model_len": m.get("max_model_len")}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:120]}


def collect_host():
    mem = {}
    try:
        for line in open("/proc/meminfo"):
            k, v = line.split(":", 1)
            mem[k.strip()] = int(v.strip().split()[0])  # kB
    except Exception:
        pass
    total, avail = mem.get("MemTotal", 0), mem.get("MemAvailable", 0)
    used = total - avail
    # CPU：/proc/stat 首行差分
    cpu_pct = None
    try:
        parts = open("/proc/stat").readline().split()
        vals = [int(x) for x in parts[1:8]]
        idle = vals[3] + vals[4]
        tot = sum(vals)
        global _CPU_LAST
        if _CPU_LAST:
            d_idle, d_tot = idle - _CPU_LAST[0], tot - _CPU_LAST[1]
            if d_tot > 0:
                cpu_pct = round(100.0 * (1 - d_idle / d_tot), 1)
        _CPU_LAST = (idle, tot)
    except Exception:
        pass
    mem_used_pct = _pct(used, total)
    now = time.time()
    with _HOST_LOCK:
        # 每 5s 采样一次（与页面刷新率解耦），720 点 = 1 小时
        if not _HOST_SERIES or now - _HOST_SERIES[-1]["t"] >= 5:
            _HOST_SERIES.append(
                {"t": now, "mem_pct": mem_used_pct, "cpu_pct": cpu_pct}
            )
            if len(_HOST_SERIES) > 720:
                del _HOST_SERIES[: len(_HOST_SERIES) - 720]
        series = list(_HOST_SERIES)
    # 整机内存：以 Windows 物理内存为总盘，WSL2 部分拆出分色（堆积条）
    machine = None
    with _GPU_LOCK:
        winmem = _GPU.get("winmem")
    if winmem:
        mt = winmem.get("total") or winmem.get("visible") or 0
        wf = winmem.get("free") or 0
        if mt:
            machine = {
                "total_b": mt,
                "wsl_used_b": used * 1024,
                "wsl_free_b": avail * 1024,
                "win_used_b": max(0, mt - wf - used * 1024),
                "win_free_b": wf,
            }
    dash_boot = "unknown"
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-enabled", "ninfer-dashboard"],
            capture_output=True, text=True, timeout=5,
        )
        dash_boot = r.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        dash_boot = "unknown"
    return {
        "mem_total": _human(total * 1024),
        "mem_used": _human(used * 1024),
        "mem_avail": _human(avail * 1024),
        "mem_used_pct": mem_used_pct,
        "cpu_pct": cpu_pct,
        "mem_machine": machine,
        "platform": PLATFORM,
        "gpu_backend": GPU_BACKEND,
        "dash_boot": dash_boot,
        "series": series,
    }


# ---- serve 日志解析（尾部扫描）

_RE_CAPACITY = re.compile(
    r"capacity \| KV ([\d,]+) tokens, (\w+), (\w+) \| pages ([\d,]+)/([\d,]+)"
    r" \| runtime ([\d.]+) GiB \| free ([\d.]+) MiB"
)
_RE_CTXCACHE = re.compile(
    r"context cache \| (\d+) active \+ (\d+) cached device states"
    r" \| host (\d+) states, ([\d.]+) GiB KV \| private (\d+) \| shared (\d+)"
    r" \| anchors (\d+)"
)
_RE_HOSTKV = re.compile(r"host KV pinned \| ([\d.]+) GiB \| ([\d.]+)s")
_RE_TP = re.compile(r"throughput \| [\d.]+s \|(.*)$")
_RE_TP_PRE = re.compile(r"prefill ([\d.]+)(k?) tok/s \(([\d,]+) tok\)")
_RE_TP_DEC = re.compile(r"decode ([\d.]+)(k?) tok/s \(([\d,]+) tok\)")
_RE_TP_RUN = re.compile(r"running (\d+)")
_RE_TP_BATCH = re.compile(r"batch ([\d.]+)")
_RE_TP_HOST = re.compile(r"host ([\d.]+)%")
_RE_RESSNAP = re.compile(
    r"RES_SNAP lanes (\d+)/(\d+) dst (\d+)/(\d+) dkv (\d+)/(\d+)"
    r" dbkv (\d+)/(\d+) hst (\d+)/(\d+) hkv (\d+)/(\d+)"
)
_RE_GATE = re.compile(
    r"GATE_SUMMARY idx_occ=(\d+) stale=(\d+) mismatch=(\d+) pnc=(\d+) pir=(\d+)"
    r" snc=(\d+) sir=(\d+) idx_empty=(\d+) emptyslot=(\d+) priv_cand=(\d+)"
    r" shared_cand=(\d+) key_rej=(\d+) prompt_frontier=(\d+)"
    r" shared_best_all=(\d+) shared_best_matched=(\d+)"
)
_RE_STALE = re.compile(r"STALE_EXIT_(\d+) total=(\d+)")
_RE_PEAKMISS = re.compile(r"PEAK_MISS_(\d+) used=(\d+) added=(\d+) cap=(\d+)")
_RE_PEAKFULL = re.compile(
    r"PEAK_FULL lanes u=(\d+) a=(\d+) c=(\d+) dst u=(\d+) a=(\d+) c=(\d+)"
    r" dkv u=(\d+) a=(\d+) c=(\d+) dbkv u=(\d+) a=(\d+) c=(\d+)"
    r" hst u=(\d+) a=(\d+) c=(\d+) hkv u=(\d+) a=(\d+) c=(\d+)"
)
_RE_ENTRYCLR = re.compile(r"ENTRY_CLR kind=(\w+) owner=(\d+) rev=(\d+) force=(\d)")
_GATE_CUM = ("stale", "mismatch", "pnc", "pir", "snc", "sir", "idx_empty",
             "emptyslot", "priv_cand", "shared_cand", "key_rej")
_RE_DUR = r"((?:\d+m )?[\d.]+ ?(?:s|ms))"
_RE_REQ = re.compile(
    r"req#(\d+) done \| (\S+) \|.*?prompt ([\d,]+) \| output ([\d,]+)"
    r"(?: \| cache ([\d,]+) \(([\d.]+)%(?:, ([^)]+))?\))?"
    r" \| TTFT " + _RE_DUR + r" \| total " + _RE_DUR
    + r"(?: \| queue " + _RE_DUR + r")?"
    r"(?: \| prefill ([\d.]+)(k?) tok/s)?"
    r"(?: \| decode ([\d.]+)(k?) tok/s)?"
    r"(?: \| mtp accepted (\d+)/(\d+) \(([\d.]+)%\))?"
)


def _parse_dur(s):
    """'17.3s' / '121 ms' / '1m 12.4s' → 秒"""
    if not s:
        return None
    m = re.fullmatch(r"(?:(\d+)m )?([\d.]+) ?(s|ms)", s)
    if not m:
        return None
    mins = int(m.group(1)) if m.group(1) else 0
    val = float(m.group(2))
    return mins * 60 + (val if m.group(3) == "s" else val / 1000)


def _tail_lines(path, nbytes=1024 * 1024, maxlines=2000):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > nbytes:
                f.seek(-nbytes, os.SEEK_END)
            data = f.read()
        lines = data.decode("utf-8", "replace").splitlines()
        if size > nbytes:
            lines = lines[1:]
        return lines[-maxlines:]
    except FileNotFoundError:
        return []


_RE_LOGTS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def _parse_throughput_line(line):
    m = _RE_TP.search(line)
    if not m:
        return None
    rest = m.group(1)
    d = {}
    mp = _RE_TP_PRE.search(rest)
    if mp:
        d["prefill_tps"] = float(mp.group(1)) * (1000 if mp.group(2) == "k" else 1)
        d["prefill_tok"] = int(mp.group(3).replace(",", ""))
    md = _RE_TP_DEC.search(rest)
    if md:
        d["decode_tps"] = float(md.group(1)) * (1000 if md.group(2) == "k" else 1)
        d["decode_tok"] = int(md.group(3).replace(",", ""))
    mr = _RE_TP_RUN.search(rest)
    if mr:
        d["running"] = int(mr.group(1))
    mb = _RE_TP_BATCH.search(rest)
    if mb:
        d["batch"] = float(mb.group(1))
    mh = _RE_TP_HOST.search(rest)
    if mh:
        d["host_pct"] = float(mh.group(1))
    return d or None


def collect_engine():
    """尾部扫描 serve 日志：capacity / context cache / host KV / throughput / req#N"""
    lines = _tail_lines(LOG_FILE, 512 * 1024, 3000)
    eng = {
        "capacity": None, "ctxcache": None, "host_kv_pinned": None,
        "throughput": None, "last_req": None, "log_lines": len(lines),
        "log_mtime": None,
    }
    try:
        st = os.stat(LOG_FILE)
        eng["log_mtime"] = int(st.st_mtime)
    except OSError:
        pass
    cap = ctx = hkv = tp = req = probe = None
    for line in lines:
        m = _RE_CAPACITY.search(line)
        if m:
            cap = {
                "tokens": int(m.group(1).replace(",", "")),
                "dtype": m.group(2), "mode": m.group(3),
                "pages_used": int(m.group(4).replace(",", "")),
                "pages_total": int(m.group(5).replace(",", "")),
                "runtime_gib": float(m.group(6)),
                "free_mib": float(m.group(7)),
            }
        m = _RE_CTXCACHE.search(line)
        if m:
            ctx = {
                "dev_active": int(m.group(1)), "dev_cached": int(m.group(2)),
                "host_states": int(m.group(3)), "host_kv_gib": float(m.group(4)),
                "private": int(m.group(5)), "shared": int(m.group(6)),
                "anchors": int(m.group(7)),
            }
        m = _RE_HOSTKV.search(line)
        if m:
            hkv = {"gib": float(m.group(1)), "elapsed_s": float(m.group(2))}
        _t = _parse_throughput_line(line)
        if _t:
            tp = _t
            _mts = _RE_LOGTS.match(line)
            if _mts:
                try:
                    tp["line_ts"] = int(
                        datetime.strptime(_mts.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
                    )
                except ValueError:
                    pass
        m = _RE_RESSNAP.search(line)
        if m:
            probe = {
                "lanes": f"{m.group(1)}/{m.group(2)}",
                "dst": f"{m.group(3)}/{m.group(4)}",
                "dkv_used": int(m.group(5)), "dkv_total": int(m.group(6)),
                "hst_used": int(m.group(9)), "hst_total": int(m.group(10)),
                "hkv_used": int(m.group(11)), "hkv_total": int(m.group(12)),
            }
        m = _RE_REQ.search(line)
        if m:
            req = {
                "id": int(m.group(1)), "endpoint": m.group(2),
                "prompt": int(m.group(3).replace(",", "")),
                "output": int(m.group(4).replace(",", "")),
                "cache": int(m.group(5).replace(",", "")) if m.group(5) else None,
                "cache_pct": float(m.group(6)) if m.group(6) else None,
                "cache_tag": m.group(7) if m.group(7) else None,
                "ttft_s": _parse_dur(m.group(8)),
                "total_s": _parse_dur(m.group(9)),
                "queue": _parse_dur(m.group(10)) if m.group(10) else None,
                "prefill_tps": (
                    float(m.group(11)) * (1000 if m.group(12) == "k" else 1)
                    if m.group(11) else None
                ),
                "decode_tps": (
                    float(m.group(13)) * (1000 if m.group(14) == "k" else 1)
                    if m.group(13) else None
                ),
                "mtp_acc": int(m.group(15)) if m.group(15) else None,
                "mtp_dra": int(m.group(16)) if m.group(16) else None,
                "mtp_pct": float(m.group(17)) if m.group(17) else None,
            }
            if ctx is not None:
                with _STATE_LOCK:
                    _REQ_STATE["req_ctx"][str(req["id"])] = dict(ctx)
                    if len(_REQ_STATE["req_ctx"]) > 200:
                        for k in list(_REQ_STATE["req_ctx"])[:-200]:
                            del _REQ_STATE["req_ctx"][k]
    # capacity / host-KV 行只在服务启动时输出一次：日志增长后滚出 3000 行尾窗，
    # 全文件兜底 + 进程内缓存（服务重启后新行出现在尾窗 → 覆盖缓存）
    with _STATE_LOCK:
        st = _REQ_STATE
        if cap is not None:
            st["_capacity_cache"] = cap
        elif st.get("_capacity_cache") is None:
            try:
                full = open(LOG_FILE, encoding="utf-8", errors="replace").read()
            except OSError:
                full = ""
            for fl in full.splitlines():
                m = _RE_CAPACITY.search(fl)
                if m:
                    st["_capacity_cache"] = {
                        "tokens": int(m.group(1).replace(",", "")),
                        "dtype": m.group(2), "mode": m.group(3),
                        "pages_used": int(m.group(4).replace(",", "")),
                        "pages_total": int(m.group(5).replace(",", "")),
                        "runtime_gib": float(m.group(6)),
                        "free_mib": float(m.group(7)),
                    }
                m = _RE_HOSTKV.search(fl)
                if m:
                    st["_hostkv_cache"] = {
                        "gib": float(m.group(1)), "elapsed_s": float(m.group(2))
                    }
                m = _RE_CTXCACHE.search(fl)
                if m:
                    st["_ctxcache_cache"] = {
                        "dev_active": int(m.group(1)), "dev_cached": int(m.group(2)),
                        "host_states": int(m.group(3)), "host_kv_gib": float(m.group(4)),
                        "private": int(m.group(5)), "shared": int(m.group(6)),
                        "anchors": int(m.group(7)),
                    }
        if hkv is not None:
            st["_hostkv_cache"] = hkv
        if ctx is not None:
            st["_ctxcache_cache"] = ctx
        if cap is None and st.get("_capacity_cache") is not None:
            cap = dict(st["_capacity_cache"])
        if hkv is None and st.get("_hostkv_cache") is not None:
            hkv = dict(st["_hostkv_cache"])
        if ctx is None and st.get("_ctxcache_cache") is not None:
            ctx = dict(st["_ctxcache_cache"])

    eng["capacity"], eng["ctxcache"], eng["host_kv_pinned"] = cap, ctx, hkv
    eng["throughput"] = tp  # 最新一行（仪表/实时判定用）；曲线点由 _tp_loop 独立维护
    eng["last_req"] = req
    eng["probe"] = probe

    # 探针画像：启动时全量回填一次，之后增量尾扫只处理"回填点之后的新行"
    #（以回填时刻日志最后一行为标记；避免回填基线比窗口内旧 GATE 行更新 → 假"计数器复位"）
    with _STATE_LOCK:
        st = _REQ_STATE
        if st["_probe_backfilled"]:
            # 日志被截断/重开（引擎/OS 重启）→ 清空重新回填
            try:
                if os.path.getsize(LOG_FILE) < st.get("_probe_backfill_size", 0):
                    st["req_probe"] = {}
                    st["req_ctx"] = {}
                    st["_gate_baseline"] = None
                    st["_probe_backfilled"] = False
            except OSError:
                pass
        if not st["_probe_backfilled"]:
            try:
                full = open(LOG_FILE, encoding="utf-8", errors="replace").read().splitlines()
            except OSError:
                full = []
            p, c, lg = _walk_probe_lines(full)
            st["req_probe"].update(p)
            st["req_ctx"].update(c)
            if lg is not None:
                st["_gate_baseline"] = lg
            st["_probe_tail_marker"] = full[-1] if full else None
            try:
                st["_probe_backfill_size"] = os.path.getsize(LOG_FILE)
            except OSError:
                st["_probe_backfill_size"] = 0
            st["_probe_backfilled"] = True
        marker = st.get("_probe_tail_marker")
        if marker:
            try:
                new_lines = lines[lines.index(marker) + 1:]
            except ValueError:
                new_lines = lines  # 标记已滚出尾窗 → 整窗都新于回填点
        else:
            new_lines = lines
        p2, c2, lg2 = _walk_probe_lines(new_lines, st["_gate_baseline"])
        st["req_probe"].update(p2)
        st["req_ctx"].update(c2)
        if lg2 is not None:
            st["_gate_baseline"] = lg2
        if len(st["req_probe"]) > 300:
            for k in list(st["req_probe"])[:-300]:
                del st["req_probe"][k]
        if len(st["req_ctx"]) > 300:
            for k in list(st["req_ctx"])[:-300]:
                del st["req_ctx"][k]

    # 看门狗
    wd = {}
    try:
        wd["failcount"] = int(open(WATCHDOG_FAILCOUNT).read().strip() or 0)
    except Exception:
        wd["failcount"] = None
    wlines = _tail_lines(WATCHDOG_LOG, 64 * 1024, 5)
    wd["recent"] = wlines[-3:]
    try:
        r = subprocess.run(["systemctl", "--user", "is-active", "ninfer-health.timer"],
                           capture_output=True, text=True, timeout=5)
        wd["timer"] = r.stdout.strip() or "unknown"
    except Exception:
        wd["timer"] = "unknown"
    eng["watchdog"] = wd

    return eng


# ---- jsonl 增量解析

def _reset_req_state():
    st = _REQ_STATE
    st.update(
        offset=0, seen=0, prompt=0, completion=0, cache_hit=0,
        drafted=0, accepted=0, errors=0, recent=[], error_rows=[],
        hourly={}, host_kv_occupied=None, host_kv_capacity=None,
    )


def _parse_gate_line(line):
    m = _RE_GATE.search(line)
    if not m:
        return None
    d = {k: int(m.group(i + 1)) for k, i in zip(_GATE_CUM, range(1, 12))}
    d.update(
        idx_occ=int(m.group(1)),
        prompt_frontier=int(m.group(13)),
        shared_best_all=int(m.group(14)),
        shared_best_matched=int(m.group(15)),
    )
    return d


def _walk_probe_lines(lines, baseline=None):
    """按序走查日志行：GATE 累计差值 + STALE/PEAK/ENTRY 事件 → 归属到 req#。

    返回 (probes, ctxs, last_gate)。基线跨调用传递，避免窗口首行差值虚高。
    """
    gate_hist = [baseline] if baseline else []
    ev_stale, ev_peak, ev_peak_full, ev_clr = {}, {}, None, []
    probes, ctxs = {}, {}
    last_ctx = None
    last_gate = None
    for line in lines:
        m = _RE_CTXCACHE.search(line)
        if m:
            last_ctx = {
                "dev_active": int(m.group(1)), "dev_cached": int(m.group(2)),
                "host_states": int(m.group(3)), "host_kv_gib": float(m.group(4)),
                "private": int(m.group(5)), "shared": int(m.group(6)),
                "anchors": int(m.group(7)),
            }
        g = _parse_gate_line(line)
        if g:
            gate_hist.append(g)
            if len(gate_hist) > 3:
                gate_hist = gate_hist[-3:]
            last_gate = g
        m = _RE_STALE.search(line)
        if m:
            ev_stale[m.group(1)] = ev_stale.get(m.group(1), 0) + 1
        m = _RE_PEAKMISS.search(line)
        if m:
            ev_peak[m.group(1)] = ev_peak.get(m.group(1), 0) + 1
        m = _RE_PEAKFULL.search(line)
        if m:
            ev_peak_full = {
                "lanes": (int(m.group(1)), int(m.group(2)), int(m.group(3))),
                "dst": (int(m.group(4)), int(m.group(5)), int(m.group(6))),
                "dkv": (int(m.group(7)), int(m.group(8)), int(m.group(9))),
                "dbkv": (int(m.group(10)), int(m.group(11)), int(m.group(12))),
                "hst": (int(m.group(13)), int(m.group(14)), int(m.group(15))),
                "hkv": (int(m.group(16)), int(m.group(17)), int(m.group(18))),
            }
        m = _RE_ENTRYCLR.search(line)
        if m:
            ev_clr.append({"kind": m.group(1), "owner": m.group(2), "force": m.group(3)})
            if len(ev_clr) > 8:
                ev_clr = ev_clr[-8:]
        m = _RE_REQ.search(line)
        if m:
            rid = m.group(1)
            prof = None
            if len(gate_hist) >= 2:
                base_g, cur_g = gate_hist[-2], gate_hist[-1]
                reset = any(cur_g[k] < base_g[k] for k in _GATE_CUM)
                diff = {k: max(0, cur_g[k] - base_g[k]) for k in _GATE_CUM}
                prof = {
                    "gate": {
                        **diff,
                        "idx_occ": cur_g["idx_occ"],
                        "prompt_frontier": cur_g["prompt_frontier"],
                        "shared_best_all": cur_g["shared_best_all"],
                        "shared_best_matched": cur_g["shared_best_matched"],
                    },
                    "gate_reset": reset,
                }
                gate_hist = [cur_g]
            if prof is None and (ev_stale or ev_peak or ev_peak_full or ev_clr):
                prof = {"gate": None, "gate_reset": False}
            if prof is not None:
                prof.update(
                    stale=ev_stale or None, peak=ev_peak or None,
                    peak_full=ev_peak_full, entry_clr=ev_clr or None,
                )
                probes[rid] = prof
            ev_stale, ev_peak, ev_peak_full, ev_clr = {}, {}, None, []
            if last_ctx is not None:
                ctxs[rid] = last_ctx
    return probes, ctxs, last_gate


_STALE_DIMS = {
    "1": "上下文事务/pending 未决",
    "2": "host 分配被阻塞",
    "3": "无保护",
    "4": "物理峰值不足",
    "5": "lane/epoch/活跃 continuation 冲突",
    "6": "私有 source 槽非 Catalogued",
    "7": "共享 source 槽非 Catalogued",
    "8": "私有 victim 槽非 Catalogued",
    "9": "私有 victim 决策不匹配",
    "10": "共享 victim 槽非 Catalogued",
}
_PEAK_DIMS = {
    "1": "device active lanes", "2": "device state slots",
    "3": "device 主 KV 页", "4": "device 后端 KV 页",
    "5": "host state 槽", "6": "host KV 字节",
}


def _probe_evidence(prof, ctx):
    parts = []
    if prof:
        g = prof.get("gate")
        if g:
            d = " ".join(
                f"{k}+{v}" for k, v in g.items()
                if isinstance(v, int) and v and k not in (
                    "idx_occ", "prompt_frontier", "shared_best_all", "shared_best_matched"
                )
            )
            parts.append(
                f"gate Δ: {d or '—'} | idx_occ={g.get('idx_occ')} | "
                f"候选 priv+{g.get('priv_cand', 0)}/shared+{g.get('shared_cand', 0)}"
            )
            if g.get("shared_best_all"):
                parts.append(
                    f"shared_best {g.get('shared_best_matched'):,}/{g.get('shared_best_all'):,} tok"
                )
        if prof.get("stale"):
            parts.append(
                "stale_exit " + " ".join(f"{k}×{v}" for k, v in sorted(prof["stale"].items()))
            )
        if prof.get("peak"):
            parts.append(
                "peak_miss " + " ".join(f"dim{k}×{v}" for k, v in sorted(prof["peak"].items()))
            )
        if prof.get("peak_full"):
            pf = prof["peak_full"]
            parts.append("peak_full: " + " ".join(
                f"{k} {a}+{b}/{c}" for k, (a, b, c) in pf.items()
            ))
        if prof.get("entry_clr"):
            parts.append("entry_clr " + " ".join(
                f"{e['kind']}#{e['owner']} force={e['force']}" for e in prof["entry_clr"][:3]
            ))
    if ctx:
        parts.append(
            f"ctx: dev {ctx.get('dev_active', 0)}a+{ctx.get('dev_cached', 0)}c "
            f"host {ctx.get('host_states', 0)}st "
            f"priv {ctx.get('private', 0)}/shared {ctx.get('shared', 0)}/anch {ctx.get('anchors', 0)}"
        )
    return " · ".join(parts) or "—"


def _classify_miss(row, ctx, prev_gap_s, prof=None):
    """缓存未命中深度归因：返回 (原因, 证据)"""
    ev = _probe_evidence(prof, ctx)
    since = None
    if _SERVICE_START_EPOCH and row.get("time"):
        try:
            rt = datetime.strptime(
                f"{datetime.now():%Y-%m-%d} {row['time']}", "%Y-%m-%d %H:%M:%S"
            ).timestamp()
            since = int(rt - _SERVICE_START_EPOCH)
        except ValueError:
            pass
    if since is not None and since < 0:
        # 请求早于当前服务实例（重启前的历史请求）：不能用负数说"启动仅 -Xs"，
        # 其缓存状态已随重启清空，归因仅供参考（2026-09-28 修复）
        return "历史请求：早于当前服务实例（重启前），缓存已随重启清空，归因仅供参考", ev
    if since is not None and since < 120:
        return f"冷启动/重启：服务启动仅 {since}s，缓存尚未建立", ev
    if (
        ctx
        and since is not None
        and since < 1800
        and ctx.get("dev_active", 0) == 0
        and ctx.get("dev_cached", 0) == 0
    ):
        return f"服务重启后空池：启动 {since // 60} 分钟，device 无任何可复用 state（首批请求）", ev
    if prev_gap_s is not None and prev_gap_s > 600:
        return f"切换会话/长闲置：距上一请求 {prev_gap_s // 60} 分钟，前缀过期", ev
    g = prof.get("gate") if prof else None
    if g and prof.get("gate_reset"):
        return "引擎计数器复位：引擎刚重启，本条为重启后首批请求", ev
    if g:
        cands = g.get("priv_cand", 0) + g.get("shared_cand", 0)
        if g.get("idx_occ", 0) == 0:
            return "前缀索引为空（idx_occ=0）：池内无可复用条目（尚未写入或已被清空）", ev
        if cands == 0:
            if g.get("mismatch", 0) > 0:
                return (
                    f"未进候选集：摘要失配（mismatch +{g['mismatch']}）——"
                    f"同前沿内容实际变了（工具/提示词/注入块）"
                ), ev
            if g.get("stale", 0) > 0:
                return f"未进候选集：陈旧索引条目（stale +{g['stale']}，校验失败）", ev
            if g.get("idx_empty", 0) > 0:
                return f"前缀索引为空（idx_empty +{g['idx_empty']}）：索引被 rebuild 清空未重建", ev
            return (
                f"无候选：idx_occ={g.get('idx_occ')} 但 8 道闸全部过滤"
                f"（pnc+{g.get('pnc', 0)} pir+{g.get('pir', 0)} "
                f"snc+{g.get('snc', 0)} sir+{g.get('sir', 0)}）"
            ), ev
        stale = prof.get("stale") or {}
        peak = prof.get("peak") or {}
        if stale.get("4", 0) > 0 or peak or prof.get("peak_full"):
            dims = sorted(set(list(peak.keys()) + (["4"] if stale.get("4", 0) else [])))
            desc = ", ".join(f"dim{d}={_PEAK_DIMS.get(d, '?')}" for d in dims)
            pf = prof.get("peak_full")
            if pf:
                desc += " | " + " ".join(
                    f"{k} {a}+{b}/{c}" for k, (a, b, c) in pf.items()
                )
            return f"有候选但装不下：物理峰值失败（{desc}）", ev
        other = {k: v for k, v in stale.items() if k != "4"}
        if other:
            desc = ", ".join(
                f"{k}={_STALE_DIMS.get(k, '?')}×{v}" for k, v in sorted(other.items())
            )
            return f"有候选但复验被拒：{desc}", ev
        if g.get("key_rej", 0) > 0:
            return f"共享键被拒（key_rej +{g['key_rej']}）：条目前沿键与我方不一致（内容已变）", ev
        if g.get("snc", 0) or g.get("sir", 0):
            return (
                f"共享槽不可用：snc+{g.get('snc', 0)}（非 Catalogued）/"
                f"sir+{g.get('sir', 0)}（inspect 被拒）"
            ), ev
        if g.get("pnc", 0) or g.get("pir", 0):
            return (
                f"私有槽不可用：pnc+{g.get('pnc', 0)}（非 Catalogued）/"
                f"pir+{g.get('pir', 0)}（inspect 被拒）"
            ), ev
        if g.get("shared_best_all", 0) > g.get("shared_best_matched", 0):
            return (
                f"最佳共享前缀分叉：索引最佳前沿 {g['shared_best_all']:,} tok，"
                f"实际只匹配到 {g['shared_best_matched']:,} tok"
            ), ev
        return "有候选但未复用：探针无闸口失败信号（需 prefix-dump 深潜定位）", ev
    if prof and prof.get("entry_clr"):
        e = prof["entry_clr"][0]
        return f"前缀条目被清除：kind={e['kind']} owner={e['owner']} force={e['force']}", ev
    if prof and (prof.get("peak") or prof.get("peak_full")):
        peak = prof.get("peak") or {}
        desc = ", ".join(f"dim{k}={_PEAK_DIMS.get(k, '?')}×{v}" for k, v in sorted(peak.items()))
        return f"物理峰值失败：{desc or '见 peak_full'}", ev
    if ctx and (ctx.get("anchors", 0) == 0 or ctx.get("private", 0) == 0) and ctx.get("shared", 0) > 0:
        return "小 owner 丢失：私有锚点/端点被驱逐（shared 前缀还在，私有续接未复用）", ev
    if ctx and ctx.get("host_states", 0) == 0:
        return "host KV 未复用：host 上无可复用 state（全部过期/未卸载）", ev
    return "前缀不匹配：无探针数据（GATE_SUMMARY 未捕获，或该请求未走候选收集）", ev


def _miss_rows(st):
    rows = [r for r in st["recent"] if r.get("path") == "root" or r.get("cache") in (0, None)][:2]
    times = [r.get("time") for r in st["recent"]]
    out = []
    for row in rows:
        idx = next((j for j, r in enumerate(st["recent"]) if r is row), None)
        gap = None
        if idx is not None and idx + 1 < len(times):
            try:
                t1 = datetime.strptime(times[idx], "%H:%M:%S")
                t2 = datetime.strptime(times[idx + 1], "%H:%M:%S")
                gap = int((t1 - t2).total_seconds())
                if gap < -18000:
                    gap += 86400
            except (ValueError, TypeError):
                gap = None
        ctx = st["req_ctx"].get(str(row.get("id")))
        prof = st["req_probe"].get(str(row.get("id")))
        reason, ev = _classify_miss(row, ctx, gap, prof)
        base = {k: row.get(k) for k in ("id", "time", "prompt", "cache", "cache_pct", "path", "finish")}
        base["reason"] = reason
        base["evidence"] = ev
        out.append(base)
    return out


_PREFIX_DUMP = CONFIG["prefix_dump"]
_PREFIX_DIVE_CACHE = {"size": None, "mtime": None, "result": None, "ts": 0.0}
_NPFX_MAGIC = 0x4E5046584C4C  # "NPFXLL"


def _scan_prefix_frames(data):
    """第一遍：只解帧头（C 级 find 跳转，71MB 文件 <0.5s）。"""
    import struct

    magic = struct.pack("<Q", _NPFX_MAGIC)
    n = len(data)
    frames = []
    off = 0
    while True:
        off = data.find(magic, off)
        if off < 0 or off + 72 > n:
            break
        tag = data[off + 8:off + 16].rstrip(b"\x00").decode()
        seq, frontier, flags, ntok, nrew, nbuck, nvis = struct.unpack_from(
            "<7I", data, off + 16
        )
        vh, d0, d1 = struct.unpack_from("<3Q", data, off + 44)
        itag = struct.unpack_from("<I", data, off + 68)[0]
        frames.append(
            {
                "off": off, "tag": tag, "seq": seq, "frontier": frontier,
                "flags": flags, "ntok": ntok, "nrew": nrew,
                "d0": d0, "d1": d1, "itag": itag,
            }
        )
        off += 72 + ntok * 4 + ntok + nrew * 4 + nbuck * 3 * 8
    return frames


def _load_frame_tokens(data, frame):
    """第二遍：只解需要比对的帧的 token 载荷。"""
    import struct

    if not frame.get("ntok"):
        return None
    return struct.unpack_from(
        f"<{frame['ntok']}i", data, frame["off"] + 72
    )


def _prefix_dive():
    """最近一次键失配（REJ 帧）的前缀深潜：定位首分叉 token。

    REJ 帧 = 条目前沿 + 条目摘要；REQ 帧 = 我方完整 token 序列。
    条目来源 ≈ 最近一个 frontier≥条目前沿 的上一 REQ 帧（启发式，结论里标注）。
    """
    import struct

    try:
        st = os.stat(_PREFIX_DUMP)
    except OSError:
        return None
    cache = _PREFIX_DIVE_CACHE
    if cache["size"] == st.st_size and cache["mtime"] == int(st.st_mtime):
        return cache["result"]
    # 活跃期 dump 文件随请求增长 → 每次 status 都会触发 70MB+ 全文件重读；
    # 5s 内结果视为新鲜（深潜只展示"最近一次键失配"，5s 延迟无感知），限流防 1s 轮询反复读盘
    if cache["result"] is not None and time.time() - cache["ts"] < 5:
        return cache["result"]
    try:
        data = open(_PREFIX_DUMP, "rb").read()
    except OSError:
        return None
    frames = _scan_prefix_frames(data)
    rejs = [f for f in frames if f["tag"] == "REJ"]
    if not rejs:
        res = {"note": "全文件无 REJ 键失配帧（长前缀失配才会落 REJ）"}
        cache.update(size=st.st_size, mtime=int(st.st_mtime), result=res, ts=time.time())
        return res
    rej = rejs[-1]
    cur = next((f for f in frames if f["tag"] == "REQ" and f["seq"] == rej["seq"]), None)
    src = None
    for f in frames:
        if f["tag"] == "REQ" and f["seq"] < rej["seq"] and f["frontier"] >= rej["frontier"]:
            src = f
    out = {
        "rej_seq": rej["seq"],
        "frontier": rej["frontier"],
        "entry_digest": f"{rej['d0']:016x} / {rej['d1']:016x}",
        "identity_tag": rej["itag"],
        "our_tokens": cur["ntok"] if cur else None,
    }
    cur_tok = _load_frame_tokens(data, cur) if cur else None
    src_tok = _load_frame_tokens(data, src) if src else None
    if cur and src and cur_tok and src_tok:
        F = min(rej["frontier"], len(cur_tok), len(src_tok))
        out["src_seq"] = src["seq"]
        out["src_frontier"] = src["frontier"]
        first = None
        if F > 0:
            pa = struct.pack(f"<{F}i", *cur_tok[:F])
            pb = struct.pack(f"<{F}i", *src_tok[:F])
            if pa != pb:
                step = 4096
                for i in range(0, F, step):
                    if pa[i * 4:(i + step) * 4] != pb[i * 4:(i + step) * 4]:
                        for j in range(i, min(i + step, F)):
                            if cur_tok[j] != src_tok[j]:
                                first = j
                                break
                        break
        if first is not None:
            rew = struct.unpack_from(
                f"<{cur['nrew']}I", data, cur["off"] + 72 + cur["ntok"] * 5
            ) if cur["nrew"] else ()
            turn = 1 + sum(1 for rw in rew if rw <= first)
            out["first_diverge"] = first
            out["turn"] = turn
            out["verdict"] = (
                f"首分叉点：token #{first:,}（共 {F:,}）；条目来源≈REQ seq#{src['seq']}"
                f"（frontier {src['frontier']:,}，启发式匹配）；分叉点位于第 {turn} 轮段"
            )
        else:
            out["verdict"] = (
                f"前 {F:,} token 完全一致：分叉在条目前沿之后，或摘要差异来自 token_types/位置轴"
            )
    elif cur and not src:
        out["verdict"] = "条目来源快照缺失：条目可能由 <64k 请求发布（REQ 只落长前缀）或上次启动发布"
    elif not cur:
        out["verdict"] = "请求侧快照缺失（prompt <64k，REQ 未落盘）"
    else:
        out["verdict"] = "快照 token 载荷缺失"
    cache.update(size=st.st_size, mtime=int(st.st_mtime), result=out, ts=time.time())
    return out


def collect_requests():
    st = _REQ_STATE
    with _STATE_LOCK:
        try:
            size = os.path.getsize(REQ_JSONL)
        except OSError:
            return {"available": False}
        if size < st["offset"]:
            _reset_req_state()  # 文件被截断/重开（引擎重启或日志轮转）
        if size == st["offset"]:
            summary = _req_summary()
        else:
            try:
                with open(REQ_JSONL, "rb") as f:
                    f.seek(st["offset"])
                    chunk = f.read()
            except OSError as e:
                return {"available": False, "error": str(e)}
            st["offset"] = size
            for line in chunk.decode("utf-8", "replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                try:
                    _consume_record(rec)
                except Exception:  # noqa: BLE001
                    pass  # 单条坏记录不拖垮整段增量
            summary = _req_summary()
    # prefix_dive 要全文件扫 70MB+ bin，放锁外执行，避免阻塞其他采集器（1s 轮询下会拖慢整页）
    summary["prefix_dive"] = _prefix_dive()
    return summary


def _consume_record(rec):
    ev = rec.get("event")
    ts_ms = rec.get("timestamp_unix_ms", 0)
    ts = ts_ms / 1000 if ts_ms else time.time()
    st = _REQ_STATE
    if ev == "server_start":
        mem = rec.get("memory")
        if isinstance(mem, dict):
            st["startup_memory"] = {
                k: mem.get(k) for k in (
                    "weights", "kv_payload_bytes", "runtime_reservation_bytes",
                    "host_kv_capacity_bytes", "host_kv_occupied_bytes",
                    "host_state_capacity_slots", "host_state_occupied_slots",
                    "vision_workspace", "cuda_graph_allowance_bytes",
                    "minimum_runtime_reservation_bytes",
                ) if mem.get(k) is not None
            }
        return
    if ev == "request_done":
        res = rec.get("result", {})
        tim = rec.get("timings_seconds", {})
        spec = rec.get("speculative", {})
        mem = rec.get("memory", {})
        req = rec.get("request", {})
        prompt = res.get("prompt_tokens", 0)
        comp = res.get("completion_tokens", 0)
        hit = res.get("prefix_cache_hit_tokens", 0)
        st["seen"] += 1
        st["prompt"] += prompt
        st["completion"] += comp
        st["cache_hit"] += hit
        st["drafted"] += spec.get("drafted_tokens", 0)
        st["accepted"] += spec.get("accepted_tokens", 0)
        if mem:
            st["host_kv_occupied"] = mem.get("host_kv_occupied_bytes", st["host_kv_occupied"])
            st["host_kv_capacity"] = mem.get("host_kv_capacity_bytes", st["host_kv_capacity"])
        hour = datetime.fromtimestamp(ts).strftime("%m-%d %H")
        h = st["hourly"].setdefault(hour, {"prompt": 0, "completion": 0, "cache_hit": 0})
        h["prompt"] += prompt
        h["completion"] += comp
        h["cache_hit"] += hit
        if len(st["hourly"]) > 96:
            for k in list(st["hourly"])[:-96]:
                del st["hourly"][k]
        st["recent"].insert(0, {
            "id": req.get("request_id"),
            "time": datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
            "prompt": prompt, "completion": comp, "cache": hit,
            "cache_pct": _pct(hit, prompt),
            "path": res.get("prefix_reuse_path"),
            "ttft_s": round(tim.get("ttft", 0), 2),
            "total_s": round(tim.get("total", 0), 2),
            "queue_s": round(rec.get("engine_timing", {}).get("queue_wait_seconds", 0), 3),
            "mtp": (
                f"{spec.get('accepted_tokens', 0)}/{spec.get('drafted_tokens', 0)}"
                if spec.get("drafted_tokens") else None
            ),
            "finish": res.get("finish_reason"),
        })
        if len(st["recent"]) > 1000:
            st["recent"] = st["recent"][:1000]
    elif ev in ("request_rejected", "request_error"):
        st["errors"] += 1
        res = rec.get("result", {})
        st["error_rows"].insert(0, {
            "id": rec.get("request", {}).get("request_id"),
            "event": ev,
            "time": datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
            "status": res.get("status"),
            "reason": res.get("reason") or res.get("error") or "",
            "detail": json.dumps(res, ensure_ascii=False)[:200],
        })
        if len(st["error_rows"]) > 20:
            st["error_rows"] = st["error_rows"][:20]


def _req_summary():
    st = _REQ_STATE
    hit_rate = _pct(st["cache_hit"], st["prompt"])
    return {
        "available": True,
        "requests": st["seen"],
        "prompt_tokens": st["prompt"],
        "completion_tokens": st["completion"],
        "cache_hit_tokens": st["cache_hit"],
        "cache_hit_rate": hit_rate,
        "mtp": {
            "drafted": st["drafted"], "accepted": st["accepted"],
            "accept_rate": _pct(st["accepted"], st["drafted"]),
        },
        "errors": st["errors"],
        "error_rows": st["error_rows"],
        "recent": st["recent"],
        "miss_rows": _miss_rows(st),
        "hourly": [
            {"hour": k, **v} for k, v in sorted(st["hourly"].items())
        ],
        "host_kv_occupied": st["host_kv_occupied"],
        "host_kv_capacity": st["host_kv_capacity"],
        "startup_memory": st["startup_memory"],
        "series_throughput": st["series_throughput"][-720:],
        "note": "计数为“本次引擎启动以来”（日志截断/引擎重启后归零）",
    }


# ---- 单会话命中率（OpenClaw agent sqlite，只读）

def collect_sessions():
    """单会话命中率（OpenClaw agent sqlite，只读）。

    sessions_source=off 或文件缺失 → rows=[]（UI 显示“未配置”/错误）；
    非 OpenClaw 用户可设 off，或将 openclaw_sqlite 指向同结构的 sqlite。
    """
    if CONFIG.get("sessions_source", "openclaw") != "openclaw":
        return {"rows": [], "configured": False}
    if not os.path.exists(AGENT_SQLITE):
        return {"rows": [], "configured": True, "error": f"sqlite 缺失: {AGENT_SQLITE}"}
    rows = []
    try:
        c = sqlite3.connect(
            f"file:{AGENT_SQLITE}?mode=ro", uri=True, timeout=3
        )
        cur = c.execute(
            "select session_key, entry_json, updated_at from session_nodes "
            "where entry_valid=1"
        )
        for key, entry, upd in cur.fetchall():
            try:
                d = json.loads(entry)
            except (json.JSONDecodeError, TypeError):
                continue
            prov = d.get("modelProvider")
            if prov not in NINFER_PROVIDERS:
                continue
            inp = d.get("inputTokens") or 0
            outp = d.get("outputTokens") or 0
            cache = d.get("cacheRead") or 0
            if inp + outp + cache == 0:
                continue
            label = d.get("label") or d.get("displayName") or key.split(":")[-1][:12]
            rows.append({
                "label": str(label)[:40],
                "key": key,
                "provider": prov,
                "model": d.get("model"),
                "input": inp, "output": outp, "cache_read": cache,
                "cache_write": d.get("cacheWrite") or 0,
                # OpenClaw 口径：inputTokens=非缓存输入，cacheRead=缓存输入（源码 usage 累加确认）
                "hit_rate": _pct(cache, inp + cache) if (inp + cache) else None,
                "updated": (
                    datetime.fromtimestamp(upd / 1000).strftime("%m-%d %H:%M")
                    if upd else None
                ),
            })
        c.close()
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:160], "rows": [], "configured": True}
    rows.sort(key=lambda r: r.get("updated") or "", reverse=True)
    return {"rows": rows[:15], "configured": True}


# ---------------------------------------------------------------- 汇总

_COLLECTORS = concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="dash")


def build_status():
    """并行采集：单个采集器慢/挂不拖垮整个 /api/status"""
    futs = {
        "service": _COLLECTORS.submit(collect_service),
        "model": _COLLECTORS.submit(collect_model),
        "gpu": _COLLECTORS.submit(
            lambda: {
                "data": _GPU["data"], "ts": _GPU["ts"], "error": _GPU["error"],
                "series": list(_GPU["series"]),
                "energy_kwh": round(_ENERGY["energy_j"] / 3600000, 3),
            }
        ),
        "host": _COLLECTORS.submit(collect_host),
        "engine": _COLLECTORS.submit(collect_engine),
        "requests": _COLLECTORS.submit(collect_requests),
        "sessions": _COLLECTORS.submit(collect_sessions),
    }
    out = {"ts": int(time.time()), "clock": _now_str()}
    for name, f in futs.items():
        try:
            out[name] = f.result(timeout=12)
        except Exception as e:  # noqa: BLE001
            out[name] = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
    svc = out.get("service")
    up = svc.get("uptime_s") if isinstance(svc, dict) else None
    out["uptime_str"] = _fmt_uptime(up)
    return out


def serve_logs(tail=200, q=None, hide_probe=True):
    lines = _tail_lines(LOG_FILE, 1024 * 1024, 4000)
    if hide_probe:
        lines = [l for l in lines if not l.startswith("[CACHE-PROBE]")]
    if q:
        ql = q.lower()
        lines = [l for l in lines if ql in l.lower()]
    lines = lines[-tail:]
    joined = "\n".join(lines)
    return {"lines": lines, "hash": md5(joined.encode()).hexdigest()[:12]}


def do_action(action):
    orig = action
    allowed = {"start", "stop", "restart", "enable", "disable",
               "wd_enable", "wd_disable",
               "dash_boot_on", "dash_boot_off"}
    if action not in allowed:
        return {"ok": False, "error": f"unknown action: {action}"}
    unit = SERVICE
    if action == "wd_enable":
        action, unit = "enable", "ninfer-health.timer"
    elif action == "wd_disable":
        action, unit = "disable", "ninfer-health.timer"
    if action in ("dash_boot_on", "dash_boot_off"):
        action, unit = ("enable" if action == "dash_boot_on" else "disable"), "ninfer-dashboard"
    with _ACTION_LOCK:
        try:
            r = subprocess.run(
                ["systemctl", "--user", action, unit],
                capture_output=True, text=True, timeout=180,
            )
            ok = r.returncode == 0
            # keep-stopped 标记（2026-09-28）：手动停止→网关不自动拉起；手动启动/重启→清除
            if ok and unit == SERVICE:
                try:
                    if orig == "stop":
                        with open(KEEP_STOPPED_FILE, "w") as f:
                            f.write(f"stopped at {_now_str()}\n")
                    elif orig in ("start", "restart"):
                        if os.path.exists(KEEP_STOPPED_FILE):
                            os.remove(KEEP_STOPPED_FILE)
                except OSError:
                    pass
            return {
                "ok": ok,
                "rc": r.returncode,
                "stdout": r.stdout.strip()[:400],
                "stderr": r.stderr.strip()[:400],
            }
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": f"{action} 超时(180s)"}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": str(e)[:200]}


def do_probe():
    """向引擎发一条最小 chat 请求，验证模型链路可用"""
    try:
        _, mb = _http_get(NINFER_HOST + "/v1/models", timeout=3)
        mid = json.loads(mb)["data"][0]["id"]
    except Exception:
        mid = CONFIG.get("probe_model_fallback") or ""
    if not mid:
        return {"ok": False, "error": "无法获取模型 ID（/v1/models 不可达且未配 probe_model_fallback）"}
    payload = {
        "model": mid,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 16,
        "temperature": 0,
        "stream": False,
    }
    ctk = CONFIG.get("probe_chat_template_kwargs") or {}
    if ctk:
        payload["chat_template_kwargs"] = ctk
    payload = json.dumps(payload).encode()
    t0 = time.time()
    try:
        req = urllib.request.Request(
            NINFER_HOST + "/v1/chat/completions", data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=90) as r:
            body = json.loads(r.read().decode("utf-8", "replace"))
        total = time.time() - t0
        u = body.get("usage", {})
        content = (body.get("choices") or [{}])[0].get("message", {}).get("content", "")
        return {
            "ok": True, "content": (content or "")[:80],
            "total_s": round(total, 2),
            "prompt_tokens": u.get("prompt_tokens"),
            "completion_tokens": u.get("completion_tokens"),
        }
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:200], "total_s": round(time.time() - t0, 2)}


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = "NInferDash/1.0"

    def log_message(self, fmt, *args):  # 静默
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not getattr(self, "_head_only", False):
            self.wfile.write(data)

    def do_HEAD(self):
        self._head_only = True
        self.do_GET()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            self._send(200, PAGE_HTML, "text/html; charset=utf-8")
        elif path == "/api/status":
            try:
                self._send(200, json.dumps(build_status(), ensure_ascii=False))
            except Exception as e:  # noqa: BLE001
                self._send(500, json.dumps({"error": str(e)}))
        elif path == "/api/logs":
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(self.path).query)
            try:
                tail = min(int((qs.get("tail") or ["200"])[0]), 1000)
            except (ValueError, TypeError):
                tail = 200
            tail = max(1, tail)
            q = (qs.get("q") or [None])[0]
            hide = (qs.get("hideprobe") or ["1"])[0] != "0"
            self._send(200, json.dumps(serve_logs(tail, q, hide), ensure_ascii=False))
        elif path == "/api/action":
            # legacy 入口：GET 视为探针（正式入口是 POST /api/probe）
            self._send(200, json.dumps(do_probe(), ensure_ascii=False))
        elif path == "/api/ping":
            self._send(200, json.dumps({"ok": True, "ts": int(time.time())}))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/probe":
            try:
                self._send(200, json.dumps(do_probe(), ensure_ascii=False))
            except Exception as e:  # noqa: BLE001
                self._send(500, json.dumps({"ok": False, "error": str(e)[:200]}))
            return
        if path != "/api/action":
            self._send(404, json.dumps({"error": "not found"}))
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}")
            action = str(body.get("action", ""))
            self._send(200, json.dumps(do_action(action), ensure_ascii=False))
        except Exception as e:  # noqa: BLE001
            self._send(400, json.dumps({"ok": False, "error": str(e)[:200]}))


# ---------------------------------------------------------------- 页面

PAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html")
try:
    PAGE_HTML = open(PAGE_FILE, encoding="utf-8").read()
except OSError:
    PAGE_HTML = "<h1>ninfer-dashboard</h1><p>page.html 缺失，请检查 scripts/ninfer-dashboard/ 目录</p>"

def main():
    threading.Thread(target=_gpu_loop, daemon=True).start()
    threading.Thread(target=_tp_loop, daemon=True).start()
    srv = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"[ninfer-dashboard] listening on http://{BIND}:{PORT} "
          f"(platform={PLATFORM}, gpu={GPU_BACKEND}, engine={NINFER_HOST})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
