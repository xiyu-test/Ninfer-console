#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NInfer Console 解析器/配置 golden-file 单测。

运行:  python3 tests/test_parsers.py   （零依赖，stdlib only）
覆盖:  serve 日志各固定格式行、jsonl 事件、探针归因、配置加载（tomllib + fallback 解析器）。
"""
import importlib.util
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
GOLDEN = os.path.join(HERE, "golden")

spec = importlib.util.spec_from_file_location("ninfer_dash", os.path.join(ROOT, "server.py"))
srv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(srv)

FAILED = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {detail}")
        FAILED.append(name)


def golden_lines(fname):
    with open(os.path.join(GOLDEN, fname), encoding="utf-8") as f:
        return [l.rstrip("\n") for l in f if l.strip()]


def main():
    lines = golden_lines("serve.log.sample")
    by_prefix = {}
    for l in lines:
        for key in ("capacity |", "context cache |", "host KV pinned |", "throughput |",
                    "GATE_SUMMARY", "STALE_EXIT", "PEAK_MISS", "RES_SNAP", "ENTRY_CLR",
                    "req#42", "req#43"):
            if key in l:
                by_prefix[key] = l

    print("[1] serve 日志行解析")
    m = srv._RE_CAPACITY.search(by_prefix["capacity |"])
    check("capacity", m is not None and m.group(1) == "262,144" and m.group(4) == "12,345"
          and m.group(5) == "32,768" and abs(m.group(6) and float(m.group(6)) - 18.32) < 1e-9
          and float(m.group(7)) == 1234.0)
    m = srv._RE_CTXCACHE.search(by_prefix["context cache |"])
    check("context cache", m is not None and m.group(1) == "3" and m.group(2) == "12"
          and m.group(3) == "45" and float(m.group(4)) == 6.20
          and m.group(5) == "8" and m.group(6) == "37" and m.group(7) == "12")
    m = srv._RE_HOSTKV.search(by_prefix["host KV pinned |"])
    check("host KV pinned", m is not None and float(m.group(1)) == 24.0 and float(m.group(2)) == 3.2)
    tp = srv._parse_throughput_line(by_prefix["throughput |"])
    check("throughput", tp is not None and tp.get("prefill_tps") == 12300.0
          and tp.get("decode_tps") == 87.5 and tp.get("running") == 2
          and abs(tp.get("batch") - 1.4) < 1e-9 and tp.get("host_pct") == 26.0
          and tp.get("prefill_tok") == 24690 and tp.get("decode_tok") == 437)

    print("[2] req#done 行解析")
    m = srv._RE_REQ.search(by_prefix["req#42"])
    check("req#42 完整行", m is not None and m.group(1) == "42"
          and m.group(3) == "45,230" and m.group(4) == "512" and m.group(5) == "43,500"
          and float(m.group(6)) == 96.2
          and abs(srv._parse_dur(m.group(8)) - 1.2) < 1e-9
          and abs(srv._parse_dur(m.group(9)) - 7.4) < 1e-9
          and abs(srv._parse_dur(m.group(10)) - 0.12) < 1e-9
          and abs(float(m.group(11)) * 1000 - 37700) < 1e-6
          and m.group(15) == "400" and m.group(16) == "480" and float(m.group(17)) == 83.3)
    m = srv._RE_REQ.search(by_prefix["req#43"])
    check("req#43 最小行（无 cache/queue/mtp）", m is not None and m.group(3) == "1,024"
          and m.group(5) is None and m.group(10) is None
          and abs(srv._parse_dur(m.group(8)) - 0.085) < 1e-9)

    print("[3] 探针行 + 归因")
    g = srv._parse_gate_line(by_prefix["GATE_SUMMARY"])
    check("GATE_SUMMARY", g is not None and g["stale"] == 3 and g["mismatch"] == 1
          and g["shared_cand"] == 2 and g["idx_occ"] == 120
          and g["prompt_frontier"] == 45230 and g["shared_best_all"] == 44000
          and g["shared_best_matched"] == 43500)
    m = srv._RE_STALE.search(by_prefix["STALE_EXIT"])
    check("STALE_EXIT", m is not None and m.group(1) == "4" and m.group(2) == "120")
    m = srv._RE_PEAKMISS.search(by_prefix["PEAK_MISS"])
    check("PEAK_MISS", m is not None and m.group(1) == "5" and m.group(2) == "40"
          and m.group(3) == "2" and m.group(4) == "38")
    m = srv._RE_RESSNAP.search(by_prefix["RES_SNAP"])
    check("RES_SNAP", m is not None and m.group(1) == "2" and m.group(2) == "4"
          and m.group(5) == "10" and m.group(6) == "12" and m.group(11) == "3" and m.group(12) == "8")
    m = srv._RE_ENTRYCLR.search(by_prefix["ENTRY_CLR"])
    check("ENTRY_CLR", m is not None and m.group(1) == "private" and m.group(2) == "7"
          and m.group(4) == "1")
    probes, ctxs, last = srv._walk_probe_lines(lines)
    check("_walk_probe_lines 归属", "42" in probes and probes["42"]["gate"] is not None
          and probes["42"]["gate"]["stale"] == 3 and probes["42"]["peak"] == {"5": 1}
          and probes["42"]["entry_clr"] is not None and "42" in ctxs
          and ctxs["42"]["dev_active"] == 3)
    reason, ev = srv._classify_miss({"id": "42", "time": "10:00:10", "prompt": 45230,
                                     "cache": 0, "cache_pct": 0.0, "path": "root",
                                     "finish": "stop"}, ctxs.get("42"), 300, probes.get("42"))
    check("_classify_miss 有候选但装不下", "装不下" in reason, reason)
    reason2, _ = srv._classify_miss({"id": "43", "time": "10:00:15", "prompt": 1024,
                                     "cache": 0, "cache_pct": 0.0, "path": "root",
                                     "finish": "stop"}, None, 720, None)
    check("_classify_miss 长闲置", "长闲置" in reason2, reason2)

    print("[4] 小工具")
    check("_parse_dur", abs(srv._parse_dur("1m 12.4s") - 72.4) < 1e-9
          and abs(srv._parse_dur("121 ms") - 0.121) < 1e-9
          and abs(srv._parse_dur("17.3s") - 17.3) < 1e-9 and srv._parse_dur("") is None)
    check("_human", srv._human(1536) == "1.5 KiB" and srv._human(5 * 1024 ** 3) == "5.0 GiB")
    check("_pct", srv._pct(1, 4) == 25.0 and srv._pct(1, 0) is None)

    print("[5] jsonl 事件消费")
    srv._reset_req_state()
    st = srv._REQ_STATE
    with open(os.path.join(GOLDEN, "req.jsonl.sample"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                srv._consume_record(json.loads(line))
    check("server_start 内存账本", st["startup_memory"] is not None
          and st["startup_memory"]["host_kv_capacity_bytes"] == 25769803776
          and st["startup_memory"]["host_state_capacity_slots"] == 112)
    check("request_done 计数", st["seen"] == 2 and st["prompt"] == 46254
          and st["completion"] == 576 and st["cache_hit"] == 43500
          and st["drafted"] == 480 and st["accepted"] == 400)
    check("host_kv_occupied", st["host_kv_occupied"] == 6626097024)
    check("request_rejected", st["errors"] == 1 and st["error_rows"][0]["reason"] == "prompt too long")
    check("recent 行", st["recent"][0]["id"] == "req-0043"
          and st["recent"][1]["cache_pct"] == 96.2
          and st["recent"][1]["mtp"] == "400/480")
    summary = srv._req_summary()
    check("_req_summary 命中率", abs(summary["cache_hit_rate"] - 94.1) < 0.1
          and abs(summary["mtp"]["accept_rate"] - 83.3) < 0.1)

    print("[6] 配置加载")
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "config.toml")
        with open(p, "w", encoding="utf-8") as f:
            f.write('# comment\nengine_url = "http://10.0.0.5:9999"\ndashboard_port = 7777\n'
                    'gpu_mode = "nvidia-smi"\nsession_providers = ["a", "b"]\n'
                    'probe_chat_template_kwargs = { enable_thinking = false }\n')
        cfg = srv._load_config_file(p)
        check("tomllib/fallback 解析", cfg.get("engine_url") == "http://10.0.0.5:9999"
              and cfg.get("dashboard_port") == 7777 and cfg.get("gpu_mode") == "nvidia-smi"
              and cfg.get("session_providers") == ["a", "b"]
              and cfg.get("probe_chat_template_kwargs") == {"enable_thinking": False})
    check("缺失文件 → 空配置", srv._load_config_file("/nonexistent/config.toml") == {})
    check("GPU 后端选择(显式)", srv._select_gpu_backend("nvidia-smi") == "nvidia-smi"
          and srv._select_gpu_backend("windows-powershell") == "windows-powershell")
    check("GPU 后端选择(auto 结果合法)", srv.GPU_BACKEND in ("nvidia-smi", "windows-powershell"))

    print()
    if FAILED:
        print(f"结果: {len(FAILED)} 项失败 → {FAILED}")
        return 1
    print("结果: 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
