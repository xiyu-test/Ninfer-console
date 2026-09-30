#!/usr/bin/env bash
# NInfer Console 重启脚本
# 优先走 systemd user 单元（已安装 ninfer-dashboard.service 时）；否则按端口找 PID 杀 + nohup 兜底
# 可用环境变量覆盖：PY（python 解释器，默认 python3）/ PORT（默认 8090）
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8090}"
PY="${PY:-$(command -v python3 || echo python3)}"

if systemctl --user cat ninfer-dashboard >/dev/null 2>&1; then
  systemctl --user restart ninfer-dashboard
  sleep 1
  newpid="$(ss -tlnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
  if [ -n "${newpid}" ]; then
    echo "restarted via systemd (ninfer-dashboard) pid ${newpid} → http://127.0.0.1:${PORT}"
  else
    echo "FAILED: systemd restarted but port ${PORT} not listening; journalctl --user -u ninfer-dashboard -n 20"
    exit 1
  fi
  exit 0
fi

pid="$(ss -tlnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
if [ -n "${pid}" ]; then
  kill "${pid}" 2>/dev/null || true
  sleep 1
  kill -9 "${pid}" 2>/dev/null || true
  echo "killed old pid ${pid}"
fi
setsid nohup "${PY}" "${DIR}/server.py" > /tmp/ninfer-dashboard.log 2>&1 < /dev/null &
sleep 1
newpid="$(ss -tlnp 2>/dev/null | grep ":${PORT} " | grep -oP 'pid=\K[0-9]+' | head -1 || true)"
if [ -n "${newpid}" ]; then
  echo "started pid ${newpid} → http://127.0.0.1:${PORT}"
else
  echo "FAILED to start, see /tmp/ninfer-dashboard.log"
  tail -5 /tmp/ninfer-dashboard.log
  exit 1
fi
