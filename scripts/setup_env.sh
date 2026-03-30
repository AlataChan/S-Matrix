#!/usr/bin/env bash
# setup_env.sh — 初始化 .env 文件，自动填充可生成的变量
# 用法：bash scripts/setup_env.sh
#       （docker-compose up 之前运行一次即可）
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_FILE="$ROOT_DIR/.env.example"

# 如果 .env 不存在，从 .env.example 复制
if [[ ! -f "$ENV_FILE" ]]; then
    cp "$EXAMPLE_FILE" "$ENV_FILE"
    echo "[setup_env] 已从 .env.example 创建 .env"
fi

# 读取变量当前值（空行、注释和无值的行返回空字符串）
get_val() {
    local key="$1"
    python3 - "$ENV_FILE" "$key" <<'PY'
import sys
from pathlib import Path
env = Path(sys.argv[1])
target = sys.argv[2]
for raw in env.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    if k.strip() == target:
        print(v.strip())
        sys.exit(0)
sys.exit(0)
PY
}

# 将 KEY=VALUE 写入 .env（追加或替换已有行）
set_val() {
    local key="$1"
    local value="$2"
    local file="$ENV_FILE"
    if grep -qE "^${key}=" "$file" 2>/dev/null; then
        # 已存在：替换（macOS/Linux 兼容）
        python3 - "$file" "$key" "$value" <<'PY'
import sys, re
from pathlib import Path
f = Path(sys.argv[1])
key, val = sys.argv[2], sys.argv[3]
text = re.sub(rf'^{re.escape(key)}=.*', f'{key}={val}', f.read_text(encoding="utf-8"), flags=re.MULTILINE)
f.write_text(text, encoding="utf-8")
PY
    else
        echo "${key}=${value}" >> "$file"
    fi
}

# ── SMATRIX_API_KEY（必填，自动生成） ──────────────────────────────
SMATRIX_API_KEY="$(get_val SMATRIX_API_KEY)"
if [[ -z "$SMATRIX_API_KEY" ]]; then
    SMATRIX_API_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
    set_val SMATRIX_API_KEY "$SMATRIX_API_KEY"
    echo "[setup_env] 已自动生成 SMATRIX_API_KEY"
else
    echo "[setup_env] SMATRIX_API_KEY 已配置，跳过"
fi

# ── ENCRYPTION_KEY（使用数据源同步时必填，自动生成） ────────────────
ENCRYPTION_KEY="$(get_val ENCRYPTION_KEY)"
if [[ -z "$ENCRYPTION_KEY" ]]; then
    ENCRYPTION_KEY="$(python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
    set_val ENCRYPTION_KEY "$ENCRYPTION_KEY"
    echo "[setup_env] 已自动生成 ENCRYPTION_KEY"
else
    echo "[setup_env] ENCRYPTION_KEY 已配置，跳过"
fi

# ── DEEPSEEK_API_KEY（必须用户填写，不能自动生成） ─────────────────
DEEPSEEK_API_KEY="$(get_val DEEPSEEK_API_KEY)"
if [[ -z "$DEEPSEEK_API_KEY" ]]; then
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  请填写 DEEPSEEK_API_KEY（从 platform.deepseek.com 获取）║"
    echo "║  编辑文件：$ENV_FILE"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    echo "[setup_env] 警告：DEEPSEEK_API_KEY 未配置，自然语言查询功能将不可用"
else
    echo "[setup_env] DEEPSEEK_API_KEY 已配置，跳过"
fi

echo ""
echo "[setup_env] 完成。现在可以运行：docker-compose up -d"
