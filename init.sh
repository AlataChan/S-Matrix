#!/bin/bash
# S-Matrix 初始化脚本 (Linux/Mac)
# 用于首次部署或重新部署时自动完成所有配置
# 使用方法: ./init.sh 或 ./init.sh --reset

set -e

RESET=false

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --reset|-r)
            RESET=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

echo "========================================"
echo "  S-Matrix 自动部署脚本"
echo "========================================"
echo ""

# 进入脚本所在目录
cd "$(dirname "$0")"

# 如果指定了 Reset 参数，清除所有数据
if [ "$RESET" = true ]; then
    echo -e "\033[33m[警告] 即将清除所有数据...\033[0m"
    read -p "确定要清除所有数据吗？(y/N) " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        echo "停止并删除容器..."
        docker compose down -v
        echo "清除数据目录..."
        rm -rf ./data/fe/doris-meta/* 2>/dev/null || true
        rm -rf ./data/fe/log/* 2>/dev/null || true
        rm -rf ./data/be/storage/* 2>/dev/null || true
        rm -rf ./data/be/log/* 2>/dev/null || true
        echo -e "\033[32m数据已清除\033[0m"
    else
        echo "取消清除操作"
    fi
fi

# 1. 停止现有服务
echo ""
echo -e "\033[34m[1/5] 停止现有服务...\033[0m"
docker compose down

# 2. 创建数据目录
echo ""
echo -e "\033[34m[2/5] 确保数据目录存在...\033[0m"
mkdir -p ./data/fe/doris-meta
mkdir -p ./data/fe/log
mkdir -p ./data/be/storage
mkdir -p ./data/be/log
echo -e "  \033[32m✓ 数据目录已创建\033[0m"

# 3. 启动服务
echo ""
echo -e "\033[34m[3/5] 启动 Docker 服务...\033[0m"
docker compose up -d --build

# 4. 等待服务启动
echo ""
echo -e "\033[34m[4/5] 等待服务启动完成...\033[0m"
echo "  这可能需要 2-3 分钟，请耐心等待..."

max_attempts=60
attempt=0
fe_ready=false
be_ready=false

while [ $attempt -lt $max_attempts ] && ([ "$fe_ready" = false ] || [ "$be_ready" = false ]); do
    sleep 5
    attempt=$((attempt + 1))
    
    # 检查 FE 状态
    if [ "$fe_ready" = false ]; then
        fe_health=$(docker inspect --format='{{.State.Health.Status}}' smatrix-fe 2>/dev/null || echo "unknown")
        if [ "$fe_health" = "healthy" ]; then
            echo -e "  \033[32m✓ FE 服务已就绪\033[0m"
            fe_ready=true
        else
            echo "  · 等待 FE 启动... ($attempt/$max_attempts)"
        fi
    fi
    
    # 检查 BE 状态
    if [ "$fe_ready" = true ] && [ "$be_ready" = false ]; then
        be_health=$(docker inspect --format='{{.State.Health.Status}}' smatrix-be 2>/dev/null || echo "unknown")
        if [ "$be_health" = "healthy" ]; then
            echo -e "  \033[32m✓ BE 服务已就绪\033[0m"
            be_ready=true
        else
            echo "  · 等待 BE 启动... ($attempt/$max_attempts)"
        fi
    fi
done

if [ "$fe_ready" = false ] || [ "$be_ready" = false ]; then
    echo ""
    echo -e "\033[31m[错误] 服务启动超时，请检查日志：\033[0m"
    echo -e "\033[33m  docker compose logs smatrix-fe smatrix-be\033[0m"
    exit 1
fi

# 5. 注册 BE 节点
echo ""
echo -e "\033[34m[5/5] 注册 BE 节点到集群...\033[0m"

# 等待额外时间确保服务完全就绪
sleep 10

# 检查 BE 是否已注册
be_check=$(docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>/dev/null || echo "")
if echo "$be_check" | grep -q "172.30.0.3"; then
    echo -e "  \033[32m✓ BE 节点已存在，跳过注册\033[0m"
else
    # 注册 BE 节点
    if docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '172.30.0.3:9050';" 2>/dev/null; then
        echo -e "  \033[32m✓ BE 节点注册成功\033[0m"
    else
        echo -e "  \033[33m! BE 节点可能已存在或注册失败\033[0m"
    fi
fi

# 等待 BE 上线
echo "  等待 BE 节点上线..."
sleep 15

# 显示最终状态
echo ""
echo "========================================"
echo -e "  \033[32m部署完成！\033[0m"
echo "========================================"
echo ""
echo "服务状态："
docker compose ps
echo ""
echo "BE 节点状态："
docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | grep -E "Alive|Host|HeartbeatPort" || true
echo ""
echo "访问地址："
echo -e "  - Web UI:    \033[36mhttp://localhost:35173\033[0m"
echo -e "  - Doris UI:  \033[36mhttp://localhost:38030\033[0m"
echo -e "  - API:       \033[36mhttp://localhost:38018\033[0m"
echo -e "  - MySQL:     \033[36mmysql -h127.0.0.1 -P39030 -uroot\033[0m"
echo ""
