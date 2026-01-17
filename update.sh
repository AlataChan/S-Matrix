#!/bin/bash
# S-Matrix 快速更新脚本 (Linux/Mac)
# 从 Git 拉取更新并重启服务
# 使用方法: ./update.sh

echo "========================================"
echo "  S-Matrix 快速更新脚本"
echo "========================================"
echo ""

# 进入脚本所在目录
cd "$(dirname "$0")"

# 1. 拉取最新代码
echo -e "\033[34m[1/3] 拉取最新代码...\033[0m"
git pull origin main || echo -e "\033[33m[警告] Git pull 失败，继续重启服务...\033[0m"

# 2. 重新构建并启动服务
echo ""
echo -e "\033[34m[2/3] 重新构建服务...\033[0m"
docker compose up -d --build

# 3. 等待服务就绪
echo ""
echo -e "\033[34m[3/3] 等待服务启动...\033[0m"
echo "  等待约30秒..."
sleep 30

# 显示服务状态
echo ""
echo "========================================"
echo -e "  \033[32m更新完成！\033[0m"
echo "========================================"
docker compose ps
echo ""
echo "访问地址："
echo -e "  - Web UI:    \033[36mhttp://localhost:35173\033[0m"
echo -e "  - API:       \033[36mhttp://localhost:38018\033[0m"
echo ""
