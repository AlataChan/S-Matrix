# S-Matrix 快速更新脚本 (Windows PowerShell)
# 从 Git 拉取更新并重启服务
# 使用方法: .\update.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  S-Matrix 快速更新脚本" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 进入项目目录
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# 1. 拉取最新代码
Write-Host "[1/3] 拉取最新代码..." -ForegroundColor Blue
git pull origin main
if ($LASTEXITCODE -ne 0) {
    Write-Host "[警告] Git pull 失败，继续重启服务..." -ForegroundColor Yellow
}

# 2. 重新构建并启动服务
Write-Host ""
Write-Host "[2/3] 重新构建服务..." -ForegroundColor Blue
docker compose up -d --build

# 3. 等待服务就绪
Write-Host ""
Write-Host "[3/3] 等待服务启动..." -ForegroundColor Blue
Write-Host "  等待约30秒..." -ForegroundColor Gray
Start-Sleep -Seconds 30

# 显示服务状态
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  更新完成！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
docker compose ps --format "table {{.Name}}\t{{.Status}}"
Write-Host ""
Write-Host "访问地址：" -ForegroundColor White
Write-Host "  - Web UI:    http://localhost:35173" -ForegroundColor Cyan
Write-Host "  - API:       http://localhost:38018" -ForegroundColor Cyan
Write-Host ""
