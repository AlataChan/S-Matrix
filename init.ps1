# S-Matrix 初始化脚本 (Windows PowerShell)
# 用于首次部署或重新部署时自动完成所有配置

param(
    [switch]$Reset  # 使用 -Reset 参数清除所有数据重新开始
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  S-Matrix 自动部署脚本" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 进入项目目录
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# 如果指定了 Reset 参数，清除所有数据
if ($Reset) {
    Write-Host "[警告] 即将清除所有数据..." -ForegroundColor Yellow
    $confirm = Read-Host "确定要清除所有数据吗？(y/N)"
    if ($confirm -eq 'y' -or $confirm -eq 'Y') {
        Write-Host "停止并删除容器..." -ForegroundColor Yellow
        docker compose down -v
        Write-Host "清除数据目录..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "./data/fe/doris-meta/*"
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "./data/fe/log/*"
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "./data/be/storage/*"
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue "./data/be/log/*"
        Write-Host "数据已清除" -ForegroundColor Green
    } else {
        Write-Host "取消清除操作" -ForegroundColor Gray
    }
}

# 1. 停止现有服务
Write-Host ""
Write-Host "[1/5] 停止现有服务..." -ForegroundColor Blue
docker compose down

# 2. 创建数据目录
Write-Host ""
Write-Host "[2/5] 确保数据目录存在..." -ForegroundColor Blue
New-Item -ItemType Directory -Force -Path "./data/fe/doris-meta" | Out-Null
New-Item -ItemType Directory -Force -Path "./data/fe/log" | Out-Null
New-Item -ItemType Directory -Force -Path "./data/be/storage" | Out-Null
New-Item -ItemType Directory -Force -Path "./data/be/log" | Out-Null
Write-Host "  ✓ 数据目录已创建" -ForegroundColor Green

# 3. 启动服务
Write-Host ""
Write-Host "[3/5] 启动 Docker 服务..." -ForegroundColor Blue
docker compose up -d --build

# 4. 等待服务启动
Write-Host ""
Write-Host "[4/5] 等待服务启动完成..." -ForegroundColor Blue
Write-Host "  这可能需要 2-3 分钟，请耐心等待..." -ForegroundColor Gray

$maxAttempts = 60
$attempt = 0
$feReady = $false
$beReady = $false

while ($attempt -lt $maxAttempts -and (-not $feReady -or -not $beReady)) {
    Start-Sleep -Seconds 5
    $attempt++
    
    # 检查 FE 状态
    if (-not $feReady) {
        $feHealth = docker inspect --format='{{.State.Health.Status}}' smatrix-fe 2>$null
        if ($feHealth -eq "healthy") {
            Write-Host "  ✓ FE 服务已就绪" -ForegroundColor Green
            $feReady = $true
        } else {
            Write-Host "  · 等待 FE 启动... ($attempt/$maxAttempts)" -ForegroundColor Gray
        }
    }
    
    # 检查 BE 状态
    if ($feReady -and -not $beReady) {
        $beHealth = docker inspect --format='{{.State.Health.Status}}' smatrix-be 2>$null
        if ($beHealth -eq "healthy") {
            Write-Host "  ✓ BE 服务已就绪" -ForegroundColor Green
            $beReady = $true
        } else {
            Write-Host "  · 等待 BE 启动... ($attempt/$maxAttempts)" -ForegroundColor Gray
        }
    }
}

if (-not $feReady -or -not $beReady) {
    Write-Host ""
    Write-Host "[错误] 服务启动超时，请检查日志：" -ForegroundColor Red
    Write-Host "  docker compose logs smatrix-fe smatrix-be" -ForegroundColor Yellow
    exit 1
}

# 5. 注册 BE 节点
Write-Host ""
Write-Host "[5/5] 注册 BE 节点到集群..." -ForegroundColor Blue

# 等待额外时间确保服务完全就绪
Start-Sleep -Seconds 10

# 检查 BE 是否已注册
$beCheck = docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS;" 2>$null
if ($beCheck -match "172.30.0.3") {
    Write-Host "  ✓ BE 节点已存在，跳过注册" -ForegroundColor Green
} else {
    # 注册 BE 节点
    docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '172.30.0.3:9050';"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ BE 节点注册成功" -ForegroundColor Green
    } else {
        Write-Host "  ! BE 节点可能已存在或注册失败" -ForegroundColor Yellow
    }
}

# 等待 BE 上线
Write-Host "  等待 BE 节点上线..." -ForegroundColor Gray
Start-Sleep -Seconds 15

# 显示最终状态
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  部署完成！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "服务状态：" -ForegroundColor White
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
Write-Host ""
Write-Host "BE 节点状态：" -ForegroundColor White
docker exec smatrix-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G" 2>$null | Select-String -Pattern "Alive|Host|HeartbeatPort"
Write-Host ""
Write-Host "访问地址：" -ForegroundColor White
Write-Host "  - Web UI:    http://localhost:35173" -ForegroundColor Cyan
Write-Host "  - Doris UI:  http://localhost:38030" -ForegroundColor Cyan
Write-Host "  - API:       http://localhost:38018" -ForegroundColor Cyan
Write-Host "  - MySQL:     mysql -h127.0.0.1 -P39030 -uroot" -ForegroundColor Cyan
Write-Host ""
