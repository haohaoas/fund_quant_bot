# 阿里云部署指南（Ubuntu 22.04）

目标：把 `backend` 部署到阿里云公网，供 Flutter 移动端（iOS/Android）访问。

## 1. 服务器规格建议

- 起步可用：`2核2G`
- 适用场景：开发/小流量
- 建议升级到 `2核4G` 的场景：并发请求增加、AKShare 抓取频繁、推荐计算耗时变高

## 2. 安全组与系统准备

放行入站端口：

- `22`（SSH）
- `8000`（API，先跑通）

在服务器执行：

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

# 安装 Docker
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

## 3. 上传代码并配置环境变量

```bash
# 示例目录
mkdir -p ~/apps
cd ~/apps

# 你可以用 git clone 或上传压缩包
# git clone <your_repo_url> fund_quant_bot
cd fund_quant_bot

cp deploy/.env.example deploy/.env
```

编辑 `deploy/.env`：

- 必填：`DEEPSEEK_API_KEY`
- 可选：`TUSHARE_TOKEN`

## 4. 启动服务

```bash
mkdir -p runtime-data
docker compose -f deploy/docker-compose.aliyun.yml --env-file deploy/.env up -d --build
```

查看状态：

```bash
docker compose -f deploy/docker-compose.aliyun.yml ps
docker compose -f deploy/docker-compose.aliyun.yml logs -f backend
```

健康检查：

```bash
curl http://127.0.0.1:8000/api/health
# 期望返回: {"ok":true}
```

公网检查：

```bash
curl http://<你的公网IP>:8000/api/health
```

## 5. 移动端连接公网 API

移动端工程是：`mobile_flutter`

### 5.1 开发调试运行

```bash
cd mobile_flutter
flutter run --dart-define=API_BASE_URL=http://<你的公网IP>:8000
```

### 5.2 打包

Android：

```bash
flutter build apk --release --dart-define=API_BASE_URL=http://<你的公网IP>:8000
```

iOS：

```bash
flutter build ipa --release --dart-define=API_BASE_URL=http://<你的公网IP>:8000
```

## 6. 上线前建议（HTTPS）

当前方案为 HTTP + 8000 直连，适合先跑通。正式环境建议：

- 配域名
- Nginx/Caddy 反代到 `127.0.0.1:8000`
- 开启 HTTPS（80/443）
- 安全组只开放 `22/80/443`

