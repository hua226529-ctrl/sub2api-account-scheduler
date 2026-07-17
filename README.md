# Sub2API Account Scheduler

[![CI](https://github.com/hua226529-ctrl/sub2api-account-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/hua226529-ctrl/sub2api-account-scheduler/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

面向 Sub2API 的独立账号调度中心。它在不修改 Sub2API 源码和数据库结构的前提下，持续融合渠道监控、真实请求、余额、倍率和账号状态，自动完成暂停、恢复、负载调节与令牌分组救灾，并提供一个权限严格受限的智能运维智能体。

[English overview](README_EN.md)

> [!WARNING]
> 本项目能够修改 Sub2API 账号的调度状态、负载系数以及已确认令牌的分组。首次部署必须保持观察模式，核对映射、判定和拟执行动作后再启用自动控制。

## 核心能力

- 每 50 秒同步账号和渠道监控，根据规范化上游地址自动建立映射。
- 每 2 分钟增量读取监控历史和 Sub2API 真实请求记录，不主动调用模型进行探测。
- 区分凭据、基础设施、容量、模型、语义和客户端错误，避免黄色性能下降被当作硬故障。
- 组合管理渠道健康锁、余额锁、倍率待命锁和人工锁，只在所有锁解除且暂停归属明确时自动恢复。
- 以 NewAPI 或 Sub2 账号密码查询余额、令牌、分组和倍率，默认每 10 分钟刷新。
- 在整个模型调度池完全不可用时，按已确认的主分组、备用分组、紧急分组执行受控救灾。
- 通过智能体持久化目标、事件、检查点、定时任务、策略版本与动作结果，支持分析、协调和日报。
- 提供冻结智能体和冻结全部自动化两个管理员硬开关。

## 运行结构

```text
Sub2API 管理接口
       |
       +-- 50 秒：账号、监控与确定性调度
       +-- 2 分钟：监控历史与真实请求采集
       +-- 10 分钟：余额、倍率与令牌分组刷新
       |
       v
独立 Go 服务 + SQLite + 嵌入式 Vue 管理端
       |
       +-- 30 分钟：智能体全量分析
       +-- 严重事件：最多每 5 分钟紧急唤醒
       +-- 北京时间 00:10：上一自然日日报
```

真实请求来自 Sub2API 管理接口中的请求与错误记录。调度器不代理用户流量，也不会为可用性分析额外发送模型请求。

## 快速部署

### 前置条件

- 已运行的 Sub2API，且可以生成全局管理员密钥。
- Docker Engine 与 Docker Compose v2。
- 一个同时连接 Sub2API 与本调度器的外部 Docker 网络。
- 一个提供 HTTPS 的反向代理；仓库附带 Caddy 路径代理示例。

### 1. 准备网络和配置

```bash
git clone https://github.com/hua226529-ctrl/sub2api-account-scheduler.git
cd sub2api-account-scheduler

# 若 Sub2API 已在某个 Docker 网络中，请使用该网络名。
export SUB2API_DOCKER_NETWORK=sub2api_sub2api-network
docker network inspect "$SUB2API_DOCKER_NETWORK"

cp .env.example .env
openssl rand -base64 32
openssl rand -base64 32
```

将两次生成的不同随机值分别写入 `UPSTREAM_CREDENTIAL_KEY` 和 `AGENT_CREDENTIAL_KEY`，再填写 `SUB2API_ADMIN_API_KEY`。不要把 `.env` 提交到 Git。

```bash
chmod 600 .env
mkdir -p data
```

默认 `SUB2API_BASE_URL=http://sub2api:8080`，因此 Sub2API 容器在外部网络中的名称应为 `sub2api`。名称不同时，请把它改为实际可访问的容器地址。

### 2. 启动

```bash
SUB2API_DOCKER_NETWORK="$SUB2API_DOCKER_NETWORK" docker compose up -d --build
docker compose logs -f scheduler
```

容器内进程默认监听 `:8323`，Compose 只把宿主机的 `127.0.0.1:8323` 映射到容器，因此管理端不会直接暴露到公网。

### 3. 配置 HTTPS 路径代理

仓库中的 `Caddyfile.snippet` 假设 Caddy 运行在宿主机。将其放进自己的 HTTPS 站点块，例如：

```caddyfile
api.example.com {
    redir /scheduler /scheduler/ 308

    handle_path /scheduler/* {
        reverse_proxy 127.0.0.1:8323
    }
}
```

访问 `https://api.example.com/scheduler/`，使用 `SUB2API_ADMIN_API_KEY` 登录。

### 4. 验证状态

```bash
curl --fail http://127.0.0.1:8323/healthz
curl --fail http://127.0.0.1:8323/readyz
```

- `/healthz` 检查进程和 SQLite 是否可用。
- `/readyz` 要求最近两个账号读取周期内成功同步 Sub2API。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SUB2API_ADMIN_API_KEY` | 无 | 必填。Sub2API 全局管理员密钥，同时用于管理端登录。 |
| `SUB2API_BASE_URL` | `http://sub2api:8080` | Sub2API 管理接口地址。 |
| `LISTEN_ADDRESS` | `:8323` | 进程监听地址。 |
| `BASE_PATH` | `/scheduler/` | 管理端部署路径，会自动规范化首尾斜杠。 |
| `DATABASE_PATH` | `/data/scheduler.db` | 独立 SQLite 数据库路径。 |
| `POLL_INTERVAL` | `50s` | 账号和监控读取周期，最短 `10s`。 |
| `TELEMETRY_POLL_INTERVAL` | `2m` | 监控历史和真实请求读取周期，最短 `1m`。 |
| `BALANCE_POLL_INTERVAL` | `10m` | 余额、倍率和令牌分组读取周期，最短 `1m`。 |
| `REQUEST_TIMEOUT` | `12s` | Sub2API 及上游请求超时。 |
| `SESSION_IDLE_TIMEOUT` | `30m` | 管理端会话空闲超时，活动请求会续期。 |
| `COOKIE_SECURE` | `true` | HTTPS 部署必须保持 `true`；本机纯 HTTP 调试时才设为 `false`。 |
| `DRY_RUN` | `true` | 首次建库时的观察模式默认值。 |
| `FAILURE_THRESHOLD` | `3` | 首次建库时的连续异常暂停门槛。 |
| `RECOVERY_THRESHOLD` | `3` | 首次建库时的连续正常恢复门槛。 |
| `MANUAL_HOLD_MINUTES` | `10` | 首次建库时的人工操作保护时间。 |
| `FLAP_WINDOW_MINUTES` | `60` | 首次建库时的抖动统计窗口。 |
| `FLAP_PAUSE_THRESHOLD` | `3` | 首次建库时触发抖动保护的暂停次数。 |
| `FLAP_RECOVERY_THRESHOLD` | `10` | 首次建库时抖动保护的连续恢复门槛。 |
| `UPSTREAM_CREDENTIAL_KEY` | 无 | 上游账号密码的 AES-256-GCM 主密钥，Base64 或十六进制编码后必须表示 32 字节。 |
| `AGENT_CREDENTIAL_KEY` | 无 | 模型 API 密钥的独立 AES-256-GCM 主密钥，格式要求相同。 |
| `ALLOW_INSECURE_UPSTREAMS` | `false` | 是否允许上游使用 HTTP，仅建议本机测试使用。 |
| `SUB2API_DOCKER_NETWORK` | `sub2api_sub2api-network` | Compose 外部网络名，不传入应用进程。 |

`DRY_RUN`、暂停恢复阈值、人工保护和抖动参数只用于填充 SQLite 中尚不存在的设置。数据库建立后，应从管理页面修改这些策略；仅修改 `.env` 不会覆盖已持久化设置。

`UPSTREAM_CREDENTIAL_KEY` 和 `AGENT_CREDENTIAL_KEY` 必须彼此不同并长期保持不变。替换或丢失密钥后，已保存的上游密码或模型密钥将无法解密。已有对应配置但启动时缺少主密钥，服务会拒绝启动。

## 判定与调度

- `operational` 和 `degraded` 都表示硬可用；性能下降只影响质量和负载，不直接暂停账号。
- 凭据失效可立即暂停。其他硬故障默认要求连续 3 次，或最近 10 次中至少 5 次基础设施故障。
- 最近 10 分钟真实请求样本不少于 20 且成功率达到 95% 时，会压制仅由监控产生的暂停；低于 80% 时强化故障判断。
- 参数错误和模型不支持不会污染整条账号的可用率，模型能力问题会单独展示。
- 负载采用 100%、80%、50%、25% 档位，并按观察窗口分阶段恢复。
- 同一个 `last_checked_at` 只产生一条决策快照，重复读取不会重复计数。
- 监控关闭、无数据、失联、采集失败或历史尚未追平时冻结写操作，保持当前账号状态。
- 人工暂停是无期限 `ManualHold`，必须显式解除；人工恢复和普通负载调整默认创建 30 分钟临时 Override；Agent 自主账号动作默认 15 分钟且最长 2 小时。
- 所有账号暂停、恢复和负载写入都经过持久 journal、账号级串行、安全检查和写后回读；非本服务暂停的账号不会被自动恢复。
- 账号在滚动 60 分钟内第 3 次被自动暂停后进入抖动保护，本次恢复至少需要连续 10 次正常。

评分和调度阈值可以按“账号 > 上游池 > 全局”继承，并通过版本化策略发布和回退。

## 余额与三级分组救灾

余额中心仅支持 NewAPI 与 Sub2 的管理后台账号密码登录。凭据通过 AES-256-GCM 加密后写入 SQLite，登录会话和刷新令牌只在进程内存中维护。

每枚受控令牌必须：

1. 明确绑定实际使用它的 Sub2API 账号。
2. 选择互不相同的主、备用和紧急分组。
3. 对当前策略版本进行人工确认。

仅当整个模型调度池没有任何可调度渠道，同时满足数据新鲜度、连续硬失败和真实流量证据时，系统才允许从主组升级到备用组，再升级到紧急组。黄色性能下降、客户端错误、模型不支持、凭据错误和数据失联不会触发自动切组。

所有切换都先写入唯一幂等流水，再调用上游并回读确认。切换受冷却、频率限制、人工保护和全局冻结约束。恢复主组需要连续稳定 30 分钟、至少 10 个正常监控结果，以及最近 30 分钟不少于 20 个真实样本且成功率不低于 98%。

## 智能体安全边界

智能体是调度中心内的最高业务操作者，但不是服务器管理员。它可以查询脱敏业务数据，执行已注册的调度能力，并版本化修改 `DispatchPolicy`；它不能：

- 修改源码或运行 Shell。
- 访问服务器文件系统。
- 执行任意 SQL 或任意 HTTP 请求。
- 读取、更换或输出上游密码、模型密钥和加密主密钥。
- 绕过审计、幂等、资源校验、精确管理员授权或冻结开关。

每个写动作都遵循“意图落库、前置回读、外部写入、再次回读、本地提交”。状态不明确时进入协调，只回读确认，不盲目重放。

管理员对话是异步持久任务。智能体每轮上下文包含最近 24 小时的相关分析、动作、结果、目标和对话记忆；记录默认保留 90 天。定时命令默认使用 `Asia/Shanghai`。

## 观察门槛

确定性调度首次部署应保持 `DRY_RUN=true` 至少 30 分钟，确认账号映射、拟暂停、拟恢复和拟调负载均符合预期。

智能体首次启用、重新启用或更换模型后固定进入观察模式。只有同时满足以下条件，服务端才自动进入完全自治：

- 连续观察满 24 小时。
- 至少 40 次成功的定时分析。
- 模拟动作可执行率不低于 95%。
- 越权次数为零。
- 结构错误次数为零。

网页不能绕过该门槛，服务重启不会重置观察进度。模型不可用时，50 秒确定性调度仍按照最后生效的策略运行。

## 数据与备份

- 监控历史默认保留 14 天。
- 真实流量默认保留 7 天。
- 决策快照默认保留 30 天。
- 智能体分析、动作、对话和记忆默认保留 90 天。

备份必须同时包含：

- `data/scheduler.db` 及 SQLite 的 WAL/SHM 文件，或在停止服务后复制完整 `data` 目录。
- 权限为 `600` 的 `.env`。
- 当前镜像标签或可执行文件版本。

升级前停止写入或停止容器，完成备份，再构建新版本。回滚时同时恢复兼容的程序与数据库备份。

## 本地开发

需要 Go 1.26.3 和 Node.js 24。

```bash
cd frontend
npm ci
npm test
npm run build
cd ..

gofmt -w cmd internal
go test -buildvcs=false -count=1 ./...
go vet ./...
go build -buildvcs=false ./cmd/scheduler
```

前端构建会生成 `internal/webui/dist` 并嵌入 Go 程序。生成目录、`node_modules`、本地数据库、日志和 `.env` 不应提交。

## 参与项目

- 开发和提交规范见 [CONTRIBUTING.md](CONTRIBUTING.md)。
- 安全问题请按 [SECURITY.md](SECURITY.md) 私下报告，不要在公开 Issue 中粘贴密钥或漏洞细节。
- 版本变化见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

本项目使用 [Apache License 2.0](LICENSE)。
