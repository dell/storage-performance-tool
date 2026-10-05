# Dell Dev VM：最小前后端链路

对应 10/01 epic：将 server 放在 VM，代码放进仓库，每个成员能在自己的 sandbox build/run，从笔记本访问网页。这里没有真实 VM 凭证，生成时只在当前环境做了服务验证，没有替你部署到 Dell VM。

## 每人独立运行

在各自的 clone/worktree 里执行。Node.js 22.6+，建议 22 LTS 或更新版本。

```bash
npm ci
npm run build
npm start -- --host 0.0.0.0 --port 8081
```

另一位组员在自己的目录里使用另一个端口：

```bash
npm start -- --host 0.0.0.0 --port 8082
```

也支持 Linux 环境变量：

```bash
HOST=0.0.0.0 PORT=8081 npm start
```

`--port` 优先于 `PORT`。每个进程维护自己的 mock run，不会跨端口共享状态；访问同一端口的用户会看到同一个 run。先在团队里约定端口，遇到 EADDRINUSE 就换未占用端口。

确保没有旧 `.env.local` 把生产构建强制设为 `VITE_DATA_SOURCE=mock`，否则网页会绕过 server API 使用浏览器本地 mock。默认不需要复制 `.env.example`。

## 从笔记本访问

在允许连接 UW 网络的环境（如校园网络或学校 VPN）中访问：

```text
http://capstone-dell-dev:8081/
```

hostname 请以你们实际可解析的 VM 地址为准。服务绑定 `0.0.0.0` 允许外部连接，但是否可以直接访问取决于 UW 的网络和防火墙配置，这份代码不能保证外部网络可达。

先在 VM 本机检查：

```bash
curl http://127.0.0.1:8081/api/health
```

再从笔记本检查：

```bash
curl http://capstone-dell-dev:8081/api/health
```

Windows PowerShell 可用 `curl.exe`。健康接口应包含 `status: ok`、`dataSource: mock`、`sptConnected: false` 及当前端口。

若 VM 本机可访问但笔记本不能直连，可以用 9/23 提到的 SSH forwarding。在笔记本终端运行：

```bash
ssh -N -L 8081:127.0.0.1:8081 your_username@capstone-dell-dev
```

然后访问 `http://localhost:8081/`，保持 SSH 终端打开。这是访问方式备选；若验收要求直接访问 VM 的端口，仍需验证校园网络下的直连可达性。不需要额外安装 Tailscale。

## 进程与验收

前台运行：保持启动终端打开，Ctrl+C 结束。可在 VM 的 tmux session 中运行以便断开 SSH 后继续运行（若已安装 tmux）。

验收步骤：

1. 在自己的 clone/worktree 中 `npm ci && npm run build` 成功。
2. 启动进程并检查 `/api/health` 返回自己的端口。
3. 笔记本打开网页，显示 SERVER MOCK，网络面板里看到 `/api/runs/current`。
4. 配置新 run，确认 network 面板出现 `POST /api/runs`，所有访问同一端口的页面显示同一 run。
5. 第二位组员在另一个端口运行，新建 run 不影响第一人的 run。
6. 仓库记录代码与部署说明，Jira ticket 分配给完成该任务的成员。

Node server 是本 epic 的最小实现，并非 Go / `--gui` stretch goal；不执行 SPT 或 SBT 命令。会议中的 SBT 与前面 SPT 的命名差异保留为待团队确认的事项。
