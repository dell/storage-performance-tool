# Dashboard API v3

## 请求

- `GET /api/runs/current`：返回当前或最近一次运行的 `Snapshot`。
- `POST /api/runs`：请求体为 `RunConfig`；成功返回新运行的初始 `Snapshot`。
- 错误返回非 2xx 状态。前端保留最后一次成功的响应并显示连接错误。

新增 `source: "mock" | "spt"` 表明数据来源。默认 production 构建通过同源 API 读取 server 提供的数据。

完整结构见 `src/types.ts`。`schemaVersion` 为 3；ISO 8601 UTC 时间戳。

## 数据语义

| 字段                           | 语义                                           |
| ------------------------------ | ---------------------------------------------- |
| aggregate.currentOps           | 当前成功操作速率，ops/s；完成后为 0            |
| aggregate.averageOps           | run/operation 的成功数 / 有效耗时              |
| aggregate.bandwidthMiB         | 当前 MiB/s，DELETE 为 null                     |
| aggregate.transferredBytes     | 累计 bytes，不是 MiB                           |
| success / failures             | 同一 run 和 operation 范围的累计 resolved 计数 |
| corrupt                        | failures 的子集，不能再次相加                  |
| latency / duration / ttfb      | 最近报告窗口的分布；字段统一 ms，不可互相替代  |
| samples[].throughput           | 对应采样区间的速率，不是累计运行平均速率       |
| samples[].p50 / p99            | 对应采样窗口的延迟百分位，ms                   |
| samples[].bandwidth            | 对应采样区间的 MiB/s；不适用为 null            |
| reportingNodes / expectedNodes | 当前包含数据的节点数 / 配置节点数              |
| partial                        | 汇总缺少节点；不完整数据也必须明确标记         |
| progressPercent                | 有界运行的总体百分比；无上限时 null            |
| nodes[].reporting              | 后端基于心跳/时间规则判断，不由前端猜测        |

不能平均各节点 P99 得到集群 P99。后端应提供有效集群分布、使用可合并 histogram，或将缺失的集群 timing 设置为 null。示例里的分布均为独立合成 fixtures。

失败百分比：`failures / (success + failures) * 100`。分母为 0 显示 `—`。DELETE 的 aggregate.failures 包含完全失败及部分成功的请求，aggregate.success 为全成功请求；对象失败预算在 deleteMetrics.failureBudget 单独统计。

原始 SPT 若返回 μs，adapter 转换为 ms：`value / 1000`。当前没有原始 response sample，不能确认真实 JSON 字段名。

后端保存有序时间序列，并决定长期运行的采样保留、降采样规则；前端当前轮询完整 samples 数组，适合原型。生产环境可改为增量游标、SSE 或 WebSocket。

DELETE 的 `requests` 与 `objects` 是两套计数。accepted 仅表示 API 接受，`verifiedAbsent` 表示验证确实不存在。`completion.requestPercent` 的分母为计划请求量，`completion.objectPercent` 的分母为 selected objects，不能直接等同于时间进度。

## 配置请求示例

```json
{
  "name": "Read benchmark",
  "operation": "READ",
  "durationSeconds": 120,
  "threadsPerClient": 64,
  "expectedNodes": 3,
  "objectSizeMiB": 1
}
```

后端需要单独约定 endpoint、bucket、凭证、run 生命周期和实际 SPT 命令执行。此项目暂未把它们假装实现。

完整 DELETE v2 契约与计数规则见 `delete-metrics.md`。缺失、不适用或尚无测量的 timing 使用 null，不能用 0 冒充已测量值。

可选 `mixedMetrics` 用于 Mixed 比例表，定义和 attempted 口径见 `mixed-metrics.md`。单操作运行可省略或返回 null。

## v3 Guided 必需字段

`coreSteps[]` 提供各 step 的 id/label/state、elapsedSeconds、completionPercent、unbounded、configuredThreads、expectedNodes/reportingNodes/partial。
每个 step 的 `scopes[]` 按 CREATE/READ/STAT/DELETE 区分，包含该 step/operation 的 aggregate 和 nodes。不能把 run 累计值复制到每个 step。

Metrics 新增 `currentFailureOps`、`rateWindowSeconds`、`averageBandwidthMiB`。
Current success / failure / byte rates 使用所声明窗口的移动平均；示例为 trailing 5s 的 1s 样本平均。Average successful rate = success / step elapsed；average bandwidth = bytes / 1048576 / step elapsed。
Latency 和 Duration 都提供 mean/min/p50/p99/max ms，额外 Expert percentiles 仍保留。API 微秒必须在 adapter 中除以 1000，不能只改标签。

`unbounded` 是明确的 run 字段；无界 run 与 step completion 为 null，状态不会因配置 duration 自动完成。
step.mixedMetrics 的 scope 为 `cumulative step`；顶层 mixedMetrics 为 `cumulative run`，观察比例均基于 attempted counts。
