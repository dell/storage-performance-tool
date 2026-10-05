# LIST / shard discovery metrics

这版新增可选 `RunConfig.shardDiscovery`、`Snapshot.discoveryMetrics`，不改变 schemaVersion 2。后端映射真实 metrics API，不能解析 CLI 文本来生成实测指标。

## 显示条件

配置 `shardDiscovery.enabled = true` 且存在 enabled discoveryMetrics 时才显示。配置默认关闭；关闭时 mock 返回 null，页面不显示任何 LIST 数值。enabled 但暂时零采样时 objects/pages 为 0，averagePageResponseMs 为 null。真实后端尚未提供 discoveryMetrics 时不会展示虚构数值。

## 字段

| 需求                            | 字段                                                             |
| ------------------------------- | ---------------------------------------------------------------- |
| Total objects discovered        | totalObjectsDiscovered                                           |
| Total pages returned            | totalPagesReturned                                               |
| Average page response time · ms | averagePageResponseMs                                            |
| Splits 总数                     | totalSplits                                                      |
| 最大分片深度                    | maxDepth                                                         |
| 各 split reason 计数            | splitsByReason[].reason / count                                  |
| Per-prefix shard progress       | prefixes[].objectsDiscovered / expectedObjects / progressPercent |
| Active / stalled                | prefixes[].status，可为 pending / active / stalled / completed   |
| 活跃 shard 数、上次进展时间     | prefixes[].activeShards / lastProgressAt                         |

prefix 进度 percentage 只有已知 expectedObjects 时才能计算；未知数量应返回 expectedObjects=null 和 progressPercent=null，页面显示 Unknown total，不编造百分比。页面不基于“短暂没有增加”自行判断 stalled，状态由数据源提供。

平均响应时间是按已返回 pages 数加权的平均，不应简单平均各 prefix 的平均值。原始 API 若返回 μs，后端除以 1000 转为 ms。

mock 生成 4 个 prefix，使用独立发现数据，不从 benchmark 成功操作数推算。默认每页最多 500 个发现对象；page-limit / dense-prefix 仅为模拟 split reason 标签，不代表 SPT API 实际枚举。

Simulate stalled prefix 是演示选项。logs/ 在 25%–50% 的 run 时间中不进展，然后恢复，其他 prefix 继续进行。真实 adapter 不使用这个 mock 字段来制造 stalled。

现阶段是指标视图和 mock 接口准备，没有单独 LIST workload 执行器，也未验证真实 SPT shard schema。
