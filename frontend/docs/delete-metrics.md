# DELETE 完整指标：normalized schema v2

这份契约用于 dashboard，不声称是原始 SPT JSON 字段名。实际后端必须读取 SPT metrics API 并进行映射。mock 显示全套指标，但不是实测结果。

| 需求                                                                                 | 字段/页面                                            |
| ------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| Requests：attempted/full success/partial/failed/unresolved，requests/s               | requests + Requests 区域                             |
| Objects：selected/attempted/accepted/failed/unattempted/unresolved，objects/s        | objects + Objects 区域                               |
| 请求与对象完成率                                                                     | completion.requestPercent / objectPercent            |
| configured/observed batch size、mean objects/request、full/partial batch 数和百分比  | batching，Observed minimum/maximum + Mean            |
| 最多 100 个 per-bucket，剩余归 other                                                 | buckets.items (max 100) / other / groupedBucketCount |
| seed/discovery/pre-validation/scheduled delete/drain/post-verification/cleanup/total | phaseTimings，单位秒                                 |
| Request latency & duration mean/min/p50/p90/p99/p99.9/max                            | requestTiming，单位 ms；不创建 per-object latency    |
| failed objects、observed failure %、allowed maximum、outcome                         | failureBudget                                        |
| verified absent/still present/verification unresolved/removal confirmed              | verification                                         |

## 口径

- attempted requests = fullSuccess + partial + failed + unresolved。这版 mock 用互斥请求状态。后端须根据真实 SPT 状态的含义映射，不重复计数。
- selected objects = attempted + unattempted。
- attempted objects = accepted + failed + unresolved（mock 对象响应状态互斥）。
- requests.perSecond 是“尝试发送的请求数”每秒增量；objects.perSecond 是“尝试删除的对象数”每秒增量。顶部 Current successful requests 单独显示全成功请求速率。
- partial request success 不等于 partial-sized batch：前者为部分对象响应失败，后者指该请求对象数低于配置批量大小。
- observed batch size 以 observedMinSize / observedMaxSize 表示观测范围；meanObjectsPerRequest = attempted objects / attempted requests。分母为 0 显示 `—`。
- requestPercent = attempted requests / planned requests × 100；objectPercent = attempted objects / selected objects × 100。100% 表示都尝试过，不等于全部成功或验证完毕。
- failureBudget.observedFailurePercent = failed objects / attempted objects × 100。无 attempted objects 时为 null。
- failureBudget.outcome：运行时 running；结束后超过 allowedFailurePercent 则 failed；零失败为 completed cleanly；有失败但不超过上限则 completed within budget。
- verification.unresolved 是已经尝试验证但结果未确定的对象，独立于 objects.unresolved（删除响应未确定）。verification.unverified 是尚未完成验证的对象。
- accepted 仅代表 API 接受；只有 verifiedAbsent 才证明该次验证中不存在。
- removalConfirmed 为 true 要求完成、全部 selected objects 验证不存在，且 stillPresent / unresolved / unverified 均为 0。
- “completed within budget”可以同时有 removalConfirmed = false，因为允许部分失败不表示全删成功。
- 验证未确定场景中，budget 可为 completed cleanly（删除响应无失败），但整体 run state 为 failed（验证不完整）。两者判定范围不同。

## mock 生命周期

固定 sequential phases：seed 4%、discovery 10%、pre-validation 6%、scheduled delete 55%、drain 5%、post-verification 15%、cleanup 5%。百分比用于分配模拟时间，不假设真实 SPT 都按这些比例或串行执行。阶段耗时为已发生耗时，未开始时为 0。总 wall time 单独提供，真实 backend 不应强制把可能重叠的阶段耗时相加。

默认 bucketCount 103，可设置 1–500；单个请求 configured batchSize 1–1000。观测部分 batch 为较小请求。响应和验证均独立模拟，以演示 accepted 未验证、未尝试、pending responses 和 verification unresolved。

## API 兼容性

v0.3 将 schemaVersion 从 1 升至 2，因为旧 DELETE 字段结构改变。旧 `requests.successful` 改为 fullSuccess；旧 completion 和 removalConfirmed 移入 completion / verification；配置新增可选 deleteOptions。默认 Node server 与 frontend 已一起更新。未更新的独立 backend 会显示 unsupported response，避免误读旧数据。
