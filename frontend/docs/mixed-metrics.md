# Mixed workload 比例表

v0.4 添加原型 UI 和可选 `Snapshot.mixedMetrics`，保持 schemaVersion 2。

- basis: attempted；scope: cumulative run。
- totalAttempted: 所有 Mixed 操作类型的累计尝试数。
- operations[].operation: CREATE / READ / STAT / DELETE。
- configuredSharePercent: 配置目标比例（0–100），所有操作合计 100%。
- attemptedCount: 当前操作的累计尝试次数，所有操作合计 totalAttempted。
- observedSharePercent: attemptedCount / totalAttempted × 100；total 为 0 时 null。
- UI difference: observedSharePercent − configuredSharePercent，单位百分点 pp。

这些 counts 是操作/请求次数，不是传输 bytes 或 DELETE 对象数。一个批量 DELETE 请求算一次 operation，不能用该请求所包含的对象数量代替。

mock 固定配置为 READ 60 / CREATE 25 / STAT 10 / DELETE 5%，观察计数用独立权重产生，再根据 count 计算比例。此处只演示比例差异；不声称驱动真实调度器。mock 的总体带宽使用简化传输权重，不能当成真实混合性能模型。

后续后端映射应从 metrics API 获取配置比例和 per-operation attempted counts，在同一 run、step/统计窗口中计算；若选择 successful basis，需同时更新类型、文案和计算，不能混用。此版本没有单独 STAT run、shares 编辑、Mixed DELETE 专项统计，也不执行真实 Mixed workload。

启动演示：Configure run → MIXED → Review settings → Start，dashboard 的 Client comparison 上方显示对比表。
