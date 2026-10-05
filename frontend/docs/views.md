# Guided / Expert 文件与职责

| 文件                               | 内容                                                               |
| ---------------------------------- | ------------------------------------------------------------------ |
| src/App.tsx                        | Guided/Expert 切换；Expert 先选择、Apply 后显示、返回修改选择      |
| src/components/GuidedDashboard.tsx | 所有 step/operation/node 直接展开及可视化进度条；清单全部核心 KPI、Scale、Progress、Mixed |
| src/components/ExpertSelection.tsx | 可勾选项目清单、Select all、Clear、Apply                           |
| src/components/Dashboard.tsx       | 根据选择显示原有 Expert 卡片与详细区块                             |
| src/components/ExpertDetails.tsx   | 完整 timing、Concurrency、Integrity、可选 raw                      |
| src/components/DeleteDetails.tsx   | 完整 DELETE 指标                                                   |
| src/components/ShardDiscovery.tsx  | LIST 与 shard discovery                                            |
| src/components/MixedWorkload.tsx   | Configured / observed share 表                                     |
| src/types.ts                       | schema v3、CoreStep/CoreScope、Metrics 契约                        |
| src/data/coreMock.ts               | step / operation 范围 mock 计数、分布、节点数据                    |
| src/data/mock.ts                   | mock run 生命周期、顶层指标、samples                               |
| src/services/http.ts               | HTTP 接口；检查 schema v3                                          |

Guided 不包含清单外的 DELETE detail、LIST、TTFB、p90/p999、Fleet、raw JSON 或导出。这些保留在 Expert 或现有公共控制中。

Expert 选择的是卡片/数据区块；完整 DELETE、完整 timing 等作为一个区块选择。Apply 前没有指标数据面板。配置了相应 workload 或 discovery 后，相关区块才有数据；勾选不会启用采集。

Guided 不再显示统计范围选择器；所有后端提供的 step、operation、节点均直接展开。无界运行显示持续运行与 elapsed，不显示百分比进度条。Expert 删除 Run Notes 及对应勾选项；Guided 保留 Run Notes。

最新调整：Guided 仅显示当前 step 的第一种 operation 的集群核心指标、简洁节点比较和 Mixed 比例，不展开历史 step 或每个节点的整套指标；与原先无选择时的默认范围一致。Expert 的嵌入核心指标区块仍展开所有 step/operation/node。

Guided 保留 FleetTelemetry 趋势图与右侧 RunNotes（Things to watch、Reading the metrics、Last received）；仅 Expert 删除 Run Notes，Expert 勾选面板也没有此项。

Expert 的完整 Core KPIs 按 step + operation 合并为 ExpertCoreTable：集群汇总第一行、各节点各一行、指标为列；限制高度并支持横纵滚动，固定表头、节点名及集群汇总行。Guided 不变。

默认 mock 现仅一个 Step 1，覆盖整个 run，无 Warm-up/Measurement 分段。coreSteps 数组和 Expert 按 step 展示结构保留，供以后对接实际 API 多 step 数据。

单 step 隐藏步骤标题和重复进度条；指标标题仅显示 operation。Scale 的 expected/reporting count 和 partial 标记继续显示。多个实际 step 时恢复步骤标题与各自进度。
