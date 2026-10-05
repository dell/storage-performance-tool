# 会议意见对应修改

| 会议建议                                      | 这版实现                                                                       | 状态                                                   |
| --------------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------ |
| 9/23：IOPS、Bandwidth、Latency 在同一图上对齐 | Fleet telemetry 合并折线，共享时间轴，不同颜色；每条独立尺度明确标示           | 已实现                                                 |
| 9/23：缩放、滚动/查看时间区间                 | Zoom in/out、左右平移、Fit run、双时间滑块、Follow latest                      | 已实现                                                 |
| 9/23：visuals 可配置；具体需求还应询问客户    | KPI 显示选择、图表曲线选择                                                     | 原型级实现，不表示已通过客户验证                       |
| 9/23：用户是有技术基础的工程师                | Guided 保留指标及说明；Expert 展示完整 timing 分布，保留 run 与当前 chart 状态 | 已保留                                                 |
| 9/23：异常指标                                | 显示真实计数或 mock 计数、节点 partial 提示；没有基线时不声称性能正常/异常     | 已保留，自动异常识别待规则定义                         |
| 10/01：VM process + outside access            | Node server 提供 dist 页面和 JSON API，host/port 参数可配置                    | 代码和本地验证完成；实际 VM 部署和外部可达性待团队验收 |
| 10/01：成员各自 sandbox 和端口                | 每个进程独立 mock source、port 和 clone/worktree 使用说明                      | 已实现                                                 |
| 10/01：实际实现应直接读取 metrics API         | HTTP source / typed Snapshot 保留；当前 Node mock API 验证前后端链路           | 真实 SPT adapter 尚未实现，符合 epic 最小要求          |
| 10/01：Go server / CLI --gui                  | 当前使用简单 Node 服务                                                         | stretch goal 尚未实现                                  |
| 10/01：OKRs、Jira、采访报告                   | 不塞进 benchmark UI；建议以团队文档和 Jira 记录                                | 团队工作，代码不能代替                                 |

注意：合并图的 y 轴是各曲线独立尺度的相对位置，不可比较不同单位的绝对值。原始单位和每条曲线 scale 显示在 legend。比起叠加三个没有标注的轴，这样更容易避免单位误读。可后续用用户访谈验证 overlay 与 separate lanes 的偏好。
