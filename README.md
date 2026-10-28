# 性能优化工程

对 5 个开源代码库的性能优化点静态审计（E0 级：代码走读 + 成本模型，未运行基准）。

## 覆盖范围

| 库 | 说明 | 候选数 |
|---|---|---|
| [Daft](https://github.com/getdaft/daft) | 分布式 DataFrame（Rust 内核 + Python 前端） | ~44 |
| [Lance](https://github.com/lancedb/lance) | 列式数据格式（Rust） | ~51 |
| [PyO3](https://github.com/PyO3/pyo3) | Rust ↔ Python 绑定 | ~34 |
| [SGLang](https://github.com/sgl-project/sglang) | LLM 推理服务框架 | ~54 |
| [scx](https://github.com/sched-ext/scx) | Linux sched_ext BPF 调度器 | ~61 |

## 方法

- **五问法**：必须做吗 / 必须现在做吗 / 必须做这么多次吗 / 必须做这么重吗 / 必须在这里做吗
- **12 类浪费清单**：重复、不必要、等待、搬运、分配、放大、重算、粒度、局部性、串行/争用、算法/结构、配置/构建
- **优先级** = 收益上界 × 置信度 ÷ 改动代价
- 每条候选给出 file:line、代码证据、上界推算、建议修法、风险与 E1 验证路径
- 分析过程：两轮共 13 个并行分析 agent（第一轮 5 个全库深扫 + 第二轮 8 个定向深挖），主审对 21 处关键引用做代码级抽查，全部通过

## 文档

- [五库性能优化点审计总报告](performance-audit-2026-10-28.md) — 每库 benchmark 目录 + Top 10 优化点 + 第二梯队清单 + 跨库共性模式
- [optimizing-performance/](optimizing-performance/SKILL.md) — 审计所用的方法论 skill：五问法 + 12 类浪费 + 上界×置信÷代价评分，含 11 个参考文档（工作模型、浪费分类、发现路径、分层扫描清单、证据分级、分阶段统计与 perf 剖析、优先级、修法模式、结论自检、交付、案例模式）与 3 个开工模板（工作账本、候选清单、首轮扫描清单）

## 最有价值的发现（速览）

| 库 | 最大单项 |
|---|---|
| Daft | sort 全链路单线程（分片排序+归并，端到端 1.5-3x） |
| Lance | dict 编码决策 `hll.count()` 每值调用（写吞吐数倍） |
| PyO3 | free-threaded 逐元素 critical section |
| SGLang | EAGLE verify 免同步 host-plan 默认关闭 |
| scx | p2dq ATQ 把 taskc 指针当 pid（正确性 bug） |

五个"半天级"杠杆（改默认值 / 一行修）：

1. Lance compaction 默认 `Reencode` → `TryBinaryCopy`（compaction CPU 80-95%）
2. SGLang `SGLANG_OPT_FLASHINFER_VERIFY_HOST_PLAN` 默认翻 true（每次 verify replay 省多次阻塞 D2H）
3. scx p2dq `debug` 默认 2 → 0（异常加载路径下 5-15% 单核）
4. Daft `maintain_order` 自动判定（远端多文件 10-30%）
5. scx p2dq ATQ bug 修复（0.1 人日，正确性 + 公平性）

## 证据等级声明

全部结论为 **E0（静态代码 + 成本模型）**：上界是数量级估计，能回答"值不值得投入"，不能回答"真实收益是多少"。落地任何候选前，请按报告内各条目标注的 E1 测量路径（多数是现成 benchmark 的 A/B 差分或一个计数器）先确认工作量存在与占比。
