# Daft / Lance / PyO3 / SGLang / scx 五库性能优化点审计报告

日期：2026-10-28 | 方法：optimizing-performance skill（五问法 + 12 类浪费 + 上界×置信÷代价）
证据等级：E0（静态代码 + 成本模型）；两轮共 13 个并行 subagent，主 agent 逐份审核并对 21 处关键引用做代码级抽查，全部通过。

---

## 总览

| 库 | 候选数 | 最大的单项 | 最高置信发现 |
|---|---|---|---|
| Daft | ~44 | sort 全链路单线程（端到端 1.5-3x） | PythonArray size_bytes 每节点 pickle 采样 |
| Lance | ~51 | dict 编码决策 hll.count() 每值调用（写吞吐数倍） | FSST 解码 8x 缓冲+双拷贝（0.85） |
| PyO3 | ~34 | free-threaded 逐元素 critical section | Vec 提取无 list 快路径（0.7） |
| SGLang | ~54 | EAGLE verify 免同步 host-plan 默认关闭 | 每步两次全量 filter_batch 重扫 |
| scx | ~61 | cgroup_bw accounting timer 全树遍历（1ms 间隔可达数核） | p2dq ATQ 把 taskc 指针当 pid（正确性 bug，0.85） |

五个"零半人日"级杠杆（改默认值/一行修）：lance compaction 默认 Reencode→TryBinaryCopy；sglang `SGLANG_OPT_FLASHINFER_VERIFY_HOST_PLAN` 默认翻 true；scx p2dq `debug` 默认 2→0；daft `maintain_order` 自动判定；scx p2dq ATQ bug 修复。

---

## 1. Daft（分布式 DataFrame：Python 前端 + Rust 内核）

仓库：`C:\myprogram\code\daft\daft`（Rust workspace 在 src/，60+ crates）

### Benchmark 目录
- `benchmarking/`（宏基准，手动/集群）：tpch（SF10，CSV 输出，Ray 或 native）、tpcds、clickbench（hits 100M，供 pytest 用）、parquet（pytest-benchmark：bulk_reads/filter_pushdown/num_rowgroups/types_and_codecs）、vllm、asof_join（+memray）、lerobot、common_crawl、ai（README 有 vs Ray/Spark 结果）
- `tests/benchmarks/`（pytest-benchmark + **Codspeed CI 守护**，`pr-test-suite.yml` 的 benchmark-codspeed job，blacksmith-8vcpu，simulation 模式）：test_clickbench（14 查询）、test_local_tpch（q1-10）、test_interactive_reads（冷启动/交互延迟）、test_json_reads、test_parquet_writes、test_streaming_writes
- `tests/microbenchmarks/`（concat/filter/join/sort/take 等旧式微基准，无 CI）
- Rust 侧：`src/daft-parquet/benches`（tango-bench）、`src/daft-recordbatch/src/ops/bench_agg.rs`（cargo test --ignored）、daft-image/minhash benches——均无 CI 守护
- `.github/workflows/daft-profiling.yml`：py-spy 剖析 TPC-H（只剖析不守护）

### Top 优化点（上界×置信÷代价 排序）
1. **Sort 全链路单线程**：`src/daft-local-execution/src/sinks/sort.rs:137-139`（max_concurrency=1）+ `:89-95`（finalize 全量 concat 后单线程 argsort，`daft-core/src/array/ops/sort.rs:63`）。分片排序+归并可拿 sort 段 ~7x，端到端 1.5-3x（ORDER BY 类查询）。3-5 人日。
2. **PythonArray::size_bytes 采样 pickle**：`src/daft-core/src/array/ops/len.rs:24-88`——每次统计对 Python 对象列 attach GIL + pickle 最多 1MB/万对象，同一 morsel 流过 K 个节点被算 2K 次。AI/lerobot/vllm 负载 10-30%，compute 线程抢 GIL。0.5 人日。
3. **join probe 逐行 growable.extend(len=1)**：`src/daft-local-execution/src/join/inner_join.rs:26-44`——每个匹配行每列一次虚调用，千万行输出=亿次。join 内核 20-40%，0.5-2s/query。1-2 人日；顺带修 `:53-65` 闭包内每 batch 重建列名 Vec。
4. **scan 每 chunk 重建 cast 表达式**：`src/daft-local-execution/src/sources/scan_task.rs:615-641`——`cast_to_schema_with_fill` 对流中每个 chunk 重建表达式并求值（materialized_schema() 每 chunk 调 3 次）；schema 一致时整步可跳过。25-200ms/query，宽表更高。0.5-1 人日。
5. **maintain_order 默认 true**：`src/common/daft-config/src/lib.rs:195` + 顺序保持 flattener `scan_task.rs:448-474`——无 ORDER BY 语义的查询也被按任务序消费，慢任务阻塞流水线。多文件远端 10-30%。E2 验证成本极低（Codspeed A/B）。1-2 人日。
6. **ProbeTable 比较器矩阵 O(T²)**：`src/daft-recordbatch/src/probeable/probe_table.rs:75-88,131-150`——build 侧 T 个 batch 建 T(T+1)/2 个比较器闭包，probe 每 morsel 再建 T 个。0.3-1.5s/query。arrow-row 行编码替代。2-4 人日。
7. **聚合/导出前 concat 全列真拷贝**：`src/daft-micropartition/src/ops/agg.rs:9`、`sort.rs:17`、`src/daft-micropartition/src/python.rs:190-197`（to_pydict/to_pandas）——多 chunk morsel 每次聚合/排序/导出先全列 memcpy。TPC-H Q1 量级 0.3-1s/query。1-2 人日。
8. **IO/scan 并发默认 8**：`daft-config/src/lib.rs:192`（scantask_max_parallel=8）+ `common/runtime/src/lib.rs:27`（IO runtime 8 线程）——64 核机器远端扫描 in-flight 封顶 64 连接。S3 场景 scan 段 2-4x。0.5 人日。
9. **Parquet prefilter 串行**：`src/daft-parquet/src/reader/mod.rs:366-459`——逐 RG 串行 prefilter 阻塞后续并行解码，0.5-2s/大文件。2 人日。
10. **shuffle IPC 读端 memset+8KB 缓冲**：`src/daft-shuffles/src/server/stream.rs:107-127`、`flight_server.rs:329`——每条 4MB 消息先零填再被 read_exact 覆盖 + BufReader 8KB 导致百万级 syscall。每 10GB shuffle 省约 6s。0.5 人日。

### 第二梯队（择要）
- 每 morsel 固定开销族：to_field 重算（`daft-recordbatch/src/lib.rs:1299`）、size_bytes 无缓存（`intermediate_op.rs:233-236`）、metrics selectivity 双原子（`filter.rs:77-85`）、concat 时 stats union（`ops/concat.rs:18-26`）——可一次打包修
- 非内联聚合回退逐行闭包比较器（`ops/hash.rs:48-70` + `inline_agg.rs:865-895` 支持类型清单可扩 Mean/Utf8）
- JSON 解析 DOM 化 + 每行 memcpy + IndexMap（`daft-json/src/local.rs:342-375,537-549`、`deserializer.rs:52-64`）；JSON 分块边界只认 `}\n`，异常行回退单线程（`local.rs:554-566`）；异步路径逐行 String（`read.rs:579-649`）
- Window Agg 逐 group take+broadcast 三重拷贝（`sinks/window_base.rs:97-113`）；rank 每表达式重评估 order_by（`window_order_by_only.rs:130-152`）
- 分区写每 morsel 全量 concat+hash+整行比较（`daft-writers/src/partition.rs:51-105`）
- CSV/JSON 每 chunk 重新 bind 谓词（`daft-csv/src/local.rs:942-944`）
- 重试叠加 4×25×3 无总时限（`daft-io/src/retry.rs:30` + `s3.rs:255`）
- **死代码/半成品**：`daft-logical-plan/src/optimization/rules/push_down_filter.rs:194-196` 的 `let _ = new_pushdowns.with_pushed_filters(...)`——结果被丢弃，pushed_filters 字段全链路无消费者
- 已确认非问题：Parquet 写路径已按 row group 合批（`daft-writers/src/lib.rs:147-174` TargetBatchWriter），第一轮 D18 关闭
- 未覆盖：daft-parquet 页级解码内核、CSV slab pool 前半部、分布式调度器（scheduler_actor）、SQL 计划构建、huggingface xet

---

## 2. Lance（Rust 列式格式）

仓库：`C:\myprogram\code\lance\lance`（rust/ 下 15+ crates）

### Benchmark 目录
- Python `benchmarks/`：flat（单查询延迟）、auto-ivf-dot（OSS-2249，有 PROTOCOL.md+RESULTS.md）、bigann、dbpedia-openai、full_report（vbench）、hd-vila（datagen）、sift（有 lance_sift1m_stats.csv 记录结果）、tpch、wiki（索引构建）
- Rust benches：lance（scan/take/random_access/vector_throughput/ivf_pq/manifest_commit/mem_wal/frag_reuse/merge_insert 等）、lance-encoding（decoder/encoder/take/compression_levels/scheme_comparison）、lance-io（scheduler，STORE_URI 可指对象存储）、lance-index（22 个：hnsw/pq/sq/inverted/btree/zonemap 等）、lance-linalg、lance-file、lance-select（row_addr_mask 专门暴露 O(N) 弱点）、lance-table、lance-core
- Python `python/python/benchmarks/`（pytest-benchmark：test_scan 含 **scan_reader_bridge** 直测 Python↔Rust 桥开销）
- CI：`rust-benchmark.yml` 每日只跑 lance-linalg + lance-index——**核心数据路径（scan/解码/take）不在 CI 守护内**

### Top 优化点
1. **dict 编码决策 hll.count() 每值调用**：`rust/lance-encoding/src/array_encoding/strategy.rs:780-802`——每个字符串值插入后全量遍历 4096 个 HLL 寄存器（O(N×4096)），且在写主线程。低基数大列（恰是 dict 编码目标场景）单页单列 2-6 秒纯 CPU，写吞吐可提升数倍。HashSet 阈值判定替代。0.5 人日。
2. **FSST 解码 8x 缓冲清零 + 双拷贝 + 不截断**：`rust/lance-encoding/src/array_encoding/physical/fsst.rs:88-118` + `data.rs:946-967`——`vec![0; len*8]` 预清零、解压后整缓冲 extend_from_slice 再拷一次、Arrow 值缓冲不按实际输出截断（内存 8x 驻留）。每 GiB 压缩数据 ~0.8-1.1s 纯 memset/memcpy，字符串扫描 20-35%。含上游 TODO 自认。1-2 人日。
3. **compaction 默认 Reencode**：`rust/lance/src/dataset/optimize.rs:163-168,376-399`——binary copy 已完整实现（`optimize/binary_copy.rs`）但需 opt-in；默认全列 decode→re-encode。翻默认值可省 compaction CPU 80-95%。1.5 人日（回归面大，保守方案先改推荐配置）。
4. **flat KNN mask 全真仍整批深拷贝**：`rust/lance/src/io/exec/knn.rs:1052-1055`——NaN/null 检查后无条件 `filter_record_batch`（含向量列），数据完好时 61MB/批纯拷贝。单查询 20-30%。0.5 人日。
5. **PQ 距离表每 (partition,query) 重建**：`rust/lance-index/src/vector/pq/storage.rs:756-794` 调用链 `flat/index.rs:298-308`——Dot/无 residual 路径表只依赖 (query,codebook)，却每分区重建；批查询 Q=100×P=20 时 95% 构建纯冗余。`IndexTiming::QueryPrepare` 计时器现成（E1 成本为零）。1.5 人日。
6. **blob 双趟解析 + 串行 await + 双 HEAD**：`rust/lance/src/dataset/blob/materialize.rs:434-437,552-575,808-841`——estimate 趟完整解析每个 blob（size 其实在 descriptor 里），materialize 趟再来一遍。take_blob bench 现成。2 人日。
7. **Python 桥每 batch spawn 链**：`python/src/executor.rs:77-120`（每调用 2×tokio spawn+1×spawn_blocking+channel，30-100µs）+ `reader.rs:142-163`（每 batch 同构）。scan_reader_bridge bench 直测。2 人日。
8. **压缩定宽页三段拷贝**：`rust/lance-encoding/src/array_encoding/physical/value.rs:130-145`——整页压缩数据 to_vec + 每区间再拷。压缩列扫描 10-25%。0.5 人日。同类：FSST 写侧 2x 预清零（`fsst.rs:165-167`）。
9. **全列元数据解码门限**：`rust/lance-file/src/reader.rs:830-859`——投影 ≥25% 列即整段元数据+全列 protobuf 解码（含上游 TODO）。宽表点查打开开销 20-50%。Indexed 按需路径已存在，调门限/惰性化。2 人日。
10. **三层重试 16x 无退避**：`rust/lance-io/src/object_reader.rs:119-133`（内层固定 3 次无 sleep）× 外层 3 × store RetryConfig——故障路径单读 16 次请求，过载雪崩放大。0.5 人日。

### 第二梯队（择要）
- bitpack 非对齐随机解码标量逐位循环（`bitpack.rs:400-513`，随机 take 5-10x 空间）
- 字典解码逐行 Option 循环+cast（`dictionary.rs:202-245`）；写侧 SipHash（`:357-400`，twox-hash 已在依赖）
- ngram Regex posting 位图深拷贝（`lance-index/src/scalar/ngram.rs:704-707`，0.5 人日）
- RowAddrMask O(N)（`lance-select/src/mask.rs:360+`，团队 bench 自证线性，改动大先 E1）
- HNSW prefilter 暴力 flat_search 未用 fused fast-scan（`hnsw/builder.rs:636-700` vs `flat/index.rs:170-187`）
- delta 索引逐段重复 IVF 分区排名（`io/exec/knn.rs:1454-1499`，TODO 自认）
- FSST variable_width_bytes 固定 x8 污染批次预算（`fsst.rs:65-72`）
- 本地读 spawn_blocking/次 + Windows seek_read 双 syscall（`lance-io/src/local.rs:323-352,411-440`）
- AIMD 节流每 GET 两把 Mutex + IoTrackingStore 非测试构建克隆即弃（`object_store/throttle.rs:380-407`、`utils/tracking_store.rs:431-452`）
- s3+ddb 每次 Dataset open 重建 DDB client（`dataset/builder.rs:587-600`，50-200ms/open）
- commit 每次防御性全链（`src/io/commit.rs:1782-1962`）
- mem_wal：bloom 逐行 4 次 downcast（`mem_wal/memtable.rs:885-911`，还有 Float/Timestamp 不哈希的正确性缺口）；WAL schema 元数据每 append 克隆（`wal.rs:1594-1639`）
- 未覆盖：HNSW 完整游走、RQ/Rabit、FTS 查询侧、uring 路径、写路径全链、分布式构建

---

## 3. PyO3（Rust↔Python 绑定）

仓库：`C:\myprogram\code\py03\pyo3`（v0.30.0 开发版）

### Benchmark 目录
- `pyo3-benches/benches/`（codspeed-criterion-compat，21 个文件）：attach/attach_scaling、call、extract、frompyobject、intopyobject、bigint/int128/decimal、list/tuple/dict/set、intern、pyclass、err、critical_sections、comparisons、any、pystring_from_fmt——CI 在 ubuntu **Python 3.14t free-threaded** 上跑（benches.yml，每 PR）
- `pytests/`（pytest-codspeed，32 个基准）：test_pyfunctions 21 个（含 many_keyword_arguments 16-kwarg）、test_pyclasses 11 个（getter/setter/proto/method）
- **缺口**：无 bench_coroutine / bench_buffer / bench_gc——本轮三条重点路径零覆盖，任何差分需先补 bench

### Top 优化点
1. **Vec/dict/set 提取走通用迭代器协议**：`src/conversions/std/vec.rs:83-99`——`try_iter()` 每千元素 list：GetIter 分配 iterator 对象 + 1000 次虚拟分派 + 2000 次 incref/decref；exact list/tuple 快路径可省 50-65%（12-23µs/千元素）。全生态每个容器参数绑定受益。2-3 人日。
2. **PyBytes::new_with 双写**：`src/types/bytes.rs:118-134`——`PyBytes_FromStringAndSize(NULL)` 后 `write_bytes(0)` 零填整个 buffer，init 随后全部覆写。序列化/压缩输出类高频调用每 n 字节省一次 memset（10MB ≈ 1ms）。MaybeUninit 变体。1 人日。
3. **free-threaded 逐元素 critical section**：`src/types/list.rs:599-613`、`dict.rs:537-549`——每元素 2 次 FFI + per-object 锁（3.14t 上每元素 ~50-100ns 额外）。fold 特化已示范整轮一锁可行，扩到 collect/批量 API。3 人日。
4. **引用池 push 侧全局 Mutex**：`src/internal/state.rs:219-228`——`register_decref` 逐对象锁（drain 侧已用 dirty 标志优化过，push 侧未动）。thread-local 缓冲批量搬移，锁摊销 50-100x。3 人日。
5. **rust_decimal 先试 i128 必败**：`src/conversions/rust_decimal.rs:74-85`——Decimal 无 __index__，`obj.extract::<i128>()` 永远 Err（PyErr fetch 物化+drop ~0.2-0.5µs/次）。先 `is_exact_instance_of::<PyInt>()` 分流。ROI 最高：0.5 人日。
6. **协程每 yield 创建 asyncio.Future**：`src/coroutine.rs:111-118` + `coroutine/waker.rs:60-68`——每个 Pending poll ~0.8-1.2µs 的 Python 调用链（get_running_loop+create_future+GetIter）。缓存 bound method / `_get_running_loop` C 入口省 20-40%。配套：waker 同线程仍 `call_soon_threadsafe`（写 self-pipe，`waker.rs:71-92`）。2-3 人日。
7. **lazy PyErr normalize 无条件 detach**：`src/err/err_state.rs:90-135`——首次 inspect 无条件释放再重取 GIL 防死锁，仅 Once 真竞争才需要。乐观快路径。1-2 人日。
8. **tp_traverse 每对象 MRO 走读**：`src/impl_/pymethods.rs:449-508`——上游 TODO 自认；编译期解析 super traverse 省 5-15ns/对象/遍历（10^7 实例一次 gen2 收集 50-150ms 的一部分）。3-4 人日。
9. **PyErr 构造 Box 分配**：`src/err/err_state.rs:37-48`——lazy 闭包+Once+Mutex 每次错误构造。小 enum 内联。1-2 人日。
10. **trampoline 簿记**：`src/impl_/trampoline.rs:302-321`——每次 Python→Rust 调用 catch_unwind+3 次 TLS RMW。合并读+增。生态乘数最大但单次最小（3-8ns）。2 人日。

### 第二梯队（择要）
- dict 提取每 entry 2 次无效 incref（`types/dict.rs:499-516`，Borrowed 中转）
- PyBuffer::get 每次 Box 堆分配（`src/buffer.rs:530-546`，作用域 API 消除）+ 无条件请求 PyBUF_FULL_RO（`:538`）+ is_c_contiguous 未内联 FFI（`:741-749`）
- BorrowChecker 共享借用 CAS 循环可换 fetch_add（`pycell/impl_.rs:62-99`）
- 所有 pyclass 无条件 HAVE_GC（`pyclass/create_type_object.rs:91`，10^7 实例 160-240MB GC 头；`no_gc` opt-out）
- pyclass DOC 常量逐字节 const 复制（编译时间税，`pyo3-macros-backend/src/pyclass.rs:3075-3083`）
- freelist 全局 Mutex（`src/impl_/freelist.rs:20-29`）
- 已确认非问题：trampoline 单态化（#6187/#5930 已落地）、String::extract、int extract、get_type incref
- 未覆盖：call.rs 内部、pycell 借用链、sequence/mapping/memoryview 槽、marshal、free-threaded 协程叠加

---

## 4. SGLang（LLM 推理服务）

仓库：`C:\myprogram\code\sglang\sglang`（核心 `python/sglang/srt/` + Rust `sgl-router`/`sgl-model-gateway`/`rust/`）

### Benchmark 目录
- `python/sglang/benchmark/serving.py`（2893 行，vLLM 移植：TTFT/TPOT/ITL/goodput，8+ 数据集）与 `one_batch_server.py`（1610 行：批×长网格、**分 stage profile**、/metrics 抓取、CI 结果落盘）
- `benchmark/`：kernels（20+ 子目录：attention/fused_moe/sampling/deepep/allreduce 等 + scheduler_batch 两个调度器小 kernel）、scheduler（bench_token_storage）、lora、disaggregation（bench_cached_prefix）、hicache、dllm、simulator、prefill_only、adaptive_speculative 等
- `test/` + `nightly_bench_utils.py`（夜间回归挂载点；diffusion/H100/B200 有 baseline 守护，**decode 主路径无硬性守护**）
- Rust：sgl-router/benches（tree_lookup/policy_select + BENCHMARKS.md 数据）、sgl-model-gateway/benches（7 个）、rust/sglang-bench（Rust 版压测客户端，对照 Python 18.2k vs 2.2k tok/s）
- **缺口**：无 per-step 调度器 CPU 计时器；rust_server/push_generation 路径零基准

### Top 优化点
1. **FlashInfer EAGLE verify 免同步 host-plan 默认关闭**：`layers/attention/flashinfer_backend.py:888-902` + `environ.py:1566`（`SGLANG_OPT_FLASHINFER_VERIFY_HOST_PLAN` 默认 False）——功能已实现（DFLASH 已默认启用同款），默认路径每次 verify replay ≥3 次阻塞 D2H drain。0.5 人日翻默认+补测试。
2. **encoder-decoder can_run_graph 隐式 GPU 同步**：`model_executor/runner/decode_cuda_graph_runner.py:901-925`——`torch.all(encoder_lens>0)` 在 Python and 链中触发 bool()→.item()→D2H，每步 2-3 次。host 镜像判断。0.5 人日。
3. **NPU DeepEP 每层每步 inspect.signature**：`layers/moe/token_dispatcher/deepep.py:577-599`——每次 MoE dispatch 反射 buffer.dispatch 签名（~30-100µs）；58 层 MoE ≈ 2.9ms/forward。init 缓存。0.5 人日。
4. **每步两次全量 filter_batch 重扫**：`managers/scheduler.py:3963-3966`（注释自认"Every step"）+ `schedule_batch.py:3840-3861`——绝大多数步无请求结束，纯重扫 80-160µs/步。process 循环本就逐 req 调 finished()，加脏标志短路。0.5 人日。
5. **每 decode 步 0.4-1ms 的 Python 簿记族**（合并修，各 0.1-1 人日）：`_reqs_finishing_inflight` 每步重建 set（`scheduler.py:4444-4457`）、per-token 列表装箱 `[[t] for t in ids]`（`batch_result_processor.py:1124-1131`）、stream accumulator 每步重建 40 list+time_stats pickle（`output_streamer.py:337-438`）、`batch.copy()`+getattr 快照（`schedule_batch.py:3992-4060`）、`config_value("weight_version")` per-req（`tokenizer_manager.py:2511`，0.1 人日）
6. **stop-str 检测**：`schedule_batch.py:1843-1852` O(k²) 切片 + `:1819-1828` per-req tokenizer.decode——带 stop 流式 1-3ms/步（B 类路径单次成本最大 CPU 项）。批量化/增量匹配。0.5+2 人日。
7. **router capture_snapshot 全表重算**：`sgl-router/src/state/load_monitor/engine_reported_load.rs:570-578,385-399`——每请求全表扫描+全量 clone（60KB 分配/请求@512 entry）；version 原子已维护却没用于缓存。1-2 人日。
8. **router block_hashes 逐 token SHA256 update**：`sgl-router/src/state/kv_events/hash.rs:112-146`——cache-aware 路由每请求全 prompt 链式哈希，per-token 4 字节 update。缓冲化 1.3-2x + 前缀记忆化。1 人日。
9. **rust_server 累计流 O(T²) clone**：`rust/sglang-server/src/api_server/http/native_api.rs:628-633`——默认非 incremental 时每中间帧全量 clone 已累积 text+token_ids（T=256 时 ~130KB/请求）。Bytes 化。1-1.5 人日。
10. **router 双跳序列化**：`sgl-router/src/server/routes/chat/preparation.rs:182-189,357-362`——struct→Value 树→to_vec，省 ~10-20µs/请求（10k RPS ≈ 0.1-0.2 核）。0.5-1 人日。

### 第二梯队（择要）
- spec decode 每周期 3 个 O(bs) 循环+2 次新 CPU tensor（`mem_cache/allocation_sizing.py:81-93`、`speculative/eagle_utils.py:873-877`、`allocation.py:846-848`）
- can_run_graph/graph key 每步重复算 2-3 次（`eagle_utils.py:399-408`、`model_runner.py:1991-1995`、`decode_cuda_graph_runner.py:876-885`）
- padded replay（raw_bs≠桶）退化为全量 eager 元数据准备（`decode_cuda_graph_runner.py:1743-1750`，非 2 幂 batch 常见）
- PD-decode pop_preallocated 每 tick 全量重算（`disaggregation/decode.py:1358-1375`）；PD-prefill 每 tick 全量 poll bootstrap 队列（`disaggregation/prefill.py:497-521`）
- debug 族打包修：envs 每次 os.getenv（`environ.py:67-77`）、`_trace_e2e_sampler` eager kwargs（`layers/sampler.py:176-180`）、maybe_detect f-string（`eagle_worker_v2.py:1215-1222`）
- write_req_to_token_pool O(bs²) cumsum（`kernels/ops/memory/common.py:39-42`，代码自带 NOTE）；get_loads 全队列求和（`load_inquirer.py:135-146`）；grammar mask 每步全量分配（`sampling_batch_info.py:388-407`，reset_vocab_mask 接口已存在未接线）
- FlashInfer decode 每步 CPU cumsum+partial 探测（`flashinfer_backend.py:1823-1836`）；draft 每步重建 ForwardContext（`eagle_worker_v2.py:838-846`）；`_forward_raw` 每次重放 shared_experts 覆盖（`model_runner.py:1982-1985`）
- router metrics 5-7 次全局 Mutex+label clone（`server/metrics.rs:634-727`）；workers_for 每请求 Vec+Arc（`workers/registry.rs:166-175`）；SSE 扫描器每行 concat 分配（`proxy/sse.rs:117-133`）
- 未覆盖：trtllm_mla/dsa 等 backend 内核侧、MoE runner 胶水、NCCL 侧、mooncake/nixl 传输内部、dflash/dspark 等 spec 算法

---

## 5. scx（sched_ext 调度器）

仓库：`C:\myprogram\code\sched-ext\scx`（16 主线 + 9 experimental Rust 调度器 + lib/ 共享 BPF C 库）
成本模型：上下文切换 5 万/s 量级，BPF 侧每条指令/原子/查找被放大；100ns×5 万/s = 0.5% 单核当量。

### Benchmark/测量工具目录
- **scxtop**（tools/，BPF 剖析器：sched_switch/wakeup/DSQ kprobe + Perfetto 导出）、`scripts/dsq_lat.bt`/`process_runqlat.bt`/`slicesnoop.bt`/`vtime_dist.bt`（bpftrace）、`bpftrace_stress_wrapper.sh`（CI 风格跑批骨架）
- **scx_cake bench/**（树内唯一成套测量）：wakecost.c（唤醒路径 E1 仪器，p10-p99）、cakeload.c（负载协变量）、arith/storage/qmark_price.c（结构定价）、fnspills.py（BPF 栈成本）、migrate_cause.py（归因）、docs/AUDIT_OPERATION_COST（0-9 级成本阶梯）——A/B 打分套件已迁外部 receipt-gated 项目
- **BPF_ENABLE_STATS 逐 callback 计时**（experimental scx_cidland 540cd82bc，可直接移植到 lavd/p2dq/layered——这是验证几乎所有候选的现成 E1）
- layered 内建 lstat 计数器；pgo-lto.sh（Rust 侧 PGO+LTO）
- 结论：除 cake 外其余调度器连每回调成本都没有测量仪器

### Top 优化点
1. **p2dq ATQ 把 taskc 指针当 pid（正确性 bug）**：`scheds/rust/scx_p2dq/src/bpf/main.bpf.c:2991-2992,3123-3124`——`scx_atq_peek/pop` 返回 taskc 指针，dispatch 路径直接当 pid 传 `bpf_task_from_pid`（对照正确用法 `:2713-2714`）。atq_enabled 时任务被移出队列后查找失败→滞留。0.1 人日。
2. **layered 三个热回调无条件原子 xchg 全局标志**：`scheds/rust/scx_layered/src/bpf/main.bpf.c:620-629`，调用点 1678/1897/2716——`maybe_refresh_layer_cpumasks` 每次 wakeup×2+dispatch×1 做独占 cacheline 写；同文件 1628 已示范 READ_ONCE 模式。半人日修，~1.5% 单核当量+消除跨核串行点。
3. **p2dq debug 默认=2**：`scheds/rust/scx_p2dq/src/bpf/main.bpf.c:178`——`const volatile u32 debug = 2` 使 trace 宏全开；任何绕过 rodata 覆盖的加载路径每次 select_cpu 一次 bpf_printk（5-15% 单核）。默认改 0，0.1 人日。
4. **p2dq fork/exec 均衡 O(nr_cpus)×2 全扫**：`main.bpf.c:1312-1333`——两个位图 AND 即可替代的逐 CPU 循环；构建类负载（fork 率 1k-1 万/s）上限 0.3-30% 单核。1 人日。
5. **pandemonium dispatch 无条件 sweep：每 dispatch 16 次跨核行读**：`scx_pandemonium/src/bpf/main.bpf.c:2150-2184`（"UNCONDITIONAL…DO NOT REORDER"）——8+8 个被高频写的远端 qclock 行；lag_cap 是 8-80ms 级而扫描每 dispatch 都做。时间限速（文件内 last_spill_scan 模式现成）。1 人日。
6. **forge 全部统计计数器挤同一 BSS 行**：`scx_forge/src/bpf/main.bpf.c:149-155` + 调用点 1157/1178/1305——每 enqueue/dequeue/dispatch 一次争用原子。percpu 计数。0.5 人日。
7. **cgroup_bw accounting timer 全树遍历**：`lib/cgroup_bw.bpf.c:2921-2953`（→2137-2299）——每 1-20ms tick 全 cgroup 树 post-order×2 + O(N×(LLC+树高)) 哈希查找；2000 cgroup+1ms 间隔可达 75-225% 单核当量（仅 --enable-cpu-bw）。改增量维护的受管 id 数组。2-3 人日。
8. **p2dq dispatch 全 CPU 扫窥同 LLC affn_dsq**：`main.bpf.c:2942-2966`——无提前退出的 95 次（96 核）cpumask test+percpu lookup+DSQ peek ≈ 7µs/dispatch ≈ 0.35 核。非空位图提示。2-3 人日。
9. **mitosis 热路径杂项族**：`scx_mitosis/src/bpf/mitosis.bpf.c:1455/1509/1148`（stopping 3 次时钟中 2 次冗余+除法+set_dsq_vtime 权威检查，cake 实测同款 +28-36%）、`:585-615`（每唤醒 RCU+cgroup 三跳链验证不变量，cgroup 迁移 tracepoint 已存在可接失效）、`:1194-1218`（dispatch 无条件 2 次 DSQ peek）。合计 2-4% 单核当量。
10. **lavd/rusty/layered 共享行族**：lavd quiescent 双时钟（`scx_lavd/src/bpf/main.bpf.c:1913,1925`，0.1 人日）；p2dq 自踢 SCX_KICK_IDLE（`main.bpf.c:2429-2432`，cidland 同款 E2：锁争用 19%→11%）；layered queued_runtime/vtime_now 原子对（`:2176-2177`/`:3611-3613`）；rusty 域 bucket 锁内 ravg（`scx_rusty/src/bpf/main.bpf.c:378-385`）。

### 第二梯队（择要）
- layered：consume 风暴（每 dispatch 最多 40 次空 move_to_local，`:2818-2890`）、big.LITTLE 每次选 CPU 分配临时 cpumask（`:1314-1319`）、PRNG per layer（`:2571`）
- p2dq：PELT 状态挂 HASH 表而非 task_ctx（`:849-907`）、stopping LLC 共享原子族（`:2578-2597`）
- lavd：futex 全系统钩子无采样（lock.bpf.c:141-377，游戏负载 0.23 核）、cpdom 双共享原子（`main.bpf.c:721-812`）、ravg 读写（`:406-634`）
- cosmos：NUMA 模式每次 enqueue/dispatch HASH 查 node（`main.bpf.c:259-271`，rodata 数组替代）、running 无条件 cpuperf_set（`:1602-1639`）、5 次变除数除法（`:1149-1158`）
- pandemonium：每周期 8-10 次 ktime（`:1029-2091`）+8-10 次 knobs/stats map 查找（`:656-666`）；拓扑表全经 map lookup（`:742,982,2211`）
- tickless：dispatch 每 candidate bpf_task_from_pid（`:277-331`，verifier workaround）、主 CPU kick 全机扇出（`:359-395`）
- lib：DHQ 读取性 peek 拿排他锁（`lib/dhq.bpf.c:323-335`）+单锁罩全操作；ATQ FIFO 用 rbtree（单调键，`lib/atq.bpf.c:84-87`）；sdt_alloc 第二次全局锁只为统计（`lib/sdt_alloc.bpf.c:614-627`）；bitmap 全 __weak 非内联
- cake（自审极充分，仅增量）：frontier 离群全机扫（`cake.bpf.c:3019-3045`）、CCD 分支重读 qmask（`:2506-2530`）
- rustland 忙等 1 核（设计成本，`:365-376`）
- **可移植手法清单**（experimental→主线）：每 op 读一次时钟（cidland，ktime 占 3.8% cycles）、running 读缓存 rq 时钟（cake -3.1ns）、删热路径诊断原子、BPF_ENABLE_STATS 计时、mlfq 锁聚合→无锁虚拟时钟（1545→865 指令）、edq 锁外咨询快照+trylock、不踢自己的 rq、zero-alloc 断言、map 数组→arena 全局
- 未覆盖：bpfland/eevdf/flash/chaos/beerland、C 侧调度器（central 全机队列锁值得关注）、lib/alloc 子库、cgroup_bw 生命周期全链

---

## 6. 跨库共性模式（横向观察）

1. **默认值即杠杆**：lance compaction Reencode、sglang verify host-plan 关、scx debug=2、daft maintain_order=true、daft IO 8 线程——四个库都存在"优化已实现但默认不启用"或"保守默认在高核数/新场景下失衡"
2. **"每 X 固定开销"族最普遍**：每 morsel（daft to_field/size_bytes）、每步（sglang filter_batch/set 重建）、每值（lance hll.count）、每请求（router capture_snapshot）、每回调（scx ktime×N、map lookup×N）——都是"结果不变却每次重算"
3. **写侧清零/双拷贝**：FSST 8x 预清零、PyBytes 先零填再覆写、shuffle 消息 memset——同一模式三处出现
4. **测量缺口比优化缺口更稀缺**：pyo3 无协程/缓冲/GC bench、sglang 无 per-step CPU 计时、lance 核心数据路径不在 CI 守护、scx 除 cake 外零回调成本仪器、sgl-router BENCHMARKS.md 自述"nothing runs or gates on these results"——每个库的第一投入建议都包含补 1-2 个守护
5. **正确性即性能**：p2dq ATQ bug、mem_wal Float 不哈希、daft JSON 边界回退单线程、scx DHQ seq 锁外自增

## 7. 证据与可信度声明

- 全部为 E0（静态+成本模型），上界是数量级推导；每条候选在原始报告（两轮 subagent 交付物）中均标注了升 E1 所需的最小测量
- 21 处代码级抽查全部通过（每轮各挑 Top 候选验证 file:line 与代码内容）；未发现任何编造引用
- 各 agent 已用 git log 核对近期 perf 提交，已修项均已剔除或标注
- 如需第三轮：daft parquet 页级解码/分布式调度器、lance HNSW 完整游走/uring/写路径全链、pyo3 call.rs/pycell、sglang backend 内核/NCCL/传输引擎、scx bpfland/flash/eevdf/C 侧调度器
