# Agentic Finance 面试讲解提纲 + 高频问题答辩手册

本手册用于面试实战：

1. 快速讲清项目价值与架构（10 分钟版）
2. 深入回答技术追问（20 分钟版）
3. 现场跑出可验证结果（命令 + 操作步骤）
4. 用代码事实支撑每个论点

---

## 1. 面试展示目标（先统一评委注意力）

建议开场 30 秒先讲这三点：

1. 这是一个 **Skill-first + Workflow-fallback** 的企业数据分析 Agent 系统，不是纯 prompt demo。  
2. 系统把“可用性”和“可评估性”同时做了：在线可执行，离线可回归。  
3. 当前版本重点解决了三件事：
   - 语义契约共享（减少判定漂移）
   - 清洗策略显式化（conservative/strict）
   - 关键列守卫阻断（先防错再执行）

---

## 2. 10 分钟讲解提纲（推荐现场主线）

## 2.1 第 1 分钟：问题定义

- 企业财务数据分析常见痛点：
  1. 上传文件格式不稳定（Excel/CSV、多 sheet、非首行表头）
  2. 对账/对齐依赖人工经验，重复性高
  3. 自动化系统容易“跑错且不自知”

- 我的目标：做一个“既能自动执行、又能解释与回归验证”的 Agent。

## 2.2 第 2-4 分钟：系统架构

按这条链讲：

1. API 入口：`app/server.py`
2. 自动摄取：`app/services/ingestion.py`
3. 语义契约：`app/services/semantic_contract.py`
4. 路由与执行：`app/skills/router.py` + `app/skills/engine.py`
5. 命中 skill（L1/L2/L3/L4）优先执行；未命中走 `app/services/workflow.py`
6. workflow 代码通过 `app/services/trusted_exec.py` 沙箱执行
7. 结果与审计统一导出：`app/services/exporter.py`

一句话总结：
“在线路径保证可用，离线路径保证可证据化。”

## 2.3 第 5-6 分钟：核心设计亮点

1. **Skill-first 稳定性设计**  
   - 路由规则：`app/skills/router.py`（L1/L2/L3/L4关键词）
   - 调度中心：`app/skills/engine.py`

2. **语义契约共享**  
   - `ensure_semantic_contract` 在请求内复用语义推断，减少重复推断和结果漂移（`app/services/semantic_contract.py`）

3. **关键列守卫（Fail Fast）**  
   - `app/skills/column_guard.py`
   - L2/L3/L4 执行前检查必须列，缺列直接阻断并回传证据，不盲跑

## 2.4 第 7-8 分钟：可信执行与安全

1. 上传侧安全：文件名净化、后缀白名单、大小限制（`app/server.py`）
2. 下载侧安全：`session_id + token` 双绑定（`app/server.py`）
3. 执行侧安全：AST 校验 + 受限 builtins + import allowlist + 子进程超时中断（`app/services/trusted_exec.py`）

## 2.5 第 9 分钟：评估体系

1. 基线数据集：`golden_dataset/manifest.json`（当前 `v1.0.9`）
2. 批量评估：`golden_dataset/run_evaluation.py`
3. 断言引擎：`golden_dataset/evaluator/assertions.py`
4. 产物：`scorecard_<run_id>.csv` + `summary_<run_id>.json`

## 2.6 第 10 分钟：真实 tradeoff + roadmap

1. tradeoff：
   - 确定性技能提高稳定性，但灵活性低于全自由代码生成
   - 语义层提升鲁棒性，但带来模型依赖
2. roadmap：
   - P1：结构化 tool-calling 编排
   - P1：确定性对账模板增强
   - P2：持久化与可观测面板

---

## 3. 20 分钟深挖提纲（面试官追问时用）

## 3.1 在线执行链路（端到端）

1. `/upload`：
   - 自动摄取 + 建 session + 构建 workflow
   - 刷新语义契约缓存（`invalidate_semantic_contract`）
2. `/chat`：
   - `ensure_semantic_contract`
   - `route_skill` 判定
   - `execute_skill` 调用对应技能
   - 未命中则 `workflow_app.stream`
3. 执行结束后统一 `build_export_response` 导出

## 3.2 L1/L2 深挖

1. L1 有策略层：`CleaningPolicy`
   - 默认 conservative，减少误删
   - strict 可通过用户关键词触发
2. L2 在执行前做关键列守卫
3. 对齐逻辑采用 fuzzy 映射并审计记录

## 3.3 L3 深挖

1. 系统/银行表自动角色识别（名字 + 语义标签打分）
2. 主键/金额列守卫
3. 容差解析（支持“容差X”或“X元忽略”）
4. 先多对一聚合，再 `smart_reconcile`

## 3.4 L4 深挖

1. 自动选最适合趋势分析的表（有 date + amount）
2. 缺日期/金额列直接阻断
3. 聚合成月度趋势，输出图和结论

## 3.5 Workflow fallback 深挖

1. 仍保留 Supervisor-Worker 通用能力
2. 错误上下文会回注到下一轮代码生成（自愈）
3. `MAX_EXEC_RETRIES=3`

## 3.6 评估深挖

1. `run_evaluation.py` 的 preflight 先校验 `/health`
2. case 批跑逻辑：upload -> chat -> download -> snapshot -> assertions
3. 局限要诚实说：`retry_count` 目前按“自愈”文本计数（启发式）

---

## 4. 现场演示脚本（可直接复制执行）

以下以项目根目录为前提：

```bash
cd /Users/dexter/Documents/Dexter_Work/Data_Analysis_Agent
```

## 4.1 启动后端

```bash
uvicorn app.server:app --reload --host 0.0.0.0 --port 8000
```

预期：终端看到 Uvicorn 启动日志。

## 4.2 健康检查

新开终端执行：

```bash
curl -s http://localhost:8000/health
```

预期字段：

1. `status`
2. `llm_ready`
3. `model`
4. `active_sessions`

## 4.3 一键回归评测展示（推荐）

```bash
python golden_dataset/run_evaluation.py --api-url http://localhost:8000
```

运行结束后展示：

```bash
ls -lt golden_dataset/runs | head
cat golden_dataset/scorecard_latest.csv
```

你可以口头解释：

1. 每个 case 的 `success / retry_count / latency_seconds / audit_log_present`
2. 失败原因如何进入 `notes`

## 4.4 API 交互演示（skill 路由可视化）

### 4.4.1 创建会话 ID

```bash
SESSION_ID=$(python - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)
echo $SESSION_ID
```

### 4.4.2 上传 L1/L2 用例文件

```bash
curl -s -X POST "http://localhost:8000/upload" \
  -F "session_id=$SESSION_ID" \
  -F "files=@golden_dataset/cases/cleaning_merge/销售台账_脏数据.xlsx" \
  -F "files=@golden_dataset/cases/cleaning_merge/客户主数据_标准库.xlsx"
```

预期：返回 `Upload success` 和每个文件行数。

### 4.4.3 触发 L1/L2（conservative）

```bash
curl -s -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d "{\"session_id\":\"$SESSION_ID\",\"message\":\"请保守清洗后做主数据对齐并导出\"}"
```

预期：

1. 命中 `l1_l2_hygiene_merge`（关键词：清洗/对齐）
2. 返回分析结论 + `download_url` + `audit_summary`

### 4.4.4 触发 L3（容差对账）

先上传 L3 文件（可复用同一 session）：

```bash
curl -s -X POST "http://localhost:8000/upload" \
  -F "session_id=$SESSION_ID" \
  -F "files=@golden_dataset/cases/reconciliation/系统日记账_复杂版.xlsx" \
  -F "files=@golden_dataset/cases/reconciliation/银行流水_复杂版.xlsx"
```

执行对账：

```bash
curl -s -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d "{\"session_id\":\"$SESSION_ID\",\"message\":\"请做容差5元对账，输出差异明细\"}"
```

预期：

1. 命中 `l3_reconcile`
2. 返回状态分布文本
3. 可下载带审计的 Excel

### 4.4.5 触发 L4（趋势图）

上传 L4 文件：

```bash
curl -s -X POST "http://localhost:8000/upload" \
  -F "session_id=$SESSION_ID" \
  -F "files=@golden_dataset/cases/visualization/区域月度经营数据_2024.xlsx"
```

执行趋势分析：

```bash
curl -s -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d "{\"session_id\":\"$SESSION_ID\",\"message\":\"请分析GMV趋势并解释异常波动\"}"
```

预期：

1. 命中 `l4_visual`
2. 返回 `chart_jsons`
3. 返回峰值/低谷结论和 `download_url`

---

## 5. 面试中建议展示的“操作顺序”

## 5.1 12 分钟标准展示

1. 1 分钟：项目目标 + 架构图口述
2. 2 分钟：`/health` + 关键代码路径
3. 3 分钟：跑 `run_evaluation.py`，展示 scorecard
4. 4 分钟：做一次 L3 API 交互（上传+对账）
5. 2 分钟：讲 tradeoff 与 roadmap

## 5.2 如果只剩 5 分钟

1. 先跑 `run_evaluation.py`
2. 打开 `golden_dataset/scorecard_latest.csv`
3. 快速讲三点：skill-first、语义契约、列守卫

---

## 6. 高频问题 + 参考答案（严格对齐代码事实）

## Q1：你的系统核心架构是什么？

A：核心是 skill-first。`/chat` 先 `ensure_semantic_contract`，再 `route_skill`，命中就走 `execute_skill` 的确定性链路；未命中才进 `workflow` 自由代码生成链路。入口都在 `app/server.py`。

## Q2：为什么要 skill-first，而不是完全依赖 LLM 生成代码？

A：为了稳定性和可回归性。确定性 skill 在评测中更可控，失败模式可定位。通用 workflow 仍保留作为覆盖长尾需求的 fallback。

## Q3：语义契约解决了什么问题？

A：解决“同一请求多处重复推断导致结果漂移”。`semantic_contract.py` 用上下文签名缓存语义结果，skills 与 workflow 共用同一份语义判断。

## Q4：清洗策略怎么避免误删？

A：L1 有 `CleaningPolicy`。默认 conservative，负值/极值更多是告警保留；strict 才激进剔除。策略推断逻辑在 `infer_cleaning_policy`。

## Q5：怎么保证关键输入不足时不乱算？

A：通过 `column_guard`。L2/L3/L4 执行前检查 RequiredColumnSpec，不满足就阻断并返回“当前列 + 语义证据 + 修复建议”。

## Q6：L3 对账的关键实现点？

A：三步：表角色识别 -> 关键列校验 -> 多对一聚合后 `smart_reconcile`。容差由 `_parse_tolerance` 从用户语句提取。

## Q7：L4 可视化为什么做成 skill？

A：为了稳定输出。`l4_visual_skill` 用确定性模板做月度聚合和图表构建，减少自由生成图代码波动。

## Q8：你的安全边界在哪？

A：三层：
1. 上传下载安全（文件名、后缀、大小、token）
2. 执行前 AST 安全校验
3. 子进程执行 + 超时中断

## Q9：trusted_exec 具体阻断了什么？

A：阻断 `eval/exec/open/__import__` 等高风险调用，阻断 `read_excel/to_excel` 等直接文件 I/O，import 只允许白名单。

## Q10：怎么做评估和回归？

A：`run_evaluation.py` 按 manifest 批跑 case，调用 assertions 做下载/审计/图表/sheet 断言，输出 scorecard 和 summary。

## Q11：你如何处理线上失败可观测性？

A：当前有 `Runtime Error` 摘要与 scorecard notes，可定位到 case 级失败原因；下一步会做结构化 telemetry 和面板化监控（README roadmap 已明确）。

## Q12：retry_count 是怎么统计的？

A：当前实现是 `chat_result.response_text.count("自愈")`，是启发式口径。这是已知局限，后续会改成结构化执行指标。

## Q13：为什么评测要先调 /health？

A：避免批跑结束才发现环境问题（比如 `GOOGLE_API_KEY` 未就绪）。这是 preflight fail-fast 设计。

## Q14：如何解释“conservative 默认策略”？

A：业务场景里误删比漏删更危险。默认保守能优先保证数据安全，严格模式由用户显式触发。

## Q15：实体对齐为什么不是纯向量或纯规则？

A：`smart_merge` 是混合策略：fuzz 快速命中、向量召回扩展候选、LLM 最终裁决，兼顾速度与语义泛化。

## Q16：系统目前最大的技术债是什么？

A：
1. session 仍是进程内存，不适合多实例横向扩展  
2. retry/latency 等指标还未完全结构化  
3. 评估断言仍以结构断言为主，业务语义断言可继续增强

## Q17：如果让你下一周落地 3 件事，会做什么？

A：
1. 执行链路结构化 telemetry（真实 retry、分阶段 latency）  
2. 路由器升级为轻量 intent classifier + 置信度  
3. 增加业务语义断言（对账状态分布/KPI 断言）

## Q18：你如何证明“这不是只会写 demo”？

A：我会展示 `golden_dataset/run_evaluation.py` 这套回归框架、版本化 baseline（manifest + expected + changelog），以及每次优化如何通过 scorecard 量化收益。

---

## 7. 面试时可直接引用的代码落点

1. API 主链路：`app/server.py`
2. 路由规则：`app/skills/router.py`
3. 技能分发：`app/skills/engine.py`
4. L1/L2：`app/skills/l1_hygiene_skill.py`
5. L3：`app/skills/l3_reconcile_skill.py`
6. L4：`app/skills/l4_visual_skill.py`
7. 列守卫：`app/skills/column_guard.py`
8. 语义契约缓存：`app/services/semantic_contract.py`
9. 语义推断：`app/services/semantic_infer.py`
10. 摄取层：`app/services/ingestion.py`
11. 安全执行：`app/services/trusted_exec.py`
12. 导出层：`app/services/exporter.py`
13. 评测主程序：`golden_dataset/run_evaluation.py`
14. 断言引擎：`golden_dataset/evaluator/assertions.py`
15. 基线配置：`golden_dataset/manifest.json`、`golden_dataset/expected_snapshots.json`

---

## 8. 一句收尾（建议背下来）

“我这个项目的重点不是把 Agent 跑起来，而是把 Agent 的执行正确性、可解释性和可回归性工程化：在线可执行，离线可验证，失败可定位，策略可演进。”
