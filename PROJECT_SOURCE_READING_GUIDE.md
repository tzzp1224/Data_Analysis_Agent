# Agentic Finance 源码阅读指南（全景版）

本指南的目标是：你只靠这一份文档，就能快速建立对项目全局架构、核心逻辑链路、关键实现原理、设计权衡和未来演进方向的完整认知。

---

## 1. 先用 5 分钟建立全局地图

### 1.1 项目是什么

这是一个面向财务分析场景的 Agent 系统，核心策略是：

1. 优先走确定性技能（Skill-first）来提高稳定性和可回归性。  
2. 未命中技能时，回退到通用 LLM 工作流（Supervisor-Worker + 自愈重试）。  
3. 全链路保留审计信息，最终导出 Excel 交付。  
4. 使用 Golden Dataset 做回归评测，持续量化成功率与质量。

### 1.2 最核心的目录

1. `app/`：在线服务与 Agent 运行时
2. `app/services/`：摄取、语义、执行、安全、导出
3. `app/skills/`：L1/L2/L3/L4 确定性技能
4. `app/utils/`：审计日志、智能匹配与对账工具
5. `golden_dataset/`：评估数据集、断言、批量评测脚本

---

## 2. 推荐阅读顺序（建议按这个顺序读）

### 2.1 第 1 轮（30-45 分钟，先跑通主干）

1. `README.md`
2. `app/server.py`
3. `app/skills/router.py`
4. `app/skills/engine.py`
5. `app/skills/l1_hygiene_skill.py`
6. `app/skills/l3_reconcile_skill.py`
7. `app/skills/l4_visual_skill.py`
8. `app/services/exporter.py`
9. `golden_dataset/run_evaluation.py`

目标：知道“请求怎么进来、怎么分流、怎么出结果、怎么评测”。

### 2.2 第 2 轮（60-90 分钟，理解底层机制）

1. `app/services/ingestion.py`
2. `app/services/semantic_contract.py`
3. `app/services/semantic_infer.py`
4. `app/services/semantic_profile.py`
5. `app/services/semantic_taxonomy.py`
6. `app/services/workflow.py`
7. `app/services/trusted_exec.py`
8. `app/utils/tools.py`

目标：知道“语义怎么推、策略怎么落、代码怎么安全执行、为什么会稳定/不稳定”。

### 2.3 第 3 轮（30-45 分钟，建立质量治理认知）

1. `golden_dataset/manifest.json`
2. `golden_dataset/expected_snapshots.json`
3. `golden_dataset/evaluator/assertions.py`
4. `golden_dataset/evaluator/xlsx_inspector.py`
5. `golden_dataset/CHANGELOG.md`

目标：知道“项目如何定义正确性、如何做回归基线、如何做版本治理”。

---

## 3. 当前架构总览（你需要先记住这条主链）

## 3.1 请求生命周期（在线）

1. 用户上传文件 -> `/upload`  
2. 服务器进行自动摄取（Excel/CSV，sheet/header 自动识别）  
3. 构建语义契约缓存（同请求共享列语义）  
4. 用户发起 `/chat`  
5. skill router 判定是否命中 L1/L2/L3/L4  
6. 命中则执行确定性技能；未命中则进入 workflow（LLM 生成代码）  
7. 若 workflow 路径则由 trusted executor 沙箱执行代码  
8. 将业务表 + 审计日志 + 结果导出为 Excel  
9. 返回文本结论、图表 JSON、下载链接、审计摘要

关键文件：

1. `app/server.py`
2. `app/services/ingestion.py`
3. `app/services/semantic_contract.py`
4. `app/skills/router.py`
5. `app/skills/engine.py`
6. `app/services/workflow.py`
7. `app/services/trusted_exec.py`
8. `app/services/exporter.py`

## 3.2 回归评测生命周期（离线）

1. 从 `manifest.json` 读取 case 和 prompt  
2. 先做 `/health` 预检  
3. 批量 `upload + chat + download`  
4. 用断言引擎检查图表数、审计、sheet、数据行数等  
5. 写出 scorecard CSV 和 summary JSON  
6. 用 run 历史对比优化前后质量

关键文件：

1. `golden_dataset/run_evaluation.py`
2. `golden_dataset/evaluator/http_api.py`
3. `golden_dataset/evaluator/assertions.py`
4. `golden_dataset/evaluator/xlsx_inspector.py`

---

## 4. 分层读懂代码：每层解决什么问题、代码在哪里

## 4.1 接口编排层（API 网关）

文件：`app/server.py`

职责：

1. 管理 session 生命周期与上下文
2. 提供 `/upload` `/chat` `/download` `/health`
3. 控制 skill-first 与 workflow-fallback 路由
4. 组织响应结构（结论、图表、下载链接、审计摘要）

你要重点看：

1. `SessionData`：上下文、备份、下载 token、TTL 资源回收
2. `upload_files`：文件安全校验 + 自动摄取 + 初始化 workflow
3. `chat`：
   - 先 `ensure_semantic_contract`
   - 再 `route_skill`
   - skill 未命中才跑 `workflow_app.stream`
4. `build_export_response`：统一导出和审计摘要拼装

理解点：

1. `dfs_context` 是系统状态总线，技能与 workflow 都围绕它读写。
2. `__last_result_df__` / `__last_audit__` 是执行阶段和导出阶段的桥梁键。

---

## 4.2 数据摄取层（Ingestion）

文件：`app/services/ingestion.py`

职责：

1. 识别文件类型（Excel/CSV）
2. 自动选择 sheet
3. 自动识别 header 行（LLM 优先，启发式兜底）
4. CSV 分隔符探测、编码回退

关键函数：

1. `propose_ingestion_config`
2. `_propose_excel_ingestion_config`
3. `_propose_csv_ingestion_config`
4. `_detect_header_row_by_heuristic`
5. `apply_ingestion`
6. `load_file`

原理：

1. Excel：先 LLM 选 sheet，再用 LLM/heuristic 判定 header
2. CSV：先 sniff delimiter，再预览行内容判定 header
3. 最终用统一 `FileLoadConfig` 执行加载

设计取舍：

1. 优点：泛化能力高，真实业务文件容错更好。  
2. 代价：摄取行为受 LLM 波动影响，需要启发式兜底和基线回归保护。

---

## 4.3 语义层（Semantic Contract）

文件：

1. `app/services/semantic_contract.py`
2. `app/services/semantic_infer.py`
3. `app/services/semantic_profile.py`
4. `app/services/semantic_taxonomy.py`

职责：

1. 对列/行做语义判定（amount/date/id/text/summary_row 等）
2. 构建“单请求内共享”的语义契约，避免重复推断和判定漂移
3. 提供 fallback 机制（LLM 失败时回退 heuristic）

关键函数：

1. `ensure_semantic_contract`
2. `invalidate_semantic_contract`
3. `infer_dataframe_semantics`
4. `build_dataframe_profile`

原理：

1. 先做 profile（列统计 + 行样本特征）
2. 再 LLM 推断
3. 与 heuristic 融合
4. 失败时直接回退到 heuristic

设计取舍：

1. 优点：语义能力可扩展、可解释，技能层不必重复写规则。  
2. 代价：当前缓存范围只在 `dfs_context` 内存态，不跨进程；重启后失效。

---

## 4.4 技能路由与执行层（Deterministic Skills）

文件：

1. `app/skills/router.py`
2. `app/skills/engine.py`
3. `app/skills/contracts.py`

职责：

1. 根据用户意图快速判定 skill
2. 统一调度 skill 并共享 semantic contract
3. 返回统一结构 `SkillResult`

关键逻辑：

1. `route_skill`：L3/L1/L2/L4 关键词路由
2. `execute_skill`：按 skill_name 分发到具体函数

设计取舍：

1. 优点：显著提升稳定性和评测可重复性。  
2. 代价：路由仍是启发式关键词，不是概率分类器，边界表达可能漏判。

---

## 4.5 L1/L2 技能层（清洗 + 对齐）

文件：`app/skills/l1_hygiene_skill.py`

职责：

1. L1：语义增强清洗（去重、数值化、负值/极值处理、逻辑告警）
2. L2：销售表与主数据表实体对齐

你要重点看：

1. `CleaningPolicy` 与 `infer_cleaning_policy`
2. `_run_l1_hygiene`
3. `_apply_hygiene_to_table`
4. `run_l1_hygiene_skill`
5. `run_l1_l2_hygiene_merge_skill`
6. `resolve_required_columns`（通过 `column_guard`）

原理：

1. 先根据用户语句推断策略：`conservative` vs `strict`
2. 再用 semantic contract 决定哪些列/行可以动
3. 所有动作写入审计日志
4. L2 在必需列缺失时直接阻断并给证据反馈

设计取舍：

1. 优点：把“清洗是否删数据”从硬编码变成可控策略层。  
2. 代价：策略关键词触发是显式规则，复杂业务策略还需进一步结构化。

---

## 4.6 L3 技能层（财务对账）

文件：`app/skills/l3_reconcile_skill.py`

职责：

1. 自动识别系统表/银行表
2. 校验关键列（主键、金额）
3. 执行多对一聚合
4. 调用 `smart_reconcile` 生成对账结果

关键函数：

1. `_pick_tables`
2. `_table_role_score`
3. `_parse_tolerance`
4. `run_l3_reconcile_skill`

原理：

1. 基于表名 + 语义标签打分判角色
2. 必需列缺失直接阻断
3. 聚合后对账并输出状态分布

设计取舍：

1. 优点：从“可能错着跑”变成“缺关键列就拒绝执行”。  
2. 代价：表角色打分仍启发式，数据命名极差时需要人工提示。

---

## 4.7 L4 技能层（趋势可视化）

文件：`app/skills/l4_visual_skill.py`

职责：

1. 选择最适合趋势分析的表
2. 强制检查日期列与金额列
3. 生成 Plotly 趋势图和结构化结论
4. 输出可导出的 summary DataFrame

关键函数：

1. `_pick_target_table`
2. `_pick_dimension_column`
3. `run_l4_visual_skill`

原理：

1. 先“有日期 + 有金额”打分选表
2. 用 guard 确保关键列存在
3. 做月度聚合，必要时按维度截断 topN

设计取舍：

1. 优点：将可视化从自由生成转为确定性模板，稳定性提升明显。  
2. 代价：图表表达灵活性会低于纯 LLM 生成。

---

## 4.8 关键列守卫层（Column Guard）

文件：`app/skills/column_guard.py`

职责：

1. 声明并解析“执行某技能必须有的列”
2. 语义标签 + 名称 token 双轨匹配
3. 缺列时返回包含“当前列 + 语义证据 + 建议”的阻断文案

关键函数：

1. `RequiredColumnSpec`
2. `resolve_required_columns`
3. `build_missing_columns_message`

价值：把“错误晚发现”提前到“执行前阻断”。

---

## 4.9 Workflow 回退层（通用 LLM Agent）

文件：`app/services/workflow.py`

职责：

1. Supervisor 决策节点
2. Python Worker 生成代码
3. Executor 执行代码并处理重试
4. 失败自愈与终止控制

关键点：

1. `build_schema_context` 里注入语义摘要，降低 prompt 注入风险
2. `MAX_EXEC_RETRIES = 3`
3. `executor_router` 控制错误重试与正常结束

设计取舍：

1. 优点：具备通用能力，覆盖技能未命中场景。  
2. 代价：自由代码生成天然不稳定，评测波动更大，故当前架构主张 skill-first。

---

## 4.10 安全执行层（Trusted Exec Sandbox）

文件：`app/services/trusted_exec.py`

职责：

1. AST 安全校验
2. 受限 builtins + import allowlist
3. 子进程隔离执行 + 超时中断
4. 返回执行日志、图表、result_df、audit

关键函数：

1. `validate_code_safety`
2. `_run_sandboxed`
3. `run_trusted_code`

设计取舍：

1. 优点：比直接 `exec` 安全性大幅提升。  
2. 代价：不是系统级沙箱（无 cgroup/容器级资源隔离），仍属于应用层防护。

---

## 4.11 审计与智能工具层

文件：`app/utils/tools.py`

职责：

1. `AuditLogger`：记录操作与剔除样本
2. `smart_merge`：fuzz + 向量召回 + LLM 裁决
3. `smart_reconcile`：对账状态分类与差异计算

阅读重点：

1. `AuditLogger.info/log_exclusion/get_log_df`
2. `smart_merge` 匹配策略切换（小集合全量 LLM，大集合向量召回）
3. `smart_reconcile` 的状态判定与金额差异计算

设计取舍：

1. 优点：工具能力强，适配复杂真实数据。  
2. 代价：`smart_merge` 依赖模型与向量库，吞吐和稳定性需要评测护栏。

---

## 4.12 导出层（Delivery）

文件：`app/services/exporter.py`

职责：

1. 统一把业务表、结果表、审计日志、剔除样本写入一个 Excel
2. 自动处理 sheet 名冲突
3. 无数据时写入 fallback sheet

关键函数：

1. `save_full_context_excel`
2. `_unique_sheet_name`

---

## 4.13 配置与模型接入

文件：

1. `app/core/config.py`
2. `app/services/llm_factory.py`

职责：

1. 读取环境变量（`GOOGLE_API_KEY`、`GOOGLE_MODEL_NAME`）
2. 统一创建 Gemini 客户端

注意：

1. key 缺失会抛错，影响上传/推断/评测。

---

## 4.14 前端与 CLI

文件：

1. `app/ui.py`（Streamlit）
2. `app/main.py`（CLI）

职责：

1. 提供交互入口与可视化展示
2. 演示级工作流驱动

定位：UI/CLI 主要用于演示与手工体验，核心业务逻辑都在 server + services + skills。

---

## 4.15 评测框架层（Golden Dataset）

文件：

1. `golden_dataset/manifest.json`
2. `golden_dataset/expected_snapshots.json`
3. `golden_dataset/run_evaluation.py`
4. `golden_dataset/evaluator/assertions.py`
5. `golden_dataset/evaluator/http_api.py`
6. `golden_dataset/evaluator/xlsx_inspector.py`

原理：

1. `manifest` 定义 case 和推荐 prompt
2. `expected` 定义断言口径
3. `run_evaluation` 做批量调用与结果记录
4. `assertions` 做结构化判定
5. `xlsx_inspector` 做 workbook 快照解析

你需要关注的输出：

1. `golden_dataset/runs/scorecard_<run_id>.csv`
2. `golden_dataset/runs/summary_<run_id>.json`
3. `golden_dataset/scorecard_latest.csv`

---

## 5. 关键状态对象与上下文键（必须记住）

1. `session.dfs_context`：在线会话的数据总线
2. `session.backups`：原始备份，用于 `reload_data`
3. `__last_result_df__`：最近执行结果（导出桥接）
4. `__last_audit__`：最近审计对象（导出桥接）
5. `__semantic_contract__`：当前语义契约缓存
6. `__semantic_contract_meta__`：语义缓存签名元信息

---

## 6. 设计原理与 Tradeoff（面试必讲）

## 6.1 Skill-first vs Workflow-fallback

1. 原理：确定性技能优先，通用 LLM 兜底。  
2. 优点：稳定性、可回归性、可解释性更高。  
3. 代价：技能覆盖不到的长尾场景仍需 workflow 承担波动。

## 6.2 语义契约共享（Semantic Contract）

1. 原理：单请求内推断一次，所有层共享。  
2. 优点：减少重复推断与判定漂移。  
3. 代价：缓存作用域在内存会话，不跨实例。

## 6.3 清洗策略层（Conservative/Strict）

1. 原理：把删不删数据从硬编码改为策略控制。  
2. 优点：降低误删风险，支持业务侧显式调节。  
3. 代价：策略触发当前依赖关键词，仍有语义边界问题。

## 6.4 列守卫阻断（Fail Fast）

1. 原理：关键列缺失时直接拒绝执行。  
2. 优点：避免错误结果扩散，提升可信度。  
3. 代价：会牺牲一部分“先跑跑看”的灵活体验。

## 6.5 应用层沙箱

1. 原理：AST + 受限 builtins + 子进程 + 超时。  
2. 优点：比裸执行安全很多。  
3. 代价：不是完全隔离的系统级沙箱。

---

## 7. 从代码视角理解当前版本演进

建议按 `golden_dataset/CHANGELOG.md` 倒序读：

1. v1.0.9：引入显式清洗策略层，默认保守模式
2. v1.0.8：修复 ingestion 过清洗问题
3. v1.0.7-v1.0.6：技能架构拆分，稳定性提升
4. v1.0.5-v1.0.3：运行可靠性、可观测性、预检完善
5. v1.0.1：评测框架建立

这条线可以直接作为你讲项目“架构演进思路”的主线。

---

## 8. 未来 Roadmap（基于当前代码与仓库规划）

## 8.1 已在 README 明确的 roadmap

1. 将核心流程从自由代码生成迁移到结构化 tool-calling 编排
2. 增加确定性对账模板（多对一、容差、差异分层）
3. 增加生产级持久化（Redis + SQL/Object Storage）
4. 建立评测 + 观测仪表（成功率、延迟、重试/错误画像）

## 8.2 建议优先做的工程化补齐

1. 结构化 telemetry：不要仅靠文本统计 retry/error
2. 路由器升级：关键词路由 -> 轻量 intent 分类器
3. 评测增强：从结构断言扩展到业务语义断言
4. 测试体系：补齐 `tests/`（单测 + API 集成 + 回归门禁）
5. 多实例架构：session 从内存迁移到可共享存储

---

## 9. 实操：如何快速上手读代码并验证理解

1. 本地启动后端：`uvicorn app.server:app --reload --port 8000`
2. 上传两张对账表，发送“容差5元对账”
3. 看 `/chat` 返回是否命中 `l3_reconcile`
4. 下载 Excel，检查是否有审计 sheet
5. 运行评测：`python golden_dataset/run_evaluation.py --api-url http://localhost:8000`
6. 对照 `summary` 看失败原因，回到对应 skill/service 改逻辑

用这套闭环，你会非常快地把“阅读理解”变成“可调试可迭代能力”。

---

## 10. 读完本指南后你应该能回答的问题

1. 请求是如何从 upload 走到 skill/workflow，再到导出和评测的？
2. 语义契约如何在系统里复用，为什么能降低漂移？
3. 清洗策略为什么默认保守，严格模式如何触发？
4. L2/L3/L4 为什么要做列守卫阻断？
5. workflow 路径和 skill 路径在稳定性上的差异是什么？
6. 当前评测断言验证了什么，还没验证什么？
7. 如果要把系统推向生产，下一步先改哪三件事？

如果这些问题你都能讲清楚，说明你已经形成了对项目“架构 + 原理 + 实现 + 取舍 + 演进”的全面理解。
