# Golden Dataset (Agentic Finance)

这个目录用于长期回归评估，覆盖当前 Agent 的核心能力：
- L1 数据清洗与审计日志
- L2 实体对齐（模糊匹配/语义匹配）
- L3 财务对账（多对一、容差、单边账）
- L4 可视化分析与洞察

## 目录结构
- `cases/cleaning_merge/`：清洗 + 主数据对齐
- `cases/reconciliation/`：复杂对账
- `cases/ingestion/`：多 Sheet + 非首行表头
- `cases/visualization/`：趋势分析数据

## 用法
1. 启动后端与前端。
2. 按 `manifest.json` 中的 case 逐个上传文件并执行推荐 Prompt。
3. 记录关键指标（成功率、重试次数、耗时、输出完整性）作为优化前后对比。
4. 将结果填写到 `scorecard_template.csv`，形成可对比基线。

## 自动评测
```bash
python golden_dataset/run_evaluation.py --api-url http://localhost:8000
```
输出：
- `golden_dataset/runs/scorecard_<run_id>.csv`
- `golden_dataset/runs/summary_<run_id>.json`
- `golden_dataset/scorecard_latest.csv`（最近一次运行）

## 快照断言
- 断言配置：`expected_snapshots.json`
- 每个 case 包含关键期望（如图表数量、审计统计、关键 sheet 与最小行数）
- 用于回归比较：优化前后跑同一套 case，直接比较 pass rate 与失败原因

## 版本策略
- `v1.0.x`：只更新评测配置/工具，不改动 case 数据内容
- `v1.x.0`：新增或变更 case 数据，属于基线升级
- 变更记录见：`CHANGELOG.md`

## 版本
- 当前版本：`v1.0.1`

## 重新生成
```bash
python golden_dataset/build_golden_dataset.py
```
