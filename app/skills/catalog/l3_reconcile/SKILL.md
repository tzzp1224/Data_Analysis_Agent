---
name: l3-reconcile
description: 财务流水容差对账与差异分类。
worker: l3_reconcile
intent_keywords:
  - 对账
  - 流水
  - 容差
  - reconcile
risk_level: high
enabled: true
capabilities:
  - reconcile
  - many_to_one_aggregate
preconditions:
  - has_two_tables
  - has_amount_and_key_columns
hook_templates:
  - approve_high_risk_reconcile
  - set_tolerance
output_schema: dataframe+audit
---

# L3 Reconcile

执行系统与银行流水对账，输出状态分类与审计记录。
