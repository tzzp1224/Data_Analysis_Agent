---
name: l2-merge
description: 主数据实体对齐与多表合并。
worker: l2_merge
intent_keywords:
  - 合并
  - 对齐
  - 主数据
  - merge
risk_level: medium
enabled: true
capabilities:
  - merge
  - entity_alignment
preconditions:
  - has_two_tables
hook_templates:
  - select_merge_key
  - approve_merge
output_schema: dataframe+audit
---

# L2 Merge

执行实体键规划、键质量校验与受控合并。
