---
name: expert-excel
description: 复杂自然语言 Excel 操作的结构化兜底执行。
worker: expert_excel
intent_keywords:
  - 函数
  - 公式
  - 透视
  - pivot
  - 复杂分析
risk_level: medium
enabled: true
capabilities:
  - complex_nl_excel
  - structured_execution_spec
preconditions:
  - has_business_table
hook_templates:
  - approve_expert_route
  - select_option
output_schema: dataframe+execution_spec
---

# Expert Excel

在低置信路由场景下输出结构化 Execution Spec，并通过确定性路径执行。
