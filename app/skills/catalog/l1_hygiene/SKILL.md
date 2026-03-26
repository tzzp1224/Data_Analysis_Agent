---
name: l1-hygiene
description: 语义增强数据体检与保守/严格清洗。
worker: l1_hygiene
intent_keywords:
  - 清洗
  - 体检
  - 去重
  - hygiene
risk_level: low
enabled: true
capabilities:
  - clean
  - dedup
  - anomaly_warning
preconditions:
  - has_business_table
hook_templates:
  - approve_high_impact_cleaning
output_schema: dataframe+audit
---

# L1 Hygiene

执行语义增强的数据体检与清洗，记录完整审计日志。
