---
name: l4-visual
description: 趋势分析与可视化图表生成。
worker: l4_visual
intent_keywords:
  - 趋势
  - 图表
  - 可视化
  - chart
  - trend
risk_level: low
enabled: true
capabilities:
  - trend_analysis
  - plotting
preconditions:
  - has_date_and_amount
hook_templates:
  - approve_visual_route
output_schema: chart+dataframe
---

# L4 Visual

执行确定性趋势分析并生成图表 JSON。
