---
name: l5-anomaly
description: 异常波动检测与原因提示。
worker: l5_anomaly
intent_keywords:
  - 异常
  - 波动
  - anomaly
  - outlier
risk_level: medium
enabled: true
capabilities:
  - anomaly_detection
  - volatility_analysis
preconditions:
  - has_date_and_amount
hook_templates:
  - approve_anomaly_threshold
  - set_threshold
output_schema: dataframe+insight
---

# L5 Anomaly

检测时间序列金额异常点并输出解释性提示。
