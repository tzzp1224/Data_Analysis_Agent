from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class SemanticTypeSpec:
    type_id: str
    description: str
    cleaning_plan: str


COLUMN_TYPE_SPECS: Dict[str, SemanticTypeSpec] = {
    "amount": SemanticTypeSpec(
        type_id="amount",
        description="金额/货币数值列，通常用于结算、对账、财务统计。",
        cleaning_plan="强制数值化；高置信负值按财务风险剔除并审计；保留格式清洗日志。",
    ),
    "quantity": SemanticTypeSpec(
        type_id="quantity",
        description="数量/件数/计数列，通常为整数或接近整数。",
        cleaning_plan="数值化；负值默认告警保留（可能为退货/冲销）；极端值按阈值剔除并审计。",
    ),
    "date": SemanticTypeSpec(
        type_id="date",
        description="日期/时间列，支持多种字符串日期格式。",
        cleaning_plan="尝试解析为时间；解析失败行不自动删除，仅记录告警。",
    ),
    "id": SemanticTypeSpec(
        type_id="id",
        description="标识符列，如订单号、流水号、客户ID。",
        cleaning_plan="统一转字符串并去空格；保持原始值语义，不做数值化。",
    ),
    "text": SemanticTypeSpec(
        type_id="text",
        description="一般文本维度列，如名称、备注、类别。",
        cleaning_plan="保留文本；做空白清洗与基础标准化。",
    ),
    "unknown": SemanticTypeSpec(
        type_id="unknown",
        description="暂无法确定语义的列。",
        cleaning_plan="只做保守清洗（去重/空白处理），不做强业务裁剪。",
    ),
}


ROW_TYPE_SPECS: Dict[str, SemanticTypeSpec] = {
    "data_row": SemanticTypeSpec(
        type_id="data_row",
        description="业务数据行。",
        cleaning_plan="参与正常清洗与分析。",
    ),
    "summary_row": SemanticTypeSpec(
        type_id="summary_row",
        description="合计/汇总行。",
        cleaning_plan="高置信时可从明细分析中剔除，保留审计留痕。",
    ),
    "metadata_row": SemanticTypeSpec(
        type_id="metadata_row",
        description="说明/注释/元信息行。",
        cleaning_plan="高置信时从明细分析中剔除，保留审计留痕。",
    ),
    "empty_row": SemanticTypeSpec(
        type_id="empty_row",
        description="空白行。",
        cleaning_plan="可直接剔除。",
    ),
    "unknown": SemanticTypeSpec(
        type_id="unknown",
        description="无法确定语义的行。",
        cleaning_plan="保守处理，不自动剔除。",
    ),
}

