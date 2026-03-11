#!/usr/bin/env python3
"""
Build golden Excel datasets for Agentic Finance evaluation.

Design goals:
- No third-party dependencies (stdlib only).
- Deterministic outputs.
- Cover current agent capabilities (ingestion, cleaning, merge, reconciliation, visualization).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape
import json
import zipfile


ROOT = Path(__file__).resolve().parent
CASES_DIR = ROOT / "cases"
VERSION_FILE = ROOT / "VERSION"


def load_dataset_version(default: str = "v0.0.0") -> str:
    if not VERSION_FILE.exists():
        return default
    return VERSION_FILE.read_text(encoding="utf-8").strip() or default


def col_name(index_1_based: int) -> str:
    result = []
    n = index_1_based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result.append(chr(65 + rem))
    return "".join(reversed(result))


def xml_inline_string(text: str) -> str:
    safe = escape(text)
    preserve = ' xml:space="preserve"' if text != text.strip() else ""
    return f'<is><t{preserve}>{safe}</t></is>'


def xml_cell(cell_ref: str, value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return f'<c r="{cell_ref}" t="inlineStr">{xml_inline_string(str(value))}</c>'
    if isinstance(value, (int, float)):
        return f'<c r="{cell_ref}"><v>{value}</v></c>'
    return f'<c r="{cell_ref}" t="inlineStr">{xml_inline_string(str(value))}</c>'


def xml_sheet(rows: list[list]) -> str:
    row_xml_parts = []
    for row_idx, row in enumerate(rows, start=1):
        cells = []
        for col_idx, value in enumerate(row, start=1):
            cell = xml_cell(f"{col_name(col_idx)}{row_idx}", value)
            if cell:
                cells.append(cell)
        if cells:
            row_xml_parts.append(f'<row r="{row_idx}">{"".join(cells)}</row>')
        else:
            row_xml_parts.append(f'<row r="{row_idx}"/>')
    sheet_data = "".join(row_xml_parts)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{sheet_data}</sheetData>"
        "</worksheet>"
    )


def xml_content_types(sheet_count: int) -> str:
    overrides = []
    for i in range(1, sheet_count + 1):
        overrides.append(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
    overrides_xml = "".join(overrides)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        f"{overrides_xml}</Types>"
    )


def xml_root_rels() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        "</Relationships>"
    )


def xml_workbook(sheet_names: list[str]) -> str:
    sheets_xml = []
    for idx, name in enumerate(sheet_names, start=1):
        safe_name = escape(name[:31] or f"Sheet{idx}")
        sheets_xml.append(
            f'<sheet name="{safe_name}" sheetId="{idx}" r:id="rId{idx}"/>'
        )
    sheets_content = "".join(sheets_xml)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f"<sheets>{sheets_content}</sheets>"
        "</workbook>"
    )


def xml_workbook_rels(sheet_count: int) -> str:
    rels = []
    for i in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{i}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{i}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    rels_xml = "".join(rels)
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f"{rels_xml}</Relationships>"
    )


def xml_styles() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font></fonts>'
        '<fills count="2">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        "</fills>"
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )


def xml_core_props() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties '
        'xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<dc:creator>Agentic Finance Dataset Builder</dc:creator>"
        "<cp:lastModifiedBy>Agentic Finance Dataset Builder</cp:lastModifiedBy>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:modified>'
        "</cp:coreProperties>"
    )


def xml_app_props() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        "<Application>Agentic Finance</Application>"
        "</Properties>"
    )


def write_xlsx(path: Path, sheets: list[tuple[str, list[list]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sheet_names = [name for name, _ in sheets]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", xml_content_types(len(sheets)))
        zf.writestr("_rels/.rels", xml_root_rels())
        zf.writestr("xl/workbook.xml", xml_workbook(sheet_names))
        zf.writestr("xl/_rels/workbook.xml.rels", xml_workbook_rels(len(sheets)))
        zf.writestr("xl/styles.xml", xml_styles())
        zf.writestr("docProps/core.xml", xml_core_props())
        zf.writestr("docProps/app.xml", xml_app_props())
        for idx, (_, rows) in enumerate(sheets, start=1):
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", xml_sheet(rows))


def rows_from_dicts(headers: list[str], records: Iterable[dict]) -> list[list]:
    rows = [headers]
    for rec in records:
        rows.append([rec.get(h) for h in headers])
    return rows


def dataset_customer_master() -> list[tuple[str, list[list]]]:
    headers = ["客户ID", "标准公司名", "统一社会信用代码", "行业", "客户等级", "结算周期(天)"]
    records = [
        {"客户ID": "C001", "标准公司名": "腾讯科技有限公司", "统一社会信用代码": "9144030071526726X7", "行业": "互联网", "客户等级": "KA", "结算周期(天)": 30},
        {"客户ID": "C002", "标准公司名": "阿里巴巴集团控股有限公司", "统一社会信用代码": "91330100716105852F", "行业": "电商", "客户等级": "KA", "结算周期(天)": 30},
        {"客户ID": "C003", "标准公司名": "字节跳动有限公司", "统一社会信用代码": "91110108MA01FQ8B0B", "行业": "互联网", "客户等级": "KA", "结算周期(天)": 45},
        {"客户ID": "C004", "标准公司名": "京东世纪贸易有限公司", "统一社会信用代码": "91110302660513026G", "行业": "电商物流", "客户等级": "A", "结算周期(天)": 45},
        {"客户ID": "C005", "标准公司名": "美团点评集团", "统一社会信用代码": "91110108563673658N", "行业": "本地生活", "客户等级": "A", "结算周期(天)": 30},
        {"客户ID": "C006", "标准公司名": "拼多多网络科技有限公司", "统一社会信用代码": "91310000MA1FL2X44A", "行业": "电商", "客户等级": "B", "结算周期(天)": 60},
        {"客户ID": "C007", "标准公司名": "网易（杭州）网络有限公司", "统一社会信用代码": "91330100720028231Q", "行业": "互联网", "客户等级": "B", "结算周期(天)": 60},
        {"客户ID": "C008", "标准公司名": "百度在线网络技术（北京）有限公司", "统一社会信用代码": "91110108717752540G", "行业": "互联网", "客户等级": "A", "结算周期(天)": 45},
    ]
    return [("客户主数据", rows_from_dicts(headers, records))]


def dataset_dirty_sales() -> list[tuple[str, list[list]]]:
    detail_rows = [
        ["导出系统", "ERP_v6.2"],
        ["备注", "本表含测试脏数据，请勿直接入账"],
        ["订单号", "交易日期", "客户名称", "产品", "单价", "数量", "总金额", "销售地区", "状态", "外部参考"],
        ["SO-240001", "2024-01-03", "Tencent", "云服务器", 2999, 1, 2999, "华南", "已完成", "PO-001"],
        ["SO-240002", "2024-01-04", "腾讯科技", "企业邮箱", 399, 10, 3990, "华南", "已完成", "PO-002"],
        ["SO-240003", "2024-01-05", "阿里巴巴集团控股", "广告投放", 1200, 2, 2400, "华东", "已完成", "PO-003"],
        ["SO-240004", "2024-01-05", "AliBaba Group", "广告投放", 1200, 2, 2300, "华东", "已完成", "PO-003A"],
        ["SO-240005", "2024-01-07", "字节跳动", "SaaS订阅", 850, 3, 2550, "华北", "已完成", "PO-004"],
        ["SO-240006", "2024-01-07", "ByteDance", "SaaS订阅", 850, 3, "2,550.00", "华北", "已完成", "PO-004B"],
        ["SO-240007", "2024-01-08", "京东商城", "云服务器", -500, 2, -1000, "华东", "已完成", "PO-005"],
        ["SO-240008", "2024-01-09", "JD.com", "云服务器", 500, 200000, 100000000, "华东", "已完成", "PO-005B"],
        ["SO-240009", "2024-01-10", "", "企业邮箱", 399, 5, 1995, "华南", "已完成", "PO-006"],
        ["SO-240010", "2024-01-11", "美团点评集团", "广告投放", "¥1,500.00", 2, "¥3,000.00", "西南", "已完成", "PO-007"],
        ["SO-240011", "2024/13/40", "Meituan", "广告投放", 1500, 2, 3000, "西南", "已完成", "PO-007A"],
        ["SO-240012", "2024-01-12", "腾讯科技有限公司 ", "云服务器", 2999, 1, 2999, "华南", "退款中", "PO-008"],
        ["SO-240013", "2024-01-13", None, "SaaS订阅", 1000, 1, 1000, "华北", "已完成", "PO-009"],
        ["SO-240014", "2024-01-13", "今日头条", "SaaS订阅", 900, 2, 1800, "华北", "已完成", "PO-010"],
        ["SO-240005", "2024-01-07", "字节跳动", "SaaS订阅", 850, 3, 2550, "华北", "已完成", "PO-004"],
        ["SO-240010", "2024-01-11", "美团点评集团", "广告投放", "¥1,500.00", 2, "¥3,000.00", "西南", "已完成", "PO-007"],
        ["SO-240015", "2024-01-14", "字节跳动有限公司", "SaaS订阅", 900, 2, 9999, "华北", "已完成", "PO-011"],
        ["SO-240016", "2024-01-15", "阿里", "企业邮箱", 399, 10, 3990, "华东", "已完成", "PO-012"],
    ]
    cover = [
        ["2024Q1 区域销售台账（测试样本）"],
        [],
        ["生成目的：评估 Agent 的摄取、清洗、模糊匹配能力"],
    ]
    notes = [
        ["字段说明", "描述"],
        ["总金额", "应与 单价*数量 对齐，但样本中故意注入逻辑冲突"],
        ["客户名称", "混合中英文、简称、别名以及空值"],
        ["交易日期", "含非法日期字符串用于鲁棒性测试"],
    ]
    return [("封面", cover), ("说明", notes), ("销售明细", detail_rows)]


def dataset_system_ledger() -> list[tuple[str, list[list]]]:
    headers = ["记账日期", "订单号", "外部流水号", "应收金额", "币种", "渠道", "备注"]
    records = [
        {"记账日期": "2024-02-01", "订单号": "ORD-9001-A", "外部流水号": "TRX-9001", "应收金额": 300.00, "币种": "CNY", "渠道": "支付宝", "备注": "分笔收款-1"},
        {"记账日期": "2024-02-01", "订单号": "ORD-9001-B", "外部流水号": "TRX-9001", "应收金额": 300.00, "币种": "CNY", "渠道": "支付宝", "备注": "分笔收款-2"},
        {"记账日期": "2024-02-01", "订单号": "ORD-9001-C", "外部流水号": "TRX-9001", "应收金额": 400.00, "币种": "CNY", "渠道": "支付宝", "备注": "分笔收款-3"},
        {"记账日期": "2024-02-02", "订单号": "ORD-9002", "外部流水号": " TRX-9002 ", "应收金额": 500.00, "币种": "CNY", "渠道": "网银", "备注": "流水号含前后空格"},
        {"记账日期": "2024-02-03", "订单号": "ORD-9003", "外部流水号": "TRX-9003", "应收金额": 1200.00, "币种": "CNY", "渠道": "网银", "备注": "将触发容差匹配"},
        {"记账日期": "2024-02-04", "订单号": "ORD-9004", "外部流水号": "TRX-9004", "应收金额": 888.00, "币种": "CNY", "渠道": "银联", "备注": "系统单边账"},
        {"记账日期": "2024-02-05", "订单号": "ORD-9005-A", "外部流水号": "TRX-9005", "应收金额": 200.00, "币种": "CNY", "渠道": "支付宝", "备注": "多对一"},
        {"记账日期": "2024-02-05", "订单号": "ORD-9005-B", "外部流水号": "TRX-9005", "应收金额": 99.98, "币种": "CNY", "渠道": "支付宝", "备注": "多对一"},
        {"记账日期": "2024-02-06", "订单号": "ORD-9006", "外部流水号": "TRX-9006", "应收金额": "1,200.00", "币种": "CNY", "渠道": "微信支付", "备注": "金额为字符串"},
        {"记账日期": "2024-02-07", "订单号": "ORD-9007", "外部流水号": "", "应收金额": 150.00, "币种": "CNY", "渠道": "微信支付", "备注": "缺失流水号"},
        {"记账日期": "2024-02-01", "订单号": "ORD-9001-B", "外部流水号": "TRX-9001", "应收金额": 300.00, "币种": "CNY", "渠道": "支付宝", "备注": "重复行"},
    ]
    return [("系统日记账", rows_from_dicts(headers, records))]


def dataset_bank_statement() -> list[tuple[str, list[list]]]:
    cover = [
        ["银行流水对账单（测试）"],
        ["来源", "XX 银行企业网银"],
        ["说明", "本文件含手续费差异、单边账和别名户名"],
    ]
    data_rows = [
        ["导出批次", "2024-02-BATCH-07"],
        ["到账日期", "交易流水", "到账金额", "币种", "对方户名", "手续费", "备注"],
        ["2024-02-02", "TRX-9001", 1000.00, "CNY", "Tencent Cloud", 0.00, "对应系统3笔汇总"],
        ["2024-02-02", "TRX-9002", 500.00, "CNY", "腾讯科技有限公司", 0.00, "应完全匹配"],
        ["2024-02-03", "TRX-9003", 1195.00, "CNY", "ByteDance Ltd.", 5.00, "手续费导致5元差异"],
        ["2024-02-05", "TRX-9005", 299.98, "CNY", "JD Logistics", 0.00, "对应系统两笔合并"],
        ["2024-02-06", "TRX-9006", 1200.00, "CNY", "Alibaba Cloud", 0.00, "字符串金额清洗后应匹配"],
        ["2024-02-08", "TRX-EXTRA", 260.00, "CNY", "Unknown Supplier", 0.00, "银行单边账"],
    ]
    return [("封面", cover), ("银行流水明细", data_rows)]


def dataset_ingestion_multisheet() -> list[tuple[str, list[list]]]:
    cover = [
        ["2024年采购入库台账（含说明页）"],
        [],
        ["请勿删除说明页，系统需自动识别正确数据页。"],
    ]
    notes = [
        ["说明项", "内容"],
        ["正确数据Sheet", "采购入库明细"],
        ["表头所在行", "第5行（从1开始）"],
        ["噪声内容", "前4行为标题/批注，非真实表头"],
    ]
    detail = [
        ["某集团供应链中心"],
        ["数据统计区间：2024-03-01 ~ 2024-03-31"],
        ["导出时间：2024-04-01 09:31:22"],
        ["注意：金额字段混有货币符号与千分位"],
        ["采购单号", "供应商", "入库日期", "物料", "数量", "单价", "总额", "仓库"],
        ["PO-31001", "上海鸿运电子", "2024-03-02", "SSD 1TB", 50, 430, 21500, "上海一仓"],
        ["PO-31002", "深圳云海科技", "2024-03-03", "内存 32G", 120, "¥299.00", "¥35,880.00", "深圳二仓"],
        ["PO-31003", "苏州未来智能", "2024-03-03", "网卡 10G", 80, 560, 44800, "苏州一仓"],
        ["PO-31004", "深圳云海科技", "2024-03-05", "内存 32G", 120, "¥299.00", "¥35,880.00", "深圳二仓"],
        ["PO-31004", "深圳云海科技", "2024-03-05", "内存 32G", 120, "¥299.00", "¥35,880.00", "深圳二仓"],
        ["PO-31005", "南京格物贸易", "2024-03-06", "交换机", -2, 4500, -9000, "南京临时仓"],
    ]
    return [("封面", cover), ("说明", notes), ("采购入库明细", detail)]


def dataset_visualization() -> list[tuple[str, list[list]]]:
    headers = ["月份", "区域", "GMV", "订单量", "退款金额", "客单价"]
    rows = [headers]

    months = [f"2024-{m:02d}" for m in range(1, 13)]
    regions = ["华东", "华北", "华南"]
    base_map = {"华东": 1250000, "华北": 980000, "华南": 860000}
    seasonal = [0.92, 0.95, 1.00, 1.06, 1.12, 1.10, 1.15, 1.18, 1.08, 1.03, 1.00, 1.35]

    for month_idx, month in enumerate(months):
        for region in regions:
            base = base_map[region]
            gmv = round(base * seasonal[month_idx], 2)
            if month == "2024-08" and region == "华南":
                gmv = round(gmv * 0.72, 2)  # 异常下滑
            if month == "2024-12" and region == "华东":
                gmv = round(gmv * 1.25, 2)  # 大促高峰
            order_count = int(gmv / 320)
            refund = round(gmv * (0.018 if region != "华南" else 0.026), 2)
            aov = round(gmv / max(order_count, 1), 2)
            rows.append([month, region, gmv, order_count, refund, aov])

    return [("月度经营数据", rows)]


def build_manifest() -> dict:
    dataset_version = load_dataset_version(default="v0.0.0")
    return {
        "dataset_name": "Agentic Finance Golden Dataset",
        "version": dataset_version,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version_policy": {
            "patch": "v1.0.x: only metadata/evaluation tooling/expectation updates; no case data content changes",
            "minor": "v1.x.0: add/modify benchmark case data or business scenarios",
        },
        "snapshot_config": "expected_snapshots.json",
        "cases": [
            {
                "case_id": "L1_L2_SALES_DIRTY_MERGE",
                "files": [
                    "cases/cleaning_merge/销售台账_脏数据.xlsx",
                    "cases/cleaning_merge/客户主数据_标准库.xlsx",
                ],
                "recommended_prompt": "请先做数据体检与清洗，再把销售表按客户名称与客户主数据做智能对齐，最后导出带审计日志的结果。",
                "evaluation_focus": [
                    "正确识别销售明细Sheet并处理非首行表头",
                    "处理重复、空值、负数、极端值、金额格式字符串",
                    "smart_merge 对中英别名实体进行对齐",
                    "输出包含处理日志与剔除数据",
                ],
            },
            {
                "case_id": "L3_FIN_RECON_MANY_TO_ONE",
                "files": [
                    "cases/reconciliation/系统日记账_复杂版.xlsx",
                    "cases/reconciliation/银行流水_复杂版.xlsx",
                ],
                "recommended_prompt": "请识别系统日记账与银行流水，先处理多对一聚合，再进行容差5元对账，输出差异明细和审计日志。",
                "evaluation_focus": [
                    "对 TRX-9001 与 TRX-9005 正确执行多对一聚合",
                    "对 TRX-9003 识别容差匹配（差额5元）",
                    "识别系统单边账与银行单边账",
                ],
            },
            {
                "case_id": "INGESTION_MULTI_SHEET_HEADER",
                "files": [
                    "cases/ingestion/多Sheet_采购入库_带说明.xlsx",
                ],
                "recommended_prompt": "加载这个文件并进行数据清洗，输出处理结果和审计日志。",
                "evaluation_focus": [
                    "从封面/说明页中定位真实数据页",
                    "正确识别第5行作为表头",
                    "识别重复记录和异常负数数量",
                ],
            },
            {
                "case_id": "L4_VISUAL_TREND",
                "files": [
                    "cases/visualization/区域月度经营数据_2024.xlsx",
                ],
                "recommended_prompt": "分析区域月度GMV趋势，生成图表并给出关键洞察，重点说明异常波动。",
                "evaluation_focus": [
                    "至少生成1张可视化图表",
                    "文字洞察可解释 8 月华南异常与 12 月华东峰值",
                ],
            },
        ],
    }


def write_readme(manifest: dict) -> None:
    readme = ROOT / "README.md"
    lines = [
        "# Golden Dataset (Agentic Finance)",
        "",
        "这个目录用于长期回归评估，覆盖当前 Agent 的核心能力：",
        "- L1 数据清洗与审计日志",
        "- L2 实体对齐（模糊匹配/语义匹配）",
        "- L3 财务对账（多对一、容差、单边账）",
        "- L4 可视化分析与洞察",
        "",
        "## 目录结构",
        "- `cases/cleaning_merge/`：清洗 + 主数据对齐",
        "- `cases/reconciliation/`：复杂对账",
        "- `cases/ingestion/`：多 Sheet + 非首行表头",
        "- `cases/visualization/`：趋势分析数据",
        "",
        "## 用法",
        "1. 启动后端与前端。",
        "2. 按 `manifest.json` 中的 case 逐个上传文件并执行推荐 Prompt。",
        "3. 记录关键指标（成功率、重试次数、耗时、输出完整性）作为优化前后对比。",
        "4. 将结果填写到 `scorecard_template.csv`，形成可对比基线。",
        "",
        "## 自动评测",
        "```bash",
        "python golden_dataset/run_evaluation.py --api-url http://localhost:8000",
        "```",
        "输出：",
        "- `golden_dataset/runs/scorecard_<run_id>.csv`",
        "- `golden_dataset/runs/summary_<run_id>.json`",
        "- `golden_dataset/scorecard_latest.csv`（最近一次运行）",
        "",
        "## 快照断言",
        "- 断言配置：`expected_snapshots.json`",
        "- 每个 case 包含关键期望（如图表数量、审计统计、关键 sheet 与最小行数）",
        "- 用于回归比较：优化前后跑同一套 case，直接比较 pass rate 与失败原因",
        "",
        "## 版本策略",
        "- `v1.0.x`：只更新评测配置/工具，不改动 case 数据内容",
        "- `v1.x.0`：新增或变更 case 数据，属于基线升级",
        "- 变更记录见：`CHANGELOG.md`",
        "",
        "## 版本",
        f"- 当前版本：`{manifest['version']}`",
        "",
        "## 重新生成",
        "```bash",
        "python golden_dataset/build_golden_dataset.py",
        "```",
    ]
    readme.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build() -> None:
    datasets = [
        (
            CASES_DIR / "cleaning_merge" / "客户主数据_标准库.xlsx",
            dataset_customer_master(),
        ),
        (
            CASES_DIR / "cleaning_merge" / "销售台账_脏数据.xlsx",
            dataset_dirty_sales(),
        ),
        (
            CASES_DIR / "reconciliation" / "系统日记账_复杂版.xlsx",
            dataset_system_ledger(),
        ),
        (
            CASES_DIR / "reconciliation" / "银行流水_复杂版.xlsx",
            dataset_bank_statement(),
        ),
        (
            CASES_DIR / "ingestion" / "多Sheet_采购入库_带说明.xlsx",
            dataset_ingestion_multisheet(),
        ),
        (
            CASES_DIR / "visualization" / "区域月度经营数据_2024.xlsx",
            dataset_visualization(),
        ),
    ]

    for path, sheets in datasets:
        write_xlsx(path, sheets)
        print(f"✅ Generated: {path.relative_to(ROOT)}")

    manifest = build_manifest()
    (ROOT / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    write_readme(manifest)
    print("✅ Generated: manifest.json")
    print("✅ Generated: README.md")


if __name__ == "__main__":
    build()
