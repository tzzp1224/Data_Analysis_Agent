from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any
import posixpath
import xml.etree.ElementTree as ET
import zipfile


NS_MAIN = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
NS_REL = {"r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
REL_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
RID_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"


@dataclass
class SheetSnapshot:
    name: str
    total_rows: int
    data_rows: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "total_rows": self.total_rows,
            "data_rows": self.data_rows,
        }


@dataclass
class WorkbookSnapshot:
    sheet_count: int
    sheets: list[SheetSnapshot]

    def find_sheet(self, token: str) -> SheetSnapshot | None:
        for sheet in self.sheets:
            if token in sheet.name:
                return sheet
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sheet_count": self.sheet_count,
            "sheets": [sheet.to_dict() for sheet in self.sheets],
        }


def _normalize_target(target: str) -> str:
    normalized = target.lstrip("/")
    if normalized.startswith("xl/"):
        return normalized
    return posixpath.join("xl", normalized)


def inspect_xlsx_bytes(content: bytes) -> WorkbookSnapshot:
    with zipfile.ZipFile(BytesIO(content)) as zf:
        workbook_root = ET.fromstring(zf.read("xl/workbook.xml"))
        rels_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

        rel_map: dict[str, str] = {}
        for rel in rels_root.findall(f"{REL_NS}Relationship"):
            rel_id = rel.attrib.get("Id")
            target = rel.attrib.get("Target", "")
            if rel_id:
                rel_map[rel_id] = _normalize_target(target)

        sheets: list[SheetSnapshot] = []
        for sheet_node in workbook_root.findall("m:sheets/m:sheet", NS_MAIN):
            sheet_name = sheet_node.attrib.get("name", "")
            rid = sheet_node.attrib.get(RID_NS, "")
            target = rel_map.get(rid)
            if not target:
                continue
            sheet_root = ET.fromstring(zf.read(target))
            row_nodes = sheet_root.findall("m:sheetData/m:row", NS_MAIN)
            total_rows = len(row_nodes)
            non_empty_rows = 0
            for row in row_nodes:
                if row.findall("m:c", NS_MAIN):
                    non_empty_rows += 1
            data_rows = max(0, non_empty_rows - 1)
            sheets.append(
                SheetSnapshot(
                    name=sheet_name,
                    total_rows=total_rows,
                    data_rows=data_rows,
                )
            )

        return WorkbookSnapshot(sheet_count=len(sheets), sheets=sheets)
