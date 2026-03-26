from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, Optional

from app.skills.engine import SUPPORTED_SKILL_WORKERS


CATALOG_ROOT = Path(__file__).resolve().parent / "catalog"
REQUIRED_FRONTMATTER_KEYS = (
    "name",
    "description",
    "worker",
    "intent_keywords",
    "risk_level",
    "enabled",
)


@dataclass(frozen=True)
class SkillSpec:
    skill_id: str
    name: str
    description: str
    worker: str
    intent_keywords: tuple[str, ...]
    risk_level: str
    enabled: bool
    path: str


def _strip_quotes(value: str) -> str:
    text = str(value).strip()
    if (text.startswith("\"") and text.endswith("\"")) or (
        text.startswith("'") and text.endswith("'")
    ):
        return text[1:-1].strip()
    return text


def _split_frontmatter(content: str) -> tuple[str, str]:
    text = str(content or "")
    if not text.startswith("---\n"):
        raise ValueError("SKILL.md must start with YAML frontmatter delimited by ---")
    marker = "\n---\n"
    end = text.find(marker, 4)
    if end < 0:
        raise ValueError("SKILL.md frontmatter closing delimiter not found")
    return text[4:end], text[end + len(marker) :]


def _parse_frontmatter_yaml(block: str) -> dict:
    payload: dict[str, object] = {}
    current_list_key: Optional[str] = None

    for raw_line in block.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if ":" in line and not stripped.startswith("- "):
            key, raw_value = line.split(":", 1)
            key = key.strip()
            value = raw_value.strip()
            if not key:
                continue
            if value == "":
                payload[key] = []
                current_list_key = key
            else:
                payload[key] = _strip_quotes(value)
                current_list_key = None
            continue

        if stripped.startswith("- "):
            if not current_list_key:
                raise ValueError(f"List item found without a key: {raw_line}")
            items = payload.get(current_list_key)
            if not isinstance(items, list):
                raise ValueError(f"Invalid list field `{current_list_key}`")
            items.append(_strip_quotes(stripped[2:].strip()))
            continue

        raise ValueError(f"Unsupported frontmatter syntax: {raw_line}")

    return payload


def _to_bool(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def _normalize_keywords(value: object) -> tuple[str, ...]:
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
        return tuple(items)
    text = str(value).strip()
    if not text:
        return tuple()
    return tuple(part.strip() for part in text.split(",") if part.strip())


def parse_skill_markdown(path: Path) -> SkillSpec:
    if not path.exists():
        raise FileNotFoundError(str(path))

    raw = path.read_text(encoding="utf-8")
    frontmatter, _ = _split_frontmatter(raw)
    payload = _parse_frontmatter_yaml(frontmatter)

    missing = [key for key in REQUIRED_FRONTMATTER_KEYS if key not in payload]
    if missing:
        raise ValueError(f"Missing required frontmatter fields: {', '.join(missing)}")

    worker = str(payload.get("worker", "")).strip()
    if worker not in SUPPORTED_SKILL_WORKERS:
        raise ValueError(
            f"Invalid worker `{worker}` in {path}. Allowed: {', '.join(SUPPORTED_SKILL_WORKERS)}"
        )

    risk_level = str(payload.get("risk_level", "")).strip().lower()
    if risk_level not in {"low", "medium", "high"}:
        raise ValueError(f"Invalid risk_level `{risk_level}` in {path}")

    intent_keywords = _normalize_keywords(payload.get("intent_keywords"))
    if not intent_keywords:
        raise ValueError(f"intent_keywords cannot be empty in {path}")

    return SkillSpec(
        skill_id=path.parent.name,
        name=str(payload.get("name", "")).strip(),
        description=str(payload.get("description", "")).strip(),
        worker=worker,
        intent_keywords=intent_keywords,
        risk_level=risk_level,
        enabled=_to_bool(payload.get("enabled")),
        path=str(path),
    )


class SkillRegistry:
    def __init__(self, root: Path = CATALOG_ROOT):
        self.root = Path(root)
        self._lock = Lock()
        self._signature: tuple[tuple[str, int], ...] = tuple()
        self._specs: dict[str, SkillSpec] = {}
        self._errors: dict[str, str] = {}

    def _scan_signature(self) -> tuple[tuple[str, int], ...]:
        files = sorted(self.root.glob("*/SKILL.md"))
        signature: list[tuple[str, int]] = []
        for path in files:
            try:
                signature.append((str(path), int(path.stat().st_mtime_ns)))
            except FileNotFoundError:
                continue
        return tuple(signature)

    def _reload(self) -> None:
        specs: dict[str, SkillSpec] = {}
        errors: dict[str, str] = {}
        name_index: dict[str, str] = {}
        worker_index: dict[str, str] = {}

        for path, _ in self._signature:
            file_path = Path(path)
            skill_id = file_path.parent.name
            try:
                spec = parse_skill_markdown(file_path)
                duplicate = name_index.get(spec.name)
                if duplicate:
                    raise ValueError(
                        f"Duplicate skill name `{spec.name}` for `{skill_id}` and `{duplicate}`"
                    )
                name_index[spec.name] = skill_id

                worker_owner = worker_index.get(spec.worker)
                if worker_owner:
                    raise ValueError(
                        f"Duplicate worker mapping `{spec.worker}` for `{skill_id}` and `{worker_owner}`"
                    )
                worker_index[spec.worker] = skill_id
                specs[skill_id] = spec
            except Exception as exc:
                errors[skill_id] = f"{type(exc).__name__}: {exc}"

        self._specs = specs
        self._errors = errors

    def refresh_if_needed(self, force: bool = False) -> None:
        with self._lock:
            signature = self._scan_signature()
            if force or signature != self._signature:
                self._signature = signature
                self._reload()

    def list_specs(self, include_disabled: bool = False) -> list[SkillSpec]:
        self.refresh_if_needed()
        specs = list(self._specs.values())
        if include_disabled:
            return sorted(specs, key=lambda s: s.skill_id)
        return sorted([spec for spec in specs if spec.enabled], key=lambda s: s.skill_id)

    def get_errors(self) -> Dict[str, str]:
        self.refresh_if_needed()
        return dict(self._errors)

    def find_matches(self, instruction: str) -> list[tuple[SkillSpec, int]]:
        text = str(instruction or "").lower()
        if not text:
            return []

        matches: list[tuple[SkillSpec, int]] = []
        for spec in self.list_specs(include_disabled=False):
            score = 0
            for keyword in spec.intent_keywords:
                token = str(keyword).strip().lower()
                if token and token in text:
                    score += 1
            if score > 0:
                matches.append((spec, score))

        matches.sort(key=lambda item: (-item[1], item[0].skill_id))
        return matches

    def get_by_worker(self, worker: str) -> Optional[SkillSpec]:
        worker_name = str(worker or "").strip()
        for spec in self.list_specs(include_disabled=False):
            if spec.worker == worker_name:
                return spec
        return None


def _default_registry() -> SkillRegistry:
    return SkillRegistry()


_registry = _default_registry()


def get_skill_registry() -> SkillRegistry:
    return _registry


def route_worker_from_catalog(
    instruction: str,
    *,
    min_tables: int,
) -> Optional[str]:
    registry = get_skill_registry()
    matches = registry.find_matches(instruction)
    if not matches:
        return None

    for spec, _score in matches:
        worker = spec.worker
        if worker in {"l2_merge", "l3_reconcile"} and min_tables < 2:
            continue
        if worker in {"l1_hygiene", "l4_visual", "l5_anomaly"} and min_tables < 1:
            continue
        return worker
    return None


def list_catalog_workers() -> tuple[str, ...]:
    registry = get_skill_registry()
    workers = [spec.worker for spec in registry.list_specs(include_disabled=False)]
    return tuple(workers)


def list_catalog_skill_ids() -> tuple[str, ...]:
    registry = get_skill_registry()
    return tuple(spec.skill_id for spec in registry.list_specs(include_disabled=False))


def collect_intent_hits(instruction: str) -> dict[str, int]:
    registry = get_skill_registry()
    hits: dict[str, int] = {}
    for spec, score in registry.find_matches(instruction):
        hits[spec.worker] = max(hits.get(spec.worker, 0), score)
    return hits


def ensure_registry_ready() -> None:
    get_skill_registry().refresh_if_needed()


def is_catalog_available() -> bool:
    registry = get_skill_registry()
    specs = registry.list_specs(include_disabled=False)
    errors = registry.get_errors()
    return bool(specs) and not bool(errors)
