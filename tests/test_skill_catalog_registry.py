from __future__ import annotations

from pathlib import Path

from app.skills.catalog_registry import SkillRegistry


def _write_skill(
    path: Path,
    *,
    worker: str = "l1_hygiene",
    keyword: str = "清洗",
    name: str = "test-skill",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "---",
                f"name: {name}",
                "description: test",
                f"worker: {worker}",
                "intent_keywords:",
                f"  - {keyword}",
                "risk_level: low",
                "enabled: true",
                "---",
                "",
                "# Test Skill",
            ]
        ),
        encoding="utf-8",
    )


def test_skill_registry_loads_and_matches(tmp_path: Path):
    skill_file = tmp_path / "s1" / "SKILL.md"
    _write_skill(skill_file)

    registry = SkillRegistry(root=tmp_path)
    specs = registry.list_specs()

    assert len(specs) == 1
    assert specs[0].worker == "l1_hygiene"

    matches = registry.find_matches("请先清洗数据")
    assert matches
    assert matches[0][0].worker == "l1_hygiene"


def test_skill_registry_reports_invalid_worker(tmp_path: Path):
    skill_file = tmp_path / "broken" / "SKILL.md"
    _write_skill(skill_file, worker="unknown_worker")

    registry = SkillRegistry(root=tmp_path)
    assert registry.list_specs() == []
    errors = registry.get_errors()
    assert "broken" in errors
    assert "Invalid worker" in errors["broken"]


def test_skill_registry_reports_missing_required_fields(tmp_path: Path):
    skill_file = tmp_path / "broken" / "SKILL.md"
    skill_file.parent.mkdir(parents=True, exist_ok=True)
    skill_file.write_text(
        "\n".join(
            [
                "---",
                "name: broken-skill",
                "description: missing required keys",
                "worker: l1_hygiene",
                "---",
                "",
                "# Broken",
            ]
        ),
        encoding="utf-8",
    )

    registry = SkillRegistry(root=tmp_path)
    assert registry.list_specs() == []
    errors = registry.get_errors()
    assert "broken" in errors
    assert "Missing required frontmatter fields" in errors["broken"]


def test_skill_registry_hot_refresh(tmp_path: Path):
    skill_file = tmp_path / "s1" / "SKILL.md"
    _write_skill(skill_file, keyword="清洗")

    registry = SkillRegistry(root=tmp_path)
    assert registry.find_matches("请清洗")

    _write_skill(skill_file, keyword="对账")
    registry.refresh_if_needed(force=True)

    assert not registry.find_matches("请清洗")
    assert registry.find_matches("请对账")


def test_skill_registry_reports_duplicate_worker_mapping(tmp_path: Path):
    _write_skill(tmp_path / "s1" / "SKILL.md", worker="l1_hygiene", keyword="清洗", name="skill-a")
    _write_skill(tmp_path / "s2" / "SKILL.md", worker="l1_hygiene", keyword="体检", name="skill-b")

    registry = SkillRegistry(root=tmp_path)
    errors = registry.get_errors()
    assert "s2" in errors
    assert "Duplicate worker mapping" in errors["s2"]
