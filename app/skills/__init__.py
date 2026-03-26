"""Deterministic skill modules for high-confidence business workflows."""


def execute_skill(*args, **kwargs):
    # Lazy import to avoid heavy transitive dependencies during package discovery.
    from app.skills.engine import execute_skill as _execute_skill

    return _execute_skill(*args, **kwargs)


__all__ = ["execute_skill"]
