from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from app.orchestration import create_agent_first_workflow
from app.orchestration.agent_worker_runtime import (
    clean_code_string,
    execute_code,
    python_worker_node,
)


def create_workflow(
    dfs_context: Dict[str, pd.DataFrame],
    backups_context: Optional[Dict[str, pd.DataFrame]] = None,
):
    """Legacy compatibility shim.

    The old workflow graph has been retired.
    This entrypoint now forwards to Supervisor v2 so older imports keep working.
    """

    return create_agent_first_workflow(dfs_context, backups_context)


__all__ = [
    "clean_code_string",
    "execute_code",
    "python_worker_node",
    "create_workflow",
]
