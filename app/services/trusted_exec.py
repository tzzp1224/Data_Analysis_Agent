import ast
import builtins
import io
import multiprocessing as mp
import sys
import traceback
from typing import Dict

import numpy as np
import pandas as pd

from app.utils.tools import AuditLogger, smart_merge, smart_reconcile


class SecurityViolation(ValueError):
    """Raised when generated code violates execution security rules."""


ALLOWED_IMPORTS = {
    "pandas",
    "numpy",
    "re",
    "warnings",
    "plotly",
    "plotly.express",
    "plotly.graph_objects",
}

BLOCKED_CALLS = {
    "eval",
    "exec",
    "open",
    "compile",
    "__import__",
    "input",
    "globals",
    "locals",
    "vars",
    "getattr",
    "setattr",
    "delattr",
    "dir",
}

BLOCKED_ATTR_CALLS = {
    "system",
    "popen",
    "remove",
    "unlink",
    "rmdir",
    "rename",
    "replace",
    "read_csv",
    "read_excel",
    "to_csv",
    "to_excel",
    "to_pickle",
    "read_pickle",
}

SAFE_BUILTINS = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "next": next,
    "print": print,
    "range": range,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "iter": iter,
    "map": map,
    "filter": filter,
    "zip": zip,
    "Exception": Exception,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "IndexError": IndexError,
}


def _is_allowed_import(module_name: str) -> bool:
    return module_name in ALLOWED_IMPORTS


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    if not _is_allowed_import(name):
        raise SecurityViolation(f"Import blocked: {name}")
    return builtins.__import__(name, globals, locals, fromlist, level)


def _call_name(node: ast.Call) -> str:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def validate_code_safety(code: str) -> None:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise SecurityViolation(f"Syntax error: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if not _is_allowed_import(alias.name):
                    raise SecurityViolation(f"Import blocked: {alias.name}")

        if isinstance(node, ast.ImportFrom):
            if not node.module or not _is_allowed_import(node.module):
                raise SecurityViolation(f"Import blocked: {node.module}")

        if isinstance(node, ast.Attribute):
            if node.attr.startswith("__"):
                raise SecurityViolation(f"Dunder attribute blocked: {node.attr}")
            if node.attr in BLOCKED_ATTR_CALLS:
                raise SecurityViolation(f"Attribute blocked: {node.attr}")

        if isinstance(node, ast.Call):
            name = _call_name(node)
            if name in BLOCKED_CALLS:
                raise SecurityViolation(f"Call blocked: {name}")
            if name in BLOCKED_ATTR_CALLS:
                raise SecurityViolation(f"Call blocked: {name}")

        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise SecurityViolation(f"Dunder name blocked: {node.id}")


def _run_sandboxed(dfs: Dict[str, pd.DataFrame], code: str, queue: mp.Queue) -> None:
    import plotly.express as px
    import plotly.graph_objects as go

    audit = AuditLogger()

    def smart_merge_wrapper(left, right, left_on, right_on, threshold=None):
        return smart_merge(left, right, left_on, right_on, logger=audit)

    def smart_reconcile_wrapper(df_sys, df_bank, sys_key, bank_key, sys_amount, bank_amount, tolerance=0.01):
        return smart_reconcile(df_sys, df_bank, sys_key, bank_key, sys_amount, bank_amount, tolerance, logger=audit)

    def reload_data_wrapper(filename: str):
        backup_key = f"__backup_{filename}"
        if backup_key in dfs:
            dfs[filename] = dfs[backup_key].copy(deep=True)
            print(f"🔄 [System] 数据已还原: {filename}")
            return True
        print(f"❌ [System] 未找到备份: {filename}")
        return False

    local_vars = {
        "dfs": dfs,
        "pd": pd,
        "np": np,
        "px": px,
        "go": go,
        "audit": audit,
        "smart_merge": smart_merge_wrapper,
        "smart_reconcile": smart_reconcile_wrapper,
        "reload_data": reload_data_wrapper,
    }
    if dfs:
        local_vars["df"] = dfs[next(iter(dfs.keys()))]

    old_stdout = sys.stdout
    redirected_output = io.StringIO()
    sys.stdout = redirected_output

    try:
        validate_code_safety(code)
        safe_code = "import warnings\nwarnings.filterwarnings('ignore')\n" + code
        safe_globals = {
            "__builtins__": {**SAFE_BUILTINS, "__import__": _safe_import},
        }
        exec(compile(safe_code, "<trusted-exec>", "exec"), safe_globals, local_vars)

        chart_jsons = []
        for var_name, var_val in local_vars.items():
            if var_name.startswith("fig") and hasattr(var_val, "to_json"):
                chart_jsons.append(var_val.to_json())

        result_df = local_vars.get("result_df")
        if result_df is not None and not isinstance(result_df, pd.DataFrame):
            result_df = None

        queue.put(
            {
                "success": True,
                "dfs": local_vars["dfs"],
                "chart_jsons": chart_jsons,
                "result_df": result_df,
                "audit_logger": audit,
                "log": redirected_output.getvalue(),
            }
        )
    except SecurityViolation as exc:
        queue.put(
            {
                "success": False,
                "dfs": dfs,
                "chart_jsons": [],
                "result_df": None,
                "audit_logger": audit,
                "log": f"❌ Runtime Error:\nSecurityViolation: {exc}",
            }
        )
    except Exception:
        queue.put(
            {
                "success": False,
                "dfs": dfs,
                "chart_jsons": [],
                "result_df": None,
                "audit_logger": audit,
                "log": f"❌ Runtime Error:\n{traceback.format_exc()}",
            }
        )
    finally:
        sys.stdout = old_stdout


def run_trusted_code(dfs: Dict[str, pd.DataFrame], code: str, timeout_seconds: int = 15) -> dict:
    clean_code = str(code).strip()
    if not clean_code:
        return {
            "success": True,
            "dfs": dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": AuditLogger(),
            "log": "无代码",
        }

    try:
        ctx = mp.get_context("fork")
    except ValueError:
        ctx = mp.get_context("spawn")

    queue: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_run_sandboxed, args=(dfs, clean_code, queue), daemon=True)
    proc.start()
    proc.join(timeout_seconds)

    if proc.is_alive():
        proc.terminate()
        proc.join(1)
        return {
            "success": False,
            "dfs": dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": AuditLogger(),
            "log": f"❌ Runtime Error:\nExecutionTimeout: exceeded {timeout_seconds}s",
        }

    if queue.empty():
        return {
            "success": False,
            "dfs": dfs,
            "chart_jsons": [],
            "result_df": None,
            "audit_logger": AuditLogger(),
            "log": "❌ Runtime Error:\nSandbox process exited unexpectedly.",
        }

    return queue.get()
