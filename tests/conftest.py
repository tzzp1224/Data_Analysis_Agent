from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_stub_modules() -> None:
    if "rapidfuzz" not in sys.modules:
        rapidfuzz = types.ModuleType("rapidfuzz")
        process_mod = types.ModuleType("rapidfuzz.process")
        fuzz_mod = types.ModuleType("rapidfuzz.fuzz")

        def _extract_one(*_args, **_kwargs):
            return None

        process_mod.extractOne = _extract_one  # type: ignore[attr-defined]
        fuzz_mod.WRatio = object()  # type: ignore[attr-defined]

        rapidfuzz.process = process_mod  # type: ignore[attr-defined]
        rapidfuzz.fuzz = fuzz_mod  # type: ignore[attr-defined]
        sys.modules["rapidfuzz"] = rapidfuzz
        sys.modules["rapidfuzz.process"] = process_mod
        sys.modules["rapidfuzz.fuzz"] = fuzz_mod

    if "langchain_google_genai" not in sys.modules:
        module = types.ModuleType("langchain_google_genai")

        class _DummyModel:
            def __init__(self, *args, **kwargs):
                pass

        class _DummyEnum:
            BLOCK_NONE = "BLOCK_NONE"
            HARM_CATEGORY_HARASSMENT = "HARM_CATEGORY_HARASSMENT"
            HARM_CATEGORY_HATE_SPEECH = "HARM_CATEGORY_HATE_SPEECH"
            HARM_CATEGORY_SEXUALLY_EXPLICIT = "HARM_CATEGORY_SEXUALLY_EXPLICIT"
            HARM_CATEGORY_DANGEROUS_CONTENT = "HARM_CATEGORY_DANGEROUS_CONTENT"

        module.ChatGoogleGenerativeAI = _DummyModel  # type: ignore[attr-defined]
        module.HarmBlockThreshold = _DummyEnum  # type: ignore[attr-defined]
        module.HarmCategory = _DummyEnum  # type: ignore[attr-defined]
        sys.modules["langchain_google_genai"] = module

    if "plotly" not in sys.modules:
        plotly = types.ModuleType("plotly")
        express = types.ModuleType("plotly.express")

        class _DummyFig:
            def to_json(self):
                return "{}"

        def _line(*_args, **_kwargs):
            return _DummyFig()

        express.line = _line  # type: ignore[attr-defined]
        plotly.express = express  # type: ignore[attr-defined]
        sys.modules["plotly"] = plotly
        sys.modules["plotly.express"] = express


_install_stub_modules()
