# 配置中心，分离易变的key和model name
import os
from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

class Settings:
    
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL_NAME", "gemini-3-flash-preview")
    AGENT_FIRST_ENABLED = _env_bool("AGENT_FIRST_ENABLED", True)
    SUPERVISOR_REQUIRE_OFFICIAL = _env_bool("SUPERVISOR_REQUIRE_OFFICIAL", True)

settings = Settings()
