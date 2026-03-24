# 配置中心，分离易变的key和model name
import os
from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(item.strip() for item in str(raw).split(",") if item.strip())


class Settings:
    
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL_NAME", "gemini-3-flash-preview")
    AGENT_FIRST_ENABLED = _env_bool("AGENT_FIRST_ENABLED", True)
    SUPERVISOR_MAX_AGENT_RETRIES = int(os.getenv("SUPERVISOR_MAX_AGENT_RETRIES", "2"))
    SUPERVISOR_DIRECT_SKILL_WHITELIST = _env_csv("SUPERVISOR_DIRECT_SKILL_WHITELIST", "")
    SUPERVISOR_REQUIRE_OFFICIAL = _env_bool("SUPERVISOR_REQUIRE_OFFICIAL", True)

settings = Settings()
