# LLM工厂模式。负责生产 LLM 实例，统一管理参数（如 Temperature）和安全设置。
from langchain_google_genai import ChatGoogleGenerativeAI, HarmBlockThreshold, HarmCategory
from app.core.config import settings

def get_llm(temperature=0):
    """
    获取 Google Gemini LLM 实例。
    """
    if not settings.GOOGLE_API_KEY:
        raise ValueError("❌ 未找到 GOOGLE_API_KEY，请检查 .env 文件")

    return ChatGoogleGenerativeAI(
        google_api_key=settings.GOOGLE_API_KEY,
        model=settings.GOOGLE_MODEL_NAME,
        temperature=temperature,
        # 👇 关掉安全过滤，防止分析数据时误报
        safety_settings={
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
    )