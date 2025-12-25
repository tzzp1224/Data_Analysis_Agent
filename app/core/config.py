# 配置中心，分离易变的key和model name
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    GOOGLE_MODEL_NAME = os.getenv("GOOGLE_MODEL_NAME", "gemini-3-flash-preview")

settings = Settings()