import requests
from langchain_openai import ChatOpenAI
from langchain_community.chat_models import ChatOllama

# Optional: use try-import for Ollama
try:
    from langchain_community.chat_models import ChatOllama
    OLLAMA_AVAILABLE = True
except ImportError:
    ChatOllama = None
    OLLAMA_AVAILABLE = False

def get_fallback_llm(model_name="qwen2.5:14b", openai_model="gpt-4", temperature=0.4):
    """
    Return a chat model instance:
    - Tries Ollama (localhost:11434) if available
    - Falls back to OpenAI or other online provider
    """
    if OLLAMA_AVAILABLE:
        try:
            response = requests.get("http://localhost:11434", timeout=5)
            if response.status_code == 200:
                return ChatOllama(model=model_name, temperature=temperature)
        except requests.RequestException:
            pass
    # Fallback to OpenAI
    return ChatOpenAI(model=openai_model, temperature=temperature)
