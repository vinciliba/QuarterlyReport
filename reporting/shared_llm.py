from typing import List
import os

def call_llm(messages: List[dict[str, str]]) -> str:
    """
    Very thin wrapper – swap in llama-cpp, OpenAI, etc.
    messages: [{"role":"system", "content":…}, {"role":"user", …}]
    """
    # EXAMPLE with llama-cpp-python
    # from llama_cpp import Llama
    # llm = Llama(model_path=os.getenv("LLAMA_PATH"))
    # output = llm.create(messages, max_tokens=400)
    # return output["choices"][0]["message"]["content"]

    # → stub while no model available in Codespaces
    return "LLM-generated commentary stub."
