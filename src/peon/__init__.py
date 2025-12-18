import os
from typing import Literal
from dotenv import load_dotenv

load_dotenv()

API_HOST: str = os.getenv("API_HOST", "localhost")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
WORKER_NAME_PREFIX: str = os.getenv("WORKER_NAME_PREFIX", f"{os.name}-peon")

BASE_URL: str = f"http://{API_HOST}:{API_PORT}"

DeviceTypeLiteral = Literal["cpu", "gpu"]
AVAILABLE_DEVICES: list[DeviceTypeLiteral] = ["cpu", "gpu"]

print(f"🪓 Peon module loaded | Serving master at {BASE_URL}")
