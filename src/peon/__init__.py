import os
from typing import Literal
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

HOST: str = os.getenv("HOST", "localhost")
PORT: int = int(os.getenv("PORT", "8000"))
WORKER_NAME_PREFIX: str = os.getenv("WORKER_NAME_PREFIX", f"{os.name}-peon")
BASE_DIR: Path = Path(os.getenv("BASE_DIR", str(Path(__file__).parent.parent)))

BASE_URL: str = f"http://{HOST}:{PORT}"

DeviceTypeLiteral = Literal["cpu", "gpu"]
AVAILABLE_DEVICES: list[DeviceTypeLiteral] = ["cpu", "gpu"]

print(f"🪓 Peon module loaded | Serving master at {BASE_URL}")


def example_func(a: int, b: int) -> int:
    print(f"Example func called with {a} and {b}")
    return a + b


def example_func2(c: str) -> None:
    print(f"Example func2 called with {c}")
    return
