import aiohttp
import asyncio
from typing import Optional
from peon import BASE_URL

_session: Optional[aiohttp.ClientSession] = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        timeout = aiohttp.ClientTimeout(total=90)
        _session = aiohttp.ClientSession(timeout=timeout)
    return _session


class APIError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


async def _request(
    method: str,
    endpoint: str,
    json: Optional[dict] = None,
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> dict:
    url = f"{BASE_URL}{endpoint}"
    session = await _get_session()

    for attempt in range(max_retries):
        try:
            async with session.request(method, url, json=json) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise APIError(
                        f"API error {resp.status}: {text.strip()[:200]}", resp.status
                    )

                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    raise APIError(f"Invalid JSON response: {text.strip()[:200]}")

                return data.get("result") if "result" in data else data

        except (APIError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                await asyncio.sleep(delay)
            else:
                raise e


async def post(endpoint: str, payload: dict = {}) -> dict:
    return await _request("POST", endpoint, json=payload)


async def get(endpoint: str) -> dict:
    return await _request("GET", endpoint)


async def close_session():
    """Call this at program exit to cleanly close the session."""
    global _session
    if _session is not None and not _session.closed:
        await _session.close()
        _session = None


import atexit

atexit.register(lambda: asyncio.run(close_session()) if _session else None)
