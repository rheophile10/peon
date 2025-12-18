import os
import requests
import json
import time
from datetime import datetime
from typing import Optional

from . import BASE_URL, WORKER_NAME_PREFIX, AVAILABLE_DEVICES
from .axe import execute

WORKER_ID = f"{WORKER_NAME_PREFIX}-{os.getpid()}"


def _api_post(endpoint: str, payload):
    try:
        resp = requests.post(f"{BASE_URL}{endpoint}", json=payload)
        if resp.status_code != 200:
            print(f"⚠️ API error {endpoint}: {resp.status_code} {resp.text}")
    except requests.exceptions.ConnectionError as e:
        print(
            f"💥 can't find master. cannot think for myself. Am useless ☹️\n{endpoint}\n{e}\n"
        )
        resp = None
    return resp


def dequeue_next(device: str = "cpu") -> Optional[dict]:
    if device not in AVAILABLE_DEVICES:
        raise ValueError(f"Device must be one of {AVAILABLE_DEVICES}")

    resp = _api_post(
        "/queue/tasks/dequeue_next", {"worker_id": WORKER_ID, "device": device}
    )
    if not resp:
        return None
    result = resp.json().get("result")
    if result:
        task = result[0]
        print(
            f"🐗 Peon {WORKER_ID} grabbed task {task['id']} ({task['name']}) for {device}"
        )
        return task
    return None


def mark_complete(task_id: str):
    """Mark task as successfully completed."""
    _api_post("/queue/tasks/mark_complete", {"id": task_id})
    print(f"✅ Task {task_id} finished.")


def mark_failed(task_id: str, error_message: str):
    """Mark task as failed with error message."""
    _api_post(
        "/queue/tasks/mark_failed", {"id": task_id, "error_message": error_message}
    )
    print(f"💀 Task{task_id} failed: {error_message}")


def enqueue_task(
    func_name: str,
    data: dict,
    device: str = "cpu",
    run_at: Optional[datetime] = None,
) -> str:
    created_at = datetime.now().isoformat()
    if run_at is not None:
        run_at = run_at.isoformat()

    payload = {
        "name": func_name,
        "data": data,
        "device": device,
        "run_at": run_at,
        "created_at": created_at,
    }

    resp = _api_post("/queue/tasks/enqueue", payload)
    if resp.status_code == 200:
        print(f"📜 Enqueued {func_name} (device: {device})")
    else:
        raise RuntimeError(f"Failed to enqueue task: {resp.text}")


def execute_task(task: dict):
    task_name = task["name"]
    task_data = json.loads(task["data"])

    print(f"🪓 Work work! Executing {task_name}...")

    try:
        execute(task_name, **task_data)
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"😵 Error: {error_msg}")
        raise RuntimeError(error_msg) from e


def sisyphus(
    device: str = "cpu", poll_interval: float = 2.0, max_runtime: Optional[float] = 60
):
    print(f"🪓 Peon {WORKER_ID} reporting for duty! Device: {device.upper()}")
    print("attempting to get first task!\n")

    start_time = time.time()

    while True:
        if max_runtime and (time.time() - start_time) > max_runtime:
            print("\n🕰️ Time's up, boss!")
            break

        try:
            task = dequeue_next(device=device)
        except Exception as e:
            print(f"💥 Couldn't get task: {e}")
            task = None

        if task:
            try:
                execute_task(task)
                mark_complete(task["id"])
            except Exception as exc:
                mark_failed(task["id"], str(exc))
        else:
            time.sleep(poll_interval)
