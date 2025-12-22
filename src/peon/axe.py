import importlib
from functools import partial
from typing import Callable, Awaitable, Optional, Union, Dict, Any
import inspect
import asyncio
import json
import traceback
from peon.talk import post


AsyncOrSyncCallable = Union[Callable[..., None], Callable[..., Awaitable[None]]]


def _partial_from_func_name(
    func_name: str, data: Dict[str, Any]
) -> Optional[AsyncOrSyncCallable]:
    if "." not in func_name:
        return None
    if data is None:
        data = {}
    module_path, func_name_part = func_name.rsplit(".", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name_part)
    partial_func = partial(func, **data)
    return partial_func


async def progress(task_id: str) -> None:
    completed_task = await post("/queue/tasks/mark_complete", {"id": task_id})
    print(f"✅ Task {task_id} {completed_task[0]['name']} marked as complete.")
    scheduled_task = await post(
        "/queue/tasks/schedule_next_recurring", {"id": completed_task[0]["id"]}
    )
    print(f"   ➡️  Scheduled next occurrence: {scheduled_task}")
    return scheduled_task


async def propagate_failure(task_id: str, error_msg: str) -> None:
    failed_task = await post(
        "/queue/tasks/mark_failed", {"id": task_id, "error_message": error_msg}
    )
    print(f"❌ Task {task_id} {failed_task[0]['name']} marked as failed.")
    blocked_tasks = await post(
        "/queue/tasks/propagate_failure",
        {"id": failed_task[0]["id"], "error_message": error_msg},
    )
    for bt in blocked_tasks:
        print(f"   - Blocked task {bt['id']} {bt['name']} marked as failed.")
    return blocked_tasks


async def kill_the_stale(stale_threshold_seconds: int = 3600) -> None:
    stale_tasks = await post(
        "/queue/tasks/kill_the_stale",
        {"stale_threshold_seconds": stale_threshold_seconds},
    )
    seen_ids = set()
    failed_tasks = []
    for task in stale_tasks:
        if task["id"] in seen_ids:
            continue
        seen_ids.add(task["id"])
        blocked_tasks = await post("/queue/tasks/propagate_failure", {"id": task["id"]})
        for bt in blocked_tasks:
            if bt["id"] not in seen_ids:
                seen_ids.add(bt["id"])
                failed_tasks.append(bt)
    if len(failed_tasks) > 0:
        print(f"🗡️ Killed {len(stale_tasks)} stale tasks.")
    for task in stale_tasks:
        print(f"   -{task['name']} {task['status']}")
    return failed_tasks


async def chop(
    task: Dict[str, Any],
    *,
    heartbeat_func=None,
    mark_complete_func=None,
    mark_failed_func=None,
) -> None:
    task_id = task["id"]
    func_name = task["name"]
    data = json.loads(task["data"])

    try:
        partial_func = _partial_from_func_name(func_name, data)
    except Exception as e:
        error_msg = f"Error loading function '{func_name}': {e}"
        if mark_failed_func:
            await mark_failed_func(task_id, error_msg)
        raise

    heartbeat_task = None
    if heartbeat_func:
        heartbeat_task = asyncio.create_task(heartbeat_func(task_id))

    try:
        result = partial_func()
        if inspect.iscoroutine(result):
            await result
        if mark_complete_func:
            await mark_complete_func(task_id)

    except Exception as exc:
        error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        if mark_failed_func:
            await mark_failed_func(task_id, error)

    finally:
        if heartbeat_func:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
