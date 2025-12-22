import os
import time
import asyncio
from typing import Optional

from peon import WORKER_NAME_PREFIX
from peon.axe import chop, kill_the_stale, progress, propagate_failure
from peon.talk import post


WORKER_ID = f"{WORKER_NAME_PREFIX}-{os.getpid()}"


async def heartbeat_loop(task_id: str, interval: int = 90):
    """Background task that updates heartbeat every `interval` seconds."""
    while True:
        try:
            await post("/queue/tasks/heartbeat", {"id": task_id})
            print(f"❤️ Heartbeat sent for task {task_id}")
        except Exception as e:
            print(f"⚠️ Failed to send heartbeat for task {task_id}: {e}")
        await asyncio.sleep(interval)


async def sisyphus(
    device: str = "cpu",
    poll_interval: float = 10.0,
    max_runtime: Optional[float] = None,
):
    start_time = time.time()

    while True:
        if max_runtime is not None and (time.time() - start_time) > max_runtime:
            print("\n🕰️ Time's up, boss!")
            break

        try:

            stale_tasks = await kill_the_stale()
            tasks = await post(
                "/queue/tasks/dequeue_next", {"worker_id": WORKER_ID, "device": device}
            )
            if len(tasks) == 0:
                task = None
                print("🛌 No tasks available, sleeping...")
            else:
                task = tasks[0]
                print(f"🚧 Dequeued up task {task['id']} ({task['name']})")
        except Exception as e:
            print(f"💥 Couldn't dequeue task: {e}")
            task = None

        if task:
            await chop(
                task,
                heartbeat_func=heartbeat_loop,
                mark_complete_func=progress,
                mark_failed_func=propagate_failure,
            )
        else:
            await asyncio.sleep(poll_interval)
