from typing import Any, Dict
from pathlib import Path
import yaml
from peon import BASE_DIR
import csv
import json
from peon.talk import post, get


def _get_routine_path(routine_name: str, base_dir: Path = BASE_DIR) -> Path:
    routine_path = base_dir / "routines" / f"{routine_name}.yaml"
    if not routine_path.exists():
        raise ValueError(f"Routine file does not exist: {routine_path}")
    return routine_path


def _parse_routine_yaml(routine_path: Path) -> Dict[str, Any]:
    with open(routine_path, "r", encoding="utf-8") as f:
        routine_def = yaml.safe_load(f)

    # it would be nice to validate the yaml here

    tasks = routine_def.get("tasks", [])
    if not isinstance(tasks, list):
        raise ValueError(f"'tasks' field must be a list in routine '{routine_path}'")

    return tasks


async def enqueue_routine(
    routine_name: str,
) -> Dict[str, Any]:
    routines_path = _get_routine_path(routine_name)
    tasks = _parse_routine_yaml(routines_path)
    start_task = {
        "routine_id": None,
        "name": routine_name,
        "data": None,
        "predecessors": None,
        "device": "cpu",
        "status": "completed",
        "run_at": None,
        "frequency": None,
    }
    start_task = await post("/queue/tasks/enqueue", start_task)

    routine_id = start_task[0]["id"]
    updated_start_task = await post(
        "/queue/tasks/update_routine_id", {"id": routine_id}
    )
    tasks_with_ids = {}
    for task in tasks:
        predecessors = task.get("predecessors", [])
        mapped_predecessor_ids = [
            tasks_with_ids[pred]["db_id"]
            for pred in predecessors
            if pred in tasks_with_ids
        ]
        if len(mapped_predecessor_ids) != len(predecessors):
            raise ValueError(
                f"Predecessor not found or cycle detected in routine '{routine_name}'"
            )
        task_data_json = json.dumps(task.get("data", {}))

        task_payload = {
            "routine_id": routine_id,
            "name": task["name"],
            "data": task_data_json,
            "predecessors": ",".join([str(pid) for pid in mapped_predecessor_ids]),
            "device": task.get("device", "cpu"),
            "status": "pending",
            "run_at": task.get("run_at", None),
            "frequency": task.get("frequency", None),
        }
        task_data = await post("/queue/tasks/enqueue", task_payload)
        task_with_id = task.copy()
        task_with_id["db_id"] = task_data[0]["id"]
        tasks_with_ids[task["id"]] = task_with_id

    return tasks_with_ids.values()


async def dump_tasks_cmd(_args=None):
    print("📥 Fetching all tasks from master...")
    try:
        data = await get("/queue/tasks/get")
        print(f"📊 Retrieved {len(data)} tasks")

        csv_path = BASE_DIR / "tasks_dump.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

        print(f"💾 Tasks dumped to {csv_path.resolve()}")
    except Exception as e:
        print(f"❌ Failed to fetch or dump tasks: {e}")
