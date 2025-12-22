# peon/__main__.py
import os
import argparse
import asyncio

from peon.work_work import sisyphus
from peon.routines import enqueue_routine, dump_tasks_cmd
from peon import (
    HOST as ENV_HOST,
    PORT as ENV_PORT,
    WORKER_NAME_PREFIX as ENV_PREFIX,
    AVAILABLE_DEVICES,
)
import peon


def _normalize_core(requested: int) -> int:
    core_count = os.cpu_count() or 1
    return requested % core_count if core_count > 1 else 0


def _pin_to_core(core: int) -> None:
    pinned = False
    try:
        import psutil

        psutil.Process().cpu_affinity([core])
        print(f"Pinned to CPU core {core} (via psutil)")
        pinned = True
    except ImportError:
        pass
    except Exception as e:
        print(f"psutil pinning failed: {e}")

    if not pinned:
        try:
            os.sched_setaffinity(0, {core})
            print(f"Pinned to CPU core {core} (via sched_setaffinity)")
            pinned = True
        except AttributeError:
            pass
        except Exception as e:
            print(f"sched_setaffinity failed: {e}")

    if not pinned:
        print(f"No hard pinning — running with requested core {core} (best effort)")


def run_worker(args):
    # Safely get values (they always exist because we used default= in argparse)
    host = getattr(args, "host", ENV_HOST)
    port = getattr(args, "port", ENV_PORT)
    name = getattr(args, "name", ENV_PREFIX)

    # Update module-level variables
    peon.HOST = host
    peon.PORT = port
    peon.WORKER_NAME_PREFIX = name
    peon.BASE_URL = f"http://{host}:{port}"

    device = getattr(args, "device", "cpu")
    cpu_core = getattr(args, "cpu_core", None)

    # CPU pinning
    if device == "cpu":
        if cpu_core is not None:
            core = _normalize_core(cpu_core)
            print(f"Requested core {cpu_core} → using core {core}")
            _pin_to_core(core)
        elif getattr(args, "pin_cpu", False):
            core_count = os.cpu_count() or 1
            core = os.getpid() % core_count
            print(f"Auto-selected arbitrary core {core} (PID-based)")
            _pin_to_core(core)

    print(f"Reporting: {peon.WORKER_NAME_PREFIX}-{os.getpid()}")
    print(f"Master's hall: {peon.BASE_URL}")
    print(f"Assigned to: {device.upper()}\n")
    poll = getattr(args, "poll", 2.0)
    runtime = getattr(args, "runtime", None)

    try:
        asyncio.run(
            sisyphus(
                device=device,
                poll_interval=poll,
                max_runtime=runtime,
            )
        )
    except KeyboardInterrupt:
        print("\nZug zug... stopping gracefully.")


async def enqueue_routine_cmd(args):
    routine = getattr(args, "routine", None)
    print(f"Enqueuing routine: {routine}")
    try:
        routine = await enqueue_routine(routine)
        print(f"Routines enqueued successfully:")
        for task in routine:
            print(f" - '{task['name']}' ({task})")
    except Exception as e:
        print(f"Failed to enqueue routine '{routine}': {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Peon Worker – Zug zug! Ready to work!"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # === WORK COMMAND ===
    work_parser = subparsers.add_parser("work", help="Run the worker (default)")
    work_parser.add_argument(
        "--host",
        type=str,
        default=ENV_HOST,
        help=f"Master API host (default: {ENV_HOST})",
    )
    work_parser.add_argument(
        "--port",
        type=int,
        default=ENV_PORT,
        help=f"Master API port (default: {ENV_PORT})",
    )
    work_parser.add_argument(
        "--name",
        type=str,
        default=ENV_PREFIX,
        help=f"Worker name prefix (default: {ENV_PREFIX})",
    )
    work_parser.add_argument(
        "--device",
        choices=AVAILABLE_DEVICES,
        default="cpu",
        help="Device to use",
    )
    work_parser.add_argument(
        "--cpu-core",
        type=int,
        metavar="N",
        help="Pin to specific CPU core N (modulo if out of range)",
    )
    work_parser.add_argument(
        "--pin-cpu",
        action="store_true",
        help="Pin to arbitrary core (PID-based round-robin)",
    )
    work_parser.add_argument(
        "--runtime",
        type=float,
        default=None,
        help="Max runtime in seconds",
    )
    work_parser.add_argument(
        "--poll",
        type=float,
        default=2.0,
        help="Poll interval when idle (seconds)",
    )
    work_parser.set_defaults(func=run_worker)

    # === ENQUEUE COMMAND ===
    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a routine")
    enqueue_parser.add_argument(
        "routine", type=str, help="Routine name (without .yaml)"
    )
    enqueue_parser.set_defaults(
        func=lambda args: asyncio.run(enqueue_routine_cmd(args))
    )

    # === DUMP TASKS COMMAND ===
    dump_parser = subparsers.add_parser("dump-tasks", help="Dump all tasks to CSV")
    dump_parser.set_defaults(func=lambda args: asyncio.run(dump_tasks_cmd(args)))

    args = parser.parse_args()

    if args.command is None:
        args.command = "work"
        args.func = run_worker

    args.func(args)


if __name__ == "__main__":
    main()
