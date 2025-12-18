# peon/__main__.py
import os
import argparse
from .work_work import sisyphus

import peon


def main():
    parser = argparse.ArgumentParser(
        description="Peon Worker – Zug zug! Ready to work!"
    )
    parser.add_argument("--host", type=str, help="API host (overrides env and default)")
    parser.add_argument("--port", type=int, help="API port (overrides env and default)")
    parser.add_argument(
        "--name", type=str, help="Worker name prefix, e.g. 'scout-peon' or 'builder'"
    )
    parser.add_argument("--device", choices=peon.AVAILABLE_DEVICES, default="cpu")
    parser.add_argument(
        "--runtime", type=float, default=None, help="Max runtime in seconds"
    )
    parser.add_argument(
        "--poll", type=float, default=2.0, help="Poll interval when idle"
    )

    args = parser.parse_args()

    if args.host:
        peon.API_HOST = args.host
        peon.BASE_URL = f"http://{peon.API_HOST}:{peon.API_PORT}"
    if args.port:
        peon.API_PORT = args.port
        peon.BASE_URL = f"http://{peon.API_HOST}:{peon.API_PORT}"
    if args.name:
        peon.WORKER_NAME_PREFIX = args.name

    print(f"🐗 ORC PEON DIVISION 🐗")
    print(f"Reporting: {peon.WORKER_NAME_PREFIX}-{os.getpid()}")
    print(f"Master's hall: {peon.BASE_URL}")
    print(f"Assigned to: {args.device.upper()}\n")

    try:
        sisyphus(
            device=args.device,
            poll_interval=args.poll,
            max_runtime=args.runtime,
        )
    except KeyboardInterrupt:
        print("\n🛑 stopping... ")


if __name__ == "__main__":
    main()
