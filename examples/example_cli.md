# Peon Worker Examples

## Run the Worker

```bash
# 1. Default: just run a CPU worker (no pinning)
python -m peon

# 2. Run worker pinned to specific core (e.g. core 5)
python -m peon work --cpu-core 5

# 3. Pin to core 42 on an 8-core machine → becomes core 2 (42 % 8)
python -m peon work --cpu-core 42

# 4. Auto-pin to arbitrary core (good for launching many workers)
python -m peon work --pin-cpu

# 5. Run on GPU (if available)
python -m peon work --device gpu

# 6. Custom host/port and name
python -m peon work --host 192.168.1.100 --port 9000 --name builder

# 7. Enqueue a routine called "backup_database"
python -m peon enqueue backup_database

# 8. Dump all current tasks to tasks_dump.csv
python -m peon dump-tasks
```