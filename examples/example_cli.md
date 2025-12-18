# Peon Worker Examples

## Run the Worker

```bash
# Basic CPU worker (most common)
python -m peon --device cpu

# GPU worker (for heavy tasks)
python -m peon --device gpu

# Update extra dependencies first, then run
python -m peon --update-env --device cpu

# Run forever with faster polling
python -m peon --device cpu --poll 0.5

# Run for only 10 minutes
python -m peon --device gpu --runtime 600

# Custom worker name (helpful when running multiple)
python -m peon --name gold-miner-01 --device cpu

# Override API location
python -m peon --host 192.168.1.100 --port 9000 --device cpu
```