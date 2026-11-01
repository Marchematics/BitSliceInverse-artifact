"""Paths used by the experiment drivers."""
import os
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[2])
CODE = str(Path(__file__).resolve().parent)
WORK = os.environ.get('BSI_WORK', str(Path(ROOT) / 'work'))


def expand(value):
    value = str(value)
    if value == 'python3':
        import sys
        return sys.executable
    for prefix, base in [('../../work/', WORK + '/'),
                         ('../../research/', ROOT + '/research/'),
                         ('../../.venv/', ROOT + '/.venv/')]:
        if value.startswith(prefix):
            return base + value[len(prefix):]
    return os.path.expanduser(value)


def core():
    if os.environ.get('BSI_CORE'):
        return os.environ['BSI_CORE']
    return str(min(os.sched_getaffinity(0))) if hasattr(os, 'sched_getaffinity') else '0'


def cores(count):
    available = sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else list(range(os.cpu_count() or 1))
    if os.environ.get('BSI_CORES'):
        available = [int(x) for x in os.environ['BSI_CORES'].split(',')]
    return ','.join(map(str, available[:count]))


def python():
    import sys
    return sys.executable
