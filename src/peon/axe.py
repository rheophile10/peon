import importlib
from functools import partial
from typing import Callable, Optional


def partial_from_func_name(func_name: str, **kwargs) -> Optional[Callable]:
    if "." not in func_name:
        return None

    module_path, func_name_part = func_name.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
    except ImportError:
        return None

    try:
        func = getattr(module, func_name_part)
    except AttributeError:
        return None

    if not callable(func):
        return None

    return partial(func, **kwargs)


def execute(func_name: str, **kwargs):
    partial_func = partial_from_func_name(func_name, **kwargs)

    if partial_func is None:
        raise ValueError(f"I don't know how to '{func_name}' 🥴")
    partial_func()
