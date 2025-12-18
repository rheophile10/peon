import importlib
from functools import partial


def partial_from_func_name(func_name: str, **kwargs):
    if "." not in func_name:
        raise ValueError("func_name must contain at least one dot (module.function)")

    module_path, func_name = func_name.rsplit(".", 1)

    module = importlib.import_module(module_path)

    func = getattr(module, func_name)

    if not callable(func):
        raise TypeError(f"{func_name} resolves to {func} which is not callable")

    return partial(func, **kwargs)


def execute(func_name: str, **kwargs):
    partial_func = partial_from_func_name(func_name, **kwargs)
    return partial_func()
