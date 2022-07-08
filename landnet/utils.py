"""
Copyright (c) 2008-2021 synodriver <synodriver@gmail.com>
"""
import asyncio
import contextvars
from functools import partial, wraps
from typing import Any, Awaitable, Callable, TypeVar, Union

T = TypeVar("T")


def run_sync(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """
    一个用于包装 sync function 为 async function 的装饰器
    :param func:
    :return:
    """

    @wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        context = contextvars.copy_context()
        context_run = context.run
        result = await loop.run_in_executor(None, context_run, pfunc)
        return result

    return _wrapper


def wrap_sync(
    func: Union[Callable[..., T], Callable[..., Awaitable[T]]]
) -> Callable[..., Awaitable[T]]:
    if asyncio.iscoroutinefunction(func):
        return func
    return run_sync(func)
