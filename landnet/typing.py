"""
Copyright (c) 2008-2021 synodriver <synodriver@gmail.com>
"""
from typing import Awaitable, Callable, TypeVar, Union

T = TypeVar("T")
Handler_T = Union[Callable[..., Awaitable[T]], Callable[..., T]]
