"""
Copyright (c) 2008-2021 synodriver <synodriver@gmail.com>
"""
import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Union
from uuid import uuid4

from landnet.log import logger
from landnet.typing import Handler_T
from landnet.utils import wrap_sync

_all_services: Dict[str, "Service"] = {}
_pending_calls: Dict[int, asyncio.Future] = {}
_loop: asyncio.AbstractEventLoop = None  # 第一个启动的service设置loop 整个模块都使用它


def init(loop):
    global _loop
    _loop = loop


class _BaseService:
    def __new__(cls, name: str, *args, **kwargs):
        if name not in _all_services:
            obj = super().__new__(cls)
            _all_services[name] = obj  # type:ignore
            logger.info(f"creating service {name}")
            return obj
        else:
            raise ValueError(f"Service {name} already exists")


class Service(_BaseService):
    """基本并发单元"""

    def __init__(
        self,
        name,
        *,
        handler: Handler_T = None,
        on_startup: Handler_T = None,
        on_shutdown: Handler_T = None,
        max_size: int = 10,
        **kwargs,
    ):
        self.name = name
        if handler is not None:
            self._handler = wrap_sync(handler)  # 处理消息的
        else:
            self._handler = None

        if on_startup is not None:
            self._on_startup = wrap_sync(on_startup)
        else:
            self._on_startup = None

        if on_shutdown is not None:
            self._on_shutdown = wrap_sync(on_shutdown)
        else:
            self._on_shutdown = None
        self._max_size = max_size
        self._pending_tasks: Set[asyncio.Task] = set()

        self._loop = None
        self.kwargs = kwargs  # 额外的保留参数

    def handle(self):
        def inner(func: Handler_T) -> Handler_T:
            self._handler = wrap_sync(func)
            return func

        return inner

    def on_startup(self):
        """设置启动时的callback"""

        def inner(
            func: Callable[["Service"], Awaitable[Any]]
        ) -> Callable[["Service"], Awaitable[Any]]:
            self._on_startup = wrap_sync(func)
            return func

        return inner

    def on_shutdown(self):
        """设置关闭时的callback"""

        def inner(
            func: Callable[["Service"], Awaitable[Any]]
        ) -> Callable[["Service"], Awaitable[Any]]:
            self._on_shutdown = wrap_sync(func)
            return func

        return inner

    def put_msg(self, msg: tuple):
        """
        :msg: 第一个是args, 第二个是kw
        """
        if len(self._pending_tasks) < self._max_size:
            task = _loop.create_task(
                self._handler(*msg[0], **msg[1])
            )  # todo document about this 调用约定 穿tuple 第一个元素是args 第二个是kw
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
        else:
            raise ValueError(f"msg queue for {self.name} is full")

    async def startup(self):
        """启动service 一般由run调用"""
        if self._on_startup is not None:
            await self._on_startup(self)
        logger.info(f"staring up service {self.name}")

    async def shutdown(self):
        """关闭service 一般由run调用"""
        while self._pending_tasks:
            task = self._pending_tasks.pop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._on_shutdown is not None:
            await self._on_shutdown(self)
        logger.info(f"shuting down service {self.name}")


def get_service(name: str) -> Optional[Service]:
    """根据名字找一个service"""
    return _all_services.get(name, None)


def list_services() -> Dict[str, Service]:
    """取得全部service对象"""
    return _all_services


def send(name: Union[Service, str], *args, **kwargs) -> None:
    """知识往queue赛一个消息，不要求返回值"""
    if isinstance(name, str):
        service = get_service(name)
    elif isinstance(name, Service):
        service = name
    else:
        raise ValueError("name must be instance of str or Service")
    if service is None:
        raise ValueError(f"no service called {name}")
    service.put_msg((args, kwargs))


async def call(name: Union[Service, str], *args, **kwargs):
    """必须有返回值，调用reply 使用对应的uuid"""
    if "uuid" in kwargs:
        raise ValueError("uuid is a restore field")  # todo document about this
    uuid: int = uuid4().int
    kwargs["uuid"] = uuid
    send(name, *args, **kwargs)
    future = _loop.create_future()
    _pending_calls[uuid] = future
    timeout = kwargs.pop("timeout", 5)
    try:
        return await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        logger.warning(f"calling {name} time out")
        raise
    finally:
        del _pending_calls[uuid]


def reply(uuid: int, msg: Any, err: Any = None) -> None:
    """消息送达即可"""
    future = _pending_calls.get(uuid, None)
    if future is None:
        raise ValueError("no such call")
    if not future.done() and not future.cancelled():
        if err is None:
            future.set_result(msg)
        else:
            future.set_exception(err)
    else:
        raise ValueError("call already done")
