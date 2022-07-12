"""
Copyright (c) 2008-2021 synodriver <synodriver@gmail.com>
"""
import asyncio
import traceback
from typing import Any, Dict, Optional, Set, Union
from uuid import uuid4

from landnet.log import logger
from landnet.typing import Handler_T
from landnet.utils import wrap_sync

_all_services: Dict[int, "Service"] = {}
_pending_calls: Dict[int, asyncio.Future] = {}
_loop: asyncio.AbstractEventLoop = None  # 第一个启动的service设置loop 整个模块都使用它


def init(loop):
    global _loop
    _loop = loop


class _BaseService:
    def __new__(cls, name: str, *args, **kwargs):
        if name not in _all_services:
            obj = super().__new__(cls)
            uuid = uuid4().int
            obj.uuid = uuid
            assert uuid not in _all_services
            _all_services[uuid] = obj  # type:ignore
            logger.info(f"creating service {name} uuid {uuid}")
            return obj
        else:
            raise ValueError(f"Service {name} already exists")


class Service(_BaseService):
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
        self._queue = None
        self._main_task: asyncio.Task = None

        self._loop = None
        self.kwargs = kwargs  # 额外的保留参数

    def handle(self):
        def inner(func: Handler_T) -> Handler_T:
            self._handler = wrap_sync(func)
            return func

        return inner

    def on_startup(self):
        def inner(func: Handler_T) -> Handler_T:
            self._on_startup = wrap_sync(func)
            return func

        return inner

    def on_shutdown(self):
        def inner(func: Handler_T) -> Handler_T:
            self._on_shutdown = wrap_sync(func)
            return func

        return inner

    async def _main(self):
        while True:
            msg: tuple = await self._queue.get()
            with logger.catch():
                await self._handler(
                    *msg[0], **msg[1]
                )  # todo document about this 调用约定 穿tuple 第一个元素是args 第二个是kw

    def put_msg(self, msg: tuple):
        try:
            _loop.call_soon_threadsafe(self._queue.put_nowait, msg)
            # self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            logger.warning(f"message queue for {self.name} if full")
            raise

    async def startup(self):
        self._queue = asyncio.Queue(self._max_size)
        self._main_task: asyncio.Task = _loop.create_task(self._main())
        if self._on_startup is not None:
            await self._on_startup(self)
        logger.info(f"staring up service {self.name}")

    async def shutdown(self):
        self._main_task.cancel()
        try:
            await self._main_task
        except asyncio.CancelledError:
            pass
        if self._on_shutdown is not None:
            await self._on_shutdown(self)
        logger.info(f"shuting down service {self.name} uuid {self.uuid}")


def get_service(uuid: int) -> Optional[Service]:
    return _all_services.get(uuid, None)


def get_service_by_name(name: str):
    return [service for service in _all_services.values() if service.name == name]


def list_services() -> Dict[int, Service]:
    return _all_services


async def send(des: Union[Service, int, str], *args, **kwargs) -> None:
    """知识往queue赛一个消息，不要求返回值"""
    if isinstance(des, str):
        service = get_service_by_name(des)
    elif isinstance(des, int):
        service = get_service(des)
    elif isinstance(des, Service):
        service = des
    else:
        raise ValueError("des must be instance of str or Service")
    if not service:
        raise ValueError(f"no service called {des}")
    if isinstance(service, list):
        for i in service:
            i.put_msg((args, kwargs))
    else:
        service.put_msg((args, kwargs))
    # todo cluster


async def call(name: Union[Service, int, str], *args, **kwargs):
    """必须有返回值，调用reply 使用对应的uuid"""
    if "uuid" in kwargs:
        raise ValueError("uuid is a restore field")  # todo document about this
    uuid: int = uuid4().int
    kwargs["uuid"] = uuid
    await send(name, *args, **kwargs)
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
