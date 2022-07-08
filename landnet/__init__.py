"""
Copyright (c) 2008-2021 synodriver <synodriver@gmail.com>
"""
import asyncio

from landnet.service import Service, call, get_service, init, list_services, reply, send


async def run(shutdown_waiter=None, startup_complete: asyncio.Future = None):
    services = list_services()
    startup_tasks = [service.startup() for service in services.values()]
    await asyncio.gather(*startup_tasks)
    if startup_complete is not None:
        startup_complete.set_result(None)

    if shutdown_waiter is None:
        shutdown_waiter = asyncio.get_running_loop().create_future()
    try:
        await shutdown_waiter
    finally:
        shutdown_tasks = [service.shutdown() for service in services.values()]
        await asyncio.gather(*shutdown_tasks)
