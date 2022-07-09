import asyncio
import signal

loop = asyncio.get_event_loop()

from threading import Thread

from landnet import Service, call, get_service, init, reply, run, send

init(loop)

s = Service("ping")


@s.handle()
async def _(ping: int, uuid: int = None):
    await asyncio.sleep(1)
    if uuid is None:
        print(f"I am ping and sending {ping + 1}")
        send("pong", ping + 1)
        return
    else:
        print(f"I am ping and sending reply {ping + 1}")
        reply(uuid, ping + 1)
        send("pong", ping + 1)


s2 = Service("pong")


@s2.handle()
async def _(ping: int):
    print(f"I am pong and sending {ping + 1}")
    print(await call("ping", ping + 1))


async def main():
    startup_done = asyncio.get_running_loop().create_future()
    shutdown_waiter = asyncio.get_running_loop().create_future()
    signal.signal(signal.SIGINT, lambda x, y: shutdown_waiter.set_result(None))
    signal.signal(signal.SIGTERM, lambda x, y: shutdown_waiter.set_result(None))

    ts = loop.create_task(run(shutdown_waiter, startup_done))  # keep a strong ref
    await startup_done
    thread = Thread(target=lambda: loop.create_task(call("ping", 1)))
    thread.start()
    # send("pong", 1)
    await ts


if __name__ == "__main__":
    loop.run_until_complete(main())
