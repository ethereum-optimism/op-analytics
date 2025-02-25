import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, TypeVar


T = TypeVar("T")


def run_in_new_loop(coroutine: Coroutine[Any, Any, T]):
    """Run a coroutine in a new event loop."""
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        return new_loop.run_until_complete(coroutine)
    finally:
        new_loop.close()


def run_coroutine(coroutine: Coroutine[Any, Any, T], timeout: float = 300) -> T:
    """Run a coroutine as a sync function.

    Helps integrate async libraries with our sync codebase.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    if threading.current_thread() is threading.main_thread():
        if not loop.is_running():
            return loop.run_until_complete(coroutine)
        else:
            with ThreadPoolExecutor() as pool:
                future = pool.submit(run_in_new_loop, coroutine)
                return future.result(timeout=timeout)
    else:
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result()
