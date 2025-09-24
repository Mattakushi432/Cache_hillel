import multiprocessing
import time
import sys
import os
from typing import Set, Tuple, Optional

import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
UPPER_LIMIT = 1_000_000
CHUNK_SIZE = 10_000

REDIS_RETRIES = int(os.environ.get("REDIS_RETRIES", "5"))
REDIS_RETRY_DELAY = float(os.environ.get("REDIS_RETRY_DELAY", "0.5"))


def make_redis_client(host: str, port: int, retries: int = REDIS_RETRIES, delay: float = REDIS_RETRY_DELAY) -> Optional[
    redis.Redis]:
    for attempt in range(1, retries + 1):
        try:
            client = redis.Redis(
                host=host,
                port=port,
                socket_timeout=5,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                retry_on_timeout=True,
                decode_responses=False,
            )
            client.ping()
            return client
        except redis.exceptions.RedisError:
            if attempt == retries:
                break
            time.sleep(delay)
    return None


def collatz_cached(start_number: int, redis_client: Optional[redis.Redis]) -> bool:
    number = start_number
    path: Set[int] = set()

    def safe_exists(key: str) -> Optional[bool]:
        if redis_client is None:
            return None
        for attempt in range(REDIS_RETRIES):
            try:
                return bool(redis_client.exists(key))
            except redis.exceptions.RedisError:
                if attempt == REDIS_RETRIES - 1:
                    return None
                time.sleep(REDIS_RETRY_DELAY)
        return None

    def safe_set_many(keys: Set[int]) -> None:
        if redis_client is None or not keys:
            return
        for attempt in range(REDIS_RETRIES):
            try:
                pipe = redis_client.pipeline()
                for n in keys:
                    pipe.set(f"collatz_ok:{n}", 1)
                pipe.execute()
                return
            except redis.exceptions.RedisError:
                if attempt == REDIS_RETRIES - 1:
                    return
                time.sleep(REDIS_RETRY_DELAY)

    while True:
        exists = safe_exists(f"collatz_ok:{number}")
        if exists:
            safe_set_many(path)
            return True

        path.add(number)

        if number == 1:
            safe_set_many(path)
            return True

        if number % 2 == 0:
            number //= 2
        else:
            number = (3 * number + 1) // 2

        if number in path:
            return False


def worker(
        tasks_queue: multiprocessing.Queue,
        results_queue: multiprocessing.Queue,
        stop_event: multiprocessing.Event,
        redis_host: str,
        redis_port: int,
) -> None:
    redis_client = make_redis_client(redis_host, redis_port)
    if redis_client is None:
        print(f"The process {multiprocessing.current_process().name} was unable to connect to Redis.")
        return

    print(f"The process {multiprocessing.current_process().name} has started.")
    try:
        while not stop_event.is_set():
            task: Tuple[int, int] = tasks_queue.get()
            if task is None:
                break
            start, end = task
            for n in range(start, end + 1):
                if stop_event.is_set():
                    break
                if not collatz_cached(n, redis_client):
                    results_queue.put(n)
                    stop_event.set()
                    break
    except KeyboardInterrupt:
        pass
    finally:
        print(f"Process {multiprocessing.current_process().name} is done")


def main() -> None:
    number_of_processes = multiprocessing.cpu_count()
    print(f"We test Collatz's hypothesis up to {UPPER_LIMIT} on {number_of_processes} processes.")

    tasks_queue: multiprocessing.Queue = multiprocessing.Queue()
    results_queue: multiprocessing.Queue = multiprocessing.Queue()
    stop_event: multiprocessing.Event = multiprocessing.Event()

    processes = [
        multiprocessing.Process(
            target=worker,
            args=(tasks_queue, results_queue, stop_event, REDIS_HOST, REDIS_PORT),
            daemon=True,
        )
        for _ in range(number_of_processes)
    ]
    for p in processes:
        p.start()

    start_time = time.time()

    try:
        for i in range(1, UPPER_LIMIT + 1, CHUNK_SIZE):
            start_range = i
            end_range = min(i + CHUNK_SIZE - 1, UPPER_LIMIT)
            tasks_queue.put((start_range, end_range))

        for _ in range(number_of_processes):
            tasks_queue.put(None)

        for p in processes:
            p.join()

    except KeyboardInterrupt:
        print("\nReceived KeyboardInterrupt. Attempting graceful shutdown...", file=sys.stderr)
        stop_event.set()
    finally:
        while not tasks_queue.empty():
            try:
                tasks_queue.get_nowait()
            except Exception:
                break
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        end_time = time.time()
        print_results(results_queue, end_time - start_time)


def print_results(results_q: multiprocessing.Queue, elapsed_time: float) -> None:
    if results_q.empty():
        print("\n" + "=" * 50)
        print(f"The Collatz hypothesis is true for all numbers from 1 to {UPPER_LIMIT}")
        print("=" * 50)
    else:
        print("\n" + "!" * 50)
        print("The Collatz hypothesis is false for the following numbers:")
        while not results_q.empty():
            try:
                print(results_q.get_nowait())
            except Exception:
                break
        print("!" * 50)
    print(f"The test took {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
