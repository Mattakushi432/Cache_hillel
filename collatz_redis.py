import multiprocessing
import time
import sys
import os
from typing import Set, Tuple

import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
UPPER_LIMIT = 1_000_000
CHUNK_SIZE = 10_000


def collatz_cached(start_number: int, redis_client: redis.Redis) -> bool:
    number = start_number
    path: Set[int] = set()

    while True:
        if redis_client.exists(f"collatz_ok:{number}"):
            pipe = redis_client.pipeline()
            for n in path:
                pipe.set(f"collatz_ok:{n}", 1)
            pipe.execute()
            return True

        path.add(number)

        if number == 1:
            pipe = redis_client.pipeline()
            for n in path:
                pipe.set(f"collatz_ok:{n}", 1)
            pipe.execute()
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
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port)
        redis_client.ping()
    except redis.exceptions.ConnectionError:
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
    main()
