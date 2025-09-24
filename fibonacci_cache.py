import redis
import time
from functools import wraps
from typing import Callable

try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()
    print("Successful connection to Redis.")
except redis.exceptions.ConnectionError as e:
    print(f"Connection to Redis failed: {e}")
    exit(1)


def cache_to_redis(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(n: int, *args, **kwargs) -> int:
        cache_key = f"{func.__qualname__}:{n}"
        cache_result = redis_client.get(cache_key)
        if cache_result is not None:
            print(f"Cache HIT for {cache_key}. Result from Redis.")
            return int(cache_result)

        print(f"Cache MISS for {cache_key}. Calculating...")
        result = func(n, *args, **kwargs)
        redis_client.set(cache_key, result)
        return result

    return wrapper

@cache_to_redis
def fibonacci(n: int) -> int:
    if not isinstance(n, int) or n < 0:
        raise ValueError("The number 'n' must be a positive integer.")
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)

if __name__ == "__main__":
    number_to_calculate = 35

    print("\n--- First launch (cache filling) ---")
    statrt_time = time.time()
    result1 = fibonacci(number_to_calculate)
    end_time = time.time()
    print(f"Result of fibonacci({number_to_calculate}) = {result1}")
    print(f"Time taken = {end_time - statrt_time: 4f} seconds")

    print("\n" + "=" * 40 + "\n")

    print("--- Second launch (using cache) ---")
    statrt_time = time.time()
    result2 = fibonacci(number_to_calculate)
    end_time = time.time()
    print(f"Result of fibonacci({number_to_calculate}) = {result2}")
    print(f"Time taken = {end_time - statrt_time: 4f} seconds")
