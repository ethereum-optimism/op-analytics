import requests as r
import pandas as pd
import time
from functools import wraps

def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except r.RequestException as e:
                    retries += 1
                    if retries == max_retries:
                        raise e
                    print(f"Request failed. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor
        return wrapper
    return decorator