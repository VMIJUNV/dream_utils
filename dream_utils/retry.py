from functools import wraps
import concurrent.futures
import time

class Retry:
    def __init__(self,max_attempt: int=5,wait: float=0.1,timeout: float=10000):
        self.max_attempt = max_attempt
        self.wait = wait
        self.timeout = timeout

    def __call__(self,func: callable):
        @wraps(func)
        def wrapper(*args,**kwargs):
            
            for attempt in range(1,self.max_attempt+1):
                kwargs_with_attempt = {**kwargs, 'attempt': attempt}
                try:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(func, *args, **kwargs_with_attempt)
                        result = future.result(timeout=self.timeout)
                    return result
                except Exception as e:
                    time.sleep(self.wait)

            return None
        
        return wrapper