import hashlib
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def hash_str(s: str):
    return hashlib.md5(s.encode()).hexdigest()

def executor(task_func,task_list,num_threads=1):
    if num_threads<=1:
        for task in tqdm(task_list):
            task_func(task)
    else:
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(task_func,task) for task in task_list]
            pbar = tqdm(as_completed(futures), total=len(futures))
            for future in pbar:
                future.result()