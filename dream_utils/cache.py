from functools import wraps
from .database import BaseDB
from pathlib import Path
import hashlib
import json
import inspect

class CacheDB(BaseDB):
    def __init__(self,cache_path: str,cache_name: str):
        cache_path=Path(cache_path)
        db_path= cache_path / f'{cache_name}.db'
        table='cache'
        create_table_sql = \
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                input TEXT,
                output TEXT
                
            );
            """
        super().__init__(db_path,table,create_table_sql)

    def search_cache(self,input_hash):
        res_json = self.db.execute(f'SELECT output FROM {self.table} WHERE hash = ?',(input_hash,))
        if len(res_json) == 0:
            return None
        return res_json[0]['output']
    
    def update_cache(self,input_json,output_json,input_hash):
        self.db.execute(f'INSERT OR IGNORE INTO {self.table} (hash,input,output) VALUES (?,?,?)',(input_hash,input_json,output_json))


class Cache:
    def __init__(self,cache_dir='cache',cache_name='cache'):
        self.cache_dir = cache_dir
        self.cache_name = cache_name
        self.cache_db = CacheDB(self.cache_dir,self.cache_name)
    
    def __call__(self,func):
        @wraps(func)
        def wrapper(*args,**kwargs):
            sig = inspect.signature(func)
            bound_args = sig.bind(*args,**kwargs)
            bound_args.apply_defaults()

            inp=bound_args.arguments.copy()
            inp.pop('self',None)
            inp.pop('cls',None)
            inp_kwargs=inp.pop('kwargs',{})

            use_cache=inp_kwargs.get('use_cache',True)
            if not use_cache:
                return func(*args,**kwargs)
            
            inp_json = json.dumps(inp,ensure_ascii=False,indent=2)
            inp_hash = hashlib.md5(inp_json.encode('utf-8')).hexdigest()

            out_json = self.cache_db.search_cache(inp_hash)

            if out_json is not None:
                out = json.loads(out_json)['output']
            else:
                out = {'output': func(*args,**kwargs)}
                out_json = json.dumps(out,ensure_ascii=False,indent=2)
                self.cache_db.update_cache(inp_json,out_json,inp_hash)
                out = out['output']

            return out
    
        return wrapper