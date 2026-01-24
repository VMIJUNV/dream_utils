from functools import wraps
from ..database.sqlite_db import BaseDB
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
    
    def update_cache(self,input_hash,input_json,output_json):
        self.db.execute(f'INSERT OR IGNORE INTO {self.table} (hash,input,output) VALUES (?,?,?)',(input_hash,input_json,output_json))


class Cache:
    def __init__(self,cache_dir='cache',cache_name='cache'):
        self.cache_dir = cache_dir
        self.cache_name = cache_name
        self.cache_db = CacheDB(self.cache_dir,self.cache_name)
    
    def input_encode(self,inp):
        inp_json = json.dumps(inp,ensure_ascii=False)
        inp_hash = hashlib.md5(inp_json.encode('utf-8')).hexdigest()
        inp_encoded = {
            'hash': inp_hash,
            'json': inp_json,
        }
        return inp_encoded

    def output_encode(self,out):
        out_encoded = json.dumps(out,ensure_ascii=False)
        return out_encoded
    
    def output_decode(self,out_encoded):
        out = json.loads(out_encoded)
        return out

    def find_cache(self,inp_encoded):
        inp_hash = inp_encoded['hash']
        out_encoded = self.cache_db.search_cache(inp_hash)

        if out_encoded is None:
            out = None
        else:
            out = self.output_decode(out_encoded)
        return out
    
    def save_cache(self,inp_encoded,out):
        inp_hash = inp_encoded['hash']
        inp_json = inp_encoded['json']
        out_encoded = self.output_encode(out)
        self.cache_db.update_cache(inp_hash,inp_json,out_encoded)   

    def __call__(self,func):
        @wraps(func)
        def wrapper(*args,**kwargs):
            sig = inspect.signature(func)
            bound_args = sig.bind(*args,**kwargs)
            bound_args.apply_defaults()

            inp=bound_args.arguments.copy()
            inp.pop('self',None)
            inp.pop('cls',None)
            
            inp_encoded = self.input_encode(inp)
            out = self.find_cache(inp_encoded)

            if out is None:
                out = func(*args,**kwargs)
                self.save_cache(inp_encoded,out)
            return out
    
        return wrapper