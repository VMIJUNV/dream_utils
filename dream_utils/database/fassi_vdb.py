import faiss
import sqlite3
from pathlib import Path
import threading
import numpy as np
import json
lock = threading.Lock()

class BaseVDB:
    def __init__(self,vdb_path: str,vdb_name: str,vdb_dim: int):
        self.vdb_path = Path(vdb_path)
        self.vdb_name = vdb_name
        self.vdb_dim = vdb_dim
        self.load()
        
        self.buffer=[]

    def load(self):
        self.vdb_file_path = self.vdb_path / f'{self.vdb_name}.vdb'
        self.vdb = FassiVDB(self.vdb_file_path,self.vdb_dim)

    def save(self):
        self.vdb.save()

    def clear(self):
        self.vdb.clear()
        self.load()

    def buffer_clear(self):
        self.buffer=[]

    def vdb_add(self,data_list: list):
        if data_list == []:
            return
        ids = [data['id'] for data in data_list]
        vectors = np.array([json.loads(data['embedding']) for data in data_list])
        self.vdb.add(ids,vectors)
    
    def vdb_search(self,vector: list,topk: int=10):        
        vector = np.array(vector).reshape(1, -1)
        distances,ids = self.vdb.search(vector, topk)
        if ids is None:
            return []
        pairs = [(float(d), int(i)) for d, i in zip(distances[0], ids[0]) if i != -1]

        res=[{'distance':p[0],'id':p[1]} for p in pairs]
        return res

class FassiVDB:
    def __init__(self,vdb_path,vdb_dim):
        self.vdb_path = Path(vdb_path)
        self.vdb_dim = vdb_dim
        self.load()

    def load(self):
        if self.vdb_path.exists():
            self.vdb = faiss.read_index(str(self.vdb_path))
        elif self.vdb_dim is not None:
            self.vdb = faiss.IndexIDMap(faiss.IndexFlatL2(self.vdb_dim))
            self.save()
        else:
            self.vdb = None
        
    def save(self):
        if self.vdb is None:
            return
        with lock:
            if not self.vdb_path.parent.exists():
                self.vdb_path.parent.mkdir(parents=True)
            faiss.write_index(self.vdb, str(self.vdb_path))

    def clear(self):
        if self.vdb is None:
            return
        file_path = Path(self.vdb_path)
        if file_path.exists():
            file_path.unlink()
        self.load()

    def add(self,ids,items):
        if self.vdb is None:
            return
        with lock:
            self.vdb.add_with_ids(items,np.array(ids,dtype=np.int64))
    
    def search(self,item,topk):
        if self.vdb is None:
            return None,None
        with lock:
            distances, ids = self.vdb.search(item, topk)

        return distances,ids
