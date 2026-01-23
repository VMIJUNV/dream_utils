import sqlite3
from pathlib import Path
import threading
import json
lock = threading.Lock()



class BaseDB:
    def __init__(self,db_path: str,db_name: str,create_table_sql: str):

        self.db_path = Path(db_path)
        self.table = db_name
        self.create_table_sql = create_table_sql
        self.load()

        table_columns = self.db.execute(f"PRAGMA table_info({self.table})")
        self.table_columns = [col['name'] for col in table_columns]

    def load(self):
        self.db= SQLiteDB(self.db_path,self.table,self.create_table_sql)

    def save(self):
        self.db.save()

    def clear(self):
        self.db.clear()

    def buffer_clear(self):
        self.buffer=[]

    def add(self,data_list: list):
        if data_list == []:
            return

        keys = list(data_list[0].keys())
        keys = [k for k in keys if k in self.table_columns]
        columns = ','.join(keys)
        placeholders = ','.join('?' * len(keys))
        sql = f'INSERT OR IGNORE INTO {self.table} ({columns}) VALUES ({placeholders})'
        values_list = [tuple(data[k] for k in keys) for data in data_list]
        self.db.execute_batch(sql, values_list)

    def update(self,data_list: list):
        if data_list == []:
            return
        keys = list(data_list[0].keys())
        keys = [k for k in keys if k in self.table_columns]
        set_clause = ','.join([f"{k} = ?" for k in keys])
        sql = f"UPDATE {self.table} SET {set_clause} WHERE id = ?"
        values_list = [tuple(data[k] for k in keys) + (data['id'],) for data in data_list]
        self.db.execute_batch(sql, values_list)
    
    def update_key(self,key: str,value):
        sql = f"UPDATE {self.table} SET {key} = ?"
        self.db.execute(sql, (value,))

    def search(self,key: str,value):
        sql = f"SELECT * FROM {self.table} WHERE {key} = ?"
        return self.db.execute(sql, (value,))

    def search_all(self):
        sql = f"SELECT * FROM {self.table}"
        return self.db.execute(sql)
    
class SQLiteDB:
    def __init__(self,db_path,table,create_table_sql):
        self.db_path = Path(db_path)
        self.table = table
        self.create_table_sql = create_table_sql
        self.load()
    
    def load(self):
        if not self.db_path.parent.exists():
            self.db_path.parent.mkdir(parents=True)
        if not self.db_path.exists():
            self.db_path.touch()

        with lock:
            self.conn = sqlite3.connect(self.db_path,check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            self.cursor.execute(self.create_table_sql)
            self.conn.commit()

    def save(self):
        ...
    
    def clear(self):
        with lock:
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table}")
            self.conn.commit()
        self.load()

    def execute(self,SQL,values=()):
        with lock:
            self.cursor.execute(SQL,values)
            self.conn.commit()
            res=self.cursor.fetchall()
        res=[dict(row) for row in res]
        return res

    def execute_batch(self,SQL,values_list):
        with lock:
            self.cursor.executemany(SQL,values_list)
            self.conn.commit()