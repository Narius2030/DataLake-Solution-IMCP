import pymongo
from tqdm import tqdm
from datetime import datetime



class MongoDBOperator():
    def __init__(self, dbname:str, connection_string:str) -> None:
        self.__conn = pymongo.MongoClient(connection_string)
        self.__connstr = connection_string
        self.dbname = dbname
    
    def is_has_data(self, collection) -> bool:
        check = False
        with pymongo.MongoClient(self.__connstr) as client:
            dbconn = client[self.dbname]
            docs = dbconn[collection].find()
            if any(docs):
                check = True
        return check
    
    def build_query(self, params:dict):
        query = {
            "url": params['url'],
            "$and": [
                {"caption": params['caption']},
                {"short_caption": params['short_caption']}
            ]
        }
        return query
    
    def data_generator(self, db, datasets, batchsize):
        accepted_datas = []
        count = 0
        for data in tqdm(datasets):
            params = {
                'url': data['url'],
                'caption': data['caption'],
                'short_caption': data['short_caption']
            }
            query = self.build_query(params)
            docs = db.find(query, {"url":1,"caption":1,"short_caption":1})
            if any(docs) == False:
                accepted_datas.append(data)
            count += 1
            if count == batchsize:
                count = 0
                yield accepted_datas
        
    def write_log(self, collection, status, start_time=datetime.now(), end_time=datetime.now(), error_message="", affected_rows=0, action=""):
        with self.__conn as client:
            dbconn = client[self.dbname]
            log = {
                "layer": "bronze",
                "table_name": "raw",
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "error_message": error_message,
                "affected_rows": affected_rows,
                "action": action
            }
            dbconn[collection].insert_one(log)
            print("Writed log!")
    
    def insert(self, collection, datasets, batch_size=10000) -> int:
        affected_rows = 0
        try:
            with pymongo.MongoClient(self.__connstr) as client:
                dbconn = client[self.dbname]
                for i in range(0, len(datasets), batch_size):
                    batch = datasets[i:i + batch_size]
                    resp = dbconn[collection].insert_many(batch)
                    affected_rows += len(resp.inserted_ids)
        except Exception as ex:
            raise Exception(str(ex))
        return affected_rows
    
    def insert_many_not_duplication(self, collection, datasets, batchsize=10000):
        with self.__client as client:
            dbconn = client[self.dbname]
            for batch in self.data_generator(dbconn[collection], datasets, batchsize):
                if batch != []:
                    print("Loading...", len(batch))
                    dbconn[collection].insert_many(batch)