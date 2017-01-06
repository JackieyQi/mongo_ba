# coding:utf8

from pymongo import MongoClient

class MongoHandler(object):
    def __init__(self, db="db_name", host="localhost", port=None, username=None, password=None):
        url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)

        self.__client = MongoClient(url)
        self.db = self.__client.get_database(db)

    def close_conn(self):
        self.__client.close()

    def get_db(self):
        return self.db

    def init_unordered_bulk(self, collection_name):
        """
        usage:
            bulk = ....
            for data in iterator:
                bulk.insert(data)
                # bulk operations
            bulk.execute()
        data format: {key1: value1, key2: value2,}
        """
        bulk = self.db.get_collection(collection_name).initialize_unordered_bulk_op()
        return bulk

    def insert_doc(self, collection_name, data):
        re = self.db.get_collection(collection_name).insert_many(data)
        return re.inserted_ids

    def update_doc(self, collection_name, filter, data):
        re = self.db.get_collection(collection_name).update_many(filter, data)
        return re.matched_count

    def find_doc(self, collection_name, filter, limit_size=0):
        re = self.db.get_collection(collection_name).find(filter).limit(limit_size)
        return re
