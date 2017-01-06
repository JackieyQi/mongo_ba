# coding:utf8

from mongoba.handler import MongoHandler
from itertools import islice
import os
import json

print "*************start***************"
database = MongoHandler(host="127.0.0.1", port="27017", username="tester", password="111111", db="test_mongodb")
clc_name = "test"

filter = {"sign": True}
cursor = database.find_doc(clc_name, filter, limit_size=999)
for doc in cursor:
    value = doc.get("key_name")

file = os.path.realpath("") + "/test_file"
with open(file, "r") as f:
    # f.seek(offset, 0)

    while True:
        # 按行读取
        lines_gen = islice(f, 999)
        bulk = database.init_unordered_bulk(clc_name)

        try:
            i = next(lines_gen)
        except StopIteration:
            break
        bulk.insert(json.loads(i))

        for i in lines_gen:
            bulk.insert(json.loads(i))
        bulk.execute()

database.close_conn()
print "************over**************"
