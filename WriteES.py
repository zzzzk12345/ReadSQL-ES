# -*- encoding: utf-8 -*-
import sys
import json
from elasticsearch import Elasticsearch
from datetime import datetime 
  
class ES(object):
    
    def __init__(self,index_name,index_type,ip):
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch(ip)
    

    def create_index(self):
        _index_mappings = {
            "settings":{
                        "number_of_shards":2,
                        "number_of_replicas":1
                    },
            "mappings":{
                self.index_type:{
                    # "_all":{
                    #     "enabled":true
                    # },
                    "properties":{
                        "id":{
                            "type":"text"
                        },
                        "dianxiao_price":{
                            "type":"text"
                        },
                        "dianxiao_block":{
                            "type":"text"
                        },
                        "dianxiao_latest_time":{
                            "type":"text"
                        },
                        "call_times":{
                            "type":"integer"
                        },
                        "tag_max":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "tags":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "dianxiao_room_code":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "daikan_times":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "daikan_latest_time":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "daikan_dealerid":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "daikan_dealername":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "daikan_roominfo":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_block":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_xiaoqu":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_price":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_area":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_face":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "contract_code":{
                            "type":"text",
                            # "null_value" : "null"
                        },
                        "created_at":{
                            "type":"date",
                            "format":"yyyy-MM-dd HH:mm:ss || yyyy-MM-dd"
                        },
                    }
                }
            }
        }
        if self.es.indices.exists(index=self.index_name) is not True:
            res = self.es.indices.create(index=self.index_name,body=_index_mappings)
            print(res)
            print("索引添加成功")


    def input_data(self):
        testlist = [self.jsontool("1123","1","1","1",1,"null","1","1","1","1","1","1","1","1","1","1","1","1","1","2019-05-24")]
        for item in testlist:
            res = self.es.index(index = self.index_name,doc_type = self.index_type,body = item,id = item['id'])
        print(res)
        print("数据插入成功")

    def delete_data(self,id):
        res = self.es.delete(index = self.index_name,doc_type = self.index_type,id = id)
        print(res)
        print("数据删除成功")

    def get_data_by_body(self):
        doc = {

        }
        _search = self.es.search(index = self.index_name,doc_type = self.index_type,body = doc)
        print(_search)

        for hit in _search['hits']['hits']:
            print(hit['_source']['date'],hit['_source']['_source'],hit['_source']['link'],hit['_source']['keyword'],\
                hit['_source']['title'])

    def jsontool(self,id,dianxiao_price,dianxiao_block,dianxiao_latest_time,call_times,tag_max,tags,dianxiao_room_code,daikan_times,\
        daikan_latest_time,daikan_dealerid,daikan_dealername,daikan_roominfo,contract_block,contract_xiaoqu,contract_price,\
            contract_area,contract_face,contract_code,created_at):

            return {
                "id":id,
                "dianxiao_price":dianxiao_price,
                "dianxiao_block":dianxiao_block,
                "dianxiao_latest_time":dianxiao_latest_time,
                "call_times":call_times,
                "tag_max":tag_max,
                "tags":tags,
                "dianxiao_room_code":dianxiao_room_code,
                "daikan_times":daikan_times,
                "daikan_latest_time":daikan_latest_time,
                "daikan_dealerid":daikan_dealerid,
                "daikan_dealername":daikan_dealername,
                "daikan_roominfo":daikan_roominfo,
                "contract_block":contract_block,
                "contract_xiaoqu":contract_xiaoqu,
                "contract_price":contract_price,
                "contract_area":contract_area,
                "contract_face":contract_face,
                "contract_code":contract_code,
                "created_at":created_at
            }

if __name__=='__main__':
    target = ES('zhangkai','zhangkai_type','localhost:9200')
    target.create_index()
    target.input_data()
    # target.delete_data('1123')