# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import codecs
import json
import pymongo
from pymongo import MongoClient

from twisted.enterprise import adbapi
import MySQLdb
from MySQLdb import cursors
from scrapy import log



class PrincesmallPipeline(object):
    def __init__(self):
        self.file = codecs.open('prince.json', 'w', encoding='utf-8')
        # self.file = codecs.open('princesmall.json','wb',encoding='utf-8')
        # self.file = codecs.open('w3school_data_utf8.json', 'wb', encoding='utf-8')

    def process_item(self, item, spider):
        # line = json.dumps(dict(item), ensure_ascii=False) + '\n'
        # self.file.write(line)
        line = json.dumps(dict(item)) + '\n'
        self.file.write(line.decode("unicode_escape"))
        return item

class PrincesmallSQLPipeline(object):

    def __init__(self):
        self.dbpool = adbapi.ConnectionPool("MySQLdb",

                                            host = "localhost",
                                            db = "new_schema_prince",
                                            user = "root",
                                            passwd = "princesmall",
                                            cursorclass = MySQLdb.cursors.DictCursor,
                                            charset = "utf8",
                                            use_unicode =True
                                            )
    def process_item(self,item,spider):

        query = self.dbpool.runInteraction(self._conditional_insert,item)
        query.addErrback(self.handle_error)
        return item
    def _conditional_insert(self,tb,item):
        prince_title = item['title']
        prince_link = item['link']
        prince_time = item['time']
        # print prince_link, prince_title, prince_time, '--------'

        # 删除所有数据
        # tb.execute("DELETE FROM TABLE_NAME ")

        # 插入数据，prince_link[0]取出list列表中的数据
        tb.execute("INSERT INTO  TABLE_NAME (Prince_Title, Prince_Link, Prince_Time) VALUES ('%s', '%s', '%s')" % (prince_title[0], prince_link[0], prince_time[0]))

        log.msg("item data in :%s" % item, level=log.DEBUG)
    def handle_error(self,e):
        log.err(e)





    # db = MySQLdb.connect("localhost","user","tongle1234567","TESTDB")
    # cursor = db.cursor()
    # cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
    #
    #
    #
    # cursor.execute(sql)
    # db.close()




    # client = MongoClient('localhost',27017)
    # db = client['pymongo_test']
    # posts = db.posts
    # post_data = {
    #     'title':'Python',
    #     'content':'Pymongo is fun',
    #     'auther':'tongle',
    # }
    # result = posts.insert_one(post_data)
    # print 'one post:{0}'.format(result.inserted_id)


