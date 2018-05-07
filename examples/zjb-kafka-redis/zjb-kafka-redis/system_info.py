# coding:utf-8
from pyleus.storm import SimpleBolt
import os, re, time
import logging
from collections import namedtuple, defaultdict
from hdfs.client import Client
import redis
import datetime
from utils import IPS as ips
from utils import SYS_IPS as sys_ips

r = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)
log = logging.getLogger('log_results')
import MySQLdb
db = MySQLdb.connect("localhost", "root", "123456.", "zjb_event")
cursor = db.cursor()


class LogResultsBolt(SimpleBolt):

    OUTPUT_FIELDS = ["src_ip", "dst_ip", "system_info"]

    def initialize(self):
        # self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        # self.yesterday = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        cursor.execute("SELECT ips.ip, system_name.`name` FROM ips INNER JOIN ips_system_name ON "
                       "ips_system_name.ips_id = ips.id INNER JOIN system_name ON "
                       "ips_system_name.systemname_id = system_name.id;")
        results = cursor.fetchall()

    def process_tuple(self, tup):

        log.info("v:{0}".format(tup.values))
        if tup.values:
            s = "{0}".format(tup.values[0])
            ips[s] += 1
            log.info("ip,count:{0},{1}".format(s, ips[s]))
            # 写到hdfs中
            # if not client.status("/hadoop/ttt.log",strict=False):
            #     with client.write("/hadoop/ttt.log") as f:
            #         f.write("ip,count:{0},{1}".format(s,self.ips[s]))
            # else:
            #     with client.write("/hadoop/ttt.log",append=True) as f:
            #         f.write("ip,count:{0},{1}".format(s,self.ips[s]))
            # 直接存储到redis中
            # 判断是否存在当天的数值

            self.emit(word, self.words[word])




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/file_results.log',
        format="%(message)s",
        filemode='a', )
    # client = Client("http://40.125.161.143:50070")
    Counter = namedtuple("Counter", "src_ip count")
    # hdfs_dir_list = client.list("/hadoop")
    LogResultsBolt().run()
