# coding:utf-8
from pyleus.storm import SimpleBolt
import os,re,time
import logging
from collections import namedtuple,defaultdict
from hdfs.client import Client
import redis
import datetime

r = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)
log = logging.getLogger('log_results')

system_ips = ["2.2.2.2", "2.2.2.3"]
ips = defaultdict(int)
log.info("ips", ips)
sys_ips = defaultdict(int)
log.info("sys_ips", sys_ips)

class LogResultsBolt(SimpleBolt):

    def initialize(self):

        self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        self.yesterday = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

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
            if not r.exists(self.today) and r.exists(self.yesterday):
                r.set(self.today, {"attack_ip_num": len(ips), "attack_count_num": sum(ips.values())})
                r.delete(self.yesterday)

            if tup.values[1] in system_ips:
                sys_ips[str(tup.values[1])] += 1
                r.set("realTimeMonitoring", {"attack_count_num": len(sys_ips), "attack_ip_num": sum(sys_ips.values())})

            r.set("{0}".format(s), "{0}".format(ips[s]))

            
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/file_results.log',
        format="%(message)s",
        filemode='a',)
    # client = Client("http://40.125.161.143:50070")
    Counter = namedtuple("Counter", "src_ip count")
    #hdfs_dir_list = client.list("/hadoop")
    LogResultsBolt().run()
