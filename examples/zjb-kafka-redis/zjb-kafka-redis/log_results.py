# coding:utf-8
from pyleus.storm import SimpleBolt
import os,re,time
import logging
from collections import namedtuple,defaultdict
from hdfs.client import Client
import redis
r = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)
log = logging.getLogger('log_results')


class LogResultsBolt(SimpleBolt):
   
    def initialize(self):
        self.ips = defaultdict(int)

    def process_tuple(self, tup):
        log.info("v:{0}".format(tup.values)) 
        if tup.values:
            s = "{0}".format(tup.values[0])
            self.ips[s] += 1
            log.info("ip,count:{0},{1}".format(s,self.ips[s]))
            # 写到hdfs中
            # if not client.status("/hadoop/ttt.log",strict=False):
            #     with client.write("/hadoop/ttt.log") as f:
            #         f.write("ip,count:{0},{1}".format(s,self.ips[s]))
            # else:
            #     with client.write("/hadoop/ttt.log",append=True) as f:
            #         f.write("ip,count:{0},{1}".format(s,self.ips[s]))

            # 直接存储到redis中
            r.set("{0}".format(s), "{0}".format(self.ips[s]))


            #self.emit((s,), anchors=[tup])
            #with open('/home/qsadmin/stom-kafka.txt','a') as f:
            #    for i in data_list:
            #        i = i+"}\n"    
            #        f.write(i)
            
if __name__=="__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/file_results.log',
        format="%(message)s",
        filemode='a',)
    # client = Client("http://40.125.161.143:50070")
    Counter = namedtuple("Counter", "src_ip count")
    #hdfs_dir_list = client.list("/hadoop")
    LogResultsBolt().run()
