# coding:utf-8
from pyleus.storm import SimpleBolt
import os,re,time
import logging
from collections import namedtuple,defaultdict
from hdfs.client import Client
log = logging.getLogger('log_results')
client = Client("http://localhost:50070")
#Counter = namedtuple("Counter", "src_ip count")
class LogResultsBolt(SimpleBolt):
   
    def initialize(self):
        self.ips = defaultdict(int)

    def process_tuple(self, tup):
        log.info("v:{0}".format(tup.values)) 
        if tup.values:
            s = "{0}".format(tup.values[0])
            self.ips[s] += 1
            log.info("ip,count:{0},{1}".format(s,self.ips[s]))
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
    LogResultsBolt().run()
