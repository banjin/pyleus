# coding:utf-8
from pyleus.storm import SimpleBolt
import os,re,time
import json
import logging
# from hdfs.client import Client
log = logging.getLogger('squid_rate')
# client = Client("http://localhost:50070")
# client = Client("http://40.125.161.143:50070")


class LogSquidBolt(SimpleBolt):
    OUTPUT_FIELDS = ["src_ip", "dst_ip"]

    def process_tuple(self, tup):
        if tup.values:
            log.info("{0}".format(len(tup.values)))
            s = "{0}".format(tup.values[0])
            data_list = s.split("}")
            data_list.pop()
            for i in data_list:
                i = i+"}"
                log.info(i)
                line = json.loads(i)
                log.info("type:{0}".format(type(i)))
                src_ip = line['src_ip']
                dst_ip = line['dst_ip']
                log.info(src_ip) 
           	self.emit((src_ip, dst_ip))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/squid_results.log',
        format="%(message)s",
        filemode='a',)
    LogSquidBolt().run()
