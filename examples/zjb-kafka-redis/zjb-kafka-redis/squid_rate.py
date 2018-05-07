# coding:utf-8
from pyleus.storm import SimpleBolt
import os,re,time
import json
import logging
# from hdfs.client import Client
log = logging.getLogger('squid_rate')
# client = Client("http://localhost:50070")
# client = Client("http://40.125.161.143:50070")
import datetime


class LogSquidBolt(SimpleBolt):
    OUTPUT_FIELDS = ["src_ip", "dst_ip", "time", "provider_id"]

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
                post_time = line['time']
                provider_id = line['provider_id']

                log.info("src_ip: {0},dst_ip: {1}".format(src_ip,dst_ip))
           	self.emit((src_ip, dst_ip, post_time, provider_id))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/squid_results.log',
        format="%(message)s",
        filemode='a',)
    LogSquidBolt().run()
