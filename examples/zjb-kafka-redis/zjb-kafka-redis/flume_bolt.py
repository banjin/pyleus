# coding:utf-8

from pyleus.storm import SimpleBolt
import logging
import socket

log = logging.getLogger('flume_bolt')

# flume监听地址和端口
host = ""
port = ""
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((host, port))

import pygeoip
gi = pygeoip.GeoIP("/data/GeoLiteCity.dat")


class LogResultsBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        log.info("v:{0}".format(tup.values))
        if tup.values:
            s = "{0}".format(tup.values[0])
            log.info("ip,count:{0}".format(s))
            rec = gi.record_by_addr(s)
            if not rec:
                region = 0
                country_name = "未知"
                city = "未知"
            else:
                country_name = rec['country_name']
                city = rec['city']
                if not city:
                    city = ''
                if country_name == "Macau":
                    region = 34
                elif country_name == "Taiwan":
                    region = 35
                elif country_name == "China" or country_name == "Hong Kong":
                    region = rec['region_code']
                else:
                    region = 37
                if not region:
                    region = 37
            sock.sendall('{"ip": {src_ip}, "region": {region}, "city":{city}, "country_name":{country_name}}\n'.format(
                src_ip=s, region=str(region), city=city, country_name=country_name
            ))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/flume_bolt.log',
        format="%(message)s",
        filemode='a', )
    # Counter = namedtuple("Counter", "src_ip count")
    # hdfs_dir_list = client.list("/hadoop")
    LogResultsBolt().run()
