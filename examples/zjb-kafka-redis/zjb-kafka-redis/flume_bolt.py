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

    address_map = {"00": "香港", "01": "安徽", "02": "浙江", "03": "江西", "04": "江苏",
                   "05": "吉林", "06": "青海", "07": "福建", "08": "黑龙江",
                   "09": "河南", "10": "河北", "11": "湖南", "12": "湖北",
                   "13": "新疆", "14": "西藏", "15": "甘肃", "16": "广西",
                   "18": "贵州", "19": "辽宁", "20": "内蒙古", "21": "宁夏",
                   "22": "北京", "23": "上海", "24": "山西", "25": "山东",
                   "26": "陕西", "28": "天津", "29": "云南", "30": "广东",
                   "31": "海南", "32": "四川", "33": "重庆", "34": "澳门", "35": "台湾"
                   }

    def process_tuple(self, tup):
        log.info("v:{0}".format(tup.values))
        if tup.values:
            s = "{0}".format(tup.values[0])
            log.info("ip,count:{0}".format(s))
            rec = gi.record_by_addr(s)
            if not rec:
                region = ''
                country_name = "未知"
                city = "未知"
            else:
                country_name = rec['country_name']
                city = rec['city']
                region = rec['region_code']
                if not region:
                    region = ''
                if country_name == "Macau":
                    region = 34
                    city = "澳门"
                elif country_name == "Taiwan":
                    region = 35
                    city = "台湾"
                elif country_name == "China":
                    region = rec['region_code']
                    city = self.address_map.get(str(region), "其他")
                elif country_name == "Hong Kong":
                    region = "00"
                    city = "香港"
                if not region:
                    region = ''

            sock.sendall('{"ip": {src_ip}, "region_code": {region}, "src_province":{city}, "country_name":{country_name}}\n'.format(
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
