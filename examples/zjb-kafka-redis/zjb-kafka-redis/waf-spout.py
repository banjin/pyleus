# coding:utf-8

from pyleus.storm import SimpleBolt
import os,re,time
import json
import logging
log = logging.getLogger('squid_rate')
from utils import RDS
from utils import WAF_IPS, WAF_SYS_IPS
"""
安全防护数据
"""
from utils import get_system_ips
system_ip_list = get_system_ips()

waf_system_data = {}


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

                WAF_IPS[src_ip] += 1
                log.info("src_ip: {0},dst_ip: {1}".format(src_ip, dst_ip))
                # waf 攻击次数和攻击IP个数
                RDS.set("waf_attack_data", {"attack_ip_num": len(WAF_IPS), "attack_count_num": sum(WAF_IPS.values())})

                if dst_ip in system_ip_list:

                    tt = WAF_SYS_IPS[dst_ip].setdefault("count_num", 0)
                    tt += 1
                    WAF_SYS_IPS[dst_ip].setdefault("ip_list", []).append(src_ip)
                    waf_system_data.update({str(dst_ip): {
                        "attack_ip_num": len(list(set(WAF_SYS_IPS[dst_ip]['ip_list']))),
                        "attack_count_num": tt}})

                    RDS.set("waf_systam_attack", waf_system_data)


            # RDS.set("waf_data", {"total_attack_num":"", "total_ip_num":""})
           	self.emit((src_ip, dst_ip, post_time, provider_id))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/squid_waf.log',
        format="%(message)s",
        filemode='a',)
    LogSquidBolt().run()
