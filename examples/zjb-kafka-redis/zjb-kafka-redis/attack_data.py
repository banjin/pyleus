# coding:utf-8
from pyleus.storm import SimpleBolt
import os, re, time
import logging
from collections import namedtuple, defaultdict
import redis
from utils import IPS as ips
from utils import SYS_IPS as sys_ips
log = logging.getLogger('log_results')

log.info("ips", ips)
log.info("sys_ips", sys_ips)


class LogResultsBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        # 五元组
        log.info("v:{0}".format(tup.values))
        if tup.values:
            s = "{0}".format(tup.values[0])
            ips[s] += 1
            log.info("ip,count:{0},{1}".format(s, ips[s]))

            # WAF_IPS[src_ip] += 1
            # log.info("src_ip: {0},dst_ip: {1}".format(src_ip, dst_ip))
            # # waf 攻击次数和攻击IP个数
            # RDS.set("waf_attack_data", {"attack_ip_num": len(WAF_IPS), "attack_count_num": sum(WAF_IPS.values())})
            #
            # if dst_ip in system_ip_list:
            #     tt = WAF_SYS_IPS[dst_ip].setdefault("count_num", 0)
            #     tt += 1
            #     WAF_SYS_IPS[dst_ip].setdefault("ip_list", []).append(src_ip)
            #     waf_system_data.update({str(dst_ip): {
            #         "attack_ip_num": len(list(set(WAF_SYS_IPS[dst_ip]['ip_list']))),
            #         "attack_count_num": tt,
            #     }})
            #     waf_system_data.update({"attack_type": "waf"})
            #     RDS.set("waf_systam_attack", waf_system_data)

            pass




if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/attack_results.log',
        format="%(message)s",
        filemode='a', )
    # client = Client("http://40.125.161.143:50070")
    Counter = namedtuple("Counter", "src_ip count")
    # hdfs_dir_list = client.list("/hadoop")
    LogResultsBolt().run()


