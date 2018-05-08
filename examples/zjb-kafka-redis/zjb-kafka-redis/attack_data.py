# coding:utf-8

from pyleus.storm import SimpleBolt
import logging
from collections import namedtuple
from utils import IPS as ips, RDS,ATTACK_TYPE as att_type
from utils import SYS_IPS as sys_ips
from utils import get_system_ips

log = logging.getLogger('log_results')

log.info("ips", ips)
log.info("sys_ips", sys_ips)

system_ip_list = get_system_ips()

attack_data = {}
attack_ip_info={}
r_attack_ip = {}
type_info = {}

class LogResultsBolt(SimpleBolt):

    def initialize(self):
        pass

    def process_tuple(self, tup):
        # 五元组
        log.info("v:{0}".format(tup.values))
        if tup.values:
            # src_ip
            src_ip, dst_ip, post_time, attack_type, dst_port = tuple(tup.values)
            # src_ip = "{0}".format(tup.values[0])
            # dst_ip = "{0}".format(tup.values(1))

            # 先不用计算白名单
            # 统计被攻击IP的次数
            ips[dst_ip] += 1
            log.info(ips[dst_ip])
            # 每个被攻击ip的攻击次数
            attack_data.update({str(dst_ip): {"attack_count_num": ips[dst_ip], "attack_ip_num":0}})
            # 所有攻击ip
            # attack_ip_info.setdefault(dst_ip, []).append(src_ip)

            # 攻击类型
            att_type[attack_type] += 1
            type_info.update({attack_type: att_type[attack_type]})
            log.info("ip,count:{0},{1}".format(src_ip, ips[src_ip]))
            # 地图上方攻击次数和攻击IP个数
            RDS.set("attack_data", {"attack_ip_num": len(ips), "attack_count_num": sum(ips.values())})

            # 攻击类型
            RDS.set("attack_type", type_info)
            # 重要系统
            RDS.set("attack_system", attack_data)



            # 统计攻击重要系统的信息
            # if dst_ip in system_ip_list:
            #     sys_ips[dst_ip].setdefault("ip_list", []).append(src_ip)
            #     sys_ips[dst_ip].setdefault()




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


