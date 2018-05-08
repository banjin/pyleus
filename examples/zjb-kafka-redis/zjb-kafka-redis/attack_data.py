# coding:utf-8

from pyleus.storm import SimpleBolt
import logging
from collections import namedtuple
from utils import IPS as ips, RDS,ATTACK_TYPE as att_type,attack_ips
from utils import SYS_IPS as sys_ips
from utils import get_system_ips

log = logging.getLogger('log_results')

log.info("ips", ips)
log.info("sys_ips", sys_ips)

system_ip_list = get_system_ips()

attack_data = {}
attack_ip_info = {}
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

            dst_total_info = RDS.get("attack_system")
            if not dst_total_info:
                dst_total_info = {}
            dst_ip_info = dst_total_info.get(str(dst_ip))
            if not dst_ip_info:
                dst_ip_info = {}

            attack_count_num = dst_ip_info.get("attack_count_num")
            if not attack_count_num:
                attack_count_num = 0
            attack_count_num += 1
            attack_ip_num_set = dst_ip_info.get("attack_ip_num")

            if not attack_ip_num_set:
                attack_ip_num_set = []
            attack_ip_set = list(set(attack_ip_num_set).add(src_ip))

            # attack_ips[dst_ip].add(src_ip)
            # log.info(ips[dst_ip])
            # 每个被攻击ip的攻击次数
            dst_total_info.update({str(dst_ip): {"attack_count_num": attack_count_num, "attack_ip_num": attack_ip_set}})
            # 所有攻击ip
            # attack_ip_info.setdefault(dst_ip, []).append(src_ip)

            attack_type_dict = RDS.get("attack_type")
            if not attack_type_dict:
                attack_type_dict = {}
            attack_type_num = attack_type_dict.get(attack_type)
            if not attack_type_num:
                attack_type_num = 0

            attack_type_num += 1
            attack_type_dict.update({attack_type: attack_type_num})
            # 攻击类型
            RDS.set("attack_type", attack_type_dict)
            RDS.set("attack_system", dst_total_info)


            # 可以不统计总的，接口中计算
            # total_info = RDS.get("attack_data",{})
            # total_attack_ip_num = total_info.get("attack_ip_num", 0)
            # total_attack_count_num = total_info.get("attack_count_num", 0)

            # RDS.set("attack_type",)
            # # 攻击类型
            # att_type[attack_type] += 1
            # type_info.update({attack_type: att_type[attack_type]})
            # log.info("ip,count:{0},{1}".format(src_ip, ips[src_ip]))
            # # 地图上方攻击次数和攻击IP个数
            # RDS.set("attack_data", {"attack_ip_num": len(ips), "attack_count_num": sum(ips.values())})
            #
            #
            # # 重要系统
            # RDS.set("attack_system", attack_data)

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

