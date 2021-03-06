# coding:utf-8

from pyleus.storm import SimpleBolt
import logging
import json
import ast
from collections import namedtuple, deque
from utils import IPS as ips, RDS,ATTACK_TYPE as att_type,attack_ips
from utils import SYS_IPS as sys_ips
from utils import get_system_ips, get_write_list

log = logging.getLogger('log_results')

log.info("ips", ips)
log.info("sys_ips", sys_ips)

system_ip_list = get_system_ips()

attack_data = {}
attack_ip_info = {}
r_attack_ip = {}
type_info = {}

only_attack_ip,only_attacked_port,only_attacked_ip,lost_port,lost_attacked_ip,lost_attack_ip,total_info = get_write_list()


class LogResultsBolt(SimpleBolt):

    def initialize(self):
        OUTPUT_FIELDS = ["src_ip"]

    def process_tuple(self, tup):
        # 五元组
        log.info("v:{0}".format(tup.values))
        if tup.values:
            # src_ip
            src_ip, dst_ip, post_time, attack_type, dst_port = tuple(tup.values)
            # src_ip = "{0}".format(tup.values[0])
            # dst_ip = "{0}".format(tup.values(1))

            if src_ip in only_attack_ip:
                pass
            elif dst_ip in only_attacked_ip:
                pass
            elif dst_port in only_attacked_port:
                pass
            elif (src_ip, dst_ip) in lost_port:
                pass
            elif (src_ip, dst_port) in lost_attacked_ip:
                pass
            elif (dst_ip, dst_port) in lost_attack_ip:
                pass
            elif (src_ip, dst_ip,dst_port) in total_info:
                pass
            else:
                # ============ 地图上方数值====== 攻击源 =========================
                # 被攻击IP都是证件部
                # {"src_ip":'', "11.11.11.11":""}
                # 统计每个攻击IP攻击的次数
                src_total_info = RDS.get("attack_total_info")
                if not src_total_info:
                    src_total_info = {}
                else:
                    src_total_info = ast.literal_eval(src_total_info)
                src_ip_info = src_total_info.get(src_ip)
                if not src_ip_info:
                    src_total_info.update({src_ip: 1})
                else:
                    src_total_info.update({src_ip: int(src_ip_info)+1})
                RDS.set('attack_total_info', src_total_info)
                # total_num = src_total_info.get("total_num")
                # if not total_num:
                #     src_total_info.update({"total_num": 0})
                # else:
                #     src_total_info.update({"total_num": int(total_num)+1})

                # ==============重要系统 ==================
                # ***** 统计总数 ststem 攻击源 top5*********************
                # {"system_attack_num":{"src_ip":5},"system_attacked_num":"{"dst_ip":{"attack_ip_list":[],"attack_ip_num":0}}}

                # 攻击重要系统IP
                if dst_ip in system_ip_list:

                    system_info = RDS.get("system_info")
                    if not system_info:
                        system_info = {}
                    else:
                        system_info = ast.literal_eval(system_info)

                    system_attack_num = system_info.get("system_attack_info")
                    if not system_attack_num:
                        system_attack_num = {}

                    system_attack_ip_num = system_attack_num.get(src_ip)
                    log.info("system_attack_ip_num", system_attack_ip_num)
                    if not system_attack_ip_num:
                        log.info("xxxxxxxxx")
                        system_attack_num.update({src_ip: 1})
                    else:
                        system_attack_num.update({src_ip: int(system_attack_ip_num)+1})

                # ************* system top10 *******************
                    # {"dst_ip":{"src_ip:10}}
                    system_attacked_num = system_info.get('system_attacked_info')
                    if not system_attacked_num:
                        system_attacked_num = {}

                    dst_system_ip_info = system_attacked_num.get(dst_ip)
                    if not dst_system_ip_info:
                        dst_system_ip_info = {}
                    src_system_ip_num = dst_system_ip_info.get(src_ip, 0)
                    log.info("src_system_ip_num", src_system_ip_num)

                    dst_system_ip_info.update({src_ip: src_system_ip_num+1})
                    log.info("dst_system_ip_info", dst_system_ip_info)
                    system_attacked_num.update({dst_ip: dst_system_ip_info})

                    RDS.set("system_info", {"system_attack_info": system_attack_num, "system_attacked_info": system_attacked_num})

                """
                # 统计被攻击IP的次数

                dst_total_info = RDS.get("attack_system")
                if not dst_total_info:
                    dst_total_info = {}
                else:
                    dst_total_info = ast.literal_eval(dst_total_info)
                dst_ip_info = dst_total_info.get(str(dst_ip))
                if not dst_ip_info:
                    dst_ip_info = {}

                attack_count_num = dst_ip_info.get("attack_count_num")
                log.info("attack_count_num, {}".format(attack_count_num))
                if not attack_count_num:
                    attack_count_num = 0
                attack_count_num += 1
                attack_ip_num_set = dst_ip_info.get("attack_ip_num")
                log.info("attack_ip_num_set, {}".format(attack_ip_num_set))

                if not attack_ip_num_set:
                    attack_ip_num_set = []
                attack_ip_num_set.append(src_ip)

                # if attack_ip_num_set:
                #     set(attack_ip_num_set).add(src_ip)
                #     attack_ip_set = list(attack_ip_num_set)
                # else:
                #     b = set()
                #     b.add(src_ip)
                #     attack_ip_set = list(b)


                # attack_ips[dst_ip].add(src_ip)
                # log.info(ips[dst_ip])
                # 每个被攻击ip的攻击次数
                dst_total_info.update({str(dst_ip): {"attack_count_num": attack_count_num, "attack_ip_num": attack_ip_num_set}})
                # 所有攻击ip
                # attack_ip_info.setdefault(dst_ip, []).append(src_ip)

                attack_type_dict = RDS.get("attack_type")
                if not attack_type_dict:
                    attack_type_dict = {}
                else:
                    attack_type_dict = ast.literal_eval(attack_type_dict)
                attack_type_num = attack_type_dict.get(attack_type)
                if not attack_type_num:
                    attack_type_num = 0

                attack_type_num += 1
                attack_type_dict.update({attack_type: attack_type_num})
                # 攻击类型
                RDS.set("attack_type", attack_type_dict)
                RDS.set("attack_system", dst_total_info)
                """
                # ====================攻击检测 ===============================
                attack_type_dict = RDS.get("attack_type")
                if not attack_type_dict:
                    attack_type_dict = {}
                else:
                    attack_type_dict = ast.literal_eval(attack_type_dict)
                attack_type_num = attack_type_dict.get(attack_type)
                if not attack_type_num:
                    attack_type_num = 0

                attack_type_num += 1
                attack_type_dict.update({attack_type: attack_type_num})
                # 攻击类型
                RDS.set("attack_type", attack_type_dict)

                # =====================实时数据=======================================
                realtime_data = RDS.get("realtime_data")
                if not realtime_data:
                    realtime_data = deque()
                else:
                    realtime_data = deque(ast.literal_eval(realtime_data))
                if len(realtime_data) < 20:
                    realtime_data.append({"src_ip":src_ip,"dst_ip": dst_ip, "attack_type": attack_type, "time": post_time})
                else:
                    realtime_data.popleft()
                    realtime_data.append({"src_ip":src_ip,"dst_ip": dst_ip, "attack_type": attack_type, "time": post_time})
                realtime_data = list(realtime_data)

                RDS.set("realtime_data", realtime_data)
                RDS.set("update_value", 1)
                self.emit((src_ip, ))



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


