# coding:utf-8

"""
一些配置
"""
from collections import defaultdict
import redis
import MySQLdb
import time
IPS = defaultdict(int)
attack_ips = defaultdict(set)
ATTACK_TYPE = defaultdict(int)
SYS_IPS = defaultdict(dict)
SYS_SRC_IPS = dict()
SYSTEM_TOP10 = dict()
RDS = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)

# waf 总的IP次数和个数
WAF_IPS = defaultdict(int)
# waf 攻击重要系统的ip个数和次数
WAF_SYS_IPS = defaultdict(dict)

db = MySQLdb.connect("127.0.0.1", "root", "qssec.com", "zjb_event")


def get_system_ips():
    """"
    获取重要系统的所有IP
    """
    while 1:
        cursor = db.cursor()
        cursor.execute("SELECT ips.ip, system_name.`name` FROM ips INNER JOIN ips_system_name ON "
                       "ips_system_name.ips_id = ips.id INNER JOIN system_name ON "
                       "ips_system_name.systemname_id = system_name.id;")

        results = cursor.fetchall()
        cursor.close()
        time.sleep(10)

        return [info[0] for info in results]


def get_write_list():
    """
    获取白名单,只记录通过白名单过滤后的数据
    :return:
    """
    while 1:
        cursor = db.cursor()
        cursor.execute("select attack_ip,attacked_ip,port from white_list")

        results = cursor.fetchall()
        #  (('1.1.1.1', '2.2.2.2', 60L),)
        only_attack_ip = []
        only_attacked_port = []
        only_attacked_ip = []
        lost_port = []
        lost_attacked_ip = []
        lost_attack_ip = []
        total_info = []

        for info in results:
            # 只有attack_ip
            if info[0] and info[1] and info[2]:
                total_info.append(info)
            elif info[0] and info[1] and not info[2]:
                lost_port.append((info[0],info[1]))
            elif info[0] and not info[1] and info[2]:
                lost_attacked_ip.append((info[0],info[2]))
            elif not info[0] and info[1] and info[2]:
                lost_attack_ip.append((info[1],info[2]))
            elif info[0] and not info[1] and not info[2]:
                only_attack_ip.append(info[0])
            elif not info[0] and info[1] and not info[2]:
                only_attacked_port.append(info[1])
            elif not info[0] and not info[1] and info[2]:
                only_attacked_ip.append(info[2])
        cursor.close()
        time.sleep(60)
        return only_attack_ip,only_attacked_port,only_attacked_ip,lost_port,lost_attacked_ip,lost_attack_ip,total_info



def get_region_list():
    """
    获取区域信息
    :return:
    """

    pass


def get_info_from_redis():

    """从redis中获取数据
    """
    pass
