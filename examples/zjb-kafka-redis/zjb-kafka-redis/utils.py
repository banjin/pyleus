# coding:utf-8

"""
一些配置
"""
from collections import defaultdict
import redis
import MySQLdb

IPS = defaultdict(int)
SYS_IPS = defaultdict(dict)
SYS_SRC_IPS = dict()
SYSTEM_TOP10 = dict()
RDS = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)

# waf 总的IP次数和个数
WAF_IPS = defaultdict(int)
# waf 攻击重要系统的ip个数和次数
WAF_SYS_IPS = defaultdict(dict)


def get_system_ips():

    db = MySQLdb.connect("127.0.0.1", "root", "qssec.com", "gab_event")
    cursor = db.cursor()
    cursor.execute("SELECT ips.ip, system_name.`name` FROM ips INNER JOIN ips_system_name ON "
                   "ips_system_name.ips_id = ips.id INNER JOIN system_name ON "
                   "ips_system_name.systemname_id = system_name.id;")

    results = cursor.fetchall()

    return [info[0] for info in results]
