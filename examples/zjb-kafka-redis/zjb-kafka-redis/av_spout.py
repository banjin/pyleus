# coding:utf-8

from pyleus.storm import SimpleBolt
import json
import logging
log = logging.getLogger('av_spout')
"""
病毒类数据
"""
class LogSquidBolt(SimpleBolt):
    OUTPUT_FIELDS = ["src_ip", "dst_ip", "time", "attack_type", "port"]

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
                attack_type = u"av"
                port = line['dst_port']
           	self.emit((src_ip, dst_ip, post_time, attack_type, port))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename='/tmp/av_spout.log',
        format="%(message)s",
        filemode='a',)
    LogSquidBolt().run()


