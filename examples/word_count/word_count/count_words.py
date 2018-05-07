from collections import defaultdict
from collections import namedtuple
import logging
import redis
from pyleus.storm import SimpleBolt
r = redis.Redis(host='127.0.0.1', port=6379, password='qssec.com', db=0)
log = logging.getLogger('counter')

Counter = namedtuple("Counter", "word count")


class CountWordsBolt(SimpleBolt):

    OUTPUT_FIELDS = Counter

    def initialize(self):
        self.words = defaultdict(int)

    def process_tuple(self, tup):
        word, = tup.values
        self.words[word] += 1
        r.set("{0}".format(word), "{0}".format(self.words[word]))
        log.debug("{0} {1}".format(word, self.words[word]))
        self.emit((word, self.words[word]), anchors=[tup])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/word_count_count_words.log',
        format="%(message)s",
        filemode='a',
    )

    CountWordsBolt().run()
