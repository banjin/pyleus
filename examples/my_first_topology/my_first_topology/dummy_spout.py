
from pyleus.storm import Spout
class DummySpout(Spout):

    OUTPUT_FIELDS = ['sentence', 'name']
    def next_tuple(self):
        self.emit(("This is a sentence.", "spout",))

if __name__=="__main__":
    DummySpout().run()





