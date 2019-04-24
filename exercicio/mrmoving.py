from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRMoving(MRJob):
    w = 3

    def mapper(self, _, line):
        name, timestamp, value = line.split(',')

        yield name, (timestamp, value)

    def reducer(self, key, values):
        items = list(values)
        items.sort()

        sum = 0.0
        for i in range(len(items)):
            if i < self.w:
                moving_avg = sum/(i + 1)
            else:
                moving_avg = sum / self.w
            
            timestamp = items[i][0]

            yield key, (timestamp, moving_avg)

if __name__ == '__main__':
    MRMoving.run()
