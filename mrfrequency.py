from mrjob.job import MRJob

class Frequency(MRJob):
    def mapper(self, _, line):
        words = line.split(",")
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, values):
        yield word, sum(values)


if __name__ == '__main__':
    Frequency.run()