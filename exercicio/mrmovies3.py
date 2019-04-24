from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MrMovies(MRJob):
    top10 = [] 

    def mapper_movies(self, _, line):
        user_id, movie_id, rating, timestamp = WORD_RE.findall(line)
        yield movie_id, 1
    
    def mapper_get_top10(self, key, value):
        self.top10.append((value, key))        
        if len(self.top10) > 10:
            self.top10.sort()
            self.top10.pop(0)

    def reducer_init(self):
        for item in self.top10:
            yield None, item

    def reducer_count(self, movie_id, values):
        yield movie_id, sum(values)

    def reducer_order(self, _, values):
        top10_sorted = sorted(list(values), reverse=True)[:10]
        print(top10_sorted)
        for t in top10_sorted:
            yield t[1], t[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies,
                   reducer=self.reducer_count),

            MRStep(mapper=self.mapper_get_top10,
                   reducer=self.reducer_init),

            MRStep(reducer=self.reducer_order)
        ]

if __name__ == '__main__':
    MrMovies.run()