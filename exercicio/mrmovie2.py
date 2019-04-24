from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MrMovies(MRJob):
    def mapper_movies(self, _, line):
        user_id, movie_id, rating, timestamp = WORD_RE.findall(line)
        yield movie_id, 1 
    
    def reducer_one(self, movie_id, values):
        yield None, (sum(values), movie_id)

    def reducer_two(self, _, values):
        top10 = sorted(list(values), reverse=True)[:10]
        for m in top10:
            yield m[1], m[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies,
                   reducer=self.reducer_one),
            MRStep(reducer=self.reducer_two)
        ]

if __name__ == '__main__':
    MrMovies.run()