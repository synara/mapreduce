from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MrMovies(MRJob):
    def mapper_movies(self, _, line):
        user_id, movie_id, rating, timestamp = WORD_RE.findall(line)
        yield movie_id, float(rating)
    
    def reducer_one(self, movie_id, values):
        ratings = list(values)
        yield  sum(ratings)/len(ratings), movie_id

    def reducer_two(self, avg, values):
        movies = list(values)
        for m in movies:
            yield int(m), avg

    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies,
                   reducer=self.reducer_one),
            MRStep(reducer=self.reducer_two)
        ]

if __name__ == '__main__':
    MrMovies.run()