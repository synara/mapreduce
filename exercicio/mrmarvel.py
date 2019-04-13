from mrjob.job import MRJob
from mrjob.step import MRStep

class MrMarvel(MRJob):

    def mapper_one(self, _, line):
        heroes = line.split(' ')

        hero_id = heroes[0]
        friends = len(heroes) - 2
        yield hero_id, friends
    
    #se colocar len(f) ele conta com 1 a mais
    #por isso ficou sum(f)
    def reducer_one(self, hero_id, friends):   
        f = list(friends)     
        yield None, (sum(f), hero_id)

    def reducer_two(self, _, values):
        valuesfriends = sorted(list(values), reverse = True)
        for f in valuesfriends:
            yield f[1],  f[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_one,
                   reducer=self.reducer_one),
            MRStep(
                reducer=self.reducer_two)
        ]


if __name__ == '__main__':
    MrMarvel.run()