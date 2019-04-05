from mrjob.job import MRJob

class MRFriends(MRJob):
    def mapper(self, _, line):
        userID, name, age, numberOfFriends = line.split(",")           
        yield age, int(numberOfFriends) 

    def reducer(self, age, values):
        friendsCount = list(values)
        yield age, sum(friendsCount)/len(friendsCount)



if __name__ == '__main__':
    MRFriends.run()