from mrjob.job import MRJob

class MrCustomer(MRJob):
    def mapper(self, _, line):
        customerID, productID, price = line.split(",")           
        yield customerID, float(price) 

    def reducer(self, customerID, values):
        prices = list(values)
        yield customerID, sum(prices)/len(prices)



if __name__ == '__main__':
    MrCustomer.run()