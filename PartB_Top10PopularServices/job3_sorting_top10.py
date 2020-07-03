"""JOB 3 - TOP TEN
"""
from mrjob.job import MRJob

class sortTop10(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:
            columns = line.split()
            address = columns[0][2:-2]
            block_number = columns[1][1:-2]
            amount = int(columns[2])
            yield(None, (address, block_number, amount))
        except:
            pass

    def combiner(self, _, values1):
        sortedvalue = sorted(values1, reverse=True, key=lambda l:l[2])
        top10 = sortedvalue[:10]
        for var in top10:
            yield(None, var)

#and the reducer method goes after this line
    def reducer(self, _, values1):
        sortedvalue = sorted(values1, reverse=True, key=lambda l:l[2])
        top10 = sortedvalue[:10]
        rank =1
        for var in top10:
            varformatted = '{} - {} - {}'.format(var[0], var[1], var[2])
            yield(rank, varformatted)
            rank += 1

#this part of the python script tells to actually run the defined MapReduce job.
if __name__ == '__main__':
    sortTop10.run()
