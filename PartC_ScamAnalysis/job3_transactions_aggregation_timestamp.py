"""
PART C. DATA EXPLORATION (30%)
SCAM ANALYSIS 1.	Popular Scams:
"""
from mrjob.job import MRJob
import time

#This line declares the class Top10JobA, that extends the MRJob format.
class Top10JobA(MRJob):

# Mapper function to split and get the 'to_address' and 'value' fields from transactions dataset
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            if (len(fields)==7):
                if int(fields[3]) == 0:
                    pass
                else:
                    time_epoch = int(fields[6])
                    month = time.strftime("%m", time.gmtime(time_epoch))  # returns month of the year
                    year = time.strftime("%Y", time.gmtime(time_epoch))
                    month_year = month + '-' + year
                    yield ((fields[2], month_year), int(fields[3]))
        except:
            pass

# Combiner to perform aggregation
# input multiple k, v pairs of (to_address, value)
# output (unique_to_address, sum(value))
    def combiner(self, word, counts):
        yield(word, sum(counts))

# output (unique_to_address, sum(value))
    def reducer(self, word, counts):
        yield(word, sum(counts))

#this part of the python script tells to actually run the defined MapReduce job.
if __name__ == '__main__':
    Top10JobA.run()