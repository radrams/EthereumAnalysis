"""
PART C. DATA EXPLORATION (30%)
MISCELLANEOUS ANALYSIS
2. Gas Guzzlers
"""
from mrjob.job import MRJob
import time

#This line declares the class Top10JobA, that extends the MRJob format.
class Gas(MRJob):

# Mapper function to split and get the 'to_address' and 'value' fields from transactions dataset
    def mapper(self, _, line):
        try:
            fields = line.split(",")
            time_epoch = int(fields[6])
            if time_epoch != 0:
                month = time.strftime("%m", time.gmtime(time_epoch))  # returns month of the year
                year = time.strftime("%Y", time.gmtime(time_epoch))
                month_year = month + '-' + year
                yield (month_year,(int(fields[5]), 1))
        except:
            pass

# Combiner to perform aggregation
# input multiple k, v pairs of (to_address, value)
# output (unique_to_address, sum(value))
    def combiner(self, word, counts):
        count = 0
        total = 0
        for value in counts:
            count +=value[1]
            total += value[0]
        yield(word, (total, count))

# output (unique_to_address, sum(value))
    def reducer(self, word, counts):
        count = 0
        total = 0
        for value in counts:
            count += value[1]
            total += value[0]
        avg = (total/count)
        yield (word, avg)

#this part of the python script tells to actually run the defined MapReduce job.
if __name__ == '__main__':
    Gas.run()