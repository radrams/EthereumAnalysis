"""
PART C. DATA EXPLORATION (30%)
SCAM ANALYSIS 1.	Popular Scams:
"""
from mrjob.job import MRJob
import json

#This line declares the class Lab1, that extends the MRJob format.
class popular_scam(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:
            # with open('C:/Users/radha/OneDrive/BigDataProcessingLab_01/ec19451/NewCoursework/Scams/scams1.json') as f:
            # for line in f:
            json_data = json.loads(line)
            results = json_data['result']
            keys_lst = list(results.keys())
            for c in keys_lst:
                scam_category = results[c]['category']
                scam_addresses = results[c]['addresses']
                scam_status = results[c]['status']
                for address in scam_addresses:
                    yield ((address, scam_category, scam_status), 1)
        except:
            pass
                #no need to do anything, just ignore the line, as it was malformed

    def reducer(self, word, counts):
        #you have to implement the body of this method. Python's sum() function will probably be useful
        yield (word, sum(counts))


#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    popular_scam.run()
