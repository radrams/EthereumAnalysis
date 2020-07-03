"""
PART B. TOP TEN MOST POPULAR SERVICES
JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
"""

from mrjob.job import MRJob

class repartition_contract_join(MRJob):

    def mapper(self, _, line):
       try:
            #one mapper, we need to first differentiate among both types
            if(len(line.split('\t')) == 2):
                fields = line.split("\t")
                join_key = fields[0][1:-1]
                join_value = int(fields[1])
                yield (join_key, (join_value,1))

            elif(len(line.split(',')) == 5):
                fields = line.split(",")
                join_key = fields[0]
                join_value = fields[3]
                yield (join_key, (join_value,2))
       except:
            pass

    def reducer(self, address, values):
        block_number = None
        amount = None
        for value in values:
            if value[1] == 1:
                amount = value[0]
            elif value[1] == 2:
                block_number = value[0]

        if block_number is not None and amount is not None:
            yield ((address, block_number), amount)

if __name__ == '__main__':
    repartition_contract_join.run()