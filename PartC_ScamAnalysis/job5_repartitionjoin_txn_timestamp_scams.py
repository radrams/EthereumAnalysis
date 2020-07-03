"""
PART C. DATA EXPLORATION (30%)
SCAM ANALYSIS 1.	Popular Scams:
"""

from mrjob.job import MRJob

class repartition_contract_join(MRJob):

    def mapper(self, _, line):
       try:
            #one mapper, we need to first differentiate among both types
            fields = line.split("\t")
            f = fields[0].split(",")
            if(len(f) == 3):
                fields = line.split("\t")
                f = fields[0].split(",")
                address = f[0][2:-1]
                scam_category = f[1][2:-1]
                scam_status = f[2][2:-2]
                yield (address, (scam_category,1,scam_status))

            elif(len(f) == 2):
                join_address = f[0][2:-1]
                month_year = f[1][2:-2]
                value = fields[1]
                yield (join_address, (value, 2, month_year))
       except:
            pass

    def reducer(self, address, values):
        category = None
        amount = None
        for value in values:
            if value[1] == 2:
                amount = value[0]
                month_year = value[2]
            elif value[1] == 1:
                category = value[0]
                status = value[2]
        if category is not None and amount is not None:
            yield ((address, category, status, month_year), amount)

if __name__ == '__main__':
    repartition_contract_join.run()