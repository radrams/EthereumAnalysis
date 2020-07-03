"""
PART C. DATA EXPLORATION (30%)
MISCELLANEOUS ANALYSIS / Comparative Evaluation
INITIAL AGGREGATION, JOINING TRANSACTIONS/CONTRACTS AND FILTERING & SORTING TOP TEN
"""

import pyspark

def clean_contracts(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        return False

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        return False

sc = pyspark.SparkContext()

blocks = sc.textFile("/data/ethereum/transactions")
good_blocks = blocks.filter( is_good_line )
to_address_value = good_blocks.map(lambda b: (b.split(',')[2], int(b.split(',')[3])))
results = to_address_value.reduceByKey(lambda a,b: a+b)
#Save the intermediate output to memory
# (k,v ) pairs of (unique_address, value)
inmem = results.persist()

#Preprocess the (k,v ) pairs of (unique_address, value) to remove encoding
to_address_value_1 = inmem.map(lambda b: (b[0].encode('utf8'), b[1]))

contracts = sc.textFile("/data/ethereum/contracts")
contracts_f = contracts.filter(clean_contracts).map(lambda f: f.split(","))
#in this dataset address is the join key, block number is the value
contracts_join = contracts_f.map(lambda f: (f[0].encode('utf8'), f[3].encode('utf8')))

#from the docs: returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
#that is: (address, (value, block number))
joined_data = to_address_value_1.join(contracts_join)

#we reshape it to prepare the grouping to (address, amount) and filter out the addresses where block_number and amount is None
contracts_transactions = joined_data.map(lambda a: ((a[0], a[1][1]),  a[1][0]) if (a[1][0] != 0 and a[1][1] != 0) else None)
filtered_results = contracts_transactions.filter(lambda x: x is not None)
sorted_results = filtered_results.sortBy(lambda a: a[1])
top10_results = sorted_results.top(10, key=lambda x: x[1])
(sc.parallelize(top10_results)).saveAsTextFile("out_spark_PartB")