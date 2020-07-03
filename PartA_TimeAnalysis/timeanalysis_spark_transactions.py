import pyspark
import time

sc = pyspark.SparkContext()

#checking if the line is good
#if the line has 7 fields it is a valid line, otherwise line is broken so skip
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[6])
        return True
    except:
        return False


blocks = sc.textFile("/data/ethereum/transactions")
good_blocks = blocks.filter( is_good_line )
time_epoch = good_blocks.map(lambda b: int(b.split(',')[6]))
month_year = time_epoch.map (lambda t: ((time.strftime("%m", time.gmtime(t)), time.strftime("%Y", time.gmtime(t))), 1))
results = month_year.reduceByKey(lambda a,b: a+b)
inmem = results.persist()
inmem.saveAsTextFile("out-ethereum")