import csv
import sys
import pyspark
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 pyspark_1.py <output file name> <Number of CPUs>")
        exit(0)
    spark = SparkSession\
            .builder\
            .appName("Number of Airports by Country")\
            .getOrCreate()
    airport_value_keys = spark.read.option("header", True)\
                        .csv("Dataset/airports.csv").repartition(int(sys.argv[2])).rdd\
                        .map(lambda r: (r[5], 1))
    
    res = airport_value_keys.reduceByKey(lambda a,b: a+b)
    tuples = res.collect()
    with open(sys.argv[1], 'w', newline = '') as csvfile:
        csvwriter = csv.writer(csvfile)
        for x in tuples:
            csvwriter.writerow([x[0], x[1]])