import csv
import sys
import pyspark
from pyspark.sql import SparkSession

def maximum(a, b):
    if a[1] > b[1]:
        return a
    else:
        return b

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 pyspark_1.py <output file name> <Number of CPUs>")
        exit(0)

    spark = SparkSession\
            .builder\
            .appName("Number of Airports by Country")\
            .getOrCreate()
    
    airport_value_keys = spark.read.option("header", True)\
                        .csv("Dataset/airports.csv").rdd\
                        .map(lambda r: (r[5], 1))
    airport_counts = airport_value_keys.reduceByKey(lambda a,b: a+b)
    res = airport_counts.reduce(maximum)
    with open(sys.argv[1], 'w', newline = '') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow([res[0]])