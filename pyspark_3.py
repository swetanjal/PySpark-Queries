import csv
import sys
import pyspark
from pyspark.sql import SparkSession

def selection(a):
    if int(a[3]) >= 10 and int(a[3]) <= 90 and int(a[4]) >= -90 and int(a[4]) <= -10:
        return True
    return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 pyspark_1.py <output file name> <Number of CPUs>")
        exit(0)

    spark = SparkSession\
            .builder\
            .appName("Number of Airports by Country")\
            .getOrCreate()
    
    res = spark.read.option("header", True)\
                        .csv("Dataset/airports.csv").rdd\
                        .map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5]))\
                        .filter(selection)
    tuples = res.collect()
    with open(sys.argv[1], 'w', newline = '') as csvfile:
        csvwriter = csv.writer(csvfile)
        for x in tuples:
            csvwriter.writerow([x[2]])